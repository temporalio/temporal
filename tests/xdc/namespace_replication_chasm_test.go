package xdc

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	frontendclient "go.temporal.io/server/client/frontend"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	namespaceReplicationCHASMTestSuite struct {
		xdcBaseSuite

		observedApplyRequests chan observedNamespaceMutationApply
		failApplyRequests     atomic.Bool
	}

	observedNamespaceMutationApply struct {
		request  *adminservice.ApplyNamespaceMutationRequest
		response *adminservice.ApplyNamespaceMutationResponse
		err      error
	}

	bufferedNamespaceReplicationTask struct {
		task    *replicationspb.NamespaceTaskAttributes
		execute func() error
	}
)

func TestNamespaceReplicationCHASMTestSuite(t *testing.T) {
	t.Parallel()

	s := &namespaceReplicationCHASMTestSuite{
		observedApplyRequests: make(chan observedNamespaceMutationApply, 1024),
	}
	s.enableTransitionHistory = true
	suite.Run(t, s)
}

func (s *namespaceReplicationCHASMTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableChasm.Key(): true,
		dynamicconfig.FrontendGlobalNamespaceNamespaceReplicationInducingAPIsRPS.Key(): 1000,
		dynamicconfig.NamespaceReplicationTransportMode.Key():                          dynamicconfig.NamespaceReplicationTransportModeShadow,
		dynamicconfig.OutboundQueueCircuitBreakerSettings.Key(): dynamicconfig.CircuitBreakerSettings{
			MaxRequests: 1,
			Timeout:     time.Second,
		},
	}
	s.setupSuite(testcore.WithAdditionalServerOptions(
		temporal.WithChainedFrontendGrpcInterceptors(s.captureNamespaceMutationApply),
	))
}

func (s *namespaceReplicationCHASMTestSuite) SetupTest() {
	s.setupTest()
	s.failApplyRequests.Store(false)
	for {
		select {
		case <-s.observedApplyRequests:
		default:
			return
		}
	}
}

func (s *namespaceReplicationCHASMTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *namespaceReplicationCHASMTestSuite) TestAuthoritativeTransportConcurrentCASAndOutage() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*testTimeout)
	defer cancel()

	active := s.clusters[0]
	standby := s.clusters[1]
	activeCleanup := active.OverrideDynamicConfig(
		s.T(),
		dynamicconfig.NamespaceReplicationTransportMode,
		dynamicconfig.NamespaceReplicationTransportModeCHASM,
	)
	defer activeCleanup()
	standbyCleanup := standby.OverrideDynamicConfig(
		s.T(),
		dynamicconfig.NamespaceReplicationTransportMode,
		dynamicconfig.NamespaceReplicationTransportModeCHASM,
	)
	defer standbyCleanup()

	namespaceName := "test-namespace-" + uuid.NewString()
	legacyTasks := make(chan bufferedNamespaceReplicationTask, 1)
	standby.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationTaskInterceptor, func(
			_ context.Context,
			task *replicationspb.NamespaceTaskAttributes,
			execute func() error,
		) error {
			legacyTasks <- bufferedNamespaceReplicationTask{
				task:    proto.Clone(task).(*replicationspb.NamespaceTaskAttributes),
				execute: execute,
			}
			return nil
		}),
		namespace.Name(namespaceName),
	)

	_, err := active.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: namespaceName,
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{ClusterName: active.ClusterName()},
			{ClusterName: standby.ClusterName()},
		},
		ActiveClusterName:                active.ClusterName(),
		IsGlobalNamespace:                true,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	s.Require().NoError(err)
	s.requireSuccessfulApply(
		s.receiveObservedApply(ctx),
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_CREATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED,
	)
	s.requireNamespaceDescription(ctx, standby, namespaceName, "", 0)

	const mutationCount = 24
	retryClient := frontendclient.NewRetryableClient(
		active.FrontendClient(),
		backoff.NewExponentialRetryPolicy(10*time.Millisecond).
			WithMaximumInterval(100*time.Millisecond).
			WithExpirationInterval(testTimeout),
		common.IsServiceClientTransientError,
	)
	s.failApplyRequests.Store(true)
	defer s.failApplyRequests.Store(false)
	start := make(chan struct{})
	updateErrors := make(chan error, mutationCount)
	var updates sync.WaitGroup
	updates.Add(mutationCount)
	for i := range mutationCount {
		go func(index int) {
			defer updates.Done()
			<-start
			_, updateErr := retryClient.UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
				Namespace: namespaceName,
				UpdateInfo: &namespacepb.UpdateNamespaceInfo{
					Data: map[string]string{
						fmt.Sprintf("mutation-%02d", index): fmt.Sprintf("value-%02d", index),
					},
				},
			})
			updateErrors <- updateErr
		}(i)
	}
	close(start)
	updates.Wait()
	close(updateErrors)
	for updateErr := range updateErrors {
		s.Require().NoError(updateErr)
	}

	sourceNamespace := s.requirePersistedNamespace(ctx, active, namespaceName)
	s.Require().Equal(int64(mutationCount), sourceNamespace.Namespace.GetConfigVersion())
	for i := range mutationCount {
		s.Require().Equal(
			fmt.Sprintf("value-%02d", i),
			sourceNamespace.Namespace.GetInfo().GetData()[fmt.Sprintf("mutation-%02d", i)],
		)
	}

	failedApply := s.receiveObservedApply(ctx)
	s.requireApplyRequest(failedApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE, false)
	s.Require().Nil(failedApply.response)
	s.Require().ErrorAs(failedApply.err, new(*serviceerror.Unavailable))
	s.failApplyRequests.Store(false)
	recovered := false
	for !recovered {
		observed := s.receiveObservedApply(ctx)
		if observed.err != nil {
			s.Require().ErrorAs(observed.err, new(*serviceerror.Unavailable))
			continue
		}
		s.requireApplyRequest(observed, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE, false)
		s.Require().NotNil(observed.response)
		switch observed.response.GetOutcome() {
		case adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED:
			recovered = true
		case adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE:
		default:
			s.FailNow("unexpected recovered apply outcome", observed.response.GetOutcome())
		}
	}

	sourceFingerprint, err := nsreplication.NamespaceTaskFingerprint(
		nsreplication.NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, sourceNamespace.Namespace),
	)
	s.Require().NoError(err)
	await.RequireTruef(s.T(), func() bool {
		standbyNamespace, getErr := standby.TestBase().MetadataManager.GetNamespace(
			ctx,
			&persistence.GetNamespaceRequest{Name: namespaceName},
		)
		if getErr != nil {
			return false
		}
		standbyFingerprint, fingerprintErr := nsreplication.NamespaceTaskFingerprint(
			nsreplication.NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, standbyNamespace.Namespace),
		)
		return fingerprintErr == nil && bytes.Equal(sourceFingerprint, standbyFingerprint)
	}, replicationWaitTime, replicationCheckInterval, "standby did not converge to the exact source state")

	s.Require().Never(func() bool {
		select {
		case <-legacyTasks:
			return true
		default:
			return false
		}
	}, time.Second, 20*time.Millisecond, "authoritative stress test published a legacy namespace replication task")
}

func (s *namespaceReplicationCHASMTestSuite) TestAuthoritativeTransportConvergesWithoutFIFO() {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	active := s.clusters[0]
	standby := s.clusters[1]
	activeCleanup := active.OverrideDynamicConfig(
		s.T(),
		dynamicconfig.NamespaceReplicationTransportMode,
		dynamicconfig.NamespaceReplicationTransportModeCHASM,
	)
	defer activeCleanup()
	standbyCleanup := standby.OverrideDynamicConfig(
		s.T(),
		dynamicconfig.NamespaceReplicationTransportMode,
		dynamicconfig.NamespaceReplicationTransportModeCHASM,
	)
	defer standbyCleanup()

	namespaceName := "test-namespace-" + uuid.NewString()
	legacyTasks := make(chan bufferedNamespaceReplicationTask, 1)
	standby.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationTaskInterceptor, func(
			_ context.Context,
			task *replicationspb.NamespaceTaskAttributes,
			execute func() error,
		) error {
			legacyTasks <- bufferedNamespaceReplicationTask{
				task:    proto.Clone(task).(*replicationspb.NamespaceTaskAttributes),
				execute: execute,
			}
			return nil
		}),
		namespace.Name(namespaceName),
	)

	_, err := active.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: namespaceName,
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{ClusterName: active.ClusterName()},
			{ClusterName: standby.ClusterName()},
		},
		ActiveClusterName:                active.ClusterName(),
		IsGlobalNamespace:                true,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	s.Require().NoError(err)

	createApply := s.receiveObservedApply(ctx)
	s.requireSuccessfulApply(
		createApply,
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_CREATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED,
	)
	s.requireNamespaceDescription(ctx, active, namespaceName, "", 0)
	s.requireNamespaceDescription(ctx, standby, namespaceName, "", 0)

	const firstDescription = "first authoritative mutation"
	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespaceName,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: firstDescription,
		},
	})
	s.Require().NoError(err)
	firstUpdateApply := s.receiveObservedApply(ctx)
	s.requireSuccessfulApply(
		firstUpdateApply,
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
	)
	s.requireNamespaceDescription(ctx, standby, namespaceName, firstDescription, 1)

	const secondDescription = "second authoritative mutation"
	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespaceName,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: secondDescription,
		},
	})
	s.Require().NoError(err)
	secondUpdateApply := s.receiveObservedApply(ctx)
	s.requireSuccessfulApply(
		secondUpdateApply,
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
	)
	s.requireNamespaceDescription(ctx, standby, namespaceName, secondDescription, 2)

	for _, replay := range []struct {
		name    string
		request *adminservice.ApplyNamespaceMutationRequest
		outcome adminservice.ApplyNamespaceMutationResponse_Outcome
	}{
		{"stale update", firstUpdateApply.request, adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE},
		{"duplicate update", secondUpdateApply.request, adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE},
		{"duplicate create", createApply.request, adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE},
	} {
		s.Run(replay.name, func() {
			response, applyErr := standby.AdminClient().ApplyNamespaceMutation(
				ctx,
				proto.Clone(replay.request).(*adminservice.ApplyNamespaceMutationRequest),
			)
			s.Require().NoError(applyErr)
			s.Require().Equal(replay.outcome, response.GetOutcome())
			observed := s.receiveObservedApply(ctx)
			s.Require().Equal(replay.outcome, observed.response.GetOutcome())
		})
	}
	s.requireNamespaceDescription(ctx, standby, namespaceName, secondDescription, 2)

	s.failApplyRequests.Store(true)
	const recoveredDescription = "authoritative mutation recovered after peer outage"
	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespaceName,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: recoveredDescription,
		},
	})
	s.Require().NoError(err)
	s.requireNamespaceDescription(ctx, active, namespaceName, recoveredDescription, 3)
	failedApply := s.receiveObservedApply(ctx)
	s.requireApplyRequest(failedApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE, false)
	s.Require().Nil(failedApply.response)
	s.Require().ErrorAs(failedApply.err, new(*serviceerror.Unavailable))

	s.failApplyRequests.Store(false)
	recoveredApply := s.receiveObservedApply(ctx)
	s.requireSuccessfulApply(
		recoveredApply,
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
	)
	s.requireNamespaceDescription(ctx, standby, namespaceName, recoveredDescription, 3)

	s.Require().Never(func() bool {
		select {
		case <-legacyTasks:
			return true
		default:
			return false
		}
	}, time.Second, 20*time.Millisecond, "authoritative mode published a legacy namespace replication task")
}

func (s *namespaceReplicationCHASMTestSuite) TestShadowTransportDoesNotWrite() {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	active := s.clusters[0]
	standby := s.clusters[1]
	namespaceName := "test-namespace-" + uuid.NewString()
	legacyTasks := make(chan bufferedNamespaceReplicationTask, 2)
	standby.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationTaskInterceptor, func(
			_ context.Context,
			task *replicationspb.NamespaceTaskAttributes,
			execute func() error,
		) error {
			legacyTasks <- bufferedNamespaceReplicationTask{task: proto.Clone(task).(*replicationspb.NamespaceTaskAttributes), execute: execute}
			return nil
		}),
		namespace.Name(namespaceName),
	)

	_, err := active.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: namespaceName,
		Clusters: []*replicationpb.ClusterReplicationConfig{
			{ClusterName: active.ClusterName()},
			{ClusterName: standby.ClusterName()},
		},
		ActiveClusterName:                active.ClusterName(),
		IsGlobalNamespace:                true,
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	s.Require().NoError(err)

	createApply := s.receiveObservedApply(ctx)
	s.requireSuccessfulShadowApply(createApply, namespaceName, enumsspb.NAMESPACE_OPERATION_CREATE)
	createLegacyTask := s.receiveLegacyTask(ctx, legacyTasks)
	s.Require().Equal(enumsspb.NAMESPACE_OPERATION_CREATE, createLegacyTask.task.GetNamespaceOperation())
	s.Require().True(proto.Equal(createLegacyTask.task, createApply.request.GetNamespaceTask()),
		"CHASM create payload differs from the legacy replication task")

	_, err = standby.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: namespaceName})
	s.Require().ErrorAs(err, new(*serviceerror.NamespaceNotFound))
	s.Require().NoError(createLegacyTask.execute())

	standbyNamespace := s.requirePersistedNamespace(ctx, standby, namespaceName)
	initialConfigVersion := standbyNamespace.Namespace.GetConfigVersion()
	initialDescription := standbyNamespace.Namespace.GetInfo().GetDescription()
	activeNamespace := s.requirePersistedNamespace(ctx, active, namespaceName)
	s.Require().Equal(initialConfigVersion, activeNamespace.Namespace.GetConfigVersion())

	const updatedDescription = "updated through legacy while CHASM remains shadow-only"
	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespaceName,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: updatedDescription,
		},
	})
	s.Require().NoError(err)

	updateApply := s.receiveObservedApply(ctx)
	s.requireSuccessfulShadowApply(updateApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE)
	updateLegacyTask := s.receiveLegacyTask(ctx, legacyTasks)
	s.Require().Equal(enumsspb.NAMESPACE_OPERATION_UPDATE, updateLegacyTask.task.GetNamespaceOperation())
	s.Require().True(proto.Equal(updateLegacyTask.task, updateApply.request.GetNamespaceTask()),
		"CHASM update payload differs from the legacy replication task")

	standbyBeforeLegacyUpdate := s.requirePersistedNamespace(ctx, standby, namespaceName)
	s.Require().Equal(initialConfigVersion, standbyBeforeLegacyUpdate.Namespace.GetConfigVersion())
	s.Require().Equal(initialDescription, standbyBeforeLegacyUpdate.Namespace.GetInfo().GetDescription())
	activeAfterUpdate := s.requirePersistedNamespace(ctx, active, namespaceName)
	s.Require().Equal(initialConfigVersion+1, activeAfterUpdate.Namespace.GetConfigVersion())
	s.Require().Equal(updatedDescription, activeAfterUpdate.Namespace.GetInfo().GetDescription())

	s.Require().NoError(updateLegacyTask.execute())
	var standbyAfterLegacyUpdate *persistence.GetNamespaceResponse
	await.RequireTruef(s.T(), func() bool {
		var getErr error
		standbyAfterLegacyUpdate, getErr = standby.TestBase().MetadataManager.GetNamespace(
			ctx,
			&persistence.GetNamespaceRequest{Name: namespaceName},
		)
		return getErr == nil &&
			standbyAfterLegacyUpdate.Namespace.GetConfigVersion() == initialConfigVersion+1 &&
			standbyAfterLegacyUpdate.Namespace.GetInfo().GetDescription() == updatedDescription
	}, replicationWaitTime, replicationCheckInterval, "legacy update did not reach standby")
	s.Require().Equal(initialConfigVersion+1, standbyAfterLegacyUpdate.Namespace.GetConfigVersion())
	s.Require().Equal(updatedDescription, standbyAfterLegacyUpdate.Namespace.GetInfo().GetDescription())

	s.failApplyRequests.Store(true)
	defer s.failApplyRequests.Store(false)
	const recoveredDescription = "legacy converged while CHASM recovered from a transient failure"
	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespaceName,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: recoveredDescription,
		},
	})
	s.Require().NoError(err)

	failedApply := s.receiveObservedApply(ctx)
	s.requireShadowApplyRequest(failedApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE)
	s.Require().Nil(failedApply.response)
	s.Require().ErrorAs(failedApply.err, new(*serviceerror.Unavailable))
	s.failApplyRequests.Store(false)
	recoveredApply := s.receiveObservedApply(ctx)
	s.requireSuccessfulShadowApply(recoveredApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE)
	failedUpdateLegacyTask := s.receiveLegacyTask(ctx, legacyTasks)
	s.Require().Equal(enumsspb.NAMESPACE_OPERATION_UPDATE, failedUpdateLegacyTask.task.GetNamespaceOperation())
	s.Require().True(proto.Equal(failedApply.request.GetNamespaceTask(), recoveredApply.request.GetNamespaceTask()),
		"CHASM retry changed the namespace mutation payload")
	s.Require().True(proto.Equal(failedUpdateLegacyTask.task, recoveredApply.request.GetNamespaceTask()),
		"retried CHASM update payload differs from the legacy replication task")

	standbyBeforeFailedUpdateLegacy := s.requirePersistedNamespace(ctx, standby, namespaceName)
	s.Require().Equal(initialConfigVersion+1, standbyBeforeFailedUpdateLegacy.Namespace.GetConfigVersion())
	s.Require().Equal(updatedDescription, standbyBeforeFailedUpdateLegacy.Namespace.GetInfo().GetDescription())
	activeAfterFailedUpdate := s.requirePersistedNamespace(ctx, active, namespaceName)
	s.Require().Equal(initialConfigVersion+2, activeAfterFailedUpdate.Namespace.GetConfigVersion())
	s.Require().Equal(recoveredDescription, activeAfterFailedUpdate.Namespace.GetInfo().GetDescription())

	s.Require().NoError(failedUpdateLegacyTask.execute())
	var standbyAfterFailedUpdateLegacy *persistence.GetNamespaceResponse
	await.RequireTruef(s.T(), func() bool {
		var getErr error
		standbyAfterFailedUpdateLegacy, getErr = standby.TestBase().MetadataManager.GetNamespace(
			ctx,
			&persistence.GetNamespaceRequest{Name: namespaceName},
		)
		return getErr == nil &&
			standbyAfterFailedUpdateLegacy.Namespace.GetConfigVersion() == initialConfigVersion+2 &&
			standbyAfterFailedUpdateLegacy.Namespace.GetInfo().GetDescription() == recoveredDescription
	}, replicationWaitTime, replicationCheckInterval, "legacy update did not converge after CHASM retry")
}

func (s *namespaceReplicationCHASMTestSuite) captureNamespaceMutationApply(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	if info.FullMethod != adminservice.AdminService_ApplyNamespaceMutation_FullMethodName {
		return handler(ctx, req)
	}

	applyRequest, _ := req.(*adminservice.ApplyNamespaceMutationRequest)
	if s.failApplyRequests.Load() {
		err := serviceerror.NewUnavailable("injected CHASM namespace replication failure")
		s.recordObservedApply(applyRequest, nil, err)
		return nil, err
	}

	response, err := handler(ctx, req)
	applyResponse, _ := response.(*adminservice.ApplyNamespaceMutationResponse)
	s.recordObservedApply(applyRequest, applyResponse, err)
	return response, err
}

func (s *namespaceReplicationCHASMTestSuite) recordObservedApply(
	applyRequest *adminservice.ApplyNamespaceMutationRequest,
	applyResponse *adminservice.ApplyNamespaceMutationResponse,
	err error,
) {
	var clonedRequest *adminservice.ApplyNamespaceMutationRequest
	if applyRequest != nil {
		clonedRequest = proto.Clone(applyRequest).(*adminservice.ApplyNamespaceMutationRequest)
	}
	var clonedResponse *adminservice.ApplyNamespaceMutationResponse
	if applyResponse != nil {
		clonedResponse = proto.Clone(applyResponse).(*adminservice.ApplyNamespaceMutationResponse)
	}
	s.observedApplyRequests <- observedNamespaceMutationApply{
		request:  clonedRequest,
		response: clonedResponse,
		err:      err,
	}
}

func (s *namespaceReplicationCHASMTestSuite) receiveObservedApply(ctx context.Context) observedNamespaceMutationApply {
	select {
	case observed := <-s.observedApplyRequests:
		return observed
	case <-ctx.Done():
		s.FailNow("timed out waiting for CHASM namespace mutation apply", ctx.Err())
		return observedNamespaceMutationApply{}
	}
}

func (s *namespaceReplicationCHASMTestSuite) receiveLegacyTask(
	ctx context.Context,
	tasks <-chan bufferedNamespaceReplicationTask,
) bufferedNamespaceReplicationTask {
	select {
	case task := <-tasks:
		return task
	case <-ctx.Done():
		s.FailNow("timed out waiting for legacy namespace replication task", ctx.Err())
		return bufferedNamespaceReplicationTask{}
	}
}

func (s *namespaceReplicationCHASMTestSuite) requireSuccessfulShadowApply(
	observed observedNamespaceMutationApply,
	namespaceName string,
	operation enumsspb.NamespaceOperation,
) {
	s.requireSuccessfulApply(
		observed,
		namespaceName,
		operation,
		true,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
	)
}

func (s *namespaceReplicationCHASMTestSuite) requireShadowApplyRequest(
	observed observedNamespaceMutationApply,
	namespaceName string,
	operation enumsspb.NamespaceOperation,
) {
	s.requireApplyRequest(observed, namespaceName, operation, true)
}

func (s *namespaceReplicationCHASMTestSuite) requireSuccessfulApply(
	observed observedNamespaceMutationApply,
	namespaceName string,
	operation enumsspb.NamespaceOperation,
	shadow bool,
	outcome adminservice.ApplyNamespaceMutationResponse_Outcome,
) {
	s.Require().NoError(observed.err)
	s.Require().NotNil(observed.response)
	s.Require().Equal(outcome, observed.response.GetOutcome())
	s.requireApplyRequest(observed, namespaceName, operation, shadow)
}

func (s *namespaceReplicationCHASMTestSuite) requireApplyRequest(
	observed observedNamespaceMutationApply,
	namespaceName string,
	operation enumsspb.NamespaceOperation,
	shadow bool,
) {
	s.Require().NotNil(observed.request)
	s.Require().Equal(shadow, observed.request.GetShadow())
	s.Require().Equal(namespaceName, observed.request.GetNamespaceTask().GetInfo().GetName())
	s.Require().Equal(operation, observed.request.GetNamespaceTask().GetNamespaceOperation())
	fingerprint, err := nsreplication.NamespaceTaskFingerprint(observed.request.GetNamespaceTask())
	s.Require().NoError(err)
	s.Require().True(bytes.Equal(fingerprint, observed.request.GetFingerprint()))
}

func (s *namespaceReplicationCHASMTestSuite) requireNamespaceDescription(
	ctx context.Context,
	cluster *testcore.TestCluster,
	namespaceName string,
	description string,
	configVersion int64,
) {
	await.RequireTruef(s.T(), func() bool {
		response, err := cluster.TestBase().MetadataManager.GetNamespace(
			ctx,
			&persistence.GetNamespaceRequest{Name: namespaceName},
		)
		return err == nil &&
			response.Namespace.GetInfo().GetDescription() == description &&
			response.Namespace.GetConfigVersion() == configVersion
	}, replicationWaitTime, replicationCheckInterval, "namespace %q did not converge", namespaceName)
}

func (s *namespaceReplicationCHASMTestSuite) requirePersistedNamespace(
	ctx context.Context,
	cluster *testcore.TestCluster,
	namespaceName string,
) *persistence.GetNamespaceResponse {
	var response *persistence.GetNamespaceResponse
	await.RequireTruef(s.T(), func() bool {
		var getErr error
		response, getErr = cluster.TestBase().MetadataManager.GetNamespace(
			ctx,
			&persistence.GetNamespaceRequest{Name: namespaceName},
		)
		return getErr == nil
	}, replicationWaitTime, replicationCheckInterval, "namespace %q was not persisted", namespaceName)
	return response
}
