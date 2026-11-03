package xdc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
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
		failCreateRequests    atomic.Bool
		dropApplyField        atomic.Bool
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

func TestNamespaceMutationObservationFiltersLateRequests(t *testing.T) {
	t.Parallel()
	s := &namespaceReplicationCHASMTestSuite{observedApplyRequests: make(chan observedNamespaceMutationApply, 2)}
	s.SetT(t)
	for _, name := range []string{"previous-test", "current-test"} {
		payload, err := nsreplication.MarshalNamespaceTask(&replicationspb.NamespaceTaskAttributes{Info: &namespacepb.NamespaceInfo{Name: name}})
		require.NoError(t, err)
		s.recordObservedApply(t.Context(), &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: payload}, nil, nil)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	observed := s.receiveObservedApply(ctx, "current-test")
	require.Equal(t, "current-test", s.decodeNamespaceTask(observed.request).GetInfo().GetName())
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
	s.failCreateRequests.Store(false)
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

func (s *namespaceReplicationCHASMTestSuite) TestAuthoritativeTransportRecoversLocalCommitBoundary() {
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
	var failBeforeNextCommit atomic.Bool
	var boundaryFailuresInjected atomic.Int32
	failBeforeNextCommit.Store(true)
	active.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationBeforeLocalCommit, func(
			_ context.Context,
		) error {
			if failBeforeNextCommit.CompareAndSwap(true, false) {
				boundaryFailuresInjected.Add(1)
				return serviceerror.NewUnavailable("injected failure after metadata write")
			}
			return nil
		}),
		namespace.Name(namespaceName),
	)
	legacyTasks := make(chan struct{}, 1)
	standby.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationTaskInterceptor, func(
			_ context.Context,
			task *replicationspb.NamespaceTaskAttributes,
			execute func() error,
		) error {
			select {
			case legacyTasks <- struct{}{}:
			default:
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
	s.Require().Equal(int32(1), boundaryFailuresInjected.Load())
	s.requireSuccessfulApply(
		s.receiveObservedApply(ctx, namespaceName),
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_CREATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED,
	)

	failBeforeNextCommit.Store(true)
	const updatedDescription = "update recovered across metadata/CHASM commit boundary"
	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespaceName,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: updatedDescription,
		},
	})
	s.Require().NoError(err)
	s.Require().Equal(int32(2), boundaryFailuresInjected.Load())
	s.requireSuccessfulApply(
		s.receiveObservedApply(ctx, namespaceName),
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED,
	)

	sourceNamespace := s.requirePersistedNamespace(ctx, active, namespaceName)
	standbyNamespace := s.requirePersistedNamespace(ctx, standby, namespaceName)
	s.Require().Equal(updatedDescription, sourceNamespace.Namespace.GetInfo().GetDescription())
	s.Require().Equal(updatedDescription, standbyNamespace.Namespace.GetInfo().GetDescription())
	s.Require().True(proto.Equal(sourceNamespace.Namespace, standbyNamespace.Namespace))
	s.Require().Never(func() bool {
		select {
		case <-legacyTasks:
			return true
		default:
			return false
		}
	}, time.Second, 20*time.Millisecond, "commit-boundary recovery published a legacy namespace replication task")
}

func (s *namespaceReplicationCHASMTestSuite) TestAuthoritativeTransportRepublishesNoOpWithoutLocalWrite() {
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
		s.receiveObservedApply(ctx, namespaceName),
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_CREATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED,
	)

	sourceBefore := s.requirePersistedNamespace(ctx, active, namespaceName)
	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace: namespaceName,
	})
	s.Require().NoError(err)
	// A no-op update still republishes the authoritative snapshot. Because the
	// standby is already current, its apply-if-higher contract reports stale.
	s.requireSuccessfulApply(
		s.receiveObservedApply(ctx, namespaceName),
		namespaceName,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		false,
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE,
	)

	sourceAfter := s.requirePersistedNamespace(ctx, active, namespaceName)
	standbyAfter := s.requirePersistedNamespace(ctx, standby, namespaceName)
	s.Require().True(proto.Equal(sourceBefore.Namespace, sourceAfter.Namespace))
	s.Require().Equal(sourceBefore.NotificationVersion, sourceAfter.NotificationVersion,
		"replicate-only update must not consume a source notification-version CAS slot")
	s.Require().True(proto.Equal(sourceAfter.Namespace, standbyAfter.Namespace))
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
	legacyTasks := make(chan struct{}, 1)
	standby.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationTaskInterceptor, func(
			_ context.Context,
			task *replicationspb.NamespaceTaskAttributes,
			execute func() error,
		) error {
			select {
			case legacyTasks <- struct{}{}:
			default:
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
		s.receiveObservedApply(ctx, namespaceName),
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

	failedApply := s.receiveObservedApply(ctx, namespaceName)
	s.requireApplyRequest(failedApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE, false)
	s.Require().Nil(failedApply.response)
	s.Require().ErrorAs(failedApply.err, new(*serviceerror.Unavailable))
	s.failApplyRequests.Store(false)
	terminalVersions := make(map[int64]struct{}, mutationCount)
	recovered := false
	for len(terminalVersions) < mutationCount {
		observed := s.receiveObservedApply(ctx, namespaceName)
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
		configVersion := observed.request.GetNamespaceTask().GetConfigVersion()
		s.Require().GreaterOrEqual(configVersion, int64(1))
		s.Require().LessOrEqual(configVersion, int64(mutationCount))
		terminalVersions[configVersion] = struct{}{}
	}
	s.Require().True(recovered, "no peer apply advanced the standby namespace")

	sourceTask := nsreplication.NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, sourceNamespace.Namespace)
	await.RequireTruef(s.T(), func() bool {
		standbyNamespace, getErr := standby.TestBase().MetadataManager.GetNamespace(
			ctx,
			&persistence.GetNamespaceRequest{Name: namespaceName},
		)
		if getErr != nil {
			return false
		}
		standbyTask := nsreplication.NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, standbyNamespace.Namespace)
		return proto.Equal(sourceTask, standbyTask)
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

func (s *namespaceReplicationCHASMTestSuite) TestAuthoritativeTransportFencesDelayedAdditionAfterRemoval() {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	active, standby := s.clusters[0], s.clusters[1]
	cleanup := active.OverrideDynamicConfig(s.T(), dynamicconfig.NamespaceReplicationTransportMode, dynamicconfig.NamespaceReplicationTransportModeCHASM)
	defer cleanup()
	s.failCreateRequests.Store(true)
	defer s.failCreateRequests.Store(false)
	namespaceName := "test-namespace-" + uuid.NewString()
	_, err := active.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace: namespaceName, IsGlobalNamespace: true, ActiveClusterName: active.ClusterName(),
		Clusters:                         []*replicationpb.ClusterReplicationConfig{{ClusterName: active.ClusterName()}, {ClusterName: standby.ClusterName()}},
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	s.Require().NoError(err)
	failedCreate := s.receiveObservedApply(ctx, namespaceName)
	s.Require().ErrorAs(failedCreate.err, new(*serviceerror.Unavailable))
	s.Require().Equal(enumsspb.NAMESPACE_OPERATION_CREATE, s.decodeNamespaceTask(failedCreate.request).GetNamespaceOperation())
	_, err = standby.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: namespaceName})
	s.Require().ErrorAs(err, new(*serviceerror.NamespaceNotFound))

	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:         namespaceName,
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{Clusters: []*replicationpb.ClusterReplicationConfig{{ClusterName: active.ClusterName()}}},
	})
	s.Require().NoError(err)
	for {
		observed := s.receiveObservedApply(ctx, namespaceName)
		if observed.request.GetRemovedPeer() {
			s.requireSuccessfulApply(observed, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE, false, adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED)
			break
		}
	}
	removed, err := standby.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: namespaceName})
	s.Require().NoError(err)
	s.Require().Equal(int64(1), removed.Namespace.GetConfigVersion())
	s.Require().Equal([]string{active.ClusterName()}, removed.Namespace.GetReplicationConfig().GetClusters())

	s.failCreateRequests.Store(false)
	for {
		observed := s.receiveObservedApply(ctx, namespaceName)
		if observed.err == nil && s.decodeNamespaceTask(observed.request).GetNamespaceOperation() == enumsspb.NAMESPACE_OPERATION_CREATE {
			s.requireSuccessfulApply(observed, namespaceName, enumsspb.NAMESPACE_OPERATION_CREATE, false, adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE)
			break
		}
	}
	afterReplay, err := standby.TestBase().MetadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{Name: namespaceName})
	s.Require().NoError(err)
	s.Require().True(proto.Equal(removed.Namespace, afterReplay.Namespace), "delayed addition must not restore removed membership")
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
	legacyTasks := make(chan struct{}, 1)
	standby.InjectHook(
		s.T(),
		testhooks.NewHook(testhooks.NamespaceReplicationTaskInterceptor, func(
			_ context.Context,
			task *replicationspb.NamespaceTaskAttributes,
			execute func() error,
		) error {
			select {
			case legacyTasks <- struct{}{}:
			default:
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

	createApply := s.receiveObservedApply(ctx, namespaceName)
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
	firstUpdateApply := s.receiveObservedApply(ctx, namespaceName)
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
	secondUpdateApply := s.receiveObservedApply(ctx, namespaceName)
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
			observed := s.receiveObservedApply(ctx, namespaceName)
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
	failedApply := s.receiveObservedApply(ctx, namespaceName)
	s.requireApplyRequest(failedApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE, false)
	s.Require().Nil(failedApply.response)
	s.Require().ErrorAs(failedApply.err, new(*serviceerror.Unavailable))

	s.failApplyRequests.Store(false)
	recoveredApply := s.receiveObservedApply(ctx, namespaceName)
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
			select {
			case legacyTasks <- bufferedNamespaceReplicationTask{task: proto.Clone(task).(*replicationspb.NamespaceTaskAttributes), execute: execute}:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
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

	createApply := s.receiveObservedApply(ctx, namespaceName)
	s.requireSuccessfulShadowApply(createApply, namespaceName, enumsspb.NAMESPACE_OPERATION_CREATE)
	createLegacyTask := s.receiveLegacyTask(ctx, legacyTasks)
	s.Require().Equal(enumsspb.NAMESPACE_OPERATION_CREATE, createLegacyTask.task.GetNamespaceOperation())
	s.Require().True(proto.Equal(createLegacyTask.task, s.decodeNamespaceTask(createApply.request)),
		"CHASM create payload differs from the legacy replication task")
	s.requireLegacyReference(createApply.request, createLegacyTask.task)

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

	updateApply := s.receiveObservedApply(ctx, namespaceName)
	s.requireSuccessfulShadowApply(updateApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE)
	updateLegacyTask := s.receiveLegacyTask(ctx, legacyTasks)
	s.Require().Equal(enumsspb.NAMESPACE_OPERATION_UPDATE, updateLegacyTask.task.GetNamespaceOperation())
	s.Require().True(proto.Equal(updateLegacyTask.task, s.decodeNamespaceTask(updateApply.request)),
		"CHASM update payload differs from the legacy replication task")
	s.requireLegacyReference(updateApply.request, updateLegacyTask.task)

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

	failedApply := s.receiveObservedApply(ctx, namespaceName)
	s.requireShadowApplyRequest(failedApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE)
	s.Require().Nil(failedApply.response)
	s.Require().ErrorAs(failedApply.err, new(*serviceerror.Unavailable))
	s.failApplyRequests.Store(false)
	recoveredApply := s.receiveObservedApply(ctx, namespaceName)
	s.requireSuccessfulShadowApply(recoveredApply, namespaceName, enumsspb.NAMESPACE_OPERATION_UPDATE)
	failedUpdateLegacyTask := s.receiveLegacyTask(ctx, legacyTasks)
	s.Require().Equal(enumsspb.NAMESPACE_OPERATION_UPDATE, failedUpdateLegacyTask.task.GetNamespaceOperation())
	s.Require().Equal(failedApply.request.GetNamespaceTaskPayload(), recoveredApply.request.GetNamespaceTaskPayload(),
		"CHASM retry changed the namespace mutation payload")
	s.Require().True(proto.Equal(failedUpdateLegacyTask.task, s.decodeNamespaceTask(recoveredApply.request)),
		"retried CHASM update payload differs from the legacy replication task")
	s.requireLegacyReference(recoveredApply.request, failedUpdateLegacyTask.task)
	s.Require().Equal(failedApply.request.GetShadowLegacyTaskPayload(), recoveredApply.request.GetShadowLegacyTaskPayload())

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

	// Simulate a history converter that omits a field. Comparison with the
	// preserved frontend reference must detect the incomplete output.
	s.dropApplyField.Store(true)
	defer s.dropApplyField.Store(false)
	_, err = active.FrontendClient().UpdateNamespace(ctx, &workflowservice.UpdateNamespaceRequest{
		Namespace:  namespaceName,
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{Description: "field omitted by simulated older history converter"},
	})
	s.Require().NoError(err)
	mismatchedApply := s.receiveObservedApply(ctx, namespaceName)
	s.Require().NoError(mismatchedApply.err)
	s.Require().Equal(adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH, mismatchedApply.response.GetOutcome())
	finalLegacyTask := s.receiveLegacyTask(ctx, legacyTasks)
	s.requireLegacyReference(mismatchedApply.request, finalLegacyTask.task)
	s.Require().NoError(finalLegacyTask.execute())
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
	if s.dropApplyField.Load() {
		applyRequest = proto.Clone(applyRequest).(*adminservice.ApplyNamespaceMutationRequest)
		payloadTask := &replicationspb.NamespaceTaskAttributes{}
		if err := proto.Unmarshal(applyRequest.GetNamespaceTaskPayload(), payloadTask); err != nil {
			return nil, err
		}
		payloadTask.Info.Description = ""
		encodedTask, err := nsreplication.MarshalNamespaceTask(payloadTask)
		if err != nil {
			return nil, err
		}
		applyRequest.NamespaceTaskPayload = encodedTask
		req = applyRequest
	}
	if s.failApplyRequests.Load() || (s.failCreateRequests.Load() && applyRequest.GetNamespaceTask().GetNamespaceOperation() == enumsspb.NAMESPACE_OPERATION_CREATE) {
		err := serviceerror.NewUnavailable("injected CHASM namespace replication failure")
		s.recordObservedApply(ctx, applyRequest, nil, err)
		return nil, err
	}

	response, err := handler(ctx, req)
	applyResponse, _ := response.(*adminservice.ApplyNamespaceMutationResponse)
	s.recordObservedApply(ctx, applyRequest, applyResponse, err)
	return response, err
}

func (s *namespaceReplicationCHASMTestSuite) requireLegacyReference(
	request *adminservice.ApplyNamespaceMutationRequest,
	legacyTask *replicationspb.NamespaceTaskAttributes,
) {
	actual := &replicationspb.NamespaceTaskAttributes{}
	s.Require().NoError(proto.Unmarshal(request.GetShadowLegacyTaskPayload(), actual))
	s.Require().True(proto.Equal(legacyTask, actual), "shadow reference differs from the actual legacy queue task")
}

func (s *namespaceReplicationCHASMTestSuite) recordObservedApply(
	ctx context.Context,
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
	select {
	case s.observedApplyRequests <- observedNamespaceMutationApply{
		request:  clonedRequest,
		response: clonedResponse,
		err:      err,
	}:
	case <-ctx.Done():
	}
}

func (s *namespaceReplicationCHASMTestSuite) receiveObservedApply(ctx context.Context, namespaceName string) observedNamespaceMutationApply {
	for {
		select {
		case observed := <-s.observedApplyRequests:
			task := &replicationspb.NamespaceTaskAttributes{}
			if proto.Unmarshal(observed.request.GetNamespaceTaskPayload(), task) == nil && task.GetInfo().GetName() == namespaceName {
				return observed
			}
		case <-ctx.Done():
			s.FailNow("timed out waiting for CHASM namespace mutation apply", ctx.Err())
			return observedNamespaceMutationApply{}
		}
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
		adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH,
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
	payloadTask := s.decodeNamespaceTask(observed.request)
	s.Require().True(proto.Equal(payloadTask, observed.request.GetNamespaceTask()), "retain the typed field for older receivers")
	s.Require().Equal(namespaceName, payloadTask.GetInfo().GetName())
	s.Require().Equal(operation, payloadTask.GetNamespaceOperation())
}

func (s *namespaceReplicationCHASMTestSuite) decodeNamespaceTask(
	request *adminservice.ApplyNamespaceMutationRequest,
) *replicationspb.NamespaceTaskAttributes {
	task := &replicationspb.NamespaceTaskAttributes{}
	s.Require().NoError(proto.Unmarshal(request.GetNamespaceTaskPayload(), task))
	return task
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
