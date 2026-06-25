package tests

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	namespaceTestSuite struct {
		parallelsuite.Suite[*namespaceTestSuite]
	}
)

func TestNamespaceSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &namespaceTestSuite{}) //nolint:staticcheck // SA1019: namespace deletion tests use dedicated worker-service clusters.
}

func (s *namespaceTestSuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	baseOpts := []testcore.TestOption{
		testcore.WithWorkerService("namespace deletion tests require the system worker service"),
		testcore.WithDynamicConfig(dynamicconfig.TransferProcessorUpdateAckInterval, 1*time.Second),
		testcore.WithDynamicConfig(dynamicconfig.VisibilityProcessorUpdateAckInterval, 1*time.Second),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Empty() {
	env := s.newTestEnv()

	retention := 24 * time.Hour
	_, err := env.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := env.OperatorClient().DeleteNamespace(s.Context(), &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false
		}

		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_OverrideDelay() {
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.DeleteNamespaceNamespaceDeleteDelay, time.Hour))

	retention := 24 * time.Hour
	_, err := env.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := env.OperatorClient().DeleteNamespace(s.Context(), &operatorservice.DeleteNamespaceRequest{
		Namespace:            "ns_name_san_diego",
		NamespaceDeleteDelay: durationpb.New(0),
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false
		}

		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Empty_WithID() {
	env := s.newTestEnv()

	retention := 24 * time.Hour
	_, err := env.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := env.OperatorClient().DeleteNamespace(s.Context(), &operatorservice.DeleteNamespaceRequest{
		NamespaceId: nsID,
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false
		}

		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_WithNameAndID() {
	env := s.newTestEnv()

	retention := 24 * time.Hour
	_, err := env.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	_, err = env.OperatorClient().DeleteNamespace(s.Context(), &operatorservice.DeleteNamespaceRequest{
		Namespace:   "ns_name_san_diego",
		NamespaceId: nsID,
	})
	s.EqualError(err, "Only one of namespace name or Id should be set on request.")
}

func (s *namespaceTestSuite) Test_NamespaceDelete_WithWorkflows() {
	env := s.newTestEnv()

	retention := 24 * time.Hour
	_, err := env.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_seattle",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_seattle",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	// Start few workflow executions.
	var executions []*commonpb.WorkflowExecution
	for i := range 100 {
		wid := "wf_id_" + strconv.Itoa(i)
		resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    "ns_name_seattle",
			WorkflowId:   wid,
			WorkflowType: &commonpb.WorkflowType{Name: "workflowTypeName"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "taskQueueName", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		s.NoError(err)
		executions = append(executions, &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      resp.GetRunId(),
		})
	}

	// Terminate some workflow executions.
	for _, execution := range executions[:30] {
		_, err = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         "ns_name_seattle",
			WorkflowExecution: execution,
		})
		s.NoError(err)
	}

	delResp, err := env.OperatorClient().DeleteNamespace(s.Context(), &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_seattle",
	})
	s.NoError(err)
	s.Equal("ns_name_seattle-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())

	s.Eventually(func() bool {
		_, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false // namespace still exists
		}

		for _, execution := range executions {
			_, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: "ns_name_seattle",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: execution.GetWorkflowId(),
				},
			})
			if !errors.As(err, &notFound) {
				return false // should never happen
			}
		}
		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_WithMissingWorkflows() {
	env := s.newTestEnv()

	retention := 24 * time.Hour
	_, err := env.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_los_angeles",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_los_angeles",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	// Start few workflow executions.

	var executions []*commonpb.WorkflowExecution
	for i := range 10 {
		wid := "wf_id_" + strconv.Itoa(i)
		resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:    uuid.NewString(),
			Namespace:    "ns_name_los_angeles",
			WorkflowId:   wid,
			WorkflowType: &commonpb.WorkflowType{Name: "workflowTypeName"},
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "taskQueueName", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		})
		s.NoError(err)
		executions = append(executions, &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      resp.GetRunId(),
		})
	}

	// Delete some workflow executions from DB but not from visibility.
	// Every subsequent delete (from deleteexecutions.Workflow) from ES will take at least 1s due to bulk processor.
	for _, execution := range executions[0:5] {
		shardID := common.WorkflowIDToHistoryShard(
			nsID,
			execution.GetWorkflowId(),
			env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
		)

		err = env.GetTestCluster().ExecutionManager().DeleteWorkflowExecution(s.Context(), &persistence.DeleteWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: nsID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		s.NoError(err)
	}

	delResp, err := env.OperatorClient().DeleteNamespace(s.Context(), &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_los_angeles",
	})
	s.NoError(err)
	s.Equal("ns_name_los_angeles-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())

	s.Eventually(func() bool {
		_, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false // namespace still exists
		}

		for _, execution := range executions {
			_, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: "ns_name_los_angeles",
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: execution.GetWorkflowId(),
				},
			})
			if !errors.As(err, &notFound) {
				return false // should never happen
			}
		}
		return true
	}, 20*time.Second, time.Second)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_CrossNamespaceChild() {
	// TODO (alex): create 2 namespaces, start workflow in first namespace and start child in second namespace.
	// Delete second namespace and verify that parent received child termination signal.
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Protected() {
	tv := testvars.New(s.T())
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.ProtectedNamespaces, []string{tv.NamespaceName().String()}))

	_, err := env.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        tv.NamespaceName().String(),
		Description:                      tv.Any().String(),
		WorkflowExecutionRetentionPeriod: tv.Any().InfiniteTimeout(),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	delResp, err := env.OperatorClient().DeleteNamespace(s.Context(), &operatorservice.DeleteNamespaceRequest{
		Namespace: tv.NamespaceName().String(),
	})
	s.Error(err)
	s.Nil(delResp)

	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.Equal(fmt.Sprintf("namespace %s is protected from deletion", tv.NamespaceName().String()), failedPreconditionErr.Message)
}

func (s *namespaceTestSuite) Test_DescribeNamespace_WeakConsistency() {
	env := s.newTestEnv()

	nsName := "ns_weak_" + uuid.NewString()
	_, err := env.FrontendClient().RegisterNamespace(s.Context(), &workflowservice.RegisterNamespaceRequest{
		Namespace:                        nsName,
		Description:                      "weak consistency test",
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	var nsNotFound *serviceerror.NamespaceNotFound

	// Cache is populated by the periodic refresh, so the weak path should eventually succeed.
	var weakResp *workflowservice.DescribeNamespaceResponse
	s.Eventually(func() bool {
		weakResp, err = env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
			Namespace:       nsName,
			WeakConsistency: true,
		})
		// fail the test if error is not nil or NamespaceNotFound
		if err != nil && !errors.As(err, &nsNotFound) {
			s.Failf("unexpected error", "Expected NamespaceNotFound error, got: %v", err)
		}
		return err == nil
	}, 5*testcore.NamespaceCacheRefreshInterval, 100*time.Millisecond) //nolint:forbidigo

	strongResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace: nsName,
	})
	s.NoError(err)
	s.Equal(strongResp.GetNamespaceInfo().GetId(), weakResp.GetNamespaceInfo().GetId())
	s.Equal(strongResp.GetNamespaceInfo().GetName(), weakResp.GetNamespaceInfo().GetName())
	s.Equal(strongResp.GetIsGlobalNamespace(), weakResp.GetIsGlobalNamespace())

	// Lookup by ID should also work via the weak path.
	nsID := strongResp.GetNamespaceInfo().GetId()
	weakByIDResp, err := env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Id:              nsID,
		WeakConsistency: true,
	})
	s.NoError(err)
	s.Equal(nsName, weakByIDResp.GetNamespaceInfo().GetName())

	// Non-existent namespace should return NamespaceNotFound.
	_, err = env.FrontendClient().DescribeNamespace(s.Context(), &workflowservice.DescribeNamespaceRequest{
		Namespace:       "ns_does_not_exist_" + uuid.NewString(),
		WeakConsistency: true,
	})
	s.ErrorAs(err, &nsNotFound)
}
