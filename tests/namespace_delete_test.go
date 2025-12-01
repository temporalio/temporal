package tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
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
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type (
	namespaceTestSuite struct {
		testcore.FunctionalTestBase
	}
)

func TestNamespaceSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &namespaceTestSuite{})
}

func (s *namespaceTestSuite) SetupSuite() {
	dynamicConfigOverrides := map[dynamicconfig.Key]any{
		// Run tests at full speed.
		dynamicconfig.DeleteNamespaceDeleteActivityRPS.Key(): 1000000,

		dynamicconfig.TransferProcessorUpdateAckInterval.Key():   1 * time.Second,
		dynamicconfig.VisibilityProcessorUpdateAckInterval.Key(): 1 * time.Second,
	}
	s.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(dynamicConfigOverrides))
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Empty() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := s.OperatorClient().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
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
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	s.OverrideDynamicConfig(dynamicconfig.DeleteNamespaceNamespaceDeleteDelay, time.Hour)

	retention := 24 * time.Hour
	_, err := s.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := s.OperatorClient().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace:            "ns_name_san_diego",
		NamespaceDeleteDelay: durationpb.New(0),
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
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
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	delResp, err := s.OperatorClient().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		NamespaceId: nsID,
	})
	s.NoError(err)
	s.Equal("ns_name_san_diego-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())
	s.Eventually(func() bool {
		_, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
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
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_san_diego",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_san_diego",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	_, err = s.OperatorClient().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace:   "ns_name_san_diego",
		NamespaceId: nsID,
	})
	s.EqualError(err, "Only one of namespace name or Id should be set on request.")
}

func (s *namespaceTestSuite) Test_NamespaceDelete_WithWorkflows() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_seattle",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_seattle",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	// Start few workflow executions.
	var executions []*commonpb.WorkflowExecution
	for i := 0; i < 100; i++ {
		wid := "wf_id_" + strconv.Itoa(i)
		resp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
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
		_, err = s.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         "ns_name_seattle",
			WorkflowExecution: execution,
		})
		s.NoError(err)
	}

	delResp, err := s.OperatorClient().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_seattle",
	})
	s.NoError(err)
	s.Equal("ns_name_seattle-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())

	s.Eventually(func() bool {
		_, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false // namespace still exists
		}

		for _, execution := range executions {
			_, err = s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
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
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(10000 * time.Second)
	defer cancel()

	retention := 24 * time.Hour
	_, err := s.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        "ns_name_los_angeles",
		Description:                      "Namespace to delete",
		WorkflowExecutionRetentionPeriod: durationpb.New(retention),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	descResp, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: "ns_name_los_angeles",
	})
	s.NoError(err)
	nsID := descResp.GetNamespaceInfo().GetId()

	// Start few workflow executions.

	var executions []*commonpb.WorkflowExecution
	for i := 0; i < 10; i++ {
		wid := "wf_id_" + strconv.Itoa(i)
		resp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
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
			s.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
		)

		err = s.GetTestCluster().ExecutionManager().DeleteWorkflowExecution(ctx, &persistence.DeleteWorkflowExecutionRequest{
			ShardID:     shardID,
			NamespaceID: nsID,
			WorkflowID:  execution.GetWorkflowId(),
			RunID:       execution.GetRunId(),
			ArchetypeID: chasm.WorkflowArchetypeID,
		})
		s.NoError(err)
	}

	delResp, err := s.OperatorClient().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: "ns_name_los_angeles",
	})
	s.NoError(err)
	s.Equal("ns_name_los_angeles-deleted-"+nsID[:5], delResp.GetDeletedNamespace())

	descResp2, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Id: nsID,
	})
	s.NoError(err)
	s.Equal(enumspb.NAMESPACE_STATE_DELETED, descResp2.GetNamespaceInfo().GetState())

	s.Eventually(func() bool {
		_, err := s.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Id: nsID,
		})
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) {
			return false // namespace still exists
		}

		for _, execution := range executions {
			_, err = s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
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

// checkTestShard supports test sharding based on environment variables.
func (s *namespaceTestSuite) checkTestShard() {
	totalStr := os.Getenv("TEST_TOTAL_SHARDS")
	indexStr := os.Getenv("TEST_SHARD_INDEX")
	if totalStr == "" || indexStr == "" {
		return
	}
	total, err := strconv.Atoi(totalStr)
	if err != nil || total < 1 {
		s.T().Fatal("Couldn't convert TEST_TOTAL_SHARDS")
	}
	index, err := strconv.Atoi(indexStr)
	if err != nil || index < 0 || index >= total {
		s.T().Fatal("Couldn't convert TEST_SHARD_INDEX")
	}

	// This was determined empirically to distribute our existing test names
	// reasonably well. This can be adjusted from time to time.
	// For parallelism 4, use 11. For 3, use 26. For 2, use 20.
	const salt = "-salt-26"

	nameToHash := s.T().Name() + salt
	testIndex := int(farm.Fingerprint32([]byte(nameToHash))) % total
	if testIndex != index {
		s.T().Skipf("Skipping %s in test shard %d/%d (it runs in %d)", s.T().Name(), index+1, total, testIndex+1)
	}
	s.T().Logf("Running %s in test shard %d/%d", s.T().Name(), index+1, total)
}

func (s *namespaceTestSuite) Test_NamespaceDelete_Protected() {
	ctx := context.Background()

	tv := testvars.New(s.T())

	_, err := s.FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        tv.NamespaceName().String(),
		Description:                      tv.Any().String(),
		WorkflowExecutionRetentionPeriod: tv.Any().InfiniteTimeout(),
		HistoryArchivalState:             enumspb.ARCHIVAL_STATE_DISABLED,
		VisibilityArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
	})
	s.NoError(err)

	s.OverrideDynamicConfig(dynamicconfig.ProtectedNamespaces, []string{tv.NamespaceName().String()})

	delResp, err := s.OperatorClient().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{
		Namespace: tv.NamespaceName().String(),
	})
	s.Error(err)
	s.Nil(delResp)

	var failedPreconditionErr *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPreconditionErr)
	s.Equal(fmt.Sprintf("namespace %s is protected from deletion", tv.NamespaceName().String()), failedPreconditionErr.Message)
}
