package xdc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/tests/testcore"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
)

type deleteExecutionReplicationTestSuite struct {
	xdcBaseSuite
}

func TestDeleteExecutionReplicationTestSuite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                    string
		enableTransitionHistory bool
	}{
		{
			name:                    "DisableTransitionHistory",
			enableTransitionHistory: false,
		},
		{
			name:                    "EnableTransitionHistory",
			enableTransitionHistory: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &deleteExecutionReplicationTestSuite{}
			s.enableTransitionHistory = tc.enableTransitionHistory
			suite.Run(t, s)
		})
	}
}

func (s *deleteExecutionReplicationTestSuite) SetupSuite() {
	s.dynamicConfigOverrides = map[dynamicconfig.Key]any{
		dynamicconfig.EnableReplicationStream.Key():                   true,
		dynamicconfig.EnableReplicationTaskBatching.Key():             true,
		dynamicconfig.EnableDeleteWorkflowExecutionReplication.Key():  true,
		dynamicconfig.EnableSeparateReplicationEnableFlag.Key():       true,
		dynamicconfig.EnableWorkflowTaskStampIncrementOnFailure.Key(): true,
	}
	s.logger = log.NewTestLogger()

	s.setupSuite(
		testcore.WithFxOptionsForService(primitives.AllServices,
			fx.Decorate(
				func(_ config.DCRedirectionPolicy) config.DCRedirectionPolicy {
					return config.DCRedirectionPolicy{Policy: "noop"}
				},
			),
		),
	)
}

func (s *deleteExecutionReplicationTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *deleteExecutionReplicationTestSuite) SetupTest() {
	s.setupTest()
}

func (s *deleteExecutionReplicationTestSuite) TestDeleteClosedWorkflow_ReplicatedToPassiveCluster() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*2)
	defer cancel()

	// Create a global namespace on both clusters.
	ns := s.createGlobalNamespace()
	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.Require().NoError(err)
	nsID := nsResp.GetNamespaceInfo().GetId()

	workflowID := "test-delete-replication-" + uuid.NewString()
	taskQueue := "test-delete-tq-" + uuid.NewString()
	sourceClient := s.clusters[0].FrontendClient()

	// Start a workflow on the active cluster.
	startResp, err := sourceClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.Require().NoError(err)
	runID := startResp.GetRunId()

	// Complete the workflow.
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				},
			},
		}}, nil
	}
	//nolint:staticcheck // TODO: replace with taskpoller.TaskPoller
	poller := &testcore.TaskPoller{
		Client:              sourceClient,
		Namespace:           ns,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:            "worker",
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}
	_, err = poller.PollAndProcessWorkflowTask()
	s.Require().NoError(err)

	// Wait for workflow to be completed on active cluster.
	s.Eventually(func() bool {
		resp, err := sourceClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		if err != nil {
			return false
		}
		return resp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, time.Second*10, time.Second)

	// Wait for the workflow to be replicated to the passive cluster.
	targetClient := s.clusters[1].FrontendClient()
	s.Eventually(func() bool {
		resp, err := targetClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		if err != nil {
			return false
		}
		return resp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, replicationWaitTime, replicationCheckInterval, "Workflow should be replicated to passive cluster")

	// Delete the workflow on the active cluster.
	_, err = sourceClient.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.Require().NoError(err)

	// Verify the workflow is deleted on the active cluster.
	s.Eventually(func() bool {
		_, err := sourceClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		if err == nil {
			return false
		}
		var notFound *serviceerror.NotFound
		return errors.As(err, &notFound)
	}, time.Second*10, time.Second, "Workflow should be deleted on active cluster")

	// Verify the workflow mutable state is deleted on the passive cluster via replication.
	s.Eventually(func() bool {
		_, err := s.clusters[1].HistoryClient().DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
			NamespaceId: nsID,
			Execution:   &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			ArchetypeId: chasm.WorkflowArchetypeID,
		})
		if err == nil {
			return false
		}
		var notFound *serviceerror.NotFound
		return errors.As(err, &notFound)
	}, time.Second*30, replicationCheckInterval, "Workflow mutable state should be deleted on passive cluster via replication")
}

func (s *deleteExecutionReplicationTestSuite) TestDeleteRunningWorkflow_ReplicatedToPassiveCluster() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ns := s.createGlobalNamespace()
	nsResp, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.Require().NoError(err)
	nsID := nsResp.GetNamespaceInfo().GetId()

	workflowID := "test-delete-running-" + uuid.NewString()
	taskQueue := "test-delete-running-tq-" + uuid.NewString()
	sourceClient := s.clusters[0].FrontendClient()

	// Start a workflow on the active cluster (don't complete it — leave it running).
	startResp, err := sourceClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.Require().NoError(err)
	runID := startResp.GetRunId()

	// Wait for the workflow to be replicated to the passive cluster.
	s.Eventually(func() bool {
		_, err := s.clusters[1].FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		return err == nil
	}, replicationWaitTime, replicationCheckInterval, "Workflow should be replicated to passive cluster")

	// Delete the running workflow on the active cluster.
	// This will terminate it first (deleteAfterTerminate=true), then delete.
	_, err = sourceClient.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.Require().NoError(err)

	// Verify the workflow is deleted on the active cluster.
	s.Eventually(func() bool {
		_, err := sourceClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		if err == nil {
			return false
		}
		var notFound *serviceerror.NotFound
		return errors.As(err, &notFound)
	}, time.Second*10, time.Second, "Workflow should be deleted on active cluster")

	// Verify the workflow mutable state is deleted on the passive cluster via replication.
	s.Eventually(func() bool {
		_, err := s.clusters[1].HistoryClient().DescribeMutableState(ctx, &historyservice.DescribeMutableStateRequest{
			NamespaceId: nsID,
			Execution:   &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
			ArchetypeId: chasm.WorkflowArchetypeID,
		})
		if err == nil {
			return false
		}
		var notFound *serviceerror.NotFound
		return errors.As(err, &notFound)
	}, time.Second*30, replicationCheckInterval, "Workflow mutable state should be deleted on passive cluster via replication")
}

func (s *deleteExecutionReplicationTestSuite) TestDeleteWorkflow_NotReplicatedWhenFeatureFlagDisabled() {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	// Disable the feature flag on both clusters.
	cleanup0 := s.clusters[0].OverrideDynamicConfig(s.T(), dynamicconfig.EnableDeleteWorkflowExecutionReplication, false)
	defer cleanup0()
	cleanup1 := s.clusters[1].OverrideDynamicConfig(s.T(), dynamicconfig.EnableDeleteWorkflowExecutionReplication, false)
	defer cleanup1()

	ns := s.createGlobalNamespace()

	workflowID := "test-delete-no-repl-" + uuid.NewString()
	taskQueue := "test-delete-no-repl-tq-" + uuid.NewString()
	sourceClient := s.clusters[0].FrontendClient()

	// Start and complete a workflow.
	startResp, err := sourceClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           ns,
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "test-wf-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(time.Minute),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.Require().NoError(err)
	runID := startResp.GetRunId()

	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
				CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
					Result: payloads.EncodeString("done"),
				},
			},
		}}, nil
	}
	//nolint:staticcheck // TODO: replace with taskpoller.TaskPoller
	poller := &testcore.TaskPoller{
		Client:              sourceClient,
		Namespace:           ns,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:            "worker",
		WorkflowTaskHandler: wtHandler,
		Logger:              s.logger,
		T:                   s.T(),
	}
	_, err = poller.PollAndProcessWorkflowTask()
	s.Require().NoError(err)

	// Wait for replication to passive.
	targetClient := s.clusters[1].FrontendClient()
	s.Eventually(func() bool {
		resp, err := targetClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		if err != nil {
			return false
		}
		return resp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, replicationWaitTime, replicationCheckInterval)

	// Delete on active cluster.
	_, err = sourceClient.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace: ns,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.Require().NoError(err)

	// Wait for deletion on active.
	s.Eventually(func() bool {
		_, err := sourceClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		})
		var notFound *serviceerror.NotFound
		return errors.As(err, &notFound)
	}, time.Second*10, time.Second)

	// Workflow should still exist on the passive cluster (no replication of deletion).
	//nolint:forbidigo // need to wait to confirm deletion did NOT replicate
	time.Sleep(5 * time.Second)
	_, err = targetClient.DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
	})
	s.NoError(err, "Workflow should still exist on passive cluster when feature flag is disabled")
}

func (s *deleteExecutionReplicationTestSuite) createGlobalNamespace() string {
	ctx := testcore.NewContext()
	ns := "test-delete-ns-" + common.GenerateRandomString(5)
	_, err := s.clusters[0].FrontendClient().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		IsGlobalNamespace:                true,
		Clusters:                         s.clusterReplicationConfig(),
		ActiveClusterName:                s.clusters[0].ClusterName(),
		WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
	})
	s.Require().NoError(err)

	// Wait for namespace to be available on both clusters.
	s.Eventually(func() bool {
		_, err := s.clusters[0].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: ns,
		})
		return err == nil
	}, namespaceCacheWaitTime, namespaceCacheCheckInterval)

	s.Eventually(func() bool {
		_, err := s.clusters[1].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
			Namespace: ns,
		})
		return err == nil
	}, namespaceCacheWaitTime, namespaceCacheCheckInterval)

	return ns
}
