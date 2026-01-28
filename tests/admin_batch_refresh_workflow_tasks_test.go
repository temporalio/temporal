package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

func simpleWorkflow(_ workflow.Context) (string, error) {
	// Simple workflow that just returns
	return "done", nil
}

func TestAdminBatchRefreshWorkflowTasks(t *testing.T) {
	t.Run("RefreshWorkflowTasks_Success", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		w := worker.New(sdkClient, taskQueue, worker.Options{})
		w.RegisterWorkflow(simpleWorkflow)
		require.NoError(t, w.Start())
		defer w.Stop()

		// Helper function to create workflow
		createWorkflow := func() sdkclient.WorkflowRun {
			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
				TaskQueue: taskQueue,
			}
			workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, simpleWorkflow)
			require.NoError(t, err)
			require.NotNil(t, workflowRun)
			return workflowRun
		}

		// Create two workflows
		workflowRun1 := createWorkflow()
		workflowRun2 := createWorkflow()

		// Wait for workflows to complete
		var out string
		err = workflowRun1.Get(ctx, &out)
		require.NoError(t, err)
		err = workflowRun2.Get(ctx, &out)
		require.NoError(t, err)

		// Start admin batch operation to refresh workflow tasks using executions list
		resp, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
			Namespace: s.Namespace().String(),
			JobId:     uuid.NewString(),
			Reason:    "test refresh workflow tasks",
			Identity:  "test-identity",
			Executions: []*commonpb.WorkflowExecution{
				{WorkflowId: workflowRun1.GetID(), RunId: workflowRun1.GetRunID()},
				{WorkflowId: workflowRun2.GetID(), RunId: workflowRun2.GetRunID()},
			},
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("RefreshWorkflowTasks_WithVisibilityQuery", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		w := worker.New(sdkClient, taskQueue, worker.Options{})
		w.RegisterWorkflow(simpleWorkflow)
		require.NoError(t, w.Start())
		defer w.Stop()

		// Helper function to create workflow
		createWorkflow := func() sdkclient.WorkflowRun {
			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
				TaskQueue: taskQueue,
			}
			workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, simpleWorkflow)
			require.NoError(t, err)
			require.NotNil(t, workflowRun)
			return workflowRun
		}

		// Create workflows
		workflowRun1 := createWorkflow()
		workflowRun2 := createWorkflow()

		// Wait for workflows to complete
		var out string
		err = workflowRun1.Get(ctx, &out)
		require.NoError(t, err)
		err = workflowRun2.Get(ctx, &out)
		require.NoError(t, err)

		// Wait for workflows to be visible
		s.EventuallyWithT(func(t *assert.CollectT) {
			resp, err := s.FrontendClient().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "WorkflowType='simpleWorkflow'",
			})
			require.NoError(t, err)
			require.GreaterOrEqual(t, resp.GetCount(), int64(2))
		}, 10*time.Second, 500*time.Millisecond)

		// Start admin batch operation using visibility query
		resp, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
			Namespace:       s.Namespace().String(),
			VisibilityQuery: "WorkflowType='simpleWorkflow'",
			JobId:           uuid.NewString(),
			Reason:          "test refresh workflow tasks with query",
			Identity:        "test-identity",
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("InvalidArgument_NoOperation", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Request without operation should fail
		_, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
			Namespace: s.Namespace().String(),
			JobId:     uuid.NewString(),
			Reason:    "test",
			Executions: []*commonpb.WorkflowExecution{
				{WorkflowId: "test-wf-id", RunId: "test-run-id"},
			},
		})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	})

	t.Run("InvalidArgument_NoNamespace", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Request without namespace should fail
		_, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
			JobId:    uuid.NewString(),
			Reason:   "test",
			Identity: "test-identity",
			Executions: []*commonpb.WorkflowExecution{
				{WorkflowId: "test-wf-id", RunId: "test-run-id"},
			},
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	})

	t.Run("InvalidArgument_NoJobId", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Request without job_id should fail
		_, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
			Namespace: s.Namespace().String(),
			Reason:    "test",
			Identity:  "test-identity",
			Executions: []*commonpb.WorkflowExecution{
				{WorkflowId: "test-wf-id", RunId: "test-run-id"},
			},
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	})

	t.Run("InvalidArgument_NoExecutionsOrQuery", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Request without executions or visibility_query should fail
		_, err := s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
			Namespace: s.Namespace().String(),
			JobId:     uuid.NewString(),
			Reason:    "test",
			Identity:  "test-identity",
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	})

	t.Run("SeparateLimitFromFrontendBatchOperation", func(t *testing.T) {
		s := testcore.NewEnv(t,
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 1),
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, 1),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		w := worker.New(sdkClient, taskQueue, worker.Options{})
		w.RegisterWorkflow(simpleWorkflow)
		require.NoError(t, w.Start())
		defer w.Stop()

		// Helper function to create workflow
		createWorkflow := func() sdkclient.WorkflowRun {
			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:        testcore.RandomizeStr("wf_id-" + t.Name()),
				TaskQueue: taskQueue,
			}
			workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, simpleWorkflow)
			require.NoError(t, err)
			require.NotNil(t, workflowRun)
			return workflowRun
		}

		// Create workflows
		workflowRun1 := createWorkflow()
		workflowRun2 := createWorkflow()

		// Wait for workflows to complete
		var out string
		err = workflowRun1.Get(ctx, &out)
		require.NoError(t, err)
		err = workflowRun2.Get(ctx, &out)
		require.NoError(t, err)

		_, err = s.FrontendClient().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
			Namespace: s.Namespace().String(),
			Executions: []*commonpb.WorkflowExecution{
				{WorkflowId: workflowRun1.GetID(), RunId: workflowRun1.GetRunID()},
			},
			JobId:  uuid.NewString(),
			Reason: "test frontend batch",
			Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
				SignalOperation: &batchpb.BatchOperationSignal{
					Signal:   "test-signal",
					Input:    payloads.EncodeString("test-input"),
					Identity: "test-identity",
				},
			},
		})
		require.NoError(t, err, "frontend batch operation should succeed")

		_, err = s.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
			Namespace: s.Namespace().String(),
			Executions: []*commonpb.WorkflowExecution{
				{WorkflowId: workflowRun2.GetID(), RunId: workflowRun2.GetRunID()},
			},
			JobId:    uuid.NewString(),
			Reason:   "test admin batch",
			Identity: "test-identity",
			Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
				RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
			},
		})
		require.NoError(t, err, "admin batch operation should succeed because it uses a separate limit")
	})
}
