package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

type AdminBatchRefreshWorkflowTasksTestSuite struct {
	testcore.FunctionalTestBase
}

func TestAdminBatchRefreshWorkflowTasksTestSuite(t *testing.T) {
	s := new(AdminBatchRefreshWorkflowTasksTestSuite)
	suite.Run(t, s)
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) SetupSuite() {
	// Use a higher limit for general tests to avoid interference from batch operations
	// that haven't completed yet. The isolation test (A_SeparateLimitFromFrontendBatchOperation)
	// explicitly sets limit to 1 to verify frontend and admin batch ops use separate limits.
	s.SetupSuiteWithCluster(testcore.WithDynamicConfigOverrides(map[dynamicconfig.Key]any{
		dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace.Key(): 10,
	}))
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) simpleWorkflow(ctx workflow.Context) (string, error) {
	// Simple workflow that just returns
	return "done", nil
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) createWorkflow(ctx context.Context, workflowFn interface{}) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)
	return workflowRun
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_RefreshWorkflowTasks_Success() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(s.simpleWorkflow)

	// Create two workflows
	workflowRun1 := s.createWorkflow(ctx, s.simpleWorkflow)
	workflowRun2 := s.createWorkflow(ctx, s.simpleWorkflow)

	// Wait for workflows to complete
	var out string
	err := workflowRun1.Get(ctx, &out)
	s.NoError(err)
	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)

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
	s.NoError(err)
	s.NotNil(resp)
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_RefreshWorkflowTasks_WithVisibilityQuery() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(s.simpleWorkflow)

	// Create workflows
	workflowRun1 := s.createWorkflow(ctx, s.simpleWorkflow)
	workflowRun2 := s.createWorkflow(ctx, s.simpleWorkflow)

	// Wait for workflows to complete
	var out string
	err := workflowRun1.Get(ctx, &out)
	s.NoError(err)
	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)

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
	s.NoError(err)
	s.NotNil(resp)
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_InvalidArgument_NoOperation() {
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
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_InvalidArgument_NoNamespace() {
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
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_InvalidArgument_NoJobId() {
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
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_InvalidArgument_NoExecutionsOrQuery() {
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
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_0_SeparateLimitFromFrontendBatchOperation() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(s.simpleWorkflow)

	s.OverrideDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, 1)
	s.OverrideDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 1)

	// Create workflows
	workflowRun1 := s.createWorkflow(ctx, s.simpleWorkflow)
	workflowRun2 := s.createWorkflow(ctx, s.simpleWorkflow)

	// Wait for workflows to complete
	var out string
	err := workflowRun1.Get(ctx, &out)
	s.NoError(err)
	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)

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
	s.NoError(err, "frontend batch operation should succeed")

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
	s.NoError(err, "admin batch operation should succeed because it uses a separate limit")
}
