package tests

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/grpc/codes"
)

type AdminBatchRefreshWorkflowTasksTestSuite struct {
	parallelsuite.Suite[*AdminBatchRefreshWorkflowTasksTestSuite]
}

func TestAdminBatchRefreshWorkflowTasksTestSuite(t *testing.T) {
	parallelsuite.Run(t, &AdminBatchRefreshWorkflowTasksTestSuite{})
}

// newTestEnv creates a TestEnv with the dynamic config this suite needs.
// Additional per-test options may be passed in opts.
func (s *AdminBatchRefreshWorkflowTasksTestSuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	// Use a higher limit for general tests to avoid interference from batch operations
	// that haven't completed yet. The isolation test (A_SeparateLimitFromFrontendBatchOperation)
	// explicitly sets limit to 1 to verify frontend and admin batch ops use separate limits.
	baseOpts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) simpleWorkflow(ctx workflow.Context) (string, error) {
	// Simple workflow that just returns
	return "done", nil
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) runTdbg(env *testcore.TestEnv, args ...string) error {
	var out bytes.Buffer
	return tdbgtest.NewCliApp(
		func(params *tdbg.Params) {
			params.ClientFactory = tdbg.NewClientFactory(tdbg.WithFrontendAddress(env.FrontendGRPCAddress()))
			params.Writer = &out
			params.ErrWriter = &out
		},
	).RunContext(s.Context(), append([]string{"tdbg"}, args...))
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) createWorkflow(env *testcore.TestEnv, workflowFn any) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)
	return workflowRun
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_RefreshWorkflowTasks_Success() {
	env := s.newTestEnv()

	env.SdkWorker().RegisterWorkflow(s.simpleWorkflow)

	// Create two workflows
	workflowRun1 := s.createWorkflow(env, s.simpleWorkflow)
	workflowRun2 := s.createWorkflow(env, s.simpleWorkflow)

	// Wait for workflows to complete
	var out string
	err := workflowRun1.Get(s.Context(), &out)
	s.NoError(err)
	err = workflowRun2.Get(s.Context(), &out)
	s.NoError(err)

	// Start admin batch operation to refresh workflow tasks using executions list
	resp, err := env.AdminClient().StartAdminBatchOperation(s.Context(), &adminservice.StartAdminBatchOperationRequest{
		Namespace: env.Namespace().String(),
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

// The job's execution is covered by the xdc suite; this test only covers tdbg starting it.
func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestTdbgRefreshTasks_StartsBatchJobInSystemNamespace() {
	env := s.newTestEnv()

	env.SdkWorker().RegisterWorkflow(s.simpleWorkflow)

	// Create workflows
	workflowRun1 := s.createWorkflow(env, s.simpleWorkflow)
	workflowRun2 := s.createWorkflow(env, s.simpleWorkflow)

	// Wait for workflows to complete
	var out string
	err := workflowRun1.Get(s.Context(), &out)
	s.NoError(err)
	err = workflowRun2.Get(s.Context(), &out)
	s.NoError(err)

	ns := env.Namespace().String()
	query := "WorkflowType='simpleWorkflow'"

	// Wait for workflows to be visible
	s.Await(func(s *AdminBatchRefreshWorkflowTasksTestSuite) {
		resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: ns,
			Query:     query,
		})
		s.NoError(err)
		s.GreaterOrEqual(resp.GetCount(), int64(2))
	}, 10*time.Second, 500*time.Millisecond)

	jobID := uuid.NewString()
	s.NoError(s.runTdbg(env,
		"--"+tdbg.FlagYes,
		"--"+tdbg.FlagNamespace, ns,
		"execution", "refresh-tasks",
		"--"+tdbg.FlagVisibilityQuery, query,
		"--"+tdbg.FlagReason, "test refresh workflow tasks with query",
		"--"+tdbg.FlagJobID, jobID,
	))

	// tdbg qualifies the job ID with the namespace: the batch workflow runs in the system namespace,
	// where job IDs from every namespace share one workflow ID space.
	batchWorkflowIDPrefix := ns + ":"
	batchWorkflowID := batchWorkflowIDPrefix + jobID
	resp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: primitives.SystemLocalNamespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: batchWorkflowID},
	})
	s.NoError(err)
	s.Equal(batchWorkflowID, resp.GetWorkflowExecutionInfo().GetExecution().GetWorkflowId())
	s.Equal(primitives.PerNSWorkerTaskQueue, resp.GetExecutionConfig().GetTaskQueue().GetName())

	_, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: batchWorkflowID},
	})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)

	s.Await(func(s *AdminBatchRefreshWorkflowTasksTestSuite) {
		resp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: primitives.SystemLocalNamespace,
			Query: fmt.Sprintf("%s = '%s' AND WorkflowId STARTS_WITH '%s'",
				sadefs.TemporalNamespaceDivision,
				batcher.AdminNamespaceDivision,
				batchWorkflowIDPrefix,
			),
		})
		s.NoError(err)
		var workflowIDs []string
		for _, execution := range resp.GetExecutions() {
			workflowIDs = append(workflowIDs, execution.GetExecution().GetWorkflowId())
		}
		s.Contains(workflowIDs, batchWorkflowID)
	}, 10*time.Second, 500*time.Millisecond)
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestStartAdminBatchOperation_InvalidArgument_NoOperation() {
	env := s.newTestEnv()

	// Request without operation should fail
	_, err := env.AdminClient().StartAdminBatchOperation(s.Context(), &adminservice.StartAdminBatchOperationRequest{
		Namespace: env.Namespace().String(),
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
	env := s.newTestEnv()

	// Request without namespace should fail
	_, err := env.AdminClient().StartAdminBatchOperation(s.Context(), &adminservice.StartAdminBatchOperationRequest{
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
	env := s.newTestEnv()

	// Request without job_id should fail
	_, err := env.AdminClient().StartAdminBatchOperation(s.Context(), &adminservice.StartAdminBatchOperationRequest{
		Namespace: env.Namespace().String(),
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
	env := s.newTestEnv()

	// Request without executions or visibility_query should fail
	_, err := env.AdminClient().StartAdminBatchOperation(s.Context(), &adminservice.StartAdminBatchOperationRequest{
		Namespace: env.Namespace().String(),
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
	env := s.newTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, 1),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 1),
	)

	env.SdkWorker().RegisterWorkflow(s.simpleWorkflow)

	// Create workflows
	workflowRun1 := s.createWorkflow(env, s.simpleWorkflow)
	workflowRun2 := s.createWorkflow(env, s.simpleWorkflow)

	// Wait for workflows to complete
	var out string
	err := workflowRun1.Get(s.Context(), &out)
	s.NoError(err)
	err = workflowRun2.Get(s.Context(), &out)
	s.NoError(err)

	_, err = env.FrontendClient().StartBatchOperation(s.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
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

	_, err = env.AdminClient().StartAdminBatchOperation(s.Context(), &adminservice.StartAdminBatchOperationRequest{
		Namespace: env.Namespace().String(),
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
