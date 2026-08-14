package tests

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/tests/testcore"
	"go.temporal.io/server/tools/tdbg"
	"go.temporal.io/server/tools/tdbg/tdbgtest"
	"google.golang.org/protobuf/types/known/durationpb"
)

type AdminBatchDelegationTestSuite struct {
	parallelsuite.Suite[*AdminBatchDelegationTestSuite]
}

func TestAdminBatchDelegationTestSuite(t *testing.T) {
	parallelsuite.Run(t, &AdminBatchDelegationTestSuite{})
}

func (s *AdminBatchDelegationTestSuite) newTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	baseOpts := []testcore.TestOption{
		// The batch workflow runs on the system namespace's per-namespace worker.
		testcore.WithWorkerService("batch operations"),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *AdminBatchDelegationTestSuite) runTdbg(env *testcore.TestEnv, args ...string) error {
	var out bytes.Buffer
	return tdbgtest.NewCliApp(
		func(params *tdbg.Params) {
			params.ClientFactory = tdbg.NewClientFactory(tdbg.WithFrontendAddress(env.FrontendGRPCAddress()))
			params.Writer = &out
			params.ErrWriter = &out
		},
	).RunContext(s.Context(), append([]string{"tdbg"}, args...))
}

// startUnworkedWorkflows starts workflows on a task queue no worker polls, so they stay running
// until the batch operation acts on them.
func (s *AdminBatchDelegationTestSuite) startUnworkedWorkflows(
	env *testcore.TestEnv,
	namespace string,
	workflowTypeName string,
	count int,
) []*commonpb.WorkflowExecution {
	executions := make([]*commonpb.WorkflowExecution, 0, count)
	for i := range count {
		workflowID := fmt.Sprintf("admin-batch-delegation-%d-%s", i, uuid.NewString())
		resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          namespace,
			WorkflowId:         workflowID,
			WorkflowType:       &commonpb.WorkflowType{Name: workflowTypeName},
			TaskQueue:          &taskqueuepb.TaskQueue{Name: "admin-batch-delegation-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:           "test",
			WorkflowRunTimeout: durationpb.New(time.Hour),
		})
		s.NoError(err)
		executions = append(executions, &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: resp.GetRunId()})
	}
	return executions
}

func (s *AdminBatchDelegationTestSuite) awaitVisibilityCount(
	env *testcore.TestEnv,
	namespace string,
	query string,
	expected int64,
) {
	s.Await(func(s *AdminBatchDelegationTestSuite) {
		resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: namespace,
			Query:     query,
		})
		s.NoError(err)
		s.Equal(expected, resp.GetCount())
	}, 20*time.Second, 500*time.Millisecond)
}

// TestTdbgBatchTerminate_RunsInSystemNamespaceAgainstTargetNamespace is the case the feature
// exists for: the batch workflow runs on the system namespace's per-namespace worker, and its
// per-execution calls target another namespace.
func (s *AdminBatchDelegationTestSuite) TestTdbgBatchTerminate_RunsInSystemNamespaceAgainstTargetNamespace() {
	env := s.newTestEnv()

	ns := env.Namespace().String()
	workflowTypeName := "admin-batch-delegation-wf-" + uuid.NewString()
	query := fmt.Sprintf("WorkflowType = '%s'", workflowTypeName)

	executions := s.startUnworkedWorkflows(env, ns, workflowTypeName, 2)
	nonMatchingWorkflowType := workflowTypeName + "-non-matching-batch-query"
	nonMatchingExecution := s.startUnworkedWorkflows(env, ns, nonMatchingWorkflowType, 1)[0]
	systemExecution := s.startUnworkedWorkflows(env, primitives.SystemLocalNamespace, workflowTypeName, 1)[0]
	s.awaitVisibilityCount(env, ns, query, 2)
	s.awaitVisibilityCount(env, ns, fmt.Sprintf("WorkflowType = '%s'", nonMatchingWorkflowType), 1)
	s.awaitVisibilityCount(env, primitives.SystemLocalNamespace, query, 1)

	jobID := uuid.NewString()
	s.NoError(s.runTdbg(env,
		"--"+tdbg.FlagYes,
		"--"+tdbg.FlagNamespace, ns,
		"delegated-batch", "start",
		"--"+tdbg.FlagBatchType, "terminate-workflows",
		"--"+tdbg.FlagVisibilityQuery, query,
		"--"+tdbg.FlagReason, "test batch terminate from the system namespace",
		"--"+tdbg.FlagJobID, jobID,
	))

	// tdbg qualifies the job ID with the namespace: the batch workflow runs in the system namespace,
	// where job IDs from every namespace share one workflow ID space.
	batchWorkflowID := jobID + ":" + ns

	s.Await(func(s *AdminBatchDelegationTestSuite) {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: primitives.SystemLocalNamespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: batchWorkflowID},
		})
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, resp.GetWorkflowExecutionInfo().GetStatus())
		s.Equal(primitives.PerNSWorkerTaskQueue, resp.GetExecutionConfig().GetTaskQueue().GetName())
	}, 60*time.Second, time.Second)

	_, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: batchWorkflowID},
	})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound)

	// The operation reached the target namespace.
	for _, execution := range executions {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: ns,
			Execution: execution,
		})
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, resp.GetWorkflowExecutionInfo().GetStatus())
	}

	// Query selection is scoped to the target namespace and does not affect non-matching workflows.
	for namespace, execution := range map[string]*commonpb.WorkflowExecution{
		ns:                              nonMatchingExecution,
		primitives.SystemLocalNamespace: systemExecution,
	} {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: execution,
		})
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, resp.GetWorkflowExecutionInfo().GetStatus())
	}

	// The estimate and counts come from the target namespace's visibility, not the system namespace's.
	systemClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  env.FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	if err == nil {
		defer systemClient.Close()

		var hbd batcher.HeartBeatDetails
		s.NoError(systemClient.GetWorkflow(s.Context(), batchWorkflowID, "").Get(s.Context(), &hbd))
		s.Equal(int64(2), hbd.TotalEstimate)
		s.Equal(2, hbd.SuccessCount)
		s.Equal(0, hbd.ErrorCount)
	}
}

func (s *AdminBatchDelegationTestSuite) TestTdbgBatchTerminate_FansOutAcrossNamespaces() {
	env := s.newTestEnv()

	namespaces := []string{env.Namespace().String(), env.ExternalNamespace().String()}
	workflowTypeName := "admin-batch-delegation-wf-" + uuid.NewString()
	query := fmt.Sprintf("WorkflowType = '%s'", workflowTypeName)
	executions := make(map[string]*commonpb.WorkflowExecution, len(namespaces))
	for _, namespace := range namespaces {
		executions[namespace] = s.startUnworkedWorkflows(env, namespace, workflowTypeName, 1)[0]
		s.awaitVisibilityCount(env, namespace, query, 1)
	}

	jobID := uuid.NewString()
	s.Require().NoError(s.runTdbg(env,
		"--"+tdbg.FlagYes,
		"delegated-batch", "start",
		"--"+tdbg.FlagNamespaces, namespaces[0],
		"--"+tdbg.FlagNamespaces, namespaces[1],
		"--"+tdbg.FlagBatchType, "terminate-workflows",
		"--"+tdbg.FlagVisibilityQuery, query,
		"--"+tdbg.FlagReason, "test batch fanout across namespaces",
		"--"+tdbg.FlagJobID, jobID,
	))

	for namespace, execution := range executions {
		batchWorkflowID := jobID + ":" + namespace
		s.Await(func(s *AdminBatchDelegationTestSuite) {
			resp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: primitives.SystemLocalNamespace,
				Execution: &commonpb.WorkflowExecution{WorkflowId: batchWorkflowID},
			})
			s.Require().NoError(err)
			s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, resp.GetWorkflowExecutionInfo().GetStatus())
		}, 60*time.Second, time.Second)

		resp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: namespace,
			Execution: execution,
		})
		s.Require().NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, resp.GetWorkflowExecutionInfo().GetStatus())
	}

	systemClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  env.FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.Require().NoError(err)
	defer systemClient.Close()

	for _, namespace := range namespaces {
		var hbd batcher.HeartBeatDetails
		s.Require().NoError(systemClient.GetWorkflow(s.Context(), jobID+":"+namespace, "").Get(s.Context(), &hbd))
		s.Equal(int64(1), hbd.TotalEstimate)
		s.Equal(1, hbd.SuccessCount)
		s.Equal(0, hbd.ErrorCount)
	}
}
