package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/service/worker/adminbatcher"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/tests/testcore"
)

// AdminBatchSystemNamespaceTestSuite covers admin batch jobs hosted in
// temporal-system rather than in the target user namespace.
type AdminBatchSystemNamespaceTestSuite struct {
	parallelsuite.Suite[*AdminBatchSystemNamespaceTestSuite]
}

func TestAdminBatchSystemNamespaceTestSuite(t *testing.T) {
	parallelsuite.Run(t, &AdminBatchSystemNamespaceTestSuite{})
}

func (s *AdminBatchSystemNamespaceTestSuite) idleWorkflow(ctx workflow.Context) error {
	return workflow.Await(ctx, func() bool { return false })
}

// startWorkflows starts n workflows that never complete, so that a refresh always
// has live executions to regenerate tasks for.
func (s *AdminBatchSystemNamespaceTestSuite) startWorkflows(env *testcore.TestEnv, n int) []*commonpb.WorkflowExecution {
	env.SdkWorker().RegisterWorkflow(s.idleWorkflow)
	executions := make([]*commonpb.WorkflowExecution, 0, n)
	for range n {
		run, err := env.SdkClient().ExecuteWorkflow(s.Context(), sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
			TaskQueue: env.WorkerTaskQueue(),
		}, s.idleWorkflow)
		s.NoError(err)
		executions = append(executions, &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()})
	}
	return executions
}

func (s *AdminBatchSystemNamespaceTestSuite) startSystemJob(
	env *testcore.TestEnv,
	jobID string,
	executions []*commonpb.WorkflowExecution,
) error {
	return s.startNamespaceSystemJob(env, env.Namespace().String(), jobID, executions...)
}

func (s *AdminBatchSystemNamespaceTestSuite) startNamespaceSystemJob(
	env *testcore.TestEnv,
	ns string,
	jobID string,
	executions ...*commonpb.WorkflowExecution,
) error {
	resp, err := env.AdminClient().StartAdminBatchOperation(s.Context(), &adminservice.StartAdminBatchOperationRequest{
		Namespace:    ns,
		JobId:        jobID,
		Reason:       "admin batch system namespace test",
		Identity:     "test-identity",
		JobNamespace: adminservice.StartAdminBatchOperationRequest_JOB_NAMESPACE_SYSTEM,
		Executions:   executions,
		Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
			RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
		},
	})
	if err == nil {
		// Callers cannot reconstruct the control workflow's ID, so the response must
		// report it.
		s.Equal(primitives.SystemLocalNamespace, resp.GetJobNamespace())
		s.True(strings.HasSuffix(resp.GetJobWorkflowId(), ":"+jobID), "got %q", resp.GetJobWorkflowId())
	}
	return err
}

func (s *AdminBatchSystemNamespaceTestSuite) jobWorkflowID(env *testcore.TestEnv, jobID string) string {
	return adminbatcher.JobWorkflowID(env.NamespaceID().String(), jobID)
}

func (s *AdminBatchSystemNamespaceTestSuite) awaitJobResult(env *testcore.TestEnv, jobID string) batcher.HeartBeatDetails {
	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  env.FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	defer client.Close()

	var result batcher.HeartBeatDetails
	s.NoError(client.GetWorkflow(s.Context(), s.jobWorkflowID(env, jobID), "").Get(s.Context(), &result))
	return result
}

// TestRateLimiterAppliesToSystemNamespaceJobs pins the admin batcher's RPS well
// below the number of targets and checks the job takes at least as long as that
// limit allows. The limiter is a host-level limit shared by every admin batch job,
// and moving the job into temporal-system must not bypass it.
func (s *AdminBatchSystemNamespaceTestSuite) TestRateLimiterAppliesToSystemNamespaceJobs() {
	const rps = 2
	const numWorkflows = 8

	env := s.newSystemTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.AdminBatcherHostRPS, rps),
		testcore.WithDynamicConfig(dynamicconfig.AdminBatcherGlobalRPS, 0),
	)
	executions := s.startWorkflows(env, numWorkflows)

	jobID := uuid.NewString()
	start := time.Now()
	s.NoError(s.startSystemJob(env, jobID, executions))

	result := s.awaitJobResult(env, jobID)
	elapsed := time.Since(start)

	s.Equal(numWorkflows, result.SuccessCount)
	s.Zero(result.ErrorCount)

	// The limiter's burst equals its rps, so the first rps refreshes go through
	// immediately and the rest are paced. Unthrottled, this job takes ~50ms.
	minDuration := time.Duration(numWorkflows-rps) * time.Second / rps
	s.GreaterOrEqual(elapsed, minDuration,
		"job finished in %v, faster than %d refreshes at %d rps allows", elapsed, numWorkflows, rps)
}

// TestConcurrencyCapIsScopedToTheUserNamespace checks that the open-job cap counts
// only the jobs of the namespace being requested. Every namespace's jobs live in
// the one temporal-system namespace, so an unscoped count would let one namespace's
// jobs exhaust another's quota.
func (s *AdminBatchSystemNamespaceTestSuite) TestConcurrencyCapIsScopedToTheUserNamespace() {
	env := s.newSystemTestEnv(
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 1),
		testcore.WithDynamicConfig(dynamicconfig.AdminBatcherHostRPS, 1),
		testcore.WithDynamicConfig(dynamicconfig.AdminBatcherGlobalRPS, 0),
	)
	executions := s.startWorkflows(env, 20)

	firstJobID := uuid.NewString()
	s.NoError(s.startSystemJob(env, firstJobID, executions))
	s.awaitOpenJobIndexed(env, env.NamespaceID().String())

	err := s.startSystemJob(env, uuid.NewString(), executions[:1])
	var resourceExhausted *serviceerror.ResourceExhausted
	s.ErrorAs(err, &resourceExhausted, "a second concurrent job for the same namespace must be rejected")

	// A second namespace on the same cluster has its own quota, even though both
	// namespaces' jobs are hosted in the one temporal-system namespace.
	otherNS, otherExecution := s.registerNamespaceWithWorkflow(env)
	s.NoError(s.startNamespaceSystemJob(env, otherNS, uuid.NewString(), otherExecution),
		"another namespace's open job must not consume this namespace's quota")
}

// registerNamespaceWithWorkflow adds a second namespace to the same cluster and
// starts one execution in it. No worker is needed: refreshing tasks does not
// require the workflow to make progress.
func (s *AdminBatchSystemNamespaceTestSuite) registerNamespaceWithWorkflow(env *testcore.TestEnv) (string, *commonpb.WorkflowExecution) {
	nsName := namespace.Name(testcore.RandomizeStr("other-ns"))
	_, err := env.RegisterNamespace(nsName, 1, enumspb.ARCHIVAL_STATE_DISABLED, "", "")
	s.NoError(err)

	wfID := testcore.RandomizeStr("other-wf")
	resp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    nsName.String(),
		WorkflowId:   wfID,
		WorkflowType: &commonpb.WorkflowType{Name: "idleWorkflow"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "other-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:     "test-identity",
	})
	s.NoError(err)
	return nsName.String(), &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: resp.GetRunId()}
}

// awaitOpenJobIndexed waits until the running job is visible to the count query the
// frontend uses to enforce the cap.
func (s *AdminBatchSystemNamespaceTestSuite) awaitOpenJobIndexed(env *testcore.TestEnv, nsID string) {
	query := batcher.OpenAdminBatchOperationQuery +
		" AND " + sadefs.WorkflowID + " STARTS_WITH '" + adminbatcher.JobWorkflowIDPrefix(nsID) + "'"
	s.AwaitTruef(func() bool {
		resp, err := env.FrontendClient().CountWorkflowExecutions(s.Context(), &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: primitives.SystemLocalNamespace,
			Query:     query,
		})
		return err == nil && resp.GetCount() >= 1
	}, 20*time.Second, 200*time.Millisecond, "the open job should be indexed in temporal-system")
}

// TestSystemNamespaceJobDoesNotDisturbUserBatchOperations runs an ordinary
// user-facing batch operation alongside a system-hosted admin job. The two use
// different task queues, workflow types and quotas, so both must complete.
func (s *AdminBatchSystemNamespaceTestSuite) TestSystemNamespaceJobDoesNotDisturbUserBatchOperations() {
	env := s.newSystemTestEnv()
	executions := s.startWorkflows(env, 2)

	adminJobID := uuid.NewString()
	s.NoError(s.startSystemJob(env, adminJobID, executions))

	userJobID := uuid.NewString()
	_, err := env.FrontendClient().StartBatchOperation(s.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace:  env.Namespace().String(),
		JobId:      userJobID,
		Reason:     "user batch alongside an admin batch",
		Executions: executions,
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal:   "test-signal",
				Input:    payloads.EncodeString("test-input"),
				Identity: "test-identity",
			},
		},
	})
	s.NoError(err)

	result := s.awaitJobResult(env, adminJobID)
	s.Equal(len(executions), result.SuccessCount)

	// The user batch runs on the per-namespace worker in the user namespace, and is
	// unaffected by the admin job living in temporal-system.
	s.AwaitTruef(func() bool {
		resp, err := env.FrontendClient().DescribeBatchOperation(s.Context(), &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     userJobID,
		})
		return err == nil && resp.GetState() == enumspb.BATCH_OPERATION_STATE_COMPLETED
	}, 30*time.Second, 250*time.Millisecond, "the user batch operation should still complete")
}

// TestJobIsIsolatedFromTheUserNamespace checks the job workflow only ever exists in
// temporal-system, under a workflow ID scoped by the user namespace it acts on.
func (s *AdminBatchSystemNamespaceTestSuite) TestJobIsIsolatedFromTheUserNamespace() {
	env := s.newSystemTestEnv()
	executions := s.startWorkflows(env, 1)

	jobID := uuid.NewString()
	s.NoError(s.startSystemJob(env, jobID, executions))
	s.awaitJobResult(env, jobID)

	jobWorkflowID := s.jobWorkflowID(env, jobID)
	desc, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: primitives.SystemLocalNamespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: jobWorkflowID},
	})
	s.NoError(err)
	s.Equal(adminbatcher.WorkflowTypeName, desc.GetWorkflowExecutionInfo().GetType().GetName())
	s.Equal(primitives.DefaultWorkerTaskQueue, desc.GetWorkflowExecutionInfo().GetTaskQueue())

	for _, wfID := range []string{jobID, jobWorkflowID} {
		_, err = env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: wfID},
		})
		var notFound *serviceerror.NotFound
		s.ErrorAs(err, &notFound, "the job must not exist in the user namespace under id %q", wfID)
	}
}

func (s *AdminBatchSystemNamespaceTestSuite) newSystemTestEnv(opts ...testcore.TestOption) *testcore.TestEnv {
	base := []testcore.TestOption{
		// These tests assert on what the job actually does, so the system worker
		// that hosts it must be running. A dedicated cluster also keeps the
		// temporal-system job count free of other suites' jobs.
		testcore.WithWorkerService("admin batch jobs are hosted on the system worker"),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentAdminBatchOperationPerNamespace, 10),
	}
	return testcore.NewEnv(s.T(), append(base, opts...)...)
}
