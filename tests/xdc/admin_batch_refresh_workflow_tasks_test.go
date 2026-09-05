package xdc

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type AdminBatchRefreshWorkflowTasksTestSuite struct {
	xdcBaseSuite
}

func TestAdminBatchRefreshWorkflowTasksTestSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &AdminBatchRefreshWorkflowTasksTestSuite{})
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) SetupSuite() {
	if s.dynamicConfigOverrides == nil {
		s.dynamicConfigOverrides = make(map[dynamicconfig.Key]any)
	}
	s.dynamicConfigOverrides[dynamicconfig.FrontendMaxConcurrentAdminBatchOperation.Key()] = 10
	s.setupSuite()
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) SetupTest() {
	s.setupTest()
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) TearDownSuite() {
	s.tearDownSuite()
}

// TestRefreshWorkflowTasks_ActiveAndPassiveCluster runs an admin batch refresh-tasks job in both
// the active and the passive cluster of a global namespace. The passive cluster is the case the
// feature exists for: it has no per-namespace worker for a namespace that is not active there, and
// api.GetActiveNamespace would reject starting the batch workflow in that namespace.
func (s *AdminBatchRefreshWorkflowTasksTestSuite) TestRefreshWorkflowTasks_ActiveAndPassiveCluster() {
	ctx := testcore.NewContext()
	ns := s.createGlobalNamespace()

	// The premise of this test: cluster 0 is active for ns, cluster 1 is passive. DescribeNamespace
	// is a local API, so each cluster reports its own view.
	for _, cluster := range s.clusters {
		resp, err := cluster.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
		s.NoError(err)
		s.Equal(s.clusters[0].ClusterName(), resp.GetReplicationConfig().GetActiveClusterName(),
			"%s should see %s as active for %s", cluster.ClusterName(), s.clusters[0].ClusterName(), ns)
	}

	taskQueue := "admin-batch-refresh-tq"
	workflowTypeName := "admin-batch-refresh-wf-type-" + uuid.NewString()
	visibilityQuery := fmt.Sprintf("WorkflowType = '%s'", workflowTypeName)

	// Two workflows with no worker on the task queue, so each keeps a pending workflow task.
	executions := make([]*commonpb.WorkflowExecution, 0, 2)
	for i := range 2 {
		workflowID := fmt.Sprintf("admin-batch-refresh-%d-%s", i, uuid.NewString())
		resp, err := s.clusters[0].FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			RequestId:          uuid.NewString(),
			Namespace:          ns,
			WorkflowId:         workflowID,
			WorkflowType:       &commonpb.WorkflowType{Name: workflowTypeName},
			TaskQueue:          &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:           "test",
			WorkflowRunTimeout: durationpb.New(time.Hour),
		})
		s.NoError(err)
		executions = append(executions, &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: resp.GetRunId()})
	}

	s.waitForVisibility(ctx, s.clusters[0], ns, visibilityQuery, 2, 20*time.Second, 500*time.Millisecond)

	// The passive cluster must hold the executions locally: RefreshWorkflowTasks there resolves the
	// shard from the local namespace ID, so DescribeMutableState is the check that matters. It goes
	// through the admin service, which is not subject to DC redirection.
	for _, execution := range executions {
		await.RequireTruef(s.T(), func() bool {
			_, err := s.clusters[1].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
				Namespace: ns,
				Execution: execution,
			})
			return err == nil
		}, replicationWaitTime, replicationCheckInterval, "execution %s should replicate to the passive cluster", execution.GetWorkflowId())
	}
	s.waitForVisibility(ctx, s.clusters[1], ns, visibilityQuery, 2, replicationWaitTime, replicationCheckInterval)

	for _, cluster := range s.clusters {
		s.refreshTasksInCluster(ctx, cluster, ns, visibilityQuery)
	}

	// Both executions still have a dispatchable workflow task after being refreshed in both clusters.
	sdkClient, worker := s.newClientAndWorker(s.clusters[0].Host().FrontendGRPCAddress(), ns, taskQueue, "worker0")
	defer sdkClient.Close()
	worker.RegisterWorkflowWithOptions(
		func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: workflowTypeName},
	)
	s.NoError(worker.Start())
	defer worker.Stop()

	for _, execution := range executions {
		s.NoError(sdkClient.GetWorkflow(ctx, execution.GetWorkflowId(), execution.GetRunId()).Get(ctx, nil))
	}
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) refreshTasksInCluster(
	ctx context.Context,
	cluster *testcore.TestCluster,
	ns string,
	visibilityQuery string,
) {
	clusterName := cluster.ClusterName()
	jobID := "refresh-" + clusterName + "-" + uuid.NewString()

	_, err := cluster.AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace:       ns,
		VisibilityQuery: visibilityQuery,
		JobId:           jobID,
		Reason:          "xdc admin batch refresh tasks",
		Identity:        "test",
		Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
			RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
		},
	})
	s.NoError(err, "StartAdminBatchOperation should be accepted in %s", clusterName)

	// StartAdminBatchOperation uses the job ID as the workflow ID.
	batchWorkflowID := jobID

	// The batch workflow lives in the system namespace, not in the namespace it operates on, and its
	// tasks go to the system namespace's per-namespace worker task queue.
	var describeResp *workflowservice.DescribeWorkflowExecutionResponse
	await.Requiref(ctx, s.T(), func(t *await.T) {
		resp, err := cluster.FrontendClient().DescribeWorkflowExecution(t.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: primitives.SystemLocalNamespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: batchWorkflowID},
		})
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, resp.GetWorkflowExecutionInfo().GetStatus())
		describeResp = resp
	}, 60*time.Second, time.Second, "%s: batch workflow %s should complete in %s", clusterName, batchWorkflowID, primitives.SystemLocalNamespace)

	s.Equal(primitives.PerNSWorkerTaskQueue, describeResp.GetExecutionConfig().GetTaskQueue().GetName(),
		"%s: batch workflow must run on the %s task queue", clusterName, primitives.PerNSWorkerTaskQueue)

	// Only the system namespace's per-namespace worker polls that queue for this workflow type.
	taskQueueResp, err := cluster.FrontendClient().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     primitives.SystemLocalNamespace,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	s.NoError(err)
	// Identity is "temporal-system@<host>@<namespace>", set by the per-namespace worker manager.
	hasSystemNamespacePoller := slices.ContainsFunc(taskQueueResp.GetPollers(), func(poller *taskqueuepb.PollerInfo) bool {
		return strings.HasSuffix(poller.GetIdentity(), "@"+primitives.SystemLocalNamespace)
	})
	s.True(hasSystemNamespacePoller,
		"%s: the %s per-namespace worker must poll %s, got pollers %v",
		clusterName, primitives.SystemLocalNamespace, primitives.PerNSWorkerTaskQueue, taskQueueResp.GetPollers())

	_, err = cluster.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: batchWorkflowID},
	})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound, "%s: batch workflow must not exist in %s", clusterName, ns)

	// The job worked on the target namespace: both of its executions were counted and refreshed.
	// Had it read visibility in the system namespace, the query would have matched nothing.
	systemClient, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  cluster.Host().FrontendGRPCAddress(),
		Namespace: primitives.SystemLocalNamespace,
	})
	s.NoError(err)
	defer systemClient.Close()

	var hbd batcher.HeartBeatDetails
	s.NoError(systemClient.GetWorkflow(ctx, batchWorkflowID, "").Get(ctx, &hbd))
	s.Equal(int64(2), hbd.TotalEstimate, "%s: estimate must come from %s", clusterName, ns)
	s.Equal(2, hbd.SuccessCount, "%s: both executions must be refreshed", clusterName)
	s.Equal(0, hbd.ErrorCount, "%s: no execution should fail to refresh", clusterName)
}

func (s *AdminBatchRefreshWorkflowTasksTestSuite) waitForVisibility(
	ctx context.Context,
	cluster *testcore.TestCluster,
	ns string,
	query string,
	expected int64,
	timeout time.Duration,
	interval time.Duration,
) {
	await.RequireTruef(s.T(), func() bool {
		resp, err := cluster.FrontendClient().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: ns,
			Query:     query,
		})
		return err == nil && resp.GetCount() == expected
	}, timeout, interval, "%s should index %d executions for query %s", cluster.ClusterName(), expected, query)
}
