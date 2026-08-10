package xdc

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/worker/adminbatcher"
	"go.temporal.io/server/service/worker/batcher"
	"go.temporal.io/server/tests/testcore"
)

const (
	// The test namespaces are active in cluster 0 and passive in cluster 1.
	activeClusterIdx  = 0
	passiveClusterIdx = 1
)

type adminBatchRefreshTasksSuite struct {
	xdcBaseSuite
}

func TestAdminBatchRefreshTasksSuite(t *testing.T) {
	t.Parallel()
	s := &adminBatchRefreshTasksSuite{}
	suite.Run(t, s)
}

func (s *adminBatchRefreshTasksSuite) SetupSuite() {
	s.logger = log.NewTestLogger()
	s.setupSuite(testcore.WithClusterHistoryTaskRecorder())
}

func (s *adminBatchRefreshTasksSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *adminBatchRefreshTasksSuite) SetupTest() {
	s.setupTest()
}

// run is s.Run with the suite's require.Assertions rebound to the subtest's T, so a
// failure aborts and is reported against the subtest rather than its parent.
func (s *adminBatchRefreshTasksSuite) run(name string, subtest func()) {
	parent := s.Assertions
	s.Run(name, func() {
		s.Assertions = require.New(s.T())
		defer func() { s.Assertions = parent }()
		subtest()
	})
}

type refreshTarget struct {
	ns   string
	nsID string
	exec *commonpb.WorkflowExecution
}

// TestRefreshTasksInSystemNamespace runs admin batch refresh-tasks jobs hosted in
// temporal-system, from the cluster where the target namespace is active and from
// the cluster where it is passive. temporal-system is a local namespace and is
// therefore active in every cluster, so neither the per-namespace worker's
// active-in-cluster gate nor the rejection of workflow starts in a passive global
// namespace applies.
func (s *adminBatchRefreshTasksSuite) TestRefreshTasksInSystemNamespace() {
	ctx := testcore.NewContext()

	ns1 := s.newReplicatedNamespaceWithWorkflow(ctx)
	ns2 := s.newReplicatedNamespaceWithWorkflow(ctx)

	for _, cl := range []struct {
		name string
		idx  int
	}{
		{name: "active", idx: activeClusterIdx},
		{name: "passive", idx: passiveClusterIdx},
	} {
		for _, target := range []refreshTarget{ns1, ns2} {
			s.run(cl.name+"/"+target.ns, func() {
				s.assertNamespaceActiveIn(ctx, target.ns, activeClusterIdx)

				tasksBefore := s.countRefreshedTasks(cl.idx, target)
				jobID := s.runAdminBatch(ctx, cl.idx, target, adminservice.StartAdminBatchOperationRequest_JOB_NAMESPACE_SYSTEM)

				s.assertJobHostedInSystemNamespace(ctx, cl.idx, jobID)
				s.assertJobAbsentFrom(ctx, cl.idx, target.ns, jobID)
				s.Greater(s.countRefreshedTasks(cl.idx, target), tasksBefore,
					"refresh must regenerate tasks for the execution in the user namespace")
			})
		}
	}
}

// TestUserJobNamespaceFallback covers the pre-existing per-namespace-worker path.
// It still works where the namespace is active, and is still rejected where it is
// passive -- which is the limitation the system-namespace host removes.
func (s *adminBatchRefreshTasksSuite) TestUserJobNamespaceFallback() {
	ctx := testcore.NewContext()
	target := s.newReplicatedNamespaceWithWorkflow(ctx)

	s.run("active cluster runs the job in the user namespace", func() {
		tasksBefore := s.countRefreshedTasks(activeClusterIdx, target)
		jobID := s.runAdminBatch(ctx, activeClusterIdx, target, adminservice.StartAdminBatchOperationRequest_JOB_NAMESPACE_USER)

		desc, err := s.clusters[activeClusterIdx].FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: target.ns,
			Execution: &commonpb.WorkflowExecution{WorkflowId: jobID},
		})
		s.NoError(err)
		s.Equal(batcher.BatchWFTypeProtobufName, desc.GetWorkflowExecutionInfo().GetType().GetName())
		s.Equal(primitives.PerNSWorkerTaskQueue, desc.GetWorkflowExecutionInfo().GetTaskQueue())

		s.assertJobAbsentFrom(ctx, activeClusterIdx, primitives.SystemLocalNamespace, jobID)
		s.Greater(s.countRefreshedTasks(activeClusterIdx, target), tasksBefore)
	})

	s.run("passive cluster rejects the job", func() {
		_, err := s.startAdminBatch(ctx, passiveClusterIdx, target,
			adminservice.StartAdminBatchOperationRequest_JOB_NAMESPACE_USER, uuid.NewString())
		s.Error(err, "a job workflow cannot be created in a namespace that is passive in this cluster")
		var notActive *serviceerror.NamespaceNotActive
		s.ErrorAs(err, &notActive)
	})
}

func (s *adminBatchRefreshTasksSuite) TestJobNamespaceIsRequired() {
	ctx := testcore.NewContext()
	target := s.newReplicatedNamespaceWithWorkflow(ctx)

	_, err := s.startAdminBatch(ctx, passiveClusterIdx, target,
		adminservice.StartAdminBatchOperationRequest_JOB_NAMESPACE_UNSPECIFIED, uuid.NewString())
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
}

func (s *adminBatchRefreshTasksSuite) newReplicatedNamespaceWithWorkflow(ctx context.Context) refreshTarget {
	ns := s.createGlobalNamespace()

	wfID := "wf-" + uuid.NewString()
	resp, err := s.clusters[activeClusterIdx].FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:    uuid.NewString(),
		Namespace:    ns,
		WorkflowId:   wfID,
		WorkflowType: &commonpb.WorkflowType{Name: "admin-batch-xdc-test-type"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: "admin-batch-xdc-test-tq", Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:     "admin-batch-xdc-test",
	})
	s.NoError(err)

	target := refreshTarget{
		ns:   ns,
		nsID: s.namespaceID(ctx, ns),
		exec: &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: resp.GetRunId()},
	}

	s.waitForClusterSynced()
	// Read through the admin service: it is never redirected to the active cluster,
	// so this confirms the execution really is present in the passive cluster.
	await.RequireTrue(s.T(), func() bool {
		_, err := s.clusters[passiveClusterIdx].AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: ns,
			Execution: target.exec,
		})
		return err == nil
	}, replicationWaitTime, replicationCheckInterval)

	return target
}

func (s *adminBatchRefreshTasksSuite) namespaceID(ctx context.Context, ns string) string {
	resp, err := s.clusters[activeClusterIdx].FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{
		Namespace: ns,
	})
	s.NoError(err)
	return resp.GetNamespaceInfo().GetId()
}

func (s *adminBatchRefreshTasksSuite) assertNamespaceActiveIn(ctx context.Context, ns string, clusterIdx int) {
	for i, c := range s.clusters {
		resp, err := c.FrontendClient().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: ns})
		s.NoError(err)
		s.Equal(s.clusters[clusterIdx].ClusterName(), resp.GetReplicationConfig().GetActiveClusterName(),
			"cluster %d disagrees about which cluster is active for %s", i, ns)
	}
}

func (s *adminBatchRefreshTasksSuite) startAdminBatch(
	ctx context.Context,
	clusterIdx int,
	target refreshTarget,
	jobNamespace adminservice.StartAdminBatchOperationRequest_JobNamespace,
	jobID string,
) (*adminservice.StartAdminBatchOperationResponse, error) {
	return s.clusters[clusterIdx].AdminClient().StartAdminBatchOperation(ctx, &adminservice.StartAdminBatchOperationRequest{
		Namespace:    target.ns,
		JobId:        jobID,
		Reason:       "refresh tasks lost on this cluster",
		Identity:     "admin-batch-xdc-test",
		JobNamespace: jobNamespace,
		Executions:   []*commonpb.WorkflowExecution{target.exec},
		Operation: &adminservice.StartAdminBatchOperationRequest_RefreshTasksOperation{
			RefreshTasksOperation: &adminservice.BatchOperationRefreshTasks{},
		},
	})
}

// runAdminBatch starts a job, waits for it to finish, and asserts the targeted
// execution was refreshed without error. It returns the job workflow ID.
func (s *adminBatchRefreshTasksSuite) runAdminBatch(
	ctx context.Context,
	clusterIdx int,
	target refreshTarget,
	jobNamespace adminservice.StartAdminBatchOperationRequest_JobNamespace,
) string {
	jobID := "admin-batch-" + uuid.NewString()
	_, err := s.startAdminBatch(ctx, clusterIdx, target, jobNamespace, jobID)
	s.NoError(err)

	jobNS, jobWorkflowID := primitives.SystemLocalNamespace, adminbatcher.JobWorkflowID(target.nsID, jobID)
	if jobNamespace == adminservice.StartAdminBatchOperationRequest_JOB_NAMESPACE_USER {
		jobNS, jobWorkflowID = target.ns, jobID
	}
	client := s.namespaceClient(clusterIdx, jobNS)
	defer client.Close()

	var result batcher.HeartBeatDetails
	s.NoError(client.GetWorkflow(ctx, jobWorkflowID, "").Get(ctx, &result))
	s.Equal(1, result.SuccessCount)
	s.Zero(result.ErrorCount)
	return jobWorkflowID
}

func (s *adminBatchRefreshTasksSuite) namespaceClient(clusterIdx int, ns string) sdkclient.Client {
	client, err := sdkclient.Dial(sdkclient.Options{
		HostPort:  s.clusters[clusterIdx].Host().FrontendGRPCAddress(),
		Namespace: ns,
	})
	s.NoError(err)
	return client
}

// countRefreshedTasks counts the transfer tasks written for the target execution in
// the given cluster. RefreshWorkflowTasks regenerates them, so the count growing is
// direct evidence that the job acted on the user namespace's data.
func (s *adminBatchRefreshTasksSuite) countRefreshedTasks(clusterIdx int, target refreshTarget) int {
	return s.clusters[clusterIdx].GetHistoryTaskRecorder().CountTasksForWorkflow(
		tasks.CategoryTransfer, target.nsID, target.exec.GetWorkflowId(), target.exec.GetRunId(), nil)
}

func (s *adminBatchRefreshTasksSuite) assertJobHostedInSystemNamespace(ctx context.Context, clusterIdx int, jobID string) {
	desc, err := s.clusters[clusterIdx].FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: primitives.SystemLocalNamespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: jobID},
	})
	s.NoError(err)
	info := desc.GetWorkflowExecutionInfo()
	s.Equal(adminbatcher.WorkflowTypeName, info.GetType().GetName())
	s.Equal(primitives.DefaultWorkerTaskQueue, info.GetTaskQueue())
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, info.GetStatus())

	s.Equal(primitives.AdminBatchActivityTQ, s.activityTaskQueue(ctx, clusterIdx, jobID),
		"the batch activity must run on the dedicated admin batch queue, not the default system queue")
}

func (s *adminBatchRefreshTasksSuite) activityTaskQueue(ctx context.Context, clusterIdx int, jobID string) string {
	var scheduled []*historypb.HistoryEvent
	var nextPageToken []byte
	for {
		resp, err := s.clusters[clusterIdx].FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:     primitives.SystemLocalNamespace,
			Execution:     &commonpb.WorkflowExecution{WorkflowId: jobID},
			NextPageToken: nextPageToken,
		})
		s.NoError(err)
		for _, event := range resp.GetHistory().GetEvents() {
			if event.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
				scheduled = append(scheduled, event)
			}
		}
		nextPageToken = resp.GetNextPageToken()
		if len(nextPageToken) == 0 {
			break
		}
	}
	s.Len(scheduled, 1)
	return scheduled[0].GetActivityTaskScheduledEventAttributes().GetTaskQueue().GetName()
}

func (s *adminBatchRefreshTasksSuite) assertJobAbsentFrom(ctx context.Context, clusterIdx int, ns, jobID string) {
	_, err := s.clusters[clusterIdx].FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: ns,
		Execution: &commonpb.WorkflowExecution{WorkflowId: jobID},
	})
	var notFound *serviceerror.NotFound
	s.ErrorAs(err, &notFound, "the job must not exist in namespace %s", ns)
}
