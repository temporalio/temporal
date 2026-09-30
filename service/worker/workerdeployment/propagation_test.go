package workerdeployment

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"google.golang.org/protobuf/proto"
)

func TestAsyncPropagationRoutingConfigTargets(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"legacy", "routing", "target only"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			env := (&testsuite.WorkflowTestSuite{}).NewTestWorkflowEnvironment()
			env.SetTestTimeout(5 * time.Second)
			var a *VersionActivities
			env.RegisterActivity(a.SyncDeploymentVersionUserData)
			env.RegisterActivity(a.CheckWorkerDeploymentUserDataPropagation)
			version := &deploymentspb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "build"}
			routingConfig := &deploymentpb.RoutingConfig{RevisionNumber: 42}
			batches := [][]*deploymentspb.SyncDeploymentVersionUserDataRequest_SyncUserData{
				{{Name: "queue-a", Types: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW}}, {Name: "queue-b", Types: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY}}},
				{{Name: "queue-c", Types: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY}}},
			}
			for _, batch := range batches {
				syncRequest := &deploymentspb.SyncDeploymentVersionUserDataRequest{Version: version, UpdateRoutingConfig: routingConfig, Sync: batch}
				syncResponse := &deploymentspb.SyncDeploymentVersionUserDataResponse{}
				checkRequest := &deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{TaskQueueMaxVersions: map[string]int64{}}
				for _, tq := range batch {
					if mode != "target only" {
						if syncResponse.TaskQueueMaxVersions == nil {
							syncResponse.TaskQueueMaxVersions = map[string]int64{}
						}
						syncResponse.TaskQueueMaxVersions[tq.Name] = 100
					}
					if mode != "target only" {
						checkRequest.TaskQueueMaxVersions[tq.Name] = syncResponse.TaskQueueMaxVersions[tq.Name]
					}
					if mode != "legacy" {
						syncResponse.DeploymentName = "deployment"
						syncResponse.RevisionNumber = 42
						checkRequest.DeploymentName = "deployment"
						checkRequest.RevisionNumber = 42
						target := &deploymentspb.TaskQueuePropagationTarget{Name: tq.Name, TaskQueueTypes: tq.Types}
						syncResponse.TaskQueues = append([]*deploymentspb.TaskQueuePropagationTarget{target}, syncResponse.TaskQueues...)
						checkRequest.TaskQueues = append(checkRequest.TaskQueues, target)
					}
				}
				env.OnActivity(a.SyncDeploymentVersionUserData, mock.Anything, mock.MatchedBy(func(req *deploymentspb.SyncDeploymentVersionUserDataRequest) bool {
					return proto.Equal(req, syncRequest)
				})).Return(syncResponse, nil).Once()
				env.OnActivity(a.CheckWorkerDeploymentUserDataPropagation, mock.Anything, mock.MatchedBy(func(req *deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest) bool {
					return proto.Equal(req, checkRequest)
				})).Return(nil).Once()
			}
			testWorkflow := func(ctx workflow.Context) error {
				runner := &VersionWorkflowRunner{
					WorkerDeploymentVersionWorkflowArgs: &deploymentspb.WorkerDeploymentVersionWorkflowArgs{VersionState: &deploymentspb.VersionLocalState{Version: version, SyncBatchSize: 2}},
					workflowVersion:                     VersionDataRevisionNumber,
					logger:                              workflow.GetLogger(ctx),
				}
				runner.executeAndTrackAsyncPropagation(ctx, batches, routingConfig, nil)
				return nil
			}
			env.ExecuteWorkflow(testWorkflow)
			require.NoError(t, env.GetWorkflowError())
			env.AssertExpectations(t)
		})
	}
}

func TestWorkerDeploymentPropagationRequestsLegacyResult(t *testing.T) {
	t.Parallel()
	input := &deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{TaskQueueMaxVersions: map[string]int64{"queue": 100}}
	requests := workerDeploymentPropagationRequests("namespace", input)
	require.Len(t, requests, 1)
	require.Equal(t, int64(100), requests["queue"].GetVersion())
	require.Nil(t, requests["queue"].GetRoutingConfigTarget())
}

func TestWorkerDeploymentPropagationRequestsPreserveLegacyVersions(t *testing.T) {
	t.Parallel()
	input := &deploymentspb.CheckWorkerDeploymentUserDataPropagationRequest{
		DeploymentName: "deployment", RevisionNumber: 42,
		TaskQueueMaxVersions: map[string]int64{"queue-a": 100, "queue-b": 200},
		TaskQueues: []*deploymentspb.TaskQueuePropagationTarget{
			{Name: "queue-a", TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW}},
			{Name: "queue-b", TaskQueueTypes: []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_ACTIVITY}},
		},
	}
	requests := workerDeploymentPropagationRequests("namespace", input)
	// Older propagation activities only read this map and forward its versions.
	for queue, version := range input.GetTaskQueueMaxVersions() {
		legacyRequest := &matchingservice.CheckTaskQueueUserDataPropagationRequest{
			NamespaceId: "namespace", TaskQueue: queue, Version: version,
		}
		require.Positive(t, legacyRequest.GetVersion())
		require.Equal(t, legacyRequest.GetVersion(), requests[queue].GetVersion())
		require.Equal(t, int64(42), requests[queue].GetRoutingConfigTarget().GetRevisionNumber())
	}
}
