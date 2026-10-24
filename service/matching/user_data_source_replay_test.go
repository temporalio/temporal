package matching

import (
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/worker_versioning"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *matchingEngineSuite) TestApplyTaskQueueUserDataReplicationEventSourceReplayPreservesDestination() {
	tests := []struct {
		name          string
		currentClock  *clockspb.HybridLogicalClock
		incomingClock *clockspb.HybridLogicalClock
	}{
		{name: "destination_clock_newer", currentClock: &clockspb.HybridLogicalClock{WallClock: 20}, incomingClock: &clockspb.HybridLogicalClock{WallClock: 10}},
		{name: "source_clock_newer", currentClock: &clockspb.HybridLogicalClock{WallClock: 10}, incomingClock: &clockspb.HybridLogicalClock{WallClock: 20}},
		{name: "source_clock_missing", currentClock: &clockspb.HybridLogicalClock{WallClock: 20}},
		{name: "destination_clock_missing", incomingClock: &clockspb.HybridLogicalClock{WallClock: 20}},
		{name: "both_clocks_missing"},
		{name: "equal_clocks", currentClock: &clockspb.HybridLogicalClock{WallClock: 20}, incomingClock: &clockspb.HybridLogicalClock{WallClock: 20}},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			current := &persistencespb.TaskQueueUserData{Clock: test.currentClock, PerType: make(map[int32]*persistencespb.TaskQueueTypeUserData)}
			incoming := &persistencespb.TaskQueueUserData{Clock: test.incomingClock, PerType: make(map[int32]*persistencespb.TaskQueueTypeUserData)}
			for _, tqType := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY} {
				current.PerType[int32(tqType)] = taskQueueUserDataWithDeployment(nil, tqType, "deployment", &persistencespb.WorkerDeploymentData{
					RoutingConfig: newTestRoutingConfig(1068, "new-current"),
					Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
						"new-current":      newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
						"destination-only": newTestWorkerDeploymentVersionData(3, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINED),
						"shared":           newTestWorkerDeploymentVersionData(2, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING),
					},
				}).PerType[int32(tqType)]
				incoming.PerType[int32(tqType)] = taskQueueUserDataWithDeployment(nil, tqType, "deployment", &persistencespb.WorkerDeploymentData{
					RoutingConfig: newTestRoutingConfig(1066, "shared"),
					Versions: map[string]*deploymentspb.WorkerDeploymentVersionData{
						"old-draining": newTestWorkerDeploymentVersionData(2, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_DRAINING),
						"old-current":  newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
						"shared":       newTestWorkerDeploymentVersionData(1, enumspb.WORKER_DEPLOYMENT_VERSION_STATUS_CURRENT),
					},
				}).PerType[int32(tqType)]
			}
			for tqType := range current.PerType {
				current.PerType[tqType].DeploymentData.DeploymentsData["deployment"].RoutingConfig.CurrentVersionChangedTime = &timestamppb.Timestamp{Seconds: 20}
				incoming.PerType[tqType].DeploymentData.DeploymentsData["deployment"].RoutingConfig.CurrentVersionChangedTime = &timestamppb.Timestamp{Seconds: 10}
			}
			want := common.CloneProto(current)
			base, _, _ := selectTaskQueueUserData(current, incoming)
			want.Clock = common.CloneProto(base.GetClock())
			for tqType, perType := range want.PerType {
				versions := perType.DeploymentData.DeploymentsData["deployment"].Versions
				for _, buildID := range []string{"old-draining", "old-current"} {
					versions[buildID] = common.CloneProto(incoming.PerType[tqType].DeploymentData.DeploymentsData["deployment"].Versions[buildID])
				}
			}
			taskQueue := uuid.NewString()
			s.seedTaskQueueUserData(taskQueue, current)
			for range 3 {
				got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)
				protorequire.ProtoEqual(s.T(), want, got)
				for _, perType := range got.PerType {
					currentVersion, currentRevision, _, _, _, _, _, _ := worker_versioning.CalculateTaskQueueVersioningInfo(perType.DeploymentData)
					protorequire.ProtoEqual(s.T(), &deploymentspb.WorkerDeploymentVersion{DeploymentName: "deployment", BuildId: "new-current"}, currentVersion)
					s.Require().Equal(int64(1068), currentRevision)
				}
			}
			advanced := common.CloneProto(want)
			for _, perType := range advanced.PerType {
				perType.DeploymentData.DeploymentsData["deployment"].RoutingConfig.RevisionNumber = 1069
			}
			s.applyTaskQueueUserDataReplicationEvent(taskQueue, advanced)
			got := s.applyTaskQueueUserDataReplicationEvent(taskQueue, incoming)
			protorequire.ProtoEqual(s.T(), advanced, got)
		})
	}
}
