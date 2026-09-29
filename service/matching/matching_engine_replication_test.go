package matching

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/worker_versioning"
	"go.uber.org/mock/gomock"
)

func newUserDataReplicationTestEngine(t *testing.T) (*matchingEngineImpl, *namespace.Namespace) {
	t.Helper()
	controller := gomock.NewController(t)
	ns, namespaceRegistry := createMockNamespaceCache(controller, matchingTestNamespace)
	matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
	matchingClient.EXPECT().ForceLoadTaskQueuePartition(gomock.Any(), gomock.Any()).
		Return(&matchingservice.ForceLoadTaskQueuePartitionResponse{}, nil).AnyTimes()
	matchingClient.EXPECT().UpdateTaskQueueUserData(gomock.Any(), gomock.Any()).
		Return(&matchingservice.UpdateTaskQueueUserDataResponse{}, nil).AnyTimes()
	engine := createTestMatchingEngine(testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError), controller, defaultTestConfig(), matchingClient, namespaceRegistry)
	engine.Start()
	t.Cleanup(engine.Stop)
	return engine, ns
}

func TestApplyTaskQueueUserDataReplicationEventSnapshotClock(t *testing.T) {
	currentClock := &clockspb.HybridLogicalClock{WallClock: 10, Version: 1, ClusterId: 1}
	newerClock := &clockspb.HybridLogicalClock{WallClock: 20, ClusterId: 2}
	olderClock := &clockspb.HybridLogicalClock{WallClock: 5, ClusterId: 2}
	makeData := func(snapshotClock *clockspb.HybridLogicalClock, buildID string, revision int64) *persistencespb.TaskQueueUserData {
		return &persistencespb.TaskQueueUserData{
			Clock: snapshotClock,
			VersioningData: &persistencespb.VersioningData{
				AssignmentRules: []*persistencespb.AssignmentRule{{Rule: &taskqueuepb.BuildIdAssignmentRule{TargetBuildId: buildID}}},
				RedirectRules:   []*persistencespb.RedirectRule{{Rule: &taskqueuepb.CompatibleBuildIdRedirectRule{SourceBuildId: "source", TargetBuildId: buildID}}},
			},
			PerType: map[int32]*persistencespb.TaskQueueTypeUserData{
				int32(enumspb.TASK_QUEUE_TYPE_WORKFLOW): {
					DeploymentData: &persistencespb.DeploymentData{
						DeploymentsData: map[string]*persistencespb.WorkerDeploymentData{
							"deployment": {RoutingConfig: &deploymentpb.RoutingConfig{RevisionNumber: revision}},
						},
					},
				},
			},
		}
	}
	current := makeData(currentClock, "current", 1)
	newer := makeData(newerClock, "incoming", 2)
	older := makeData(olderClock, "older", 1)
	clockless := makeData(nil, "clockless", 2)
	logicalVersion := makeData(&clockspb.HybridLogicalClock{WallClock: 10, Version: 2, ClusterId: 1}, "incoming", 2)
	clusterID := makeData(&clockspb.HybridLogicalClock{WallClock: 10, Version: 1, ClusterId: 2}, "incoming", 2)
	tests := []struct {
		name     string
		current  *persistencespb.TaskQueueUserData
		incoming []*persistencespb.TaskQueueUserData
		want     *persistencespb.TaskQueueUserData
	}{
		{name: "newer snapshot", current: current, incoming: []*persistencespb.TaskQueueUserData{newer}, want: newer},
		{name: "out of order snapshot", current: current, incoming: []*persistencespb.TaskQueueUserData{newer, makeData(&clockspb.HybridLogicalClock{WallClock: 15, ClusterId: 2}, "out-of-order", 1)}, want: newer},
		{name: "logical version orders snapshots", current: current, incoming: []*persistencespb.TaskQueueUserData{logicalVersion}, want: logicalVersion},
		{name: "cluster ID orders snapshots", current: current, incoming: []*persistencespb.TaskQueueUserData{clusterID}, want: clusterID},
		{name: "older snapshot", current: current, incoming: []*persistencespb.TaskQueueUserData{older}, want: current},
		{name: "clockless incoming", current: current, incoming: []*persistencespb.TaskQueueUserData{clockless}, want: current},
		{name: "clockless current", current: clockless, incoming: []*persistencespb.TaskQueueUserData{newer}, want: newer},
		{name: "absent current", incoming: []*persistencespb.TaskQueueUserData{newer}, want: newer},
		{name: "newer snapshot clears data", current: current, incoming: []*persistencespb.TaskQueueUserData{{Clock: newerClock}}, want: &persistencespb.TaskQueueUserData{Clock: newerClock, VersioningData: &persistencespb.VersioningData{}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			engine, ns := newUserDataReplicationTestEngine(t)
			taskQueue := "replication-test"
			family := tqid.UnsafeTaskQueueFamily(ns.ID().String(), taskQueue)
			pm, _, err := engine.getTaskQueuePartitionManager(t.Context(), family.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseUserData)
			require.NoError(t, err)
			userDataManager := pm.GetUserDataManager()
			if test.current != nil {
				_, err = userDataManager.UpdateUserData(t.Context(), UserDataUpdateOptions{}, func(*persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
					return common.CloneProto(test.current), false, nil
				})
				require.NoError(t, err)
			}
			for _, incoming := range test.incoming {
				original := common.CloneProto(incoming)
				_, err = engine.ApplyTaskQueueUserDataReplicationEvent(t.Context(), &matchingservice.ApplyTaskQueueUserDataReplicationEventRequest{
					NamespaceId: ns.ID().String(),
					TaskQueue:   taskQueue,
					UserData:    incoming,
				})
				require.NoError(t, err)
				protorequire.ProtoEqual(t, original, incoming)
			}
			got, _, err := userDataManager.GetUserData()
			require.NoError(t, err)
			protorequire.ProtoEqual(t, test.want, got.GetData())
		})
	}
}

func TestApplyTaskQueueUserDataReplicationEventRevivalPreservesSnapshotClock(t *testing.T) {
	tests := []struct {
		name          string
		currentClock  *clockspb.HybridLogicalClock
		incomingClock *clockspb.HybridLogicalClock
		wantClock     *clockspb.HybridLogicalClock
	}{
		{name: "incoming snapshot wins", currentClock: &clockspb.HybridLogicalClock{WallClock: 1}, incomingClock: &clockspb.HybridLogicalClock{WallClock: 100, ClusterId: 2}, wantClock: &clockspb.HybridLogicalClock{WallClock: 100, ClusterId: 2}},
		{name: "current snapshot wins", currentClock: &clockspb.HybridLogicalClock{WallClock: 200, ClusterId: 1}, incomingClock: &clockspb.HybridLogicalClock{WallClock: 100, ClusterId: 2}, wantClock: &clockspb.HybridLogicalClock{WallClock: 200, ClusterId: 1}},
		{name: "clockless snapshots"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			engine, ns := newUserDataReplicationTestEngine(t)
			engine.timeSource = clock.NewEventTimeSource().Update(time.UnixMilli(300))
			engine.visibilityManager.(*manager.MockVisibilityManager).EXPECT().CountWorkflowExecutions(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *manager.CountWorkflowExecutionsRequest) (*manager.CountWorkflowExecutionsResponse, error) {
					var count int64
					if strings.Contains(req.Query, "in-use") {
						count = 1
					}
					return &manager.CountWorkflowExecutionsResponse{Count: count}, nil
				}).Times(2)
			makeVersioningData := func(state persistencespb.BuildId_State, timestamp int64) *persistencespb.VersioningData {
				return &persistencespb.VersioningData{VersionSets: []*persistencespb.CompatibleVersionSet{{
					SetIds: []string{"set"}, BecameDefaultTimestamp: &clockspb.HybridLogicalClock{WallClock: 1},
					BuildIds: []*persistencespb.BuildId{
						{Id: "in-use", State: state, StateUpdateTimestamp: &clockspb.HybridLogicalClock{WallClock: timestamp}, BecameDefaultTimestamp: &clockspb.HybridLogicalClock{WallClock: 1}},
						{Id: "default", State: state, StateUpdateTimestamp: &clockspb.HybridLogicalClock{WallClock: timestamp}, BecameDefaultTimestamp: &clockspb.HybridLogicalClock{WallClock: 2}},
					},
				}}}
			}
			taskQueue := "replication-test"
			family := tqid.UnsafeTaskQueueFamily(ns.ID().String(), taskQueue)
			pm, _, err := engine.getTaskQueuePartitionManager(t.Context(), family.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseUserData)
			require.NoError(t, err)
			_, err = pm.GetUserDataManager().UpdateUserData(t.Context(), UserDataUpdateOptions{}, func(*persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
				return &persistencespb.TaskQueueUserData{Clock: test.currentClock, VersioningData: makeVersioningData(persistencespb.STATE_ACTIVE, 10)}, false, nil
			})
			require.NoError(t, err)
			incoming := &persistencespb.TaskQueueUserData{Clock: test.incomingClock, VersioningData: makeVersioningData(persistencespb.STATE_DELETED, 20)}
			original := common.CloneProto(incoming)
			_, err = engine.ApplyTaskQueueUserDataReplicationEvent(t.Context(), &matchingservice.ApplyTaskQueueUserDataReplicationEventRequest{
				NamespaceId: ns.ID().String(),
				TaskQueue:   taskQueue,
				UserData:    incoming,
			})
			require.NoError(t, err)
			got, _, err := pm.GetUserDataManager().GetUserData()
			require.NoError(t, err)
			protorequire.ProtoEqual(t, test.wantClock, got.GetData().GetClock())
			require.Len(t, got.GetData().GetVersioningData().GetVersionSets(), 1)
			buildIDs := got.GetData().GetVersioningData().GetVersionSets()[0].GetBuildIds()
			require.Len(t, buildIDs, 2)
			for _, buildID := range buildIDs {
				require.Equal(t, persistencespb.STATE_ACTIVE, buildID.State)
				require.Equal(t, int64(300), buildID.GetStateUpdateTimestamp().GetWallClock())
			}
			protorequire.ProtoEqual(t, original, incoming)
		})
	}
}

func TestUpdateWorkerBuildIdCompatibilityAfterReplicationClock(t *testing.T) {
	for _, timestamp := range []string{"snapshot", "build state", "build default", "set default"} {
		t.Run(timestamp, func(t *testing.T) {
			t.Parallel()
			engine, ns := newUserDataReplicationTestEngine(t)
			engine.timeSource = clock.NewEventTimeSource().Update(time.UnixMilli(200))
			incoming := &persistencespb.TaskQueueUserData{Clock: &clockspb.HybridLogicalClock{WallClock: 100, ClusterId: 2}}
			wantClock := &clockspb.HybridLogicalClock{WallClock: 200, ClusterId: 1}
			if timestamp != "snapshot" {
				buildID := &persistencespb.BuildId{Id: "existing", State: persistencespb.STATE_ACTIVE}
				set := &persistencespb.CompatibleVersionSet{SetIds: []string{"existing-set"}, BuildIds: []*persistencespb.BuildId{buildID}}
				futureClock := &clockspb.HybridLogicalClock{WallClock: 300, ClusterId: 2}
				switch timestamp {
				case "build state":
					buildID.StateUpdateTimestamp = futureClock
				case "build default":
					buildID.BecameDefaultTimestamp = futureClock
				case "set default":
					set.BecameDefaultTimestamp = futureClock
				default:
					t.Fatalf("unknown timestamp %q", timestamp)
				}
				incoming.VersioningData = &persistencespb.VersioningData{VersionSets: []*persistencespb.CompatibleVersionSet{set}}
				wantClock = &clockspb.HybridLogicalClock{WallClock: 300, Version: 1, ClusterId: 1}
			}
			_, err := engine.ApplyTaskQueueUserDataReplicationEvent(t.Context(), &matchingservice.ApplyTaskQueueUserDataReplicationEventRequest{
				NamespaceId: ns.ID().String(), TaskQueue: "replication-test", UserData: incoming,
			})
			require.NoError(t, err)
			_, err = engine.UpdateWorkerBuildIdCompatibility(t.Context(), &matchingservice.UpdateWorkerBuildIdCompatibilityRequest{
				NamespaceId: ns.ID().String(), TaskQueue: "replication-test",
				Operation: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_{
					ApplyPublicRequest: &matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest{
						Request: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest{
							Operation: &workflowservice.UpdateWorkerBuildIdCompatibilityRequest_AddNewBuildIdInNewDefaultSet{AddNewBuildIdInNewDefaultSet: "local"},
						},
					},
				},
			})
			require.NoError(t, err)
			family := tqid.UnsafeTaskQueueFamily(ns.ID().String(), "replication-test")
			pm, _, err := engine.getTaskQueuePartitionManager(t.Context(), family.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseUserData)
			require.NoError(t, err)
			got, _, err := pm.GetUserDataManager().GetUserData()
			require.NoError(t, err)
			protorequire.ProtoEqual(t, wantClock, got.GetData().GetClock())
			setIndex, buildIndex := worker_versioning.FindBuildId(got.GetData().GetVersioningData(), "local")
			require.NotEqual(t, -1, setIndex)
			protorequire.ProtoEqual(t, wantClock, got.GetData().GetVersioningData().GetVersionSets()[setIndex].GetBuildIds()[buildIndex].GetStateUpdateTimestamp())
		})
	}
}
