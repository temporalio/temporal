package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

func newTaskQueueUserDataChasmEnv(t *testing.T, enabled bool, opts ...testcore.TestOption) *testcore.TestEnv {
	t.Helper()
	options := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.MatchingTaskQueueUserDataWriteToChasm, enabled),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueReadPartitions, 1),
		testcore.WithDynamicConfig(dynamicconfig.MatchingNumTaskqueueWritePartitions, 1),
	}
	return testcore.NewEnv(t, append(options, opts...)...)
}

func updateTaskQueueUserDataConfig(t *testing.T, env *testcore.TestEnv, taskQueue string, taskType enumspb.TaskQueueType, weight float32) {
	t.Helper()
	_, err := env.FrontendClient().UpdateTaskQueueConfig(testcore.NewContext(t.Context()), &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace: env.Namespace().String(), TaskQueue: taskQueue, TaskQueueType: taskType,
		SetFairnessWeightOverrides: map[string]float32{"priority": weight},
	})
	require.NoError(t, err)
}

func taskQueueUserDataSnapshotRequest(env *testcore.TestEnv, taskQueue string) *tquserdatapb.GetTaskQueueUserDataRequest {
	return &tquserdatapb.GetTaskQueueUserDataRequest{NamespaceId: env.NamespaceID().String(), TaskQueue: taskQueue}
}

func readTaskQueueUserDataFromDB(t *testing.T, env *testcore.TestEnv, taskQueue string) *persistencespb.VersionedTaskQueueUserData {
	t.Helper()
	response, err := env.GetTestCluster().TestBase().TaskMgr.GetTaskQueueUserData(testcore.NewContext(t.Context()), &persistence.GetTaskQueueUserDataRequest{
		NamespaceID: env.NamespaceID().String(), TaskQueue: taskQueue,
	})
	require.NoError(t, err)
	return response.UserData
}

func reloadTaskQueueUserData(t *testing.T, env *testcore.TestEnv, taskQueue string) {
	t.Helper()
	matchingClient := env.GetTestCluster().MatchingClient()
	response, err := matchingClient.ForceUnloadTaskQueuePartition(testcore.NewContext(t.Context()), &matchingservice.ForceUnloadTaskQueuePartitionRequest{
		NamespaceId:        env.NamespaceID().String(),
		TaskQueuePartition: &taskqueuespb.TaskQueuePartition{TaskQueue: taskQueue, TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW},
	})
	require.NoError(t, err)
	require.True(t, response.GetWasLoaded())
	_, err = matchingClient.GetTaskQueueUserData(testcore.NewContext(t.Context()), &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId: env.NamespaceID().String(), TaskQueue: taskQueue, TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	require.NoError(t, err)
}

func requireTaskQueueUserDataChasmSnapshot(t *testing.T, env *testcore.TestEnv, taskQueue string, expected *tquserdatapb.GetTaskQueueUserDataResponse) {
	t.Helper()
	// Matching initialization and History shard reacquisition can finish after the triggering RPC.
	await.Require(testcore.NewContext(t.Context()), t, func(t *await.T) {
		snapshot, err := env.GetTestCluster().TaskQueueUserDataClient().GetTaskQueueUserData(t.Context(), taskQueueUserDataSnapshotRequest(env, taskQueue))
		require.NoError(t, err)
		protorequire.ProtoEqual(t, expected, snapshot)
	}, 15*time.Second, 50*time.Millisecond)
}

func TestTaskQueueUserDataChasmBootstrap(t *testing.T) {
	t.Parallel()
	env := newTaskQueueUserDataChasmEnv(t, false, testcore.WithDedicatedCluster())
	taskQueue := testvars.New(t).TaskQueue().GetName()
	ctx := testcore.NewContext(t.Context())
	client := env.GetTestCluster().TaskQueueUserDataClient()

	for _, weight := range []float32{1, 2, 3} {
		updateTaskQueueUserDataConfig(t, env, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, weight)
	}
	db := readTaskQueueUserDataFromDB(t, env, taskQueue)
	require.Equal(t, int64(3), db.Version)
	_, err := client.GetTaskQueueUserData(ctx, taskQueueUserDataSnapshotRequest(env, taskQueue))
	require.ErrorAs(t, err, new(*serviceerror.NotFound))

	env.OverrideDynamicConfig(dynamicconfig.MatchingTaskQueueUserDataWriteToChasm, true)
	reloadTaskQueueUserData(t, env, taskQueue)
	expected := &tquserdatapb.GetTaskQueueUserDataResponse{Version: 1, PerType: db.Data.PerType, Clock: db.Data.Clock}
	requireTaskQueueUserDataChasmSnapshot(t, env, taskQueue, expected)

	reloadTaskQueueUserData(t, env, taskQueue)
	requireTaskQueueUserDataChasmSnapshot(t, env, taskQueue, expected)
	env.CloseShard(env.NamespaceID().String(), taskQueue)
	requireTaskQueueUserDataChasmSnapshot(t, env, taskQueue, expected)

	emptyTaskQueue := testvars.New(t).WithTaskQueueNumber(2).TaskQueue().GetName()
	_, err = env.GetTestCluster().MatchingClient().GetTaskQueueUserData(ctx, &matchingservice.GetTaskQueueUserDataRequest{
		NamespaceId: env.NamespaceID().String(), TaskQueue: emptyTaskQueue, TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
	})
	require.NoError(t, err)
	_, err = client.GetTaskQueueUserData(ctx, taskQueueUserDataSnapshotRequest(env, emptyTaskQueue))
	require.ErrorAs(t, err, new(*serviceerror.NotFound))
}

func TestTaskQueueUserDataChasmDualWrite(t *testing.T) {
	t.Parallel()
	env := newTaskQueueUserDataChasmEnv(t, true)
	taskQueue := testvars.New(t).TaskQueue().GetName()
	for index, update := range []struct {
		taskType enumspb.TaskQueueType
		weight   float32
	}{
		{enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1},
		{enumspb.TASK_QUEUE_TYPE_ACTIVITY, 2},
		{enumspb.TASK_QUEUE_TYPE_WORKFLOW, 3},
	} {
		updateTaskQueueUserDataConfig(t, env, taskQueue, update.taskType, update.weight)
		db := readTaskQueueUserDataFromDB(t, env, taskQueue)
		requireTaskQueueUserDataChasmSnapshot(t, env, taskQueue, &tquserdatapb.GetTaskQueueUserDataResponse{
			Version: int64(index + 1), PerType: db.Data.PerType, Clock: db.Data.Clock,
		})
	}
}

func TestTaskQueueUserDataChasmBootstrapClockOrdering(t *testing.T) {
	t.Parallel()
	for _, testCase := range []struct {
		name        string
		offset      int64
		wantVersion int64
	}{
		{"DB newer", -1, 2}, {"equal clocks", 0, 1}, {"CHASM newer", 1, 1},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			env := newTaskQueueUserDataChasmEnv(t, false)
			taskQueue := testvars.New(t).TaskQueue().GetName()
			updateTaskQueueUserDataConfig(t, env, taskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, 1)
			db := readTaskQueueUserDataFromDB(t, env, taskQueue)
			clock := common.CloneProto(db.Data.Clock)
			clock.WallClock += testCase.offset
			client := env.GetTestCluster().TaskQueueUserDataClient()
			_, err := client.UpsertTaskQueueUserData(testcore.NewContext(t.Context()), &tquserdatapb.UpsertTaskQueueUserDataRequest{
				NamespaceId: env.NamespaceID().String(), TaskQueue: taskQueue, PerType: db.Data.PerType, Clock: clock,
			})
			require.NoError(t, err)
			env.OverrideDynamicConfig(dynamicconfig.MatchingTaskQueueUserDataWriteToChasm, true)
			reloadTaskQueueUserData(t, env, taskQueue)
			if testCase.offset < 0 {
				clock = db.Data.Clock
			}
			requireTaskQueueUserDataChasmSnapshot(t, env, taskQueue, &tquserdatapb.GetTaskQueueUserDataResponse{
				Version: testCase.wantVersion, PerType: db.Data.PerType, Clock: clock,
			})
		})
	}
}
