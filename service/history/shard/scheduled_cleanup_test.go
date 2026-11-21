package shard

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/resolver"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	historytests "go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestScheduledCleanupAfterOwnershipTransfer characterizes a delayed cleanup
// deleting a successor-owned timer below an unpersisted predecessor frontier.
// A future fix should preserve that row and keep it reachable to queue readers.
// Engine callbacks control publication and polling; workflow execution is not run.
func TestScheduledCleanupAfterOwnershipTransfer(t *testing.T) {
	options := persistencetests.GetSQLiteMemoryTestClusterOption()
	testCluster := sql.NewTestCluster(
		options.SQLDBPluginName, options.DBName, options.DBUsername, options.DBPassword,
		options.DBHost, options.DBPort, options.ConnectAttributes, options.SchemaDir, nil, log.NewNoopLogger(),
	)
	testCluster.SetupTestDatabase()
	t.Cleanup(testCluster.TearDownTestDatabase)
	serializer := serialization.NewSerializer()
	persistenceConfig := testCluster.Config()
	factory := sql.NewFactory(*persistenceConfig.DataStores[persistenceConfig.DefaultStore].SQL,
		resolver.NewNoopResolver(), cluster.TestCurrentClusterName, log.NewNoopLogger(), metrics.NoopMetricsHandler, serializer)
	t.Cleanup(factory.Close)
	shardStore, err := factory.NewShardStore()
	require.NoError(t, err)
	shardManager := persistence.NewShardManager(shardStore, serializer)
	t.Cleanup(shardManager.Close)
	executionStore, err := factory.NewExecutionStore()
	require.NoError(t, err)
	executionManager := persistence.NewExecutionManager(executionStore, serializer, nil, log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(4*1024*1024), dynamicconfig.GetBoolPropertyFn(false))
	t.Cleanup(executionManager.Close)

	for index, tc := range []struct {
		name                 string
		persistFrontier      bool
		pollBeforeAssignment bool
		survivesCleanup      bool
	}{
		{name: "unpersisted-frontier", survivesCleanup: false},
		{name: "persisted-frontier", persistFrontier: true, survivesCleanup: true},
		{name: "reader-poll-before-assignment", pollBeforeAssignment: true, survivesCleanup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
			clockSource := clock.NewEventTimeSource().Update(now)
			shardID := int32(index + 1)
			_, err := shardManager.GetOrCreateShard(t.Context(), &persistence.GetOrCreateShardRequest{
				ShardID:          shardID,
				InitialShardInfo: &persistencespb.ShardInfo{ShardId: shardID, RangeId: 1},
			})
			require.NoError(t, err)

			newOwner := func(onPublished func(*ContextTest)) *ContextTest {
				ctrl := gomock.NewController(t)
				cfg := historytests.NewDynamicConfig()
				cfg.TimerProcessorMaxTimeShift = dynamicconfig.GetDurationPropertyFn(time.Second)
				cfg.ShardUpdateMinInterval = dynamicconfig.GetDurationPropertyFn(0)
				owner := NewTestContextWithTimeSource(ctrl, &persistencespb.ShardInfo{ShardId: shardID, RangeId: 1}, cfg, clockSource)
				// The test helper only replaces the generator's clock; the production
				// constructor supplies the same clock to the manager and generator.
				owner.taskKeyManager.timeSource = clockSource
				owner.persistenceShardManager = shardManager
				owner.executionManager = executionManager
				owner.state = contextStateAcquiring
				owner.shardInfo = nil
				owner.engineFuture = future.NewFuture[historyi.Engine]()
				owner.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
				owner.Resource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
				owner.Resource.NamespaceCache.EXPECT().GetNamespaceByID(historytests.NamespaceID).Return(historytests.LocalNamespaceEntry, nil).AnyTimes()
				factory := NewMockEngineFactory(ctrl)
				engine := historyi.NewMockEngine(ctrl)
				owner.engineFactory = factory
				factory.EXPECT().CreateEngine(owner.ContextImpl).Return(engine)
				engine.EXPECT().Start()
				engine.EXPECT().Stop()
				called := false
				engine.EXPECT().NotifyNewTasks(gomock.Any()).Do(func(_ map[tasks.Category][]tasks.Task) {
					if !called {
						called = true
						require.True(t, owner.engineFuture.Ready())
						require.Equal(t, contextStateAcquired, owner.state)
						if onPublished != nil {
							onPublished(owner)
						}
					}
				}).AnyTimes()
				t.Cleanup(owner.FinishStop)
				owner.acquireShard()
				require.Equal(t, contextStateAcquired, owner.state)
				require.True(t, called)
				return owner
			}

			predecessor := newOwner(nil)
			predecessorRange := predecessor.GetRangeID()
			highWatermark := predecessor.GetQueueExclusiveHighReadWatermark(tasks.CategoryTimer)
			require.True(t, highWatermark.FireTime.After(now))
			if tc.persistFrontier {
				require.NoError(t, predecessor.SetQueueState(tasks.CategoryTimer, 0, &persistencespb.QueueState{
					ExclusiveReaderHighWatermark: &persistencespb.TaskKey{FireTime: timestamppb.New(highWatermark.FireTime)},
				}))
			}
			cleanup := &persistence.RangeCompleteHistoryTasksRequest{
				ShardID:             shardID,
				TaskCategory:        tasks.CategoryTimer,
				InclusiveMinTaskKey: tasks.NewKey(now.Add(-time.Second), 0),
				ExclusiveMaxTaskKey: highWatermark,
			}
			workflowKey := definition.NewWorkflowKey(historytests.NamespaceID.String(), historytests.WorkflowID, historytests.RunID)
			timerTask := &tasks.WorkflowRunTimeoutTask{WorkflowKey: workflowKey, VisibilityTimestamp: now, Version: 1}
			successor := newOwner(func(owner *ContextTest) {
				if tc.pollBeforeAssignment {
					owner.GetQueueExclusiveHighReadWatermark(tasks.CategoryTimer)
				}
				require.NoError(t, owner.AddTasks(t.Context(), &persistence.AddHistoryTasksRequest{
					ShardID:     shardID,
					NamespaceID: workflowKey.NamespaceID,
					WorkflowID:  workflowKey.WorkflowID,
					ArchetypeID: chasm.WorkflowArchetypeID,
					Tasks:       map[tasks.Category][]tasks.Task{tasks.CategoryTimer: {timerTask}},
				}))
			})
			require.Greater(t, successor.GetRangeID(), predecessorRange)
			require.GreaterOrEqual(t, timerTask.TaskID, (predecessorRange+1)<<successor.config.RangeSizeBits)
			require.Equal(t, !tc.survivesCleanup, timerTask.VisibilityTimestamp.Before(highWatermark.FireTime))

			read := func() []tasks.Task {
				response, err := executionManager.GetHistoryTasks(t.Context(), &persistence.GetHistoryTasksRequest{
					ShardID:             shardID,
					TaskCategory:        tasks.CategoryTimer,
					InclusiveMinTaskKey: tasks.NewKey(now.Add(-time.Second), 0),
					ExclusiveMaxTaskKey: tasks.NewKey(now.Add(10*time.Second), 0),
					BatchSize:           10,
				})
				require.NoError(t, err)
				return response.Tasks
			}
			require.Len(t, read(), 1)
			require.NoError(t, executionManager.RangeCompleteHistoryTasks(t.Context(), cleanup))
			if tc.survivesCleanup {
				require.Len(t, read(), 1)
			} else {
				// This is the unsafe outcome being characterized, not the desired contract.
				require.Empty(t, read())
			}
			t.Logf("predecessor_range=%d successor_range=%d cleanup_before=%s successor_key=(%s,%d) survives=%t", predecessorRange, successor.GetRangeID(), highWatermark.FireTime.Format(time.RFC3339Nano), timerTask.VisibilityTimestamp.Format(time.RFC3339Nano), timerTask.TaskID, tc.survivesCleanup)

			// A stale normal task write is fenced even though range cleanup is not.
			err = executionManager.AddHistoryTasks(t.Context(), &persistence.AddHistoryTasksRequest{
				ShardID: shardID, RangeID: predecessorRange,
				NamespaceID: workflowKey.NamespaceID, WorkflowID: workflowKey.WorkflowID,
				ArchetypeID: chasm.WorkflowArchetypeID,
				Tasks:       map[tasks.Category][]tasks.Task{tasks.CategoryTimer: {timerTask}},
			})
			require.ErrorAs(t, err, new(*persistence.ShardOwnershipLostError))
		})
	}
}
