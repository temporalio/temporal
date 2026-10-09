package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

type delayedTaskWriter struct {
	persistence.TaskManager
	pending      *persistence.CreateTasksRequest
	beforeUpdate func()
	updateErr    error
}

func (s *delayedTaskWriter) CreateTasks(_ context.Context, request *persistence.CreateTasksRequest) (*persistence.CreateTasksResponse, error) {
	s.pending = request
	return nil, serviceerror.NewUnavailable("write outcome unknown")
}

func TestTaskWriteRecovery(t *testing.T) {
	for _, commitBeforeFence := range []bool{true, false} {
		name := "commit_after_fence"
		if commitBeforeFence {
			name = "commit_before_fence"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			store := &delayedTaskWriter{TaskManager: newTestTaskManager(logger)}
			family, err := tqid.NewTaskQueueFamily("namespace", "queue")
			require.NoError(t, err)
			partition := family.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
			queue := UnversionedQueueKey(partition)
			config := newTaskQueueConfig(partition.TaskQueue(), NewConfig(dynamicconfig.NewNoopCollection()), "namespace")
			pq := NewMockphysicalTaskQueueManager(gomock.NewController(t))
			pq.EXPECT().QueueKey().Return(queue).AnyTimes()
			manager := newBacklogManager(ctx, pq, config, store, logger, logger, nil, metrics.NoopMetricsHandler)
			require.NoError(t, manager.taskWriter.initReadWriteState())
			db := manager.db

			_, err = db.CreateTasks(ctx, []*writeTaskRequest{{taskInfo: &persistencespb.TaskInfo{}, fairLevel: fairLevel{id: 1}}})
			require.Error(t, err)
			pending := store.pending

			// An empty read while the write is still pending must not acknowledge its ID.
			batch, err := manager.taskReader.getTaskBatch(ctx)
			require.NoError(t, err)
			require.Empty(t, batch.tasks)
			manager.taskAckManager.setReadLevelAfterGap(batch.readLevel)
			require.Zero(t, manager.taskAckManager.getAckLevel())
			require.NoError(t, db.OldUpdateState(ctx, manager.taskAckManager.getAckLevel()))
			db.updateAckLevelAndBacklogStats(subqueueZero, 0, 0, time.Time{})
			require.Equal(t, int64(1), db.getTotalApproximateBacklogCount())

			// A later append cannot make the unresolved range readable.
			_, err = db.CreateTasks(ctx, []*writeTaskRequest{{taskInfo: &persistencespb.TaskInfo{}, fairLevel: fairLevel{id: 2}}})
			require.Error(t, err)
			require.Same(t, pending, store.pending)

			if commitBeforeFence {
				_, err = store.TaskManager.CreateTasks(ctx, pending)
				require.NoError(t, err)
			}
			state, err := db.RenewLease(ctx)
			require.NoError(t, err)
			require.Equal(t, int64(2), state.rangeID)
			if !commitBeforeFence {
				_, err = store.TaskManager.CreateTasks(ctx, pending)
				require.ErrorAs(t, err, new(*persistence.ConditionFailedError))
			}

			batch, err = manager.taskReader.getTaskBatch(ctx)
			require.NoError(t, err)
			if commitBeforeFence {
				require.Len(t, batch.tasks, 1)
				require.Equal(t, int64(1), batch.tasks[0].TaskId)
			} else {
				require.Empty(t, batch.tasks)
				manager.taskAckManager.setReadLevelAfterGap(batch.readLevel)
				require.NoError(t, db.OldUpdateState(ctx, manager.taskAckManager.getAckLevel()))
				require.Zero(t, db.getTotalApproximateBacklogCount())
			}
		})
	}
}

func (s *delayedTaskWriter) UpdateTaskQueue(ctx context.Context, request *persistence.UpdateTaskQueueRequest) (*persistence.UpdateTaskQueueResponse, error) {
	if s.beforeUpdate != nil {
		s.beforeUpdate()
	}
	if s.updateErr != nil {
		return nil, s.updateErr
	}
	return s.TaskManager.UpdateTaskQueue(ctx, request)
}

func TestTaskWriterRecoversUnknownWrite(t *testing.T) {
	for _, priority := range []bool{false, true} {
		name := "classic"
		if priority {
			name = "priority"
		}
		t.Run(name, func(t *testing.T) {
			for _, fenceErr := range []error{nil, serviceerror.NewInternal("fence failed"), &persistence.ConditionFailedError{Msg: "ownership lost"}} {
				caseName := "recovered"
				if fenceErr != nil {
					caseName = fenceErr.Error()
				}
				t.Run(caseName, func(t *testing.T) {
					ctx := t.Context()
					logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
					logger.Expect(testlogger.Error, "Persistent store operation failure")
					store := &delayedTaskWriter{TaskManager: newTestTaskManager(logger)}
					family, err := tqid.NewTaskQueueFamily("namespace", "queue")
					require.NoError(t, err)
					partition := family.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
					queue := UnversionedQueueKey(partition)
					config := newTaskQueueConfig(partition.TaskQueue(), NewConfig(dynamicconfig.NewNoopCollection()), "namespace")
					pq := NewMockphysicalTaskQueueManager(gomock.NewController(t))
					pq.EXPECT().QueueKey().Return(queue).AnyTimes()

					var db *taskQueueDB
					var assign, appendTasks func([]*writeTaskRequest) error
					var notify <-chan struct{}
					if priority {
						manager := newPriBacklogManager(ctx, pq, config, store, logger, logger, nil, metrics.NoopMetricsHandler, false)
						db = manager.db
						state, err := db.RenewLease(ctx)
						require.NoError(t, err)
						manager.taskWriter.taskIDBlock = rangeIDToTaskIDBlock(state.rangeID, config.RangeSize)
						reader := newPriTaskReader(manager, subqueueZero, 0)
						manager.subqueues = []*priTaskReader{reader}
						assign, appendTasks = manager.taskWriter.assignTaskIDs, manager.taskWriter.appendTasks
						notify = reader.notifyC
					} else {
						manager := newBacklogManager(ctx, pq, config, store, logger, logger, nil, metrics.NoopMetricsHandler)
						require.NoError(t, manager.taskWriter.initReadWriteState())
						db = manager.db
						assign, appendTasks = manager.taskWriter.assignTaskIDs, manager.taskWriter.appendTasks
						notify = manager.taskReader.notifyC
					}
					store.updateErr = fenceErr
					if fenceErr != nil {
						cause := unloadCauseOtherError
						if _, ok := fenceErr.(*persistence.ConditionFailedError); ok {
							cause = unloadCauseConflict
						}
						pq.EXPECT().UnloadFromPartitionManager(cause)
					}
					var commitErr error
					store.beforeUpdate = func() {
						_, commitErr = store.TaskManager.CreateTasks(ctx, store.pending)
					}
					requests := []*writeTaskRequest{{taskInfo: &persistencespb.TaskInfo{}}}
					require.NoError(t, assign(requests))
					err = appendTasks(requests)
					require.ErrorAs(t, err, new(*serviceerror.Unavailable))
					require.NoError(t, commitErr)
					if fenceErr != nil {
						require.Zero(t, db.GetMaxReadLevel(subqueueZero))
						require.True(t, db.unknownTaskWrite)
						return
					}

					// Recovery and reader notification happen without another append.
					require.Equal(t, int64(2), db.RangeID())
					require.Len(t, notify, 1)
					require.False(t, db.unknownTaskWrite)
					response, err := db.GetTasks(ctx, subqueueZero, 1, db.GetMaxReadLevel(subqueueZero)+1, 10)
					require.NoError(t, err)
					require.Len(t, response.Tasks, 1)
					require.Equal(t, requests[0].id, response.Tasks[0].TaskId)
					next := []*writeTaskRequest{{taskInfo: &persistencespb.TaskInfo{}}}
					require.NoError(t, assign(next))
					require.Equal(t, rangeIDToTaskIDBlock(2, config.RangeSize).start, next[0].id)
					db.store = store.TaskManager
					require.NoError(t, appendTasks(next))
				})
			}
		})
	}
}
