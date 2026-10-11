package matching

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/service/matching/counter"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestReaderReloadThresholdChange characterizes the priority reader's missed wakeup.
// A future fix should let every case read the remaining backlog after completions,
// without requiring a new write. The fair reader is a control for that invariant.
func TestReaderReloadThresholdChange(t *testing.T) {
	for _, reader := range []string{"priority", "fair"} {
		for _, tc := range []struct {
			name      string
			threshold int
			rollback  bool
			stalls    bool
		}{
			{name: "unchanged", threshold: 1},
			{name: "increase_above_loaded", threshold: 3, stalls: true},
			{name: "rollback_above_loaded", threshold: 3, rollback: true, stalls: true},
			{name: "decrease", threshold: 0},
		} {
			t.Run(reader+"/"+tc.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
					client := dynamicconfig.NewMemoryClient()
					client.OverrideSetting(dynamicconfig.MatchingGetTasksBatchSize, 4)
					client.OverrideSetting(dynamicconfig.MatchingGetTasksReloadAt, tc.threshold)
					restore := client.OverrideSetting(dynamicconfig.MatchingGetTasksReloadAt, 1)
					queue := tqid.UnsafeTaskQueueFamily("namespace-id", "reload-test").
						TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
					config := newTaskQueueConfig(queue, NewConfig(dynamicconfig.NewCollection(client, logger)), "namespace")
					pq := NewMockphysicalTaskQueueManager(gomock.NewController(t))
					pq.EXPECT().QueueKey().Return(UnversionedQueueKey(queue.NormalPartition(0))).AnyTimes()
					var captured []*internalTask
					pq.EXPECT().AddSpooledTask(gomock.Any()).DoAndReturn(func(task *internalTask) error {
						captured = append(captured, task)
						return nil
					}).AnyTimes()

					var db *taskQueueDB
					var start func()
					var loaded func() int
					var priorityReader *priTaskReader
					if reader == "fair" {
						manager := newFairBacklogManager(ctx, pq, config, newTestFairTaskManager(logger), logger, logger,
							nil, metrics.NoopMetricsHandler, func() counter.Counter { return counter.NewMapCounter(1000) }, false)
						tr := newFairTaskReader(manager, subqueueZero, fairLevel{})
						db, start, loaded = manager.db, tr.Start, tr.getLoadedTasks
					} else {
						manager := newPriBacklogManager(ctx, pq, config, newTestTaskManager(logger), logger, logger,
							nil, metrics.NoopMetricsHandler, false)
						priorityReader = newPriTaskReader(manager, subqueueZero, 0)
						db, start, loaded = manager.db, priorityReader.Start, priorityReader.getLoadedTasks
					}
					_, err := db.RenewLease(ctx)
					require.NoError(t, err)
					requests := make([]*writeTaskRequest, 6)
					for i := range requests {
						requests[i] = &writeTaskRequest{
							subqueue:  subqueueZero,
							fairLevel: fairLevel{pass: 1, id: int64(i + 1)},
							taskInfo: &persistencespb.TaskInfo{
								CreateTime: timestamppb.Now(),
								ExpiryTime: timestamppb.New(time.Now().Add(time.Hour)),
							},
						}
					}
					if reader == "fair" {
						_, err = db.CreateFairTasks(ctx, requests)
					} else {
						_, err = db.CreateTasks(ctx, requests)
					}
					require.NoError(t, err)
					start()
					synctest.Wait()
					require.Len(t, captured, 4)
					require.Equal(t, 4, loaded())

					captured[0].finish(taskFinishResult{consumedToken: true})
					captured[1].finish(taskFinishResult{consumedToken: true})
					synctest.Wait()
					require.Equal(t, 2, loaded())
					require.Len(t, captured, 4)
					remaining := captured[2:4]
					if tc.rollback {
						restore()
					} else {
						client.OverrideSetting(dynamicconfig.MatchingGetTasksReloadAt, tc.threshold)
					}
					require.Equal(t, tc.threshold, config.GetTasksReloadAt())
					for _, task := range remaining {
						task.finish(taskFinishResult{consumedToken: true})
					}
					synctest.Wait()

					if reader == "priority" && tc.stalls {
						require.Len(t, captured, 4, "current behavior: unread tasks never reach the matcher")
						require.Zero(t, loaded())
						require.Empty(t, priorityReader.notifyC)
						readLevel, ackLevel := priorityReader.getLevels()
						require.EqualValues(t, 4, readLevel)
						require.EqualValues(t, 4, ackLevel)
						require.EqualValues(t, 6, db.GetMaxReadLevel(subqueueZero))
						require.EqualValues(t, 2, db.getTotalApproximateBacklogCount())

						// A real write cannot bypass the unread backlog, so its notification
						// wakes the pump and delivers both stranded tasks and the new task.
						resp, err := db.CreateTasks(ctx, []*writeTaskRequest{{
							subqueue: subqueueZero, fairLevel: fairLevel{id: 7}, taskInfo: requests[0].taskInfo,
						}})
						require.NoError(t, err)
						priorityReader.signalNewTasks(resp.bySubqueue[subqueueZero])
						synctest.Wait()
						require.Len(t, captured, 7)
						require.Equal(t, 3, loaded())
					} else {
						require.Len(t, captured, 6, "completions should resume reading persisted backlog")
						require.Equal(t, 2, loaded())
					}
				})
			})
		}
	}
}
