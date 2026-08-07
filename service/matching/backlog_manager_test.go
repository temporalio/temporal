package matching

import (
	"container/list"
	"context"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	testutil "go.temporal.io/server/common/testing"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/matching/counter"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BacklogManagerTestSuite struct {
	suite.Suite

	cfgcli     *dynamicconfig.MemoryClient
	cfgcol     *dynamicconfig.Collection
	newMatcher bool
	fairness   bool
	logger     *testlogger.TestLogger
	blm        backlogManager
	controller *gomock.Controller
	cancelCtx  context.CancelFunc
	taskMgr    *testTaskManager
	ptqMgr     *MockphysicalTaskQueueManager
	metricsCap *metricstest.CaptureHandler

	capturedTasksLock  sync.Mutex
	capturedTasksSlice []*internalTask
}

func TestBacklogManager_Classic_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &BacklogManagerTestSuite{newMatcher: false})
}

func TestBacklogManager_Pri_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &BacklogManagerTestSuite{newMatcher: true})
}

func TestBacklogManager_Fair_Suite(t *testing.T) {
	t.Parallel()
	suite.Run(t, &BacklogManagerTestSuite{newMatcher: true, fairness: true})
}

func (s *BacklogManagerTestSuite) SetupTest() {
	s.capturedTasksLock.Lock()
	s.capturedTasksSlice = nil
	s.capturedTasksLock.Unlock()

	s.controller = gomock.NewController(s.T())
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	if s.fairness {
		s.taskMgr = newTestFairTaskManager(s.logger)
	} else {
		s.taskMgr = newTestTaskManager(s.logger)
	}

	// A capture handler discards recordings while no capture is active (see
	// CaptureHandler.record), so it behaves like a noop handler for tests that
	// don't call StartCapture.
	s.metricsCap = metricstest.NewCaptureHandler()

	s.cfgcli = dynamicconfig.NewMemoryClient()
	s.cfgcol = dynamicconfig.NewCollection(s.cfgcli, s.logger)

	f, _ := tqid.NewTaskQueueFamily("", "test-queue")
	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
	queue := UnversionedQueueKey(prtn)
	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), NewConfig(s.cfgcol), "test-namespace")

	s.ptqMgr = NewMockphysicalTaskQueueManager(s.controller)
	s.ptqMgr.EXPECT().QueueKey().Return(queue).AnyTimes()
	s.ptqMgr.EXPECT().ProcessSpooledTask(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	s.ptqMgr.EXPECT().GetFairnessWeightOverrides().AnyTimes().Return(fairnessWeightOverrides{ /* To avoid deadlock with gomock method */ })
	s.ptqMgr.EXPECT().StartScaleManager(gomock.Any()).AnyTimes()

	var ctx context.Context
	ctx, s.cancelCtx = context.WithCancel(context.Background())
	s.T().Cleanup(s.cancelCtx)

	if s.fairness {
		s.blm = newFairBacklogManager(
			ctx,
			s.ptqMgr,
			tlCfg,
			s.taskMgr,
			s.logger,
			s.logger,
			nil,
			s.metricsCap,
			func() counter.Counter { return counter.NewMapCounter(1000) },
			false,
		)
	} else if s.newMatcher {
		s.blm = newPriBacklogManager(
			ctx,
			s.ptqMgr,
			tlCfg,
			s.taskMgr,
			s.logger,
			s.logger,
			nil,
			s.metricsCap,
			false,
		)
	} else {
		s.blm = newBacklogManager(
			ctx,
			s.ptqMgr,
			tlCfg,
			s.taskMgr,
			s.logger,
			s.logger,
			nil,
			s.metricsCap,
		)
	}
}

func (s *BacklogManagerTestSuite) setupToCaptureTasks() {
	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).DoAndReturn(func(t *internalTask) error {
		s.capturedTasksLock.Lock()
		defer s.capturedTasksLock.Unlock()
		s.capturedTasksSlice = append(s.capturedTasksSlice, t)
		return nil
	}).AnyTimes()
}

func (s *BacklogManagerTestSuite) capturedTasksLen() int {
	s.capturedTasksLock.Lock()
	defer s.capturedTasksLock.Unlock()
	return len(s.capturedTasksSlice)
}

func (s *BacklogManagerTestSuite) capturedTasks() []*internalTask {
	s.capturedTasksLock.Lock()
	defer s.capturedTasksLock.Unlock()
	return slices.Clone(s.capturedTasksSlice)
}

func (s *BacklogManagerTestSuite) TestReadLevelForAllExpiredTasksInBatch() {
	if s.newMatcher {
		s.T().Skip("not compatible with new backlog manager")
	}
	blm := s.blm.(*backlogManagerImpl)

	s.NoError(blm.taskWriter.initReadWriteState())
	s.Equal(int64(1), blm.getDB().rangeID)
	s.Equal(int64(0), blm.taskAckManager.getAckLevel())
	s.Equal(int64(0), blm.taskAckManager.getReadLevel())

	// Add all expired tasks
	tasks := []*persistencespb.AllocatedTaskInfo{
		{
			Data: &persistencespb.TaskInfo{
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-60 * 60),
			},
			TaskId: 11,
		},
		{
			Data: &persistencespb.TaskInfo{
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-60 * 60),
			},
			TaskId: 12,
		},
	}

	s.NoError(blm.taskReader.addTasksToBuffer(context.TODO(), tasks))
	s.Equal(int64(0), blm.taskAckManager.getAckLevel())
	s.Equal(int64(12), blm.taskAckManager.getReadLevel())

	// Now add a mix of valid and expired tasks
	s.NoError(blm.taskReader.addTasksToBuffer(context.TODO(), []*persistencespb.AllocatedTaskInfo{
		{
			Data: &persistencespb.TaskInfo{
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-60 * 60),
			},
			TaskId: 13,
		},
		{
			Data: &persistencespb.TaskInfo{
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-60 * 60),
			},
			TaskId: 14,
		},
	}))
	s.Equal(int64(0), blm.taskAckManager.getAckLevel())
	s.Equal(int64(14), blm.taskAckManager.getReadLevel())
}

func (s *BacklogManagerTestSuite) TestTaskWriterShutdown() {
	s.blm.Start()
	defer s.blm.Stop()
	s.NoError(s.blm.WaitUntilInitialized(context.Background()))

	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).MaxTimes(1)
	err := s.blm.SpoolTask(&persistencespb.TaskInfo{})
	s.NoError(err)

	s.cancelCtx()
	s.ptqMgr.EXPECT().UnloadFromPartitionManager(unloadCauseConflict).Times(1)

	err = s.blm.SpoolTask(&persistencespb.TaskInfo{})
	s.Error(err)
}

func (s *BacklogManagerTestSuite) TestReadBatchDone() {
	if s.newMatcher {
		s.T().Skip("not compatible with new backlog manager")
	}
	blm := s.blm.(*backlogManagerImpl)

	const rangeSize = 10
	const maxReadLevel = int64(120)
	blm.config.RangeSize = rangeSize

	blm.Start()
	defer blm.Stop()
	s.NoError(blm.WaitUntilInitialized(context.Background()))

	blm.taskAckManager.setReadLevel(0)
	blm.getDB().setMaxReadLevelForTesting(subqueueZero, maxReadLevel)
	batch, err := blm.taskReader.getTaskBatch(context.Background())
	s.NoError(err)
	s.Empty(batch.tasks)
	s.Equal(int64(rangeSize*10), batch.readLevel)
	s.False(batch.isReadBatchDone)
	s.NoError(err)

	blm.taskAckManager.setReadLevel(batch.readLevel)
	batch, err = blm.taskReader.getTaskBatch(context.Background())
	s.NoError(err)
	s.Empty(batch.tasks)
	s.Equal(maxReadLevel, batch.readLevel)
	s.True(batch.isReadBatchDone)
	s.NoError(err)
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedByAppendTask() {
	if s.newMatcher {
		s.T().Skip("not compatible with new backlog manager")
	}
	blm := s.blm.(*backlogManagerImpl)

	// Add tasks on the taskWriters channel
	blm.taskWriter.appendCh <- &writeTaskRequest{
		taskInfo: &persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		},
		responseCh: make(chan<- error),
	}

	s.Equal(int64(0), totalApproximateBacklogCount(blm))

	blm.taskWriter.Start()
	// Adding tasks to the buffer will increase the in-memory counter by 1
	// and this will be written to persistence
	s.Eventually(func() bool {
		return totalApproximateBacklogCount(blm) == int64(1)
	}, time.Second*30, time.Millisecond)
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_DecrementedByCompleteTask() {
	if s.newMatcher {
		s.T().Skip("not compatible with new backlog manager")
	}
	blm := s.blm.(*backlogManagerImpl)

	_, err := blm.getDB().RenewLease(blm.tqCtx)
	s.NoError(err)

	blm.taskAckManager.addTask(int64(1))
	blm.taskAckManager.addTask(int64(2))
	blm.taskAckManager.addTask(int64(3))

	// Manually update the backlog size since adding tasks to the outstanding map does not increment the counter
	blm.getDB().updateBacklogStats(3, time.Time{})

	s.Equal(int64(3), totalApproximateBacklogCount(blm), "1 task in the backlog")
	s.Equal(int64(-1), blm.taskAckManager.getAckLevel(), "should only move ack level on completion")
	s.Equal(int64(3), blm.taskAckManager.getReadLevel(), "read level should be 1 since a task has been added")

	// Complete tasks
	ackLevel, numAcked := blm.taskAckManager.completeTask(2)
	s.Equal(int64(-1), ackLevel, "should not move the ack level")
	s.Equal(int64(0), numAcked, "should not decrease the backlog counter as ack level has not gone up")

	ackLevel, numAcked = blm.taskAckManager.completeTask(3)
	s.Equal(int64(-1), ackLevel, "should not move the ack level")
	s.Equal(int64(0), numAcked, "should not decrease the backlog counter as ack level has not gone up")

	ackLevel, numAcked = blm.taskAckManager.completeTask(1)
	s.Equal(int64(3), ackLevel, "should move the ack level")
	s.Equal(int64(3), numAcked, "should decrease the backlog counter to 0 as no more tasks in the backlog")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedBySpoolTask() {
	s.blm.Start()
	defer s.blm.Stop()
	s.NoError(s.blm.WaitUntilInitialized(context.Background()))

	taskCount := 10
	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	for range taskCount {
		s.NoError(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}
	s.Equal(int64(taskCount), totalApproximateBacklogCount(s.blm),
		"backlog count should match the number of tasks")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedBySpoolTask_Unavailable() {
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.taskMgr.addFault("CreateTasks", "Unavailable", 1.0)

	// This test is for write-path accounting only: an Unavailable error leaves the count
	// incremented (the tasks may or may not have been persisted), unlike a definite failure which
	// un-increments it. A concurrent reader can notice that the backlog is actually empty and reset
	// the count, which breaks the test. Disable the reader reloading to make the test reliable.
	s.cfgcli.OverrideSetting(dynamicconfig.MatchingGetTasksReloadAt, -1)

	s.blm.Start()
	defer s.blm.Stop()
	s.NoError(s.blm.WaitUntilInitialized(context.Background()))

	taskCount := 10
	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	for range taskCount {
		s.Error(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}
	s.Equal(int64(taskCount), totalApproximateBacklogCount(s.blm),
		"backlog count should match the number of tasks despite the errors")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_NotIncrementedBySpoolTask_CondFailedError() {
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.taskMgr.addFault("CreateTasks", "ConditionFailed", 1.0)

	s.blm.Start()
	defer s.blm.Stop()
	s.NoError(s.blm.WaitUntilInitialized(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	s.ptqMgr.EXPECT().UnloadFromPartitionManager(unloadCauseConflict).
		Do(func(_ any) { cancel() }).
		AnyTimes()

	s.Error(s.blm.SpoolTask(&persistencespb.TaskInfo{
		ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
		CreateTime: timestamp.TimeNowPtrUtc(),
	}))

	<-ctx.Done()

	s.Equal(int64(0), totalApproximateBacklogCount(s.blm),
		"backlog count should not be incremented")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_NotIncrementedBySpoolTask_PersistenceLimit() {
	// A write dropped by the persistence rate limiter definitely didn't reach the database,
	// so it should not count towards the backlog.
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.taskMgr.addFault("CreateTasks", "PersistenceLimit", 1.0)

	s.blm.Start()
	defer s.blm.Stop()
	s.Require().NoError(s.blm.WaitUntilInitialized(context.Background()))

	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	for range 10 {
		s.Require().Error(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}

	s.Equal(int64(0), totalApproximateBacklogCount(s.blm),
		"backlog count should not be incremented for rate-limited writes")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_NotIncrementedBySpoolTask_ConcurrentLimit() {
	// A write rejected by the ConcurrentRequestLimiter interceptor definitely didn't reach the
	// database, so it should not count towards the backlog.
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.taskMgr.addFault("CreateTasks", "ConcurrentLimit", 1.0)

	s.blm.Start()
	defer s.blm.Stop()
	s.Require().NoError(s.blm.WaitUntilInitialized(context.Background()))

	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	for range 10 {
		s.Require().Error(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}

	s.Equal(int64(0), totalApproximateBacklogCount(s.blm),
		"backlog count should not be incremented for concurrency-limited writes")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_ResetOnDrained() {
	if !s.newMatcher || s.fairness {
		s.T().Skip("only for priority backlog manager")
	}

	blm := s.blm.(*priBacklogManagerImpl)
	db := blm.db

	s.setupToCaptureTasks()

	s.blm.Start()
	defer s.blm.Stop()
	s.Require().NoError(s.blm.WaitUntilInitialized(context.Background()))

	// Spool 3 tasks through the real writer path.
	for range 3 {
		s.Require().NoError(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}

	// Wait for all tasks to reach the matcher via signalNewTasks/direct-add.
	s.Eventually(func() bool { return s.capturedTasksLen() == 3 }, 5*time.Second, 10*time.Millisecond)

	s.EqualValues(3, totalApproximateBacklogCount(s.blm))

	// Inject backlog count divergence (simulating accumulated drift).
	db.updateBacklogStats(2, time.Time{})
	s.EqualValues(5, totalApproximateBacklogCount(s.blm))

	// Advance maxReadLevel past all task IDs to simulate a range renewal.
	// After direct-add, readLevel == old maxReadLevel == last task ID.
	maxRL := db.GetMaxReadLevel(subqueueZero) + 100
	db.setMaxReadLevelForTesting(subqueueZero, maxRL)

	// Signal the reader pump to scan through the empty range up to the
	// new maxReadLevel, advancing readLevel.
	blm.subqueues[subqueueZero].SignalTaskLoading()

	// Wait for the reader pump to scan through the gap.
	s.Eventually(func() bool {
		rl, _ := blm.subqueues[subqueueZero].getLevels()
		return rl >= maxRL
	}, 5*time.Second, 10*time.Millisecond)

	// Complete all tasks. On the last completion:
	// - outstandingTasks is empty
	// - readLevel >= maxReadLevel (pump already scanned)
	// - isDrainedLocked() returns true
	// - ackLevel gets set to maxReadLevel
	// - backlog counts reset to 0
	for _, t := range s.capturedTasks() {
		t.finish(taskFinishResult{consumedToken: true})
	}

	_, ackLevel := blm.subqueues[subqueueZero].getLevels()
	s.Equal(ackLevel, maxRL)

	s.Zero(totalApproximateBacklogCount(s.blm))
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_ResetOnGapDrain() {
	if !s.newMatcher || s.fairness {
		s.T().Skip("only for priority backlog manager")
	}

	blm := s.blm.(*priBacklogManagerImpl)
	db := blm.db

	// Initialize the db/subqueues without starting the background reader pump, so the test can
	// drive the reader deterministically.
	_, err := db.RenewLease(blm.tqCtx)
	s.Require().NoError(err)

	// Simulate a diverged backlog count with no real tasks in persistence, e.g. a stale count
	// carried across a reload. Since nothing is spooled, there are no outstanding tasks and
	// completeTask never runs, so the only thing that can reset the count is the gap-drain path
	// (setReadLevelAfterGap), not the completeTask path.
	db.updateBacklogStats(5, time.Time{})
	s.Require().EqualValues(5, db.getTotalApproximateBacklogCount())

	// Advance maxReadLevel past the ack level to simulate a range renewal that left a gap of
	// task IDs that were never actually written.
	maxRL := db.GetMaxReadLevel(subqueueZero) + 100
	db.setMaxReadLevelForTesting(subqueueZero, maxRL)

	// Drive a reader through the empty range exactly as getTasksPump does, but synchronously.
	// Each empty batch advances the ack level via setReadLevelAfterGap.
	tr := newPriTaskReader(blm, subqueueZero, 0)
	for {
		batch, err := tr.getTaskBatch(blm.tqCtx)
		s.Require().NoError(err)
		s.Require().Empty(batch.tasks)
		tr.setReadLevelAfterGap(batch.readLevel)
		if batch.isReadBatchDone {
			break
		}
	}

	// Having read to maxReadLevel with everything acked, setReadLevelAfterGap must have pushed
	// the advanced ack level into the db, which resets the diverged count to 0.
	_, ackLevel := tr.getLevels()
	s.Equal(maxRL, ackLevel)
	s.Zero(db.getTotalApproximateBacklogCount())
}

func (s *BacklogManagerTestSuite) TestSyncState_UnloadsOnOwnershipLoss() {
	if !s.newMatcher {
		s.T().Skip("SyncState is only used by the new backlog manager")
	}

	s.cfgcli.OverrideValue(dynamicconfig.MatchingUpdateAckInterval.Key(), 100*time.Millisecond)

	s.blm.Start()
	defer s.blm.Stop()
	s.Require().NoError(s.blm.WaitUntilInitialized(context.Background()))

	// physical queue should unload soon
	var unloadCalled atomic.Bool
	s.ptqMgr.EXPECT().UnloadFromPartitionManager(unloadCauseConflict).Do(func(unloadCause) {
		unloadCalled.Store(true)
	}).AnyTimes()

	// simulate another partition stealing and releasing ownership
	db := s.blm.getDB()
	tqd := s.taskMgr.getQueueDataByKey(db.queue)
	tqd.Lock()
	tqd.rangeID++
	tqd.Unlock()

	s.Eventually(unloadCalled.Load, time.Second, 100*time.Millisecond)
}

type taskBlock struct {
	count   int
	expired bool
}

func expiredBlock(n int) taskBlock { return taskBlock{count: n, expired: true} }
func validBlock(n int) taskBlock   { return taskBlock{count: n, expired: false} }

func (s *BacklogManagerTestSuite) TestSkipExpiredTasks_ExpiredThenValid() {
	s.testSkipExpiredTasks(10, expiredBlock(33), validBlock(3))
}

func (s *BacklogManagerTestSuite) TestSkipExpiredTasks_ValidExpiredValid() {
	s.testSkipExpiredTasks(10, validBlock(3), expiredBlock(33), validBlock(3))
}

func (s *BacklogManagerTestSuite) TestSkipExpiredTasks_ValidThenExpired() {
	s.testSkipExpiredTasks(10, validBlock(3), expiredBlock(33))
}

func (s *BacklogManagerTestSuite) TestSkipExpiredTasks_AllExpired() {
	if s.newMatcher && !s.fairness {
		s.T().Skip("this case doesn't work with priTaskReader yet")
	}
	s.testSkipExpiredTasks(10, expiredBlock(33))
}

// testSkipExpiredTasks verifies that the task reader correctly skips over expired tasks
// in the DB and advances the ack level past them.
func (s *BacklogManagerTestSuite) testSkipExpiredTasks(batchSize int, blocks ...taskBlock) {
	if !s.newMatcher {
		s.T().Skip("not compatible with classic backlog manager")
	}

	s.cfgcli.OverrideValue(dynamicconfig.MatchingGetTasksBatchSize.Key(), batchSize)

	// Pre-populate the DB with tasks before starting the backlog manager.
	// This simulates tasks that were written and then expired before reading.
	ctx := context.Background()
	queue := s.ptqMgr.QueueKey()
	queueInfo := &persistencespb.TaskQueueInfo{
		NamespaceId: queue.NamespaceId(),
		Name:        queue.PersistenceName(),
		TaskType:    queue.TaskType(),
		// start with ack level at zero
	}
	_, err := s.taskMgr.CreateTaskQueue(ctx, &persistence.CreateTaskQueueRequest{
		RangeID:       1,
		TaskQueueInfo: queueInfo,
	})
	s.Require().NoError(err)

	var dbTasks []*persistencespb.AllocatedTaskInfo
	numValid := 0
	lastID := int64(0)
	for _, block := range blocks {
		for range block.count {
			lastID++
			task := &persistencespb.AllocatedTaskInfo{
				TaskId: lastID,
				Data: &persistencespb.TaskInfo{
					CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-3600),
				},
			}
			if block.expired {
				task.Data.ExpiryTime = timestamp.TimeNowPtrUtcAddSeconds(-60)
			} else {
				task.Data.ExpiryTime = timestamp.TimeNowPtrUtcAddSeconds(3600)
				numValid++
			}
			if s.fairness {
				task.TaskPass = lastID * 1000 // spread out pass numbers
			}
			dbTasks = append(dbTasks, task)
		}
	}
	_, err = s.taskMgr.CreateTasks(ctx, &persistence.CreateTasksRequest{
		TaskQueueInfo: &persistence.PersistedTaskQueueInfo{Data: queueInfo, RangeID: 1},
		Tasks:         dbTasks,
	})
	s.Require().NoError(err)

	s.setupToCaptureTasks()

	// Start backlog manager.
	s.blm.Start()
	defer s.blm.Stop()
	s.Require().NoError(s.blm.WaitUntilInitialized(context.Background()))

	// Wait for all valid tasks to be delivered.
	s.Require().Eventually(func() bool {
		return s.capturedTasksLen() >= numValid
	}, 2*time.Second, 10*time.Millisecond, "timed out waiting for valid tasks to be delivered")

	// Complete the delivered tasks.
	for _, t := range s.capturedTasks() {
		t.finish(taskFinishResult{consumedToken: true})
	}

	// Verify the ack level advances past all tasks (expired + valid).
	s.Eventually(func() bool {
		db := s.blm.getDB()
		db.Lock()
		defer db.Unlock()
		if s.fairness {
			ackLevel := fairLevelFromProto(db.subqueues[subqueueZero].FairAckLevel)
			return !ackLevel.less(fairLevel{pass: lastID * 1000, id: lastID})
		}
		return db.subqueues[subqueueZero].AckLevel >= lastID
	}, 2*time.Second, 10*time.Millisecond, "ack level did not advance past all tasks")
}

// TestExpiredTasksOnRead_EmitTasksDropped verifies tasks_dropped is emitted once per
// task that has already expired when read from persistence, with reason=expired_read.
func (s *BacklogManagerTestSuite) TestExpiredTasksOnRead_EmitTasksDropped() {
	const numExpired = 3

	capture := s.metricsCap.StartCapture()
	defer s.metricsCap.StopCapture(capture)

	droppedReasons := func() []string {
		var reasons []string
		for _, r := range capture.Snapshot()[metrics.DroppedTasksCounter.Name()] {
			reasons = append(reasons, r.Tags["reason"])
		}
		return reasons
	}

	if !s.newMatcher {
		// Classic reader: drive addTasksToBuffer directly, mirroring
		// TestReadLevelForAllExpiredTasksInBatch. The emission is synchronous.
		blm := s.blm.(*backlogManagerImpl)
		s.Require().NoError(blm.taskWriter.initReadWriteState())

		expired := make([]*persistencespb.AllocatedTaskInfo, numExpired)
		for i := range expired {
			expired[i] = &persistencespb.AllocatedTaskInfo{
				TaskId: int64(i + 1),
				Data: &persistencespb.TaskInfo{
					CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-3600),
					ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(-60),
				},
			}
		}
		s.Require().NoError(blm.taskReader.addTasksToBuffer(context.TODO(), expired))

		reasons := droppedReasons()
		s.Require().Len(reasons, numExpired)
		for _, reason := range reasons {
			s.Equal(dropReasonExpiredRead.tag().Value, reason)
		}
		return
	}

	// Pri/fair readers read the backlog from the DB asynchronously after Start.
	// Pre-populate the DB with already-expired tasks plus one valid task (a trailing
	// valid task keeps this off the priTaskReader all-expired edge case).
	ctx := context.Background()
	queue := s.ptqMgr.QueueKey()
	queueInfo := &persistencespb.TaskQueueInfo{
		NamespaceId: queue.NamespaceId(),
		Name:        queue.PersistenceName(),
		TaskType:    queue.TaskType(),
	}
	_, err := s.taskMgr.CreateTaskQueue(ctx, &persistence.CreateTaskQueueRequest{
		RangeID:       1,
		TaskQueueInfo: queueInfo,
	})
	s.Require().NoError(err)

	var dbTasks []*persistencespb.AllocatedTaskInfo
	for id := int64(1); id <= numExpired+1; id++ {
		t := &persistencespb.AllocatedTaskInfo{
			TaskId: id,
			Data: &persistencespb.TaskInfo{
				CreateTime: timestamp.TimeNowPtrUtcAddSeconds(-3600),
			},
		}
		if id <= numExpired {
			t.Data.ExpiryTime = timestamp.TimeNowPtrUtcAddSeconds(-60) // expired
		} else {
			t.Data.ExpiryTime = timestamp.TimeNowPtrUtcAddSeconds(3600) // valid
		}
		if s.fairness {
			t.TaskPass = id * 1000 // spread out pass numbers
		}
		dbTasks = append(dbTasks, t)
	}
	_, err = s.taskMgr.CreateTasks(ctx, &persistence.CreateTasksRequest{
		TaskQueueInfo: &persistence.PersistedTaskQueueInfo{Data: queueInfo, RangeID: 1},
		Tasks:         dbTasks,
	})
	s.Require().NoError(err)

	s.setupToCaptureTasks()

	s.blm.Start()
	defer s.blm.Stop()
	s.Require().NoError(s.blm.WaitUntilInitialized(context.Background()))

	// Wait for the expired tasks to be read and dropped.
	await.RequireTrue(s.T(), func() bool {
		return len(droppedReasons()) == numExpired
	}, 2*time.Second, 10*time.Millisecond)

	for _, reason := range droppedReasons() {
		s.Equal(dropReasonExpiredRead.tag().Value, reason)
	}
}

func totalApproximateBacklogCount(c backlogManager) (total int64) {
	for _, stats := range c.BacklogStatsByPriority() {
		total += stats.ApproximateBacklogCount
	}
	return total
}

func (s *BacklogManagerTestSuite) TestBypassReader() {
	if !s.newMatcher {
		s.T().Skip("bypass only applies to pri/fair")
	}

	s.setupToCaptureTasks()

	// set up initial qkey in db so that we always read one range on load
	qkey := s.ptqMgr.QueueKey()
	_, err := s.taskMgr.CreateTaskQueue(context.Background(), &persistence.CreateTaskQueueRequest{
		RangeID: 1,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId: qkey.NamespaceId(),
			Name:        qkey.PersistenceName(),
			TaskType:    qkey.TaskType(),
		},
	})
	s.Require().NoError(err)

	s.blm.Start()
	defer s.blm.Stop()
	s.Require().NoError(s.blm.WaitUntilInitialized(context.Background()))

	// wait for the initial read to complete so we're at the end
	s.Eventually(func() bool {
		return s.taskMgr.getGetTasksCount(qkey) == 1
	}, 5*time.Second, 10*time.Millisecond)

	for range 3 {
		prevCreateCount := s.taskMgr.getCreateTaskCount(qkey)
		prevCaptureCount := s.capturedTasksLen()

		// write a task
		s.Require().NoError(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))

		// we have written one batch of tasks
		s.Equal(prevCreateCount+1, s.taskMgr.getCreateTaskCount(qkey))

		// wait for the task to arrive at the matcher bypassing the read path
		s.Eventually(func() bool { return s.capturedTasksLen() == prevCaptureCount+1 }, 5*time.Second, 10*time.Millisecond)

		// we should have passed the task in memory without any more GetTasks calls
		s.Equal(1, s.taskMgr.getGetTasksCount(qkey))
	}
}

type standingBacklogParams struct {
	lower, upper   int64         // range of standing backlog
	gap            int64         // add/finish tasks as long as we're within gap of the target
	period         time.Duration // interval between peaks/troughs
	duration       time.Duration // total duration
	keys           int           // unique fairness keys
	zipfS, zipfV   float64       // parameters for fairness key distribution
	cfg            map[dynamicconfig.Key]any
	delayInjection time.Duration
	faultInjection float32
}

var defaultStandingBacklogParams = standingBacklogParams{
	lower:    20,
	upper:    200,
	gap:      2,
	period:   3 * time.Second,
	duration: 5 * time.Second,
	keys:     30,
	zipfS:    3,
	zipfV:    1,
	cfg: map[dynamicconfig.Key]any{
		// reduce these for better coverage
		dynamicconfig.MatchingGetTasksBatchSize.Key(): 100,
		dynamicconfig.MatchingGetTasksReloadAt.Key():  40,
		dynamicconfig.MatchingMaxTaskBatchSize.Key():  50,
	},
	delayInjection: 1 * time.Millisecond,
	faultInjection: 0.015,
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_Short() {
	s.testStandingBacklog(defaultStandingBacklogParams)
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_ManyKeysUniform() {
	testutil.LongTest(s)
	p := defaultStandingBacklogParams
	p.zipfS = 1.01 // not exactly uniform but closer
	p.zipfV = 10000
	p.keys = 10000
	p.period = 5 * time.Second
	p.duration = 15 * time.Second
	s.testStandingBacklog(p)
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_FullyDrain() {
	testutil.LongTest(s)
	p := defaultStandingBacklogParams
	p.lower = -20
	p.period = 3 * time.Second
	p.duration = 15 * time.Second
	s.testStandingBacklog(p)
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_WideRange() {
	testutil.LongTest(s)
	p := defaultStandingBacklogParams
	p.lower = 3
	p.upper = 1000
	p.period = 15 * time.Second
	p.duration = 15 * time.Second
	s.testStandingBacklog(p)
}

func (s *BacklogManagerTestSuite) TestStandingBacklog_FiveMin() {
	testutil.LongTest(s)
	p := defaultStandingBacklogParams
	p.lower = -10
	p.upper = 400
	p.period = time.Minute
	p.duration = 5 * time.Minute
	p.cfg = maps.Clone(p.cfg)
	p.cfg[dynamicconfig.MatchingGetTasksBatchSize.Key()] = 300
	p.cfg[dynamicconfig.MatchingGetTasksReloadAt.Key()] = 60
	p.delayInjection = 3 * time.Millisecond
	s.testStandingBacklog(p)
}

func (s *BacklogManagerTestSuite) testStandingBacklog(p standingBacklogParams) {
	if !s.newMatcher && !s.fairness {
		s.T().Skip("TestStandingBacklogs is for priority + fairness backlog manager only")
	}

	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), p.zipfS, p.zipfV, uint64(p.keys-1))

	for k, v := range p.cfg {
		s.cfgcli.OverrideValue(k, v)
	}

	// add delays and fault injection
	s.taskMgr.delayInjection = p.delayInjection
	if p.faultInjection > 0 {
		s.taskMgr.addFault("GetTasks", "Unavailable", p.faultInjection)
		s.taskMgr.addFault("CreateTasks", "Unavailable", p.faultInjection)
		s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	}

	log := func(string, ...any) {}
	// uncomment this for verbose logs:
	// log = func(f string, a ...any) { fmt.Printf(f, a...) }

	ctx, cancel := context.WithTimeout(context.Background(), p.duration+15*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var lock sync.Mutex
	var tasks list.List // this is the in-memory buffer (mock for the matcher)
	var target, inflight, processed, index atomic.Int64
	var tracker sync.Map // tracks tasks so we can find missing ones
	target.Store((p.lower + p.upper) / 2)
	const testIsOver = int64(-1000000)

	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).DoAndReturn(func(t *internalTask) error {
		lock.Lock()
		defer lock.Unlock()
		e := tasks.PushBack(t)
		if !t.setRemoveFunc(func() {
			lock.Lock()
			defer lock.Unlock()
			tasks.Remove(e)
			log("buf evict %s -> %d\n", t.fairLevel(), tasks.Len())
		}) {
			tasks.Remove(e)
			return nil
		}
		log("buf add %s -> %d\n", t.fairLevel(), tasks.Len())
		return nil
	}).AnyTimes()
	getTask := func() *internalTask {
		lock.Lock()
		defer lock.Unlock()
		e := tasks.Front()
		if e == nil {
			return nil
		}
		t := tasks.Remove(e).(*internalTask)
		log("buf remove %s -> %d\n", t.fairLevel(), tasks.Len())
		return t
	}
	makeNewTask := func() *persistencespb.TaskInfo {
		return &persistencespb.TaskInfo{
			CreateTime:       timestamppb.Now(),
			ScheduledEventId: index.Add(1),
			Priority: &commonpb.Priority{
				// TODO: add priority key option too
				FairnessKey: fmt.Sprintf("fkey-%02d", zipf.Uint64()),
			},
		}
	}
	delta := func() int64 {
		return inflight.Load() - target.Load()
	}
	sleep := func() {
		d := time.Millisecond + time.Duration(rand.Float32()*float32(3*time.Millisecond))
		_ = util.InterruptibleSleep(ctx, d)
	}
	finished := func() bool { return ctx.Err() != nil || target.Load() == testIsOver && inflight.Load() == 0 }
	sleepUntil := func(cond func() bool) bool {
		for !finished() && !cond() {
			sleep()
		}
		return !finished()
	}

	start := time.Now()
	s.blm.Start()
	defer s.blm.Stop()
	s.NoError(s.blm.WaitUntilInitialized(context.Background()))

	// writer
	wg.Go(func() {
		for sleepUntil(func() bool { return delta() <= p.gap }) {
			info := makeNewTask()
			tracker.Store(info.ScheduledEventId, info.Priority.FairnessKey)
			inflight.Add(1)
			if s.blm.SpoolTask(info) == nil {
				log("spool %5d -> %3d\n", info.ScheduledEventId, inflight.Load())
			} else {
				log("spool %5d failed\n", info.ScheduledEventId, inflight.Load())
				tracker.Delete(info.ScheduledEventId)
				inflight.Add(-1)
				sleep()
			}
		}
	})

	// poller
	wg.Go(func() {
		for sleepUntil(func() bool { return delta() >= -p.gap }) {
			if t := getTask(); t != nil {
				// TODO: error sometimes?
				t.finish(taskFinishResult{consumedToken: true})

				tindex := t.event.Data.ScheduledEventId
				if _, loaded := tracker.LoadAndDelete(tindex); loaded {
					inflight.Add(-1)
				} else {
					// this is a duplicate task (as if matching called RecordTaskStarted twice)
					log("finished task was not in tracker: %d\n", tindex)
				}
				log("finish %s -> %3d  %v  lag %5d\n", t.fairLevel(), inflight.Load(), t.getPriority().GetFairnessKey(), index.Load()-tindex)
				processed.Add(1)
			} else {
				sleep()
			}
		}
	})

	// adjust target over time
	for t := target.Load(); time.Since(start) < p.duration; sleep() {
		factor := (math.Sin(2*math.Pi*time.Since(start).Seconds()/p.period.Seconds()) + 1.0) / 2
		next := p.lower + int64(factor*float64(p.upper-p.lower+1))
		if t != next {
			t = next
			target.Store(t)
			log("target %d", t)
		}
	}

	// drain and wait until exited
	s.T().Log("draining")
	target.Store(testIsOver)
	wg.Wait()

	if !s.Zero(inflight.Load(), "did not drain all tasks!") {
		tracker.Range(func(k, v any) bool {
			s.T().Logf("  outstanding task: %d %s", k.(int64), v.(string))
			return true
		})
	}

	qkey := s.ptqMgr.QueueKey()
	s.T().Logf("reads %d, writes %d", s.taskMgr.getGetTasksCount(qkey), s.taskMgr.getCreateTaskBatchCount(qkey))
	elapsed := time.Since(start)
	s.T().Logf("processed %d tasks, %.3f/s", processed.Load(), float64(processed.Load())/elapsed.Seconds())
}

// TestFairReaderReMergeOfCompletedWriteKeepsReadLevel reproduces the write/read race that
// triggered a bug in fair reader.
func (s *BacklogManagerTestSuite) TestFairReaderReMergeOfCompletedWriteKeepsReadLevel() {
	if !s.fairness {
		s.T().Skip("the stuck-reader bug is specific to the fair task reader")
	}
	s.setupToCaptureTasks()

	// Initialize the backlog manager so its DB (ack levels, subqueue state) is set up. The real
	// subqueue reader will just read the empty queue and go idle; we drive our own reader below.
	qkey := s.ptqMgr.QueueKey()
	_, err := s.taskMgr.CreateTaskQueue(context.Background(), &persistence.CreateTaskQueueRequest{
		RangeID: 1,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId: qkey.NamespaceId(),
			Name:        qkey.PersistenceName(),
			TaskType:    qkey.TaskType(),
		},
	})
	s.Require().NoError(err)
	blm := s.blm.(*fairBacklogManagerImpl)
	s.blm.Start()
	// We advance our reader's ack level past the real (empty) subqueue reader's; skip the final
	// ack-level write on Stop so it isn't flagged as moving backwards.
	defer func() { blm.skipFinalUpdate.Store(true); blm.Stop() }()
	s.Require().NoError(s.blm.WaitUntilInitialized(context.Background()))

	// Drive our own reader so the async read loop doesn't race with our hand-sequenced merges.
	// readPending stays false so a mergeWrite is actually processed (the mergeTasks wrapper defers
	// writes while a read is pending); we keep the reader atEnd=true throughout so no completeTask
	// spawns an async read.
	tr := newFairTaskReader(blm, subqueueZero, fairLevel{})

	mkTask := func(id int64) *persistencespb.AllocatedTaskInfo {
		return &persistencespb.AllocatedTaskInfo{
			TaskPass: 1,
			TaskId:   id,
			Data: &persistencespb.TaskInfo{
				CreateTime: timestamp.TimeNowPtrUtc(),
				ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			},
		}
	}
	lvl := func(id int64) fairLevel { return fairLevel{pass: 1, id: id} }
	finish := func(id int64) {
		for _, t := range s.capturedTasks() {
			if t.fairLevel() == lvl(id) {
				t.finish(taskFinishResult{consumedToken: true})
				return
			}
		}
		s.FailNowf("no captured task to finish", "level %v", lvl(id))
	}

	// 1. Writer pins the ack level at the start of its writeBatch.
	tr.getAndPinAckLevel()

	// 2. An in-flight read pulls the just-written task 5 from the DB and delivers it to the matcher.
	//    (mergeReadToEnd: fewer than a batch, so the reader believes it's at the end.)
	tr.mergeTasks([]*persistencespb.AllocatedTaskInfo{mkTask(5)}, mergeReadToEnd)
	s.Require().Equal(1, tr.getLoadedTasks())
	s.Require().Equal(1, s.capturedTasksLen())

	// 3. The matcher completes task 5. It becomes a pre-acked (nil) entry, but the ack level can't
	//    advance because it's pinned by the writer, so the ack "piles up" in memory.
	finish(5)
	s.Require().Equal(0, tr.getLoadedTasks())
	readLevel, _ := tr.getLevels()
	s.Require().Equal(lvl(5), readLevel, "read level should be at the task we read")

	// 4. The writer's wroteNewTasks finally runs and re-merges task 5. It's already present (the
	//    ack), so merged is empty. readLevel must stay put; pre-fix it collapsed to the ack level,
	//    evicted the ack, and set atEnd=false with nothing loaded (the stuck state).
	tr.mergeTasks([]*persistencespb.AllocatedTaskInfo{mkTask(5)}, mergeWrite)
	readLevel, _ = tr.getLevels()
	s.Require().Equal(lvl(5), readLevel, "write-merge of an already-acked task must not move read level")
	s.Require().Equal(1, s.capturedTasksLen(), "already-acked task must not be re-delivered")

	// 5. Writer finishes the batch and unpins; the piled-up ack now advances.
	tr.unpinAckLevel(nil)
	_, ackLevel := tr.getLevels()
	s.Require().Equal(lvl(5), ackLevel, "ack level should advance past the completed task after unpin")

	// 6. The reader is not stuck: a subsequent write is still delivered to the matcher. On the
	//    pre-fix code the reader is at atEnd=false with a collapsed read level, so this write would
	//    be dropped (above read level) and never delivered.
	tr.mergeTasks([]*persistencespb.AllocatedTaskInfo{mkTask(6)}, mergeWrite)
	s.Require().Equal(2, s.capturedTasksLen(), "reader should still deliver new tasks")
	s.Require().Equal(1, tr.getLoadedTasks())
}
