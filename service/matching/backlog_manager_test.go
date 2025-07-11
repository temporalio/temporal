package matching

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"math/rand"
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
	"go.temporal.io/server/common/primitives/timestamp"
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

	addSpooledTask func(*internalTask) error
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
	s.controller = gomock.NewController(s.T())
	s.logger = testlogger.NewTestLogger(s.T(), testlogger.FailOnAnyUnexpectedError)
	if s.fairness {
		s.taskMgr = newTestFairTaskManager(s.logger)
	} else {
		s.taskMgr = newTestTaskManager(s.logger)
	}

	s.cfgcli = dynamicconfig.NewMemoryClient()
	s.cfgcol = dynamicconfig.NewCollection(s.cfgcli, s.logger)

	f, _ := tqid.NewTaskQueueFamily("", "test-queue")
	prtn := f.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).NormalPartition(0)
	queue := UnversionedQueueKey(prtn)
	tlCfg := newTaskQueueConfig(prtn.TaskQueue(), NewConfig(s.cfgcol), "test-namespace")

	s.ptqMgr = NewMockphysicalTaskQueueManager(s.controller)
	s.ptqMgr.EXPECT().QueueKey().Return(queue).AnyTimes()
	s.ptqMgr.EXPECT().ProcessSpooledTask(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).DoAndReturn(func(t *internalTask) error {
		if s.addSpooledTask != nil {
			return s.addSpooledTask(t)
		}
		return nil
	}).AnyTimes()
	s.addSpooledTask = nil

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
			metrics.NoopMetricsHandler,
			counter.NewMapCounter(),
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
			metrics.NoopMetricsHandler,
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
			metrics.NoopMetricsHandler,
		)
	}
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

	s.Equal(int64(0), blm.TotalApproximateBacklogCount())

	blm.taskWriter.Start()
	// Adding tasks to the buffer will increase the in-memory counter by 1
	// and this will be written to persistence
	s.Eventually(func() bool {
		return blm.TotalApproximateBacklogCount() == int64(1)
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

	s.Equal(int64(3), blm.TotalApproximateBacklogCount(), "1 task in the backlog")
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
	for i := 0; i < taskCount; i++ {
		s.NoError(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}
	s.Equal(int64(taskCount), s.blm.TotalApproximateBacklogCount(),
		"backlog count should match the number of tasks")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedBySpoolTask_ServiceError() {
	s.logger.Expect(testlogger.Error, "Persistent store operation failure")
	s.taskMgr.addFault("CreateTasks", "Unavailable", 1.0)

	s.blm.Start()
	defer s.blm.Stop()
	s.NoError(s.blm.WaitUntilInitialized(context.Background()))

	taskCount := 10
	s.ptqMgr.EXPECT().AddSpooledTask(gomock.Any()).Return(nil).AnyTimes()
	for i := 0; i < taskCount; i++ {
		s.Error(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}
	s.Equal(int64(taskCount), s.blm.TotalApproximateBacklogCount(),
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

	s.Equal(int64(0), s.blm.TotalApproximateBacklogCount(),
		"backlog count should not be incremented")
}

func (s *BacklogManagerTestSuite) TestStandingBacklogs() {
	if !s.newMatcher && !s.fairness {
		s.T().Skip("TestStandingBacklogs is for priority + fairness backlog manager only")
	}

	const lower = 20
	const upper = 200
	const gap = 2 // add/finish tasks as long as we're within gap of the target
	const period = 3 * time.Second
	const duration = 5 * time.Second
	const keys = 30
	zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 3, 1, keys-1) // more lopsided
	// zipf := rand.NewZipf(rand.New(rand.NewSource(time.Now().UnixNano())), 1.5, 10, keys-1) // more even

	// reduce these for better coverage
	// TODO: consider testing with write batch size > read batch size
	s.cfgcli.OverrideSetting(dynamicconfig.MatchingGetTasksBatchSize, 100)
	s.cfgcli.OverrideSetting(dynamicconfig.MatchingGetTasksReloadAt, 40)
	s.cfgcli.OverrideSetting(dynamicconfig.MatchingMaxTaskBatchSize, 50)

	ctx, cancel := context.WithTimeout(context.Background(), duration+5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var lock sync.Mutex
	var tasks list.List
	var target, inflight, processed, index atomic.Int64
	var tracker sync.Map
	target.Store((lower + upper) / 2)

	s.addSpooledTask = func(t *internalTask) error {
		lock.Lock()
		defer lock.Unlock()
		e := tasks.PushBack(t)
		t.removeFromMatcher = func() {
			lock.Lock()
			defer lock.Unlock()
			tasks.Remove(e)
			// fmt.Printf("buf evict -> %d\n", tasks.Len())
		}
		// fmt.Printf("buf add -> %d\n", tasks.Len())
		return nil
	}
	getTask := func() *internalTask {
		lock.Lock()
		defer lock.Unlock()
		e := tasks.Front()
		if e == nil {
			// fmt.Printf("buf was empty\n")
			return nil
		}
		// fmt.Printf("buf remove -> %d\n", tasks.Len()-1)
		return tasks.Remove(e).(*internalTask) //nolint:revive
	}
	delta := func() int64 {
		return inflight.Load() - target.Load()
	}
	sleep := func() error {
		return util.InterruptibleSleep(ctx, time.Duration(10+rand.Intn(5))*time.Millisecond)
	}
	finished := func() bool { return ctx.Err() != nil || target.Load() == 0 && inflight.Load() == 0 }
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
	wg.Add(1)
	go func() {
		defer wg.Done()
		for sleepUntil(func() bool { return delta() <= gap }) {
			info := &persistencespb.TaskInfo{
				CreateTime:       timestamppb.Now(),
				ScheduledEventId: index.Add(1),
				Priority: &commonpb.Priority{
					FairnessKey: fmt.Sprintf("fkey-%02d", zipf.Uint64()),
				},
			}
			tracker.Store(info.ScheduledEventId, info.Priority.FairnessKey)
			inflight.Add(1)
			fmt.Printf("spool %5d -> %3d\n", info.ScheduledEventId, inflight.Load())
			err := s.blm.SpoolTask(info)
			if !s.NoError(err) {
				return
			}
		}
	}()

	// reader
	wg.Add(1)
	go func() {
		defer wg.Done()
		for sleepUntil(func() bool { return delta() >= -gap }) {
			if t := getTask(); t != nil {
				// TODO: error sometimes?
				t.finish(nil, true)
				inflight.Add(-1)
				tindex := t.event.Data.ScheduledEventId
				lag := index.Load() - tindex
				fmt.Printf("finish %5d -> %3d  %v  %5d\n", tindex, inflight.Load(), t.getPriority().GetFairnessKey(), lag)
				processed.Add(1)
				tracker.Delete(tindex)
			}
		}
	}()

	// adjust target over time
	for time.Since(start) < duration {
		factor := (math.Sin(2*math.Pi*time.Since(start).Seconds()/period.Seconds()) + 1.0) / 2
		target.Store(lower + int64(factor*float64(upper-lower+1)))
	}

	// drain and wait until exited
	s.T().Log("draining")
	target.Store(0)
	wg.Wait()

	if !s.Zero(inflight.Load(), "did not drain all tasks!") {
		tracker.Range(func(k, v any) bool {
			s.T().Logf("  outstanding task: %d %s", k.(int64), v.(string))
			return true
		})
	}

	elapsed := time.Since(start)
	s.T().Logf("processed %d tasks, %.3f/s", processed.Load(), float64(processed.Load())/elapsed.Seconds())
}
