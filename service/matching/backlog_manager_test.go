package matching

import (
	"container/list"
	"context"
	"fmt"
	"maps"
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
	testutil "go.temporal.io/server/common/testing"
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
	s.ptqMgr.EXPECT().GetFairnessWeightOverrides().AnyTimes().Return(fairnessWeightOverrides{ /* To avoid deadlock with gomock method */ })

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
			func() counter.Counter { return counter.NewMapCounter() },
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
			metrics.NoopMetricsHandler,
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
	for i := 0; i < taskCount; i++ {
		s.NoError(s.blm.SpoolTask(&persistencespb.TaskInfo{
			ExpiryTime: timestamp.TimeNowPtrUtcAddSeconds(3000),
			CreateTime: timestamp.TimeNowPtrUtc(),
		}))
	}
	s.Equal(int64(taskCount), totalApproximateBacklogCount(s.blm),
		"backlog count should match the number of tasks")
}

func (s *BacklogManagerTestSuite) TestApproximateBacklogCount_IncrementedBySpoolTask_Unavailable() {
	if s.fairness {
		// fairBacklogManager is smarter about backlog count: it can sometimes reset
		// ApproximateBacklogCount to zero or small values after read operations. That defeats
		// the assumption in this test.
		s.T().Skip("this test isn't valid with fairBacklogManager")
	}

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

func totalApproximateBacklogCount(c backlogManager) (total int64) {
	for _, stats := range c.BacklogStatsByPriority() {
		total += stats.ApproximateBacklogCount
	}
	return total
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
	wg.Add(1)
	go func() {
		defer wg.Done()
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
	}()

	// poller
	wg.Add(1)
	go func() {
		defer wg.Done()
		for sleepUntil(func() bool { return delta() >= -p.gap }) {
			if t := getTask(); t != nil {
				// TODO: error sometimes?
				t.finish(nil, true)

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
	}()

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

	elapsed := time.Since(start)
	s.T().Logf("processed %d tasks, %.3f/s", processed.Load(), float64(processed.Load())/elapsed.Seconds())
}
