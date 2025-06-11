package tasks

import (
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
)

type (
	interleavedWeightedRoundRobinSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller        *gomock.Controller
		mockFIFOScheduler *MockScheduler[*testTask]

		channelKeyToWeight    map[int]int
		channelWeightUpdateCh chan struct{}

		scheduler *InterleavedWeightedRoundRobinScheduler[*testTask, int]
		ts        *clock.EventTimeSource
	}

	testTask struct {
		*MockTask

		channelKey int
	}
)

func TestInterleavedWeightedRoundRobinSchedulerSuite(t *testing.T) {
	s := new(interleavedWeightedRoundRobinSchedulerSuite)
	suite.Run(t, s)
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) SetupSuite() {
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TearDownSuite() {
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockFIFOScheduler = NewMockScheduler[*testTask](s.controller)

	s.channelKeyToWeight = map[int]int{
		0: 5,
		1: 3,
		2: 2,
		3: 1,
	}
	s.channelWeightUpdateCh = make(chan struct{}, 1)
	logger := log.NewTestLogger()
	s.ts = clock.NewEventTimeSource()

	s.scheduler = NewInterleavedWeightedRoundRobinScheduler(
		InterleavedWeightedRoundRobinSchedulerOptions[*testTask, int]{
			TaskChannelKeyFn:      func(task *testTask) int { return task.channelKey },
			ChannelWeightFn:       func(key int) int { return s.channelKeyToWeight[key] },
			ChannelWeightUpdateCh: s.channelWeightUpdateCh,
			InactiveChannelDeletionDelay: func() time.Duration {
				return time.Hour
			},
		},
		Scheduler[*testTask](s.mockFIFOScheduler),
		logger,
	)
	s.scheduler.ts = s.ts
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TearDownTest() {
	s.scheduler.Stop()
	s.controller.Finish()
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestTrySubmitSchedule_Success() {
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := newTestTask(s.controller, 0)
	s.mockFIFOScheduler.EXPECT().TrySubmit(mockTask).DoAndReturn(func(task Task) bool {
		testWaitGroup.Done()
		return true
	})

	s.True(s.scheduler.TrySubmit(mockTask))

	testWaitGroup.Wait()
	s.scheduler.Stop()
	s.Equal(int64(0), atomic.LoadInt64(&s.scheduler.numInflightTask))
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestTrySubmitSchedule_FailThenSuccess() {
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := newTestTask(s.controller, 0)
	s.mockFIFOScheduler.EXPECT().TrySubmit(mockTask).DoAndReturn(func(task Task) bool {
		return false
	}).Times(1)
	s.mockFIFOScheduler.EXPECT().Submit(mockTask).Do(func(task Task) {
		testWaitGroup.Done()
	}).Times(1)

	s.True(s.scheduler.TrySubmit(mockTask))

	testWaitGroup.Wait()
	s.scheduler.Stop()
	s.Equal(int64(0), atomic.LoadInt64(&s.scheduler.numInflightTask))
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestTrySubmitSchedule_Fail_Shutdown() {
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()
	s.scheduler.Stop()

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := newTestTask(s.controller, 0)
	mockTask.EXPECT().Abort().Do(func() {
		testWaitGroup.Done()
	}).Times(1)
	s.True(s.scheduler.TrySubmit(mockTask))

	testWaitGroup.Wait()
	s.Equal(int64(0), atomic.LoadInt64(&s.scheduler.numInflightTask))
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestSubmitSchedule_Success() {
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := newTestTask(s.controller, 0)
	s.mockFIFOScheduler.EXPECT().Submit(mockTask).Do(func(task Task) {
		testWaitGroup.Done()
	})

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
	s.scheduler.Stop()
	s.Equal(int64(0), atomic.LoadInt64(&s.scheduler.numInflightTask))
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestSubmitSchedule_Shutdown() {
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()
	s.scheduler.Stop()

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := newTestTask(s.controller, 0)
	mockTask.EXPECT().Abort().Do(func() {
		testWaitGroup.Done()
	}).Times(1)

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
	s.Equal(int64(0), atomic.LoadInt64(&s.scheduler.numInflightTask))
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestChannels() {
	// need to manually set the number of pending task to 1
	// so schedule by task priority logic will execute
	numTasks := atomic.AddInt64(&s.scheduler.numInflightTask, 1)
	s.Equal(int64(1), numTasks)
	numPendingTasks := 0
	defer func() {
		numTasks := atomic.AddInt64(&s.scheduler.numInflightTask, -1)
		s.Equal(int64(numPendingTasks), numTasks)
	}()

	var channelWeights []int

	channelWeights = nil
	mockTask0 := newTestTask(s.controller, 0)
	s.scheduler.Submit(mockTask0)
	numPendingTasks++
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 5, 5}, channelWeights)

	channelWeights = nil
	mockTask1 := newTestTask(s.controller, 1)
	s.scheduler.Submit(mockTask1)
	numPendingTasks++
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 5, 3}, channelWeights)

	channelWeights = nil
	mockTask2 := newTestTask(s.controller, 2)
	s.scheduler.Submit(mockTask2)
	numPendingTasks++
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2}, channelWeights)

	channelWeights = nil
	mockTask3 := newTestTask(s.controller, 3)
	s.scheduler.Submit(mockTask3)
	numPendingTasks++
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2, 1}, channelWeights)

	channelWeights = nil
	s.scheduler.Submit(mockTask0)
	s.scheduler.Submit(mockTask1)
	s.scheduler.Submit(mockTask2)
	s.scheduler.Submit(mockTask3)
	numPendingTasks += 4
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2, 1}, channelWeights)
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestParallelSubmitSchedule() {
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()

	numSubmitter := 200
	numTasks := 100

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(numSubmitter * numTasks)

	startWaitGroup := sync.WaitGroup{}
	endWaitGroup := sync.WaitGroup{}

	startWaitGroup.Add(numSubmitter)

	var tasksLock sync.Mutex
	submittedTasks := map[*testTask]struct{}{}
	s.mockFIFOScheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(task Task) bool {
		tasksLock.Lock()
		submittedTasks[task.(*testTask)] = struct{}{}
		tasksLock.Unlock()
		testWaitGroup.Done()
		return true
	}).AnyTimes()
	s.mockFIFOScheduler.EXPECT().Submit(gomock.Any()).Do(func(task Task) {
		tasksLock.Lock()
		submittedTasks[task.(*testTask)] = struct{}{}
		tasksLock.Unlock()
		testWaitGroup.Done()
	}).AnyTimes()

	for i := 0; i < numSubmitter; i++ {
		channel := make(chan *testTask, numTasks)
		for j := 0; j < numTasks; j++ {
			channel <- newTestTask(s.controller, rand.Intn(4))
		}
		close(channel)

		endWaitGroup.Add(1)
		go func() {
			startWaitGroup.Wait()

			for mockTask := range channel {
				s.scheduler.Submit(mockTask)
			}

			endWaitGroup.Done()
		}()
		startWaitGroup.Done()
	}
	endWaitGroup.Wait()

	testWaitGroup.Wait()

	s.scheduler.Stop()
	s.Equal(int64(0), atomic.LoadInt64(&s.scheduler.numInflightTask))
	s.Len(submittedTasks, numSubmitter*numTasks)
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestUpdateWeight() {
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()

	var taskWG sync.WaitGroup
	s.mockFIFOScheduler.EXPECT().Submit(gomock.Any()).Do(func(task Task) {
		taskWG.Done()
	}).AnyTimes()

	// need to manually set the number of pending task to 1
	// so schedule by task priority logic will execute
	atomic.AddInt64(&s.scheduler.numInflightTask, 1)

	mockTask0 := newTestTask(s.controller, 0)
	mockTask1 := newTestTask(s.controller, 1)
	mockTask2 := newTestTask(s.controller, 2)
	mockTask3 := newTestTask(s.controller, 3)

	taskWG.Add(4)
	s.scheduler.Submit(mockTask0)
	s.scheduler.Submit(mockTask1)
	s.scheduler.Submit(mockTask2)
	s.scheduler.Submit(mockTask3)

	channelWeights := []int{}
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2, 1}, channelWeights)

	// trigger weight update
	s.channelKeyToWeight = map[int]int{
		0: 8,
		1: 5,
		2: 1,
		3: 1,
	}
	totalWeight := 0
	for _, weight := range s.channelKeyToWeight {
		totalWeight += weight
	}
	s.channelWeightUpdateCh <- struct{}{}

	// we don't know when the weight update signal will be picked up
	// so need to retry a few times here.
	for i := 0; i != 10; i++ {
		// submit a task may or may not trigger a new round of dispatch loop
		// which updates weight
		taskWG.Add(1)
		s.scheduler.Submit(mockTask0)
		taskWG.Wait()

		flattenedChannels := s.scheduler.channels()
		if len(flattenedChannels) != totalWeight {
			time.Sleep(50 * time.Millisecond)
			continue
		}

		channelWeights = []int{}
		for _, channel := range flattenedChannels {
			channelWeights = append(channelWeights, channel.Weight())
		}

	}
	s.Equal([]int{8, 8, 8, 8, 5, 8, 5, 8, 5, 8, 5, 8, 5, 1, 1}, channelWeights)

	// set the number of pending task back
	atomic.AddInt64(&s.scheduler.numInflightTask, -1)
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestDeleteInactiveChannels() {
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()

	var taskWG sync.WaitGroup
	s.mockFIFOScheduler.EXPECT().Submit(gomock.Any()).Do(func(task Task) {
		taskWG.Done()
	}).AnyTimes()

	// need to manually set the number of pending task to 1
	// so schedule by task priority logic will execute
	atomic.AddInt64(&s.scheduler.numInflightTask, 1)

	mockTask0 := newTestTask(s.controller, 0)
	mockTask1 := newTestTask(s.controller, 1)
	mockTask2 := newTestTask(s.controller, 2)
	mockTask3 := newTestTask(s.controller, 3)

	taskWG.Add(4)
	s.scheduler.Submit(mockTask0)
	s.scheduler.Submit(mockTask1)
	s.scheduler.Submit(mockTask2)
	s.scheduler.Submit(mockTask3)
	taskWG.Wait()

	var channelWeights []int
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2, 1}, channelWeights)

	s.ts.Advance(30 * time.Minute)

	// Sleeping for a small duration for the current event loop to finish.
	// doDispatchTasksWithWeight reads the current time before entering the loop. We have to wait for that loop to finish.
	// Otherwise, the tasks added below will have old lastActiveTime. We don't have any other way to know if the loop has finished.
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	// Only add tasks to first two channels.
	taskWG.Add(2)
	s.scheduler.Submit(mockTask0)
	s.scheduler.Submit(mockTask1)
	taskWG.Wait()

	// Advance time past 1 hour. This will make other two channels inactive for more than 1 hour.
	s.ts.Advance(31 * time.Minute)

	s.Eventually(func() bool {
		channelWeights = []int{}
		for _, channel := range s.scheduler.channels() {
			channelWeights = append(channelWeights, channel.Weight())
		}
		if !slices.Equal([]int{5, 5, 5, 3, 5, 3, 5, 3}, channelWeights) {
			return false
		}
		return true
	}, 30*time.Second, 100*time.Millisecond)

	// set the number of pending task back
	atomic.AddInt64(&s.scheduler.numInflightTask, -1)
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestInactiveChannelDeletionDelayNotProvided() {
	s.scheduler = NewInterleavedWeightedRoundRobinScheduler(
		InterleavedWeightedRoundRobinSchedulerOptions[*testTask, int]{
			TaskChannelKeyFn:      func(task *testTask) int { return task.channelKey },
			ChannelWeightFn:       func(key int) int { return s.channelKeyToWeight[key] },
			ChannelWeightUpdateCh: s.channelWeightUpdateCh,
			// Not setting InactiveChannelDeletionDelay
		},
		Scheduler[*testTask](s.mockFIFOScheduler),
		log.NewTestLogger(),
	)
	s.scheduler.ts = s.ts
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()

	var taskWG sync.WaitGroup
	s.mockFIFOScheduler.EXPECT().Submit(gomock.Any()).Do(func(task Task) {
		taskWG.Done()
	}).AnyTimes()

	// need to manually set the number of pending task to 1
	// so schedule by task priority logic will execute
	atomic.AddInt64(&s.scheduler.numInflightTask, 1)

	mockTask0 := newTestTask(s.controller, 0)
	mockTask1 := newTestTask(s.controller, 1)
	mockTask2 := newTestTask(s.controller, 2)
	mockTask3 := newTestTask(s.controller, 3)

	taskWG.Add(4)
	s.scheduler.Submit(mockTask0)
	s.scheduler.Submit(mockTask1)
	s.scheduler.Submit(mockTask2)
	s.scheduler.Submit(mockTask3)
	taskWG.Wait()

	var channelWeights []int
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2, 1}, channelWeights)

	s.ts.Advance(30 * time.Minute)

	// Sleeping for a small duration for the current event loop to finish.
	// We read ts.Now() before the loop begins and reuse it in the loop.
	time.Sleep(100 * time.Millisecond) //nolint:forbidigo

	// Only add tasks to first two channels.
	taskWG.Add(2)
	s.scheduler.Submit(mockTask0)
	s.scheduler.Submit(mockTask1)
	taskWG.Wait()

	// Advance time past 1 hour. This will make other two channels inactive for more than 1 hour.
	// But this time, those channels should not be deleted as we haven't provided InactiveChannelDeletionDelay
	s.ts.Advance(31 * time.Minute)

	time.Sleep(100 * time.Millisecond) //nolint:forbidigo
	channelWeights = []int{}
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}

	// Check that all channels exist.
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2, 1}, channelWeights)

	// set the number of pending task back
	atomic.AddInt64(&s.scheduler.numInflightTask, -1)
}

// Test to verify that there is no race condition when a chancel reference is returned by
// getOrCreateTaskChannel to submit a task and cleanup loop deletes this channel before that happens.
func (s *interleavedWeightedRoundRobinSchedulerSuite) TestInactiveChannelDeletionRace() {
	s.scheduler = NewInterleavedWeightedRoundRobinScheduler(
		InterleavedWeightedRoundRobinSchedulerOptions[*testTask, int]{
			TaskChannelKeyFn:      func(task *testTask) int { return task.channelKey },
			ChannelWeightFn:       func(key int) int { return s.channelKeyToWeight[key] },
			ChannelWeightUpdateCh: s.channelWeightUpdateCh,
			InactiveChannelDeletionDelay: func() time.Duration {
				return 0 // Setting cleanup delay to 0 to continuously delete channels.
			},
		},
		Scheduler[*testTask](s.mockFIFOScheduler),
		log.NewTestLogger(),
	)
	s.mockFIFOScheduler.EXPECT().Start()
	s.scheduler.Start()
	s.mockFIFOScheduler.EXPECT().Stop()

	var taskWG sync.WaitGroup
	s.mockFIFOScheduler.EXPECT().Submit(gomock.Any()).Do(func(task Task) {
		taskWG.Done()
	}).AnyTimes()

	// need to manually set the number of pending task to 1
	// so schedule by task priority logic will execute
	atomic.AddInt64(&s.scheduler.numInflightTask, 1)
	defer func() {
		atomic.AddInt64(&s.scheduler.numInflightTask, -1)
	}()

	mockTask0 := newTestTask(s.controller, 0)
	mockTask1 := newTestTask(s.controller, 1)
	mockTask2 := newTestTask(s.controller, 2)
	mockTask3 := newTestTask(s.controller, 3)

	for i := 0; i < 1000; i++ {
		taskWG.Add(1)
		s.scheduler.Submit(mockTask0)
		taskWG.Wait()

		taskWG.Add(1)
		s.scheduler.Submit(mockTask1)
		taskWG.Wait()

		taskWG.Add(1)
		s.scheduler.Submit(mockTask2)
		taskWG.Wait()

		taskWG.Add(1)
		s.scheduler.Submit(mockTask3)
		taskWG.Wait()
	}

	time.Sleep(100 * time.Millisecond) //nolint:forbidigo
	s.Empty(s.scheduler.channels())
}

func newTestTask(
	controller *gomock.Controller,
	channelKey int,
) *testTask {
	return &testTask{
		MockTask:   NewMockTask(controller),
		channelKey: channelKey,
	}
}
