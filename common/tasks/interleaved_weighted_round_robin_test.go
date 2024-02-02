// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tasks

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/log"
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

	s.scheduler = NewInterleavedWeightedRoundRobinScheduler(
		InterleavedWeightedRoundRobinSchedulerOptions[*testTask, int]{
			TaskChannelKeyFn:      func(task *testTask) int { return task.channelKey },
			ChannelWeightFn:       func(key int) int { return s.channelKeyToWeight[key] },
			ChannelWeightUpdateCh: s.channelWeightUpdateCh,
		},
		Scheduler[*testTask](s.mockFIFOScheduler),
		logger,
	)
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

func newTestTask(
	controller *gomock.Controller,
	channelKey int,
) *testTask {
	return &testTask{
		MockTask:   NewMockTask(controller),
		channelKey: channelKey,
	}
}
