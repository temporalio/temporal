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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	interleavedWeightedRoundRobinSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller    *gomock.Controller
		mockProcessor *MockProcessor

		scheduler *InterleavedWeightedRoundRobinScheduler
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
	s.mockProcessor = NewMockProcessor(s.controller)

	priorityToWeight := map[int]int{
		0: 5,
		1: 3,
		2: 2,
		3: 1,
	}
	logger := log.NewTestLogger()
	metricsClient := metrics.NewClient(
		&metrics.ClientConfig{},
		tally.NewTestScope("test", nil),
		metrics.Common,
	)

	s.scheduler = NewInterleavedWeightedRoundRobinScheduler(
		InterleavedWeightedRoundRobinSchedulerOptions{
			QueueSize:   2,
			WorkerCount: 1,
		},
		priorityToWeight,
		s.mockProcessor,
		metricsClient,
		logger,
	)
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TearDownTest() {
	s.scheduler.Stop()
	s.controller.Finish()
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestSubmitSchedule_Success() {
	s.mockProcessor.EXPECT().Start()
	s.scheduler.Start()
	s.mockProcessor.EXPECT().Stop()

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockPriorityTask(s.controller)
	mockTask.EXPECT().GetPriority().Return(0).AnyTimes()
	s.mockProcessor.EXPECT().Submit(mockTask).Do(func(task Task) {
		testWaitGroup.Done()
	})

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestSubmitSchedule_Fail() {
	s.mockProcessor.EXPECT().Start()
	s.scheduler.Start()
	s.mockProcessor.EXPECT().Stop()
	s.scheduler.Stop()

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockPriorityTask(s.controller)
	mockTask.EXPECT().GetPriority().Return(0).AnyTimes()
	// either drain immediately
	mockTask.EXPECT().Reschedule().Do(func() {
		testWaitGroup.Done()
	}).MaxTimes(1)
	// or process by worker
	s.mockProcessor.EXPECT().Submit(mockTask).Do(func(task Task) {
		testWaitGroup.Done()
	}).MaxTimes(1)

	s.scheduler.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestChannels() {
	var channelWeights []int

	channelWeights = nil
	mockTask0 := NewMockPriorityTask(s.controller)
	mockTask0.EXPECT().GetPriority().Return(0).AnyTimes()
	s.scheduler.Submit(mockTask0)
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 5, 5}, channelWeights)

	channelWeights = nil
	mockTask1 := NewMockPriorityTask(s.controller)
	mockTask1.EXPECT().GetPriority().Return(1).AnyTimes()
	s.scheduler.Submit(mockTask1)
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 5, 3}, channelWeights)

	channelWeights = nil
	mockTask2 := NewMockPriorityTask(s.controller)
	mockTask2.EXPECT().GetPriority().Return(2).AnyTimes()
	s.scheduler.Submit(mockTask2)
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2}, channelWeights)

	channelWeights = nil
	mockTask3 := NewMockPriorityTask(s.controller)
	mockTask3.EXPECT().GetPriority().Return(3).AnyTimes()
	s.scheduler.Submit(mockTask3)
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2, 1}, channelWeights)

	channelWeights = nil
	s.scheduler.Submit(mockTask0)
	s.scheduler.Submit(mockTask1)
	s.scheduler.Submit(mockTask2)
	s.scheduler.Submit(mockTask3)
	for _, channel := range s.scheduler.channels() {
		channelWeights = append(channelWeights, channel.Weight())
	}
	s.Equal([]int{5, 5, 5, 3, 5, 3, 2, 5, 3, 2, 1}, channelWeights)
}

func (s *interleavedWeightedRoundRobinSchedulerSuite) TestParallelSubmitSchedule() {
	s.mockProcessor.EXPECT().Start()
	s.scheduler.Start()
	s.mockProcessor.EXPECT().Stop()

	numSubmitter := 200
	numTasks := 100

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(numSubmitter * numTasks)

	startWaitGroup := sync.WaitGroup{}
	endWaitGroup := sync.WaitGroup{}

	startWaitGroup.Add(numSubmitter)

	for i := 0; i < numSubmitter; i++ {
		channel := make(chan PriorityTask, numTasks)
		for j := 0; j < numTasks; j++ {
			mockTask := NewMockPriorityTask(s.controller)
			mockTask.EXPECT().GetPriority().Return(rand.Intn(4)).AnyTimes()
			s.mockProcessor.EXPECT().Submit(mockTask).Do(func(task Task) {
				testWaitGroup.Done()
			}).Times(1)
			channel <- mockTask
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
}
