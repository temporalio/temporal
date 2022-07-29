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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	parallelProcessorSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		processor   *ParallelProcessor
		retryPolicy backoff.RetryPolicy
	}
)

func TestParallelProcessorSuite(t *testing.T) {
	s := new(parallelProcessorSuite)
	suite.Run(t, s)
}

func (s *parallelProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.retryPolicy = backoff.NewExponentialRetryPolicy(time.Millisecond)
	s.processor = s.newTestProcessor()
	s.processor.Start()
}

func (s *parallelProcessorSuite) TearDownTest() {
	s.processor.Stop()
	s.controller.Finish()
}

func (s *parallelProcessorSuite) TestSubmitProcess_Running_Success() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	mockTask.EXPECT().Execute().Return(nil).Times(1)
	mockTask.EXPECT().Ack().Do(func() { testWaitGroup.Done() }).Times(1)

	s.processor.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *parallelProcessorSuite) TestSubmitProcess_Running_FailExecution() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	executionErr := errors.New("random error")
	mockTask.EXPECT().Execute().Return(executionErr).Times(1)
	mockTask.EXPECT().HandleErr(executionErr).Return(executionErr).Times(1)
	mockTask.EXPECT().IsRetryableError(executionErr).Return(false).MaxTimes(1)
	mockTask.EXPECT().Nack(executionErr).Do(func(_ error) { testWaitGroup.Done() }).Times(1)

	s.processor.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *parallelProcessorSuite) TestSubmitProcess_Stopped_Submission() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	s.processor.Stop()

	mockTask := NewMockTask(s.controller)

	// if task get picked up before worker goroutine receives the shutdown notification
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).MaxTimes(1)
	mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
	mockTask.EXPECT().Ack().Do(func() { testWaitGroup.Done() }).MaxTimes(1)

	// if task get drained
	mockTask.EXPECT().Reschedule().Do(func() { testWaitGroup.Done() }).MaxTimes(1)

	s.processor.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *parallelProcessorSuite) TestSubmitProcess_Stopped_FailExecution() {
	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(1)

	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
	executionErr := errors.New("random transient error")
	mockTask.EXPECT().Execute().Return(executionErr).Times(1)
	mockTask.EXPECT().HandleErr(executionErr).DoAndReturn(func(err error) error {
		s.processor.Stop()
		return err
	}).Times(1)
	mockTask.EXPECT().IsRetryableError(executionErr).Return(true).MaxTimes(1)
	mockTask.EXPECT().Reschedule().Do(func() { testWaitGroup.Done() }).Times(1)

	s.processor.Submit(mockTask)

	testWaitGroup.Wait()
}

func (s *parallelProcessorSuite) TestParallelSubmitProcess() {
	numSubmitter := 200
	numTasks := 100

	testWaitGroup := sync.WaitGroup{}
	testWaitGroup.Add(numSubmitter * numTasks)

	startWaitGroup := sync.WaitGroup{}
	endWaitGroup := sync.WaitGroup{}

	startWaitGroup.Add(numSubmitter)

	for i := 0; i < numSubmitter; i++ {
		channel := make(chan Task, numTasks)
		for j := 0; j < numTasks; j++ {
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().RetryPolicy().Return(s.retryPolicy).AnyTimes()
			switch j % 2 {
			case 0:
				// success
				mockTask.EXPECT().Execute().Return(nil).Times(1)
				mockTask.EXPECT().Ack().Do(func() { testWaitGroup.Done() }).Times(1)

			case 1:
				// fail
				executionErr := errors.New("random error")
				mockTask.EXPECT().Execute().Return(executionErr).Times(1)
				mockTask.EXPECT().HandleErr(executionErr).Return(executionErr).Times(1)
				mockTask.EXPECT().IsRetryableError(executionErr).Return(false).Times(1)
				mockTask.EXPECT().Nack(executionErr).Do(func(_ error) { testWaitGroup.Done() }).Times(1)

			default:
				s.Fail("case not expected")
			}
			channel <- mockTask
		}
		close(channel)

		endWaitGroup.Add(1)
		go func() {
			startWaitGroup.Wait()

			for mockTask := range channel {
				s.processor.Submit(mockTask)
			}

			endWaitGroup.Done()
		}()
		startWaitGroup.Done()
	}
	endWaitGroup.Wait()

	testWaitGroup.Wait()
}

func (s *parallelProcessorSuite) TestStartStopWorkers() {
	processor := s.newTestProcessor()
	// don't start the processor,
	// manually add/remove workers here to test the start/stop logic

	numWorkers := 10
	processor.startWorkers(numWorkers)
	s.Len(processor.workerShutdownCh, numWorkers)

	processor.stopWorkers(numWorkers / 2)
	s.Len(processor.workerShutdownCh, numWorkers/2)

	processor.stopWorkers(len(processor.workerShutdownCh))
	s.Empty(processor.workerShutdownCh)

	processor.shutdownWG.Wait()
}

func (s *parallelProcessorSuite) newTestProcessor() *ParallelProcessor {
	return NewParallelProcessor(
		&ParallelProcessorOptions{
			QueueSize:   1,
			WorkerCount: dynamicconfig.GetIntPropertyFn(1),
		},
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
	)
}
