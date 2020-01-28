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

package task

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
)

type (
	parallelTaskProcessorSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		processor *parallelTaskProcessorImpl
	}
)

var (
	errRetryable    = errors.New("retryable error")
	errNonRetryable = errors.New("non-retryable error")
)

func TestParallelTaskProcessorSuite(t *testing.T) {
	s := new(parallelTaskProcessorSuite)
	suite.Run(t, s)
}

func (s *parallelTaskProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.processor = NewParallelTaskProcessor(
		loggerimpl.NewDevelopmentForTest(s.Suite),
		metrics.NewClient(tally.NoopScope, metrics.Common).Scope(metrics.ParallelTaskProcessingScope),
		&ParallelTaskProcessorOptions{
			QueueSize:   0,
			WorkerCount: 1,
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Millisecond),
		},
	).(*parallelTaskProcessorImpl)
}

func (s *parallelTaskProcessorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *parallelTaskProcessorSuite) TestSubmit_Success() {
	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().Execute().Return(nil).MaxTimes(1)
	mockTask.EXPECT().Ack().MaxTimes(1)
	s.processor.Start()
	err := s.processor.Submit(mockTask)
	s.NoError(err)
	s.processor.Stop()
}

func (s *parallelTaskProcessorSuite) TestSubmit_Fail() {
	mockTask := NewMockTask(s.controller)
	s.processor.Start()
	s.processor.Stop()
	err := s.processor.Submit(mockTask)
	s.Equal(ErrTaskProcessorClosed, err)
}

func (s *parallelTaskProcessorSuite) TestTaskWorker() {
	numTasks := 5

	done := make(chan struct{})
	s.processor.workerWG.Add(1)

	go func() {
		for i := 0; i != numTasks; i++ {
			mockTask := NewMockTask(s.controller)
			mockTask.EXPECT().Execute().Return(nil).Times(1)
			mockTask.EXPECT().Ack().Times(1)
			err := s.processor.Submit(mockTask)
			s.NoError(err)
		}
		close(s.processor.shutdownCh)
		close(done)
	}()

	s.processor.taskWorker()
	<-done
}

func (s *parallelTaskProcessorSuite) TestExecuteTask_RetryableError() {
	mockTask := NewMockTask(s.controller)
	gomock.InOrder(
		mockTask.EXPECT().Execute().Return(errRetryable),
		mockTask.EXPECT().HandleErr(errRetryable).Return(errRetryable),
		mockTask.EXPECT().RetryErr(errRetryable).Return(true),
		mockTask.EXPECT().Execute().Return(errRetryable),
		mockTask.EXPECT().HandleErr(errRetryable).Return(errRetryable),
		mockTask.EXPECT().RetryErr(errRetryable).Return(true),
		mockTask.EXPECT().Execute().Return(nil),
		mockTask.EXPECT().Ack(),
	)

	s.processor.executeTask(mockTask)
}

func (s *parallelTaskProcessorSuite) TestExecuteTask_NonRetryableError() {
	mockTask := NewMockTask(s.controller)
	gomock.InOrder(
		mockTask.EXPECT().Execute().Return(errNonRetryable),
		mockTask.EXPECT().HandleErr(errNonRetryable).Return(errNonRetryable),
		mockTask.EXPECT().RetryErr(errNonRetryable).Return(false).AnyTimes(),
		mockTask.EXPECT().Nack(),
	)

	s.processor.executeTask(mockTask)
}

func (s *parallelTaskProcessorSuite) TestExecuteTask_ProcessorStopped() {
	mockTask := NewMockTask(s.controller)
	mockTask.EXPECT().Execute().Return(errRetryable).AnyTimes()
	mockTask.EXPECT().HandleErr(errRetryable).Return(errRetryable).AnyTimes()
	mockTask.EXPECT().RetryErr(errRetryable).Return(true).AnyTimes()

	done := make(chan struct{})
	go func() {
		s.processor.executeTask(mockTask)
		close(done)
	}()

	time.Sleep(100 * time.Millisecond)
	atomic.StoreInt32(&s.processor.status, common.DaemonStatusStopped)
	<-done
}
