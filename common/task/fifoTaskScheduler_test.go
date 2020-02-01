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
	"testing"
	"time"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
)

type (
	fifoTaskSchedulerSuite struct {
		*require.Assertions
		suite.Suite

		controller    *gomock.Controller
		mockProcessor *MockProcessor

		scheduler *fifoTaskSchedulerImpl
	}
)

func TestFIFOTaskSchedulerSuite(t *testing.T) {
	s := new(fifoTaskSchedulerSuite)
	suite.Run(t, s)
}

func (s *fifoTaskSchedulerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockProcessor = NewMockProcessor(s.controller)

	s.scheduler = NewFIFOTaskScheduler(
		loggerimpl.NewDevelopmentForTest(s.Suite),
		metrics.NewClient(tally.NoopScope, metrics.Common).Scope(metrics.TaskSchedulerScope),
		&FIFOTaskSchedulerOptions{
			QueueSize:   0,
			WorkerCount: 1,
			RetryPolicy: backoff.NewExponentialRetryPolicy(time.Millisecond),
		},
	).(*fifoTaskSchedulerImpl)
}

func (s *fifoTaskSchedulerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *fifoTaskSchedulerSuite) TestFIFO() {
	numTasks := 5
	tasks := []PriorityTask{}

	calls := []*gomock.Call{
		s.mockProcessor.EXPECT().Start(),
	}
	for i := 0; i != numTasks; i++ {
		mockTask := NewMockPriorityTask(s.controller)
		tasks = append(tasks, mockTask)
		calls = append(calls, s.mockProcessor.EXPECT().Submit(newMockPriorityTaskMatcher(mockTask)).Return(nil))
	}
	calls = append(calls, s.mockProcessor.EXPECT().Stop())
	gomock.InOrder(calls...)

	s.scheduler.processor = s.mockProcessor
	s.scheduler.Start()
	for _, task := range tasks {
		s.scheduler.Submit(task)
	}
	s.scheduler.Stop()
}
