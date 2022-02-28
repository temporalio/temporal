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

package executor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
)

type (
	ExecutorTestSuite struct {
		suite.Suite
	}
	testTask struct {
		next    TaskStatus
		counter *int64
	}
)

func TestExecutionTestSuite(t *testing.T) {
	suite.Run(t, new(ExecutorTestSuite))
}

func (s *ExecutorTestSuite) TestStartStop() {
	e := NewFixedSizePoolExecutor(
		4, 4, metrics.NoopClient, metrics.TaskQueueScavengerScope)
	e.Start()
	e.Stop()
}

func (s *ExecutorTestSuite) TestTaskExecution() {
	e := NewFixedSizePoolExecutor(
		32, 100, metrics.NoopClient, metrics.TaskQueueScavengerScope)
	e.Start()
	var runCounter int64
	var startWG sync.WaitGroup
	for i := 0; i < 5; i++ {
		startWG.Add(1)
		go func() {
			defer startWG.Done()
			for i := 0; i < 20; i++ {
				if i%2 == 0 {
					e.Submit(&testTask{TaskStatusDefer, &runCounter})
					continue
				}
				e.Submit(&testTask{TaskStatusDone, &runCounter})
			}
		}()
	}
	s.True(common.AwaitWaitGroup(&startWG, time.Second*10))
	s.True(s.awaitCompletion(e))
	s.Equal(int64(150), runCounter)
	e.Stop()
}

func (s *ExecutorTestSuite) awaitCompletion(e Executor) bool {
	expiry := time.Now().UTC().Add(time.Second * 10)
	for time.Now().UTC().Before(expiry) {
		if e.TaskCount() == 0 {
			return true
		}
		time.Sleep(time.Millisecond * 50)
	}
	return false
}

func (tt *testTask) Run() TaskStatus {
	atomic.AddInt64(tt.counter, 1)
	status := tt.next
	if status == TaskStatusDefer {
		tt.next = TaskStatusDone
	}
	return status
}
