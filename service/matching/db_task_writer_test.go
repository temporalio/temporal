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

package matching

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	dbTaskWriterSuite struct {
		*require.Assertions
		suite.Suite

		controller    *gomock.Controller
		taskOwnership *MockdbTaskQueueOwnership

		namespaceID   string
		taskQueueName string
		taskQueueType enumspb.TaskQueueType

		taskFlusher *dbTaskWriterImpl
	}
)

func TestDBTaskWriterSuite(t *testing.T) {
	s := new(dbTaskWriterSuite)
	suite.Run(t, s)
}

func (s *dbTaskWriterSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *dbTaskWriterSuite) TearDownSuite() {

}

func (s *dbTaskWriterSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.taskOwnership = NewMockdbTaskQueueOwnership(s.controller)

	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TASK_QUEUE_TYPE_ACTIVITY

	s.taskFlusher = newDBTaskWriter(
		persistence.TaskQueueKey{
			NamespaceID:   s.namespaceID,
			TaskQueueName: s.taskQueueName,
			TaskQueueType: s.taskQueueType,
		},
		s.taskOwnership,
		log.NewTestLogger(),
	)
}

func (s *dbTaskWriterSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *dbTaskWriterSuite) TestAppendFlushTask_Once_Success() {
	ctx := context.Background()
	task := s.randomTask()

	s.taskOwnership.EXPECT().flushTasks(gomock.Any(), task).Return(nil)

	fut := s.taskFlusher.appendTask(task)
	s.taskFlusher.flushTasks(context.Background())

	_, err := fut.Get(ctx)
	s.NoError(err)
	select {
	case <-s.taskFlusher.notifyFlushChan():
		s.Fail("there should be no signal")
	default:
		// test pass
	}
}

func (s *dbTaskWriterSuite) TestAppendFlushTask_Once_Failed() {
	ctx := context.Background()
	task := s.randomTask()
	randomErr := serviceerror.NewUnavailable("random error")

	s.taskOwnership.EXPECT().flushTasks(gomock.Any(), task).Return(randomErr)

	fut := s.taskFlusher.appendTask(task)
	s.taskFlusher.flushTasks(context.Background())

	_, err := fut.Get(ctx)
	s.Equal(randomErr, err)
	select {
	case <-s.taskFlusher.notifyFlushChan():
		s.Fail("there should be no signal")
	default:
		// test pass
	}
}

func (s *dbTaskWriterSuite) TestAppendFlushTask_Multiple_OnePage_Success() {
	numTasks := dbTaskFlusherBatchSize - 1
	ctx := context.Background()

	var futures []future.Future[struct{}]
	var tasks []interface{}
	for i := 0; i < numTasks; i++ {
		task := s.randomTask()
		fut := s.taskFlusher.appendTask(task)
		tasks = append(tasks, task)
		futures = append(futures, fut)
	}

	s.taskOwnership.EXPECT().flushTasks(gomock.Any(), tasks...).Return(nil)

	s.taskFlusher.flushTasks(context.Background())

	for _, fut := range futures {
		_, err := fut.Get(ctx)
		s.NoError(err)
	}
	select {
	case <-s.taskFlusher.notifyFlushChan():
		s.Fail("there should be no signal")
	default:
		// test pass
	}
}

func (s *dbTaskWriterSuite) TestAppendFlushTask_Multiple_OnePage_Failed() {
	numTasks := dbTaskFlusherBatchSize - 1
	ctx := context.Background()
	randomErr := serviceerror.NewUnavailable("random error")

	var futures []future.Future[struct{}]
	var tasks []interface{}
	for i := 0; i < numTasks; i++ {
		task := s.randomTask()
		fut := s.taskFlusher.appendTask(task)
		tasks = append(tasks, task)
		futures = append(futures, fut)
	}

	s.taskOwnership.EXPECT().flushTasks(gomock.Any(), tasks...).Return(randomErr)

	s.taskFlusher.flushTasks(context.Background())

	for _, fut := range futures {
		_, err := fut.Get(ctx)
		s.Equal(randomErr, err)
	}
	select {
	case <-s.taskFlusher.notifyFlushChan():
		s.Fail("there should be no signal")
	default:
		// test pass
	}
}

func (s *dbTaskWriterSuite) TestAppendFlushTask_Multiple_MultiPage_Success() {
	numTasks := 2*dbTaskFlusherBatchSize - 2
	ctx := context.Background()

	var futures []future.Future[struct{}]
	var taskBatch [][]interface{}
	var tasks []interface{}
	for i := 0; i < numTasks; i++ {
		task := s.randomTask()
		fut := s.taskFlusher.appendTask(task)
		if len(tasks) >= dbTaskFlusherBatchSize {
			taskBatch = append(taskBatch, tasks)
			tasks = nil
		}
		tasks = append(tasks, task)
		futures = append(futures, fut)
	}
	if len(tasks) > 0 {
		taskBatch = append(taskBatch, tasks)
	}

	for _, tasks := range taskBatch {
		s.taskOwnership.EXPECT().flushTasks(gomock.Any(), tasks...).Return(nil)
	}

	s.taskFlusher.flushTasks(context.Background())

	for _, fut := range futures {
		_, err := fut.Get(ctx)
		s.NoError(err)
	}
	<-s.taskFlusher.notifyFlushChan()
}

func (s *dbTaskWriterSuite) TestAppendFlushTask_Multiple_MultiPage_Failed() {
	numTasks := 2*dbTaskFlusherBatchSize - 2
	ctx := context.Background()
	randomErr := serviceerror.NewUnavailable("random error")

	var futures []future.Future[struct{}]
	var taskBatch [][]interface{}
	var tasks []interface{}
	for i := 0; i < numTasks; i++ {
		task := s.randomTask()
		fut := s.taskFlusher.appendTask(task)
		if len(tasks) >= dbTaskFlusherBatchSize {
			taskBatch = append(taskBatch, tasks)
			tasks = nil
		}
		tasks = append(tasks, task)
		futures = append(futures, fut)
	}
	if len(tasks) > 0 {
		taskBatch = append(taskBatch, tasks)
	}

	for _, tasks := range taskBatch {
		s.taskOwnership.EXPECT().flushTasks(gomock.Any(), tasks...).Return(randomErr)
	}

	s.taskFlusher.flushTasks(context.Background())

	for _, fut := range futures {
		_, err := fut.Get(ctx)
		s.Equal(randomErr, err)
	}
	<-s.taskFlusher.notifyFlushChan()
}

func (s *dbTaskWriterSuite) randomTask() *persistencespb.TaskInfo {
	return &persistencespb.TaskInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       uuid.New().String(),
		RunId:            uuid.New().String(),
		ScheduledEventId: rand.Int63(),
		CreateTime:       timestamp.TimePtr(time.Unix(0, rand.Int63())),
		ExpiryTime:       timestamp.TimePtr(time.Unix(0, rand.Int63())),
	}
}
