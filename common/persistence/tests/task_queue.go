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

package tests

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	TaskQueueSuite struct {
		suite.Suite
		*require.Assertions

		stickyTTL     time.Duration
		namespaceID   string
		taskQueueName string
		taskQueueType enumspb.TaskQueueType

		taskManager p.TaskManager
		logger      log.Logger

		ctx    context.Context
		cancel context.CancelFunc
	}
)

func NewTaskQueueSuite(
	t *testing.T,
	taskManager p.TaskStore,
	logger log.Logger,
) *TaskQueueSuite {
	return &TaskQueueSuite{
		Assertions: require.New(t),
		taskManager: p.NewTaskManager(
			taskManager,
			serialization.NewSerializer(),
		),
		logger: logger,
	}
}

func (s *TaskQueueSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *TaskQueueSuite) TearDownSuite() {

}

func (s *TaskQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), time.Second*30)

	s.stickyTTL = time.Second * 10
	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TaskQueueType(rand.Int31n(
		int32(len(enumspb.TaskQueueType_name)) + 1),
	)
}

func (s *TaskQueueSuite) TearDownTest() {
	s.cancel()
}

func (s *TaskQueueSuite) TestCreate_Normal() {
	rangID := rand.Int63()
	taskQueue := s.createTaskQueue(rangID, enumspb.TASK_QUEUE_KIND_NORMAL)

	s.assertEqualWithDB(rangID, taskQueue)
}

func (s *TaskQueueSuite) TestCreate_Sticky() {
	rangID := rand.Int63()
	taskQueue := s.createTaskQueue(rangID, enumspb.TASK_QUEUE_KIND_STICKY)

	s.assertEqualWithDB(rangID, taskQueue)
}

func (s *TaskQueueSuite) TestCreate_Normal_Dup() {
	rangID := rand.Int63()
	taskQueue := s.createTaskQueue(rangID, enumspb.TASK_QUEUE_KIND_NORMAL)

	_, err := s.taskManager.CreateTaskQueue(s.ctx, &p.CreateTaskQueueRequest{
		RangeID:       rangID,
		TaskQueueInfo: s.randomTaskQueueInfo(enumspb.TASK_QUEUE_KIND_NORMAL),
	})
	s.IsType(&p.ConditionFailedError{}, err)

	s.assertEqualWithDB(rangID, taskQueue)
}

func (s *TaskQueueSuite) TestCreate_Sticky_Dup() {
	rangID := rand.Int63()
	taskQueue := s.createTaskQueue(rangID, enumspb.TASK_QUEUE_KIND_STICKY)

	_, err := s.taskManager.CreateTaskQueue(s.ctx, &p.CreateTaskQueueRequest{
		RangeID:       rangID,
		TaskQueueInfo: s.randomTaskQueueInfo(enumspb.TASK_QUEUE_KIND_STICKY),
	})
	s.IsType(&p.ConditionFailedError{}, err)

	s.assertEqualWithDB(rangID, taskQueue)
}

func (s *TaskQueueSuite) TestUpdate_Normal() {
	prevRangeID := rand.Int63()
	_ = s.createTaskQueue(prevRangeID, enumspb.TASK_QUEUE_KIND_NORMAL)

	rangID := rand.Int63()
	taskQueue := s.randomTaskQueueInfo(enumspb.TASK_QUEUE_KIND_NORMAL)
	_, err := s.taskManager.UpdateTaskQueue(s.ctx, &p.UpdateTaskQueueRequest{
		RangeID:       rangID,
		TaskQueueInfo: taskQueue,

		PrevRangeID: prevRangeID,
	})
	s.NoError(err)

	s.assertEqualWithDB(rangID, taskQueue)
}

func (s *TaskQueueSuite) TestUpdate_Normal_Conflict() {
	prevRangeID := rand.Int63()
	taskQueue := s.createTaskQueue(prevRangeID, enumspb.TASK_QUEUE_KIND_NORMAL)

	rangID := rand.Int63()
	_, err := s.taskManager.UpdateTaskQueue(s.ctx, &p.UpdateTaskQueueRequest{
		RangeID:       rangID,
		TaskQueueInfo: s.randomTaskQueueInfo(enumspb.TASK_QUEUE_KIND_NORMAL),

		PrevRangeID: rand.Int63(),
	})
	s.IsType(&p.ConditionFailedError{}, err)

	s.assertEqualWithDB(prevRangeID, taskQueue)
}

func (s *TaskQueueSuite) TestUpdate_Sticky() {
	prevRangeID := rand.Int63()
	_ = s.createTaskQueue(prevRangeID, enumspb.TASK_QUEUE_KIND_STICKY)

	rangID := rand.Int63()
	taskQueue := s.randomTaskQueueInfo(enumspb.TASK_QUEUE_KIND_STICKY)
	_, err := s.taskManager.UpdateTaskQueue(s.ctx, &p.UpdateTaskQueueRequest{
		RangeID:       rangID,
		TaskQueueInfo: taskQueue,

		PrevRangeID: prevRangeID,
	})
	s.NoError(err)

	s.assertEqualWithDB(rangID, taskQueue)
}

func (s *TaskQueueSuite) TestUpdate_Sticky_Conflict() {
	prevRangeID := rand.Int63()
	taskQueue := s.createTaskQueue(prevRangeID, enumspb.TASK_QUEUE_KIND_STICKY)

	rangID := rand.Int63()
	_, err := s.taskManager.UpdateTaskQueue(s.ctx, &p.UpdateTaskQueueRequest{
		RangeID:       rangID,
		TaskQueueInfo: s.randomTaskQueueInfo(enumspb.TASK_QUEUE_KIND_STICKY),

		PrevRangeID: rand.Int63(),
	})
	s.IsType(&p.ConditionFailedError{}, err)

	s.assertEqualWithDB(prevRangeID, taskQueue)
}

func (s *TaskQueueSuite) TestDelete() {
	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(
		rangeID,
		enumspb.TaskQueueKind(rand.Int31n(
			int32(len(enumspb.TaskQueueKind_name))+1),
		),
	)

	err := s.taskManager.DeleteTaskQueue(s.ctx, &p.DeleteTaskQueueRequest{
		TaskQueue: &p.TaskQueueKey{
			NamespaceID:   taskQueue.NamespaceId,
			TaskQueueName: taskQueue.Name,
			TaskQueueType: taskQueue.TaskType,
		},
		RangeID: rangeID,
	})
	s.NoError(err)

	s.assertMissingFromDB(taskQueue.NamespaceId, taskQueue.Name, taskQueue.TaskType)
}

func (s *TaskQueueSuite) TestDelete_Conflict() {
	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(
		rangeID,
		enumspb.TaskQueueKind(rand.Int31n(
			int32(len(enumspb.TaskQueueKind_name))+1),
		),
	)

	err := s.taskManager.DeleteTaskQueue(s.ctx, &p.DeleteTaskQueueRequest{
		TaskQueue: &p.TaskQueueKey{
			NamespaceID:   taskQueue.NamespaceId,
			TaskQueueName: taskQueue.Name,
			TaskQueueType: taskQueue.TaskType,
		},
		RangeID: rand.Int63(),
	})
	s.IsType(&p.ConditionFailedError{}, err)

	s.assertEqualWithDB(rangeID, taskQueue)
}

func (s *TaskQueueSuite) TesList() {
	// TODO there exists a SQL impl, but no cassandra impl ...
}

func (s *TaskQueueSuite) createTaskQueue(
	rangeID int64,
	taskQueueKind enumspb.TaskQueueKind,
) *persistencespb.TaskQueueInfo {
	taskQueue := s.randomTaskQueueInfo(taskQueueKind)
	_, err := s.taskManager.CreateTaskQueue(s.ctx, &p.CreateTaskQueueRequest{
		RangeID:       rangeID,
		TaskQueueInfo: taskQueue,
	})
	s.NoError(err)
	return taskQueue
}

func (s *TaskQueueSuite) randomTaskQueueInfo(
	taskQueueKind enumspb.TaskQueueKind,
) *persistencespb.TaskQueueInfo {
	now := timestamp.TimePtr(time.Now().UTC())
	var expiryTime *time.Time
	if taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		expiryTime = timestamp.TimePtr(now.Add(s.stickyTTL))
	}

	return &persistencespb.TaskQueueInfo{
		NamespaceId:    s.namespaceID,
		Name:           s.taskQueueName,
		TaskType:       s.taskQueueType,
		Kind:           taskQueueKind,
		AckLevel:       rand.Int63(),
		ExpiryTime:     expiryTime,
		LastUpdateTime: now,
	}
}

func (s *TaskQueueSuite) assertMissingFromDB(
	namespaceID string,
	taskQueue string,
	taskType enumspb.TaskQueueType,
) {
	_, err := s.taskManager.GetTaskQueue(s.ctx, &p.GetTaskQueueRequest{
		NamespaceID: namespaceID,
		TaskQueue:   taskQueue,
		TaskType:    taskType,
	})
	s.IsType(&serviceerror.NotFound{}, err)
}

func (s *TaskQueueSuite) assertEqualWithDB(
	rangeID int64,
	taskQueueInfo *persistencespb.TaskQueueInfo,
) {
	resp, err := s.taskManager.GetTaskQueue(s.ctx, &p.GetTaskQueueRequest{
		NamespaceID: taskQueueInfo.NamespaceId,
		TaskQueue:   taskQueueInfo.Name,
		TaskType:    taskQueueInfo.TaskType,
	})
	s.NoError(err)

	s.Equal(rangeID, resp.RangeID)
	s.Equal(taskQueueInfo, resp.TaskQueueInfo)
}
