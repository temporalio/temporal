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

	clockspb "go.temporal.io/server/api/clock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	TaskQueueTaskSuite struct {
		suite.Suite
		*require.Assertions

		stickyTTL     time.Duration
		taskTTL       time.Duration
		namespaceID   string
		taskQueueName string
		taskQueueType enumspb.TaskQueueType

		taskManager p.TaskManager
		logger      log.Logger

		ctx    context.Context
		cancel context.CancelFunc
	}
)

func NewTaskQueueTaskSuite(
	t *testing.T,
	taskManager p.TaskStore,
	logger log.Logger,
) *TaskQueueTaskSuite {
	return &TaskQueueTaskSuite{
		Assertions: require.New(t),
		taskManager: p.NewTaskManager(
			taskManager,
			serialization.NewSerializer(),
		),
		logger: logger,
	}
}

func (s *TaskQueueTaskSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *TaskQueueTaskSuite) TearDownSuite() {

}

func (s *TaskQueueTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), time.Second*30)

	s.stickyTTL = time.Second * 10
	s.taskTTL = time.Second * 16
	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TaskQueueType(rand.Int31n(
		int32(len(enumspb.TaskQueueType_name)) + 1),
	)
}

func (s *TaskQueueTaskSuite) TearDownTest() {
	s.cancel()
}

func (s *TaskQueueTaskSuite) TestCreateGet_Conflict() {
	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(rangeID)

	taskID := rand.Int63()
	task := s.randomTask(taskID)
	_, err := s.taskManager.CreateTasks(s.ctx, &p.CreateTasksRequest{
		TaskQueueInfo: &p.PersistedTaskQueueInfo{
			RangeID: rand.Int63(),
			Data:    taskQueue,
		},
		Tasks: []*persistencespb.AllocatedTaskInfo{task},
	})
	s.IsType(&p.ConditionFailedError{}, err)

	resp, err := s.taskManager.GetTasks(s.ctx, &p.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           100,
		NextPageToken:      nil,
	})
	s.NoError(err)
	s.Equal([]*persistencespb.AllocatedTaskInfo{}, resp.Tasks)
	s.Nil(resp.NextPageToken)
}

func (s *TaskQueueTaskSuite) TestCreateGet_One() {
	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(rangeID)

	taskID := rand.Int63()
	task := s.randomTask(taskID)
	_, err := s.taskManager.CreateTasks(s.ctx, &p.CreateTasksRequest{
		TaskQueueInfo: &p.PersistedTaskQueueInfo{
			RangeID: rangeID,
			Data:    taskQueue,
		},
		Tasks: []*persistencespb.AllocatedTaskInfo{task},
	})
	s.NoError(err)

	resp, err := s.taskManager.GetTasks(s.ctx, &p.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           100,
		NextPageToken:      nil,
	})
	s.NoError(err)
	s.Equal([]*persistencespb.AllocatedTaskInfo{task}, resp.Tasks)
	s.Nil(resp.NextPageToken)
}

func (s *TaskQueueTaskSuite) TestCreateGet_Multiple() {
	numCreateBatch := 32
	createBatchSize := 32
	numTasks := int64(createBatchSize * numCreateBatch)
	minTaskID := rand.Int63()
	maxTaskID := minTaskID + numTasks

	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(rangeID)

	var expectedTasks []*persistencespb.AllocatedTaskInfo
	for i := 0; i < numCreateBatch; i++ {
		var tasks []*persistencespb.AllocatedTaskInfo
		for j := 0; j < createBatchSize; j++ {
			taskID := minTaskID + int64(i*numCreateBatch+j)
			task := s.randomTask(taskID)
			tasks = append(tasks, task)
			expectedTasks = append(expectedTasks, task)
		}
		_, err := s.taskManager.CreateTasks(s.ctx, &p.CreateTasksRequest{
			TaskQueueInfo: &p.PersistedTaskQueueInfo{
				RangeID: rangeID,
				Data:    taskQueue,
			},
			Tasks: tasks,
		})
		s.NoError(err)
	}

	var token []byte
	var actualTasks []*persistencespb.AllocatedTaskInfo
	for doContinue := true; doContinue; doContinue = len(token) > 0 {
		resp, err := s.taskManager.GetTasks(s.ctx, &p.GetTasksRequest{
			NamespaceID:        s.namespaceID,
			TaskQueue:          s.taskQueueName,
			TaskType:           s.taskQueueType,
			InclusiveMinTaskID: minTaskID,
			ExclusiveMaxTaskID: maxTaskID + 1,
			PageSize:           1,
			NextPageToken:      token,
		})
		s.NoError(err)
		token = resp.NextPageToken
		actualTasks = append(actualTasks, resp.Tasks...)
	}
	s.Equal(expectedTasks, actualTasks)
}

func (s *TaskQueueTaskSuite) TestCreateDelete_One() {
	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(rangeID)

	taskID := rand.Int63()
	task := s.randomTask(taskID)
	_, err := s.taskManager.CreateTasks(s.ctx, &p.CreateTasksRequest{
		TaskQueueInfo: &p.PersistedTaskQueueInfo{
			RangeID: rangeID,
			Data:    taskQueue,
		},
		Tasks: []*persistencespb.AllocatedTaskInfo{task},
	})
	s.NoError(err)

	err = s.taskManager.CompleteTask(s.ctx, &p.CompleteTaskRequest{
		TaskQueue: &p.TaskQueueKey{
			NamespaceID:   s.namespaceID,
			TaskQueueName: s.taskQueueName,
			TaskQueueType: s.taskQueueType,
		},
		TaskID: taskID,
	})
	s.NoError(err)

	resp, err := s.taskManager.GetTasks(s.ctx, &p.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           100,
		NextPageToken:      nil,
	})
	s.NoError(err)
	s.Equal([]*persistencespb.AllocatedTaskInfo{}, resp.Tasks)
	s.Nil(resp.NextPageToken)
}

func (s *TaskQueueTaskSuite) TestCreateDelete_Multiple() {
	numCreateBatch := 32
	createBatchSize := 32
	numTasks := int64(createBatchSize * numCreateBatch)
	minTaskID := rand.Int63()
	maxTaskID := minTaskID + numTasks

	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(rangeID)

	for i := 0; i < numCreateBatch; i++ {
		var tasks []*persistencespb.AllocatedTaskInfo
		for j := 0; j < createBatchSize; j++ {
			taskID := minTaskID + int64(i*numCreateBatch+j)
			task := s.randomTask(taskID)
			tasks = append(tasks, task)
		}
		_, err := s.taskManager.CreateTasks(s.ctx, &p.CreateTasksRequest{
			TaskQueueInfo: &p.PersistedTaskQueueInfo{
				RangeID: rangeID,
				Data:    taskQueue,
			},
			Tasks: tasks,
		})
		s.NoError(err)
	}

	_, err := s.taskManager.CompleteTasksLessThan(s.ctx, &p.CompleteTasksLessThanRequest{
		NamespaceID:        s.namespaceID,
		TaskQueueName:      s.taskQueueName,
		TaskType:           s.taskQueueType,
		ExclusiveMaxTaskID: maxTaskID + 1,
		Limit:              int(numTasks),
	})
	s.NoError(err)

	resp, err := s.taskManager.GetTasks(s.ctx, &p.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID + 1,
		PageSize:           100,
		NextPageToken:      nil,
	})
	s.NoError(err)
	s.Equal([]*persistencespb.AllocatedTaskInfo{}, resp.Tasks)
	s.Nil(resp.NextPageToken)
}

func (s *TaskQueueTaskSuite) createTaskQueue(
	rangeID int64,
) *persistencespb.TaskQueueInfo {
	taskQueueKind := enumspb.TaskQueueKind(rand.Int31n(
		int32(len(enumspb.TaskQueueKind_name)) + 1),
	)
	taskQueue := s.randomTaskQueueInfo(taskQueueKind)
	_, err := s.taskManager.CreateTaskQueue(s.ctx, &p.CreateTaskQueueRequest{
		RangeID:       rangeID,
		TaskQueueInfo: taskQueue,
	})
	s.NoError(err)
	return taskQueue
}

func (s *TaskQueueTaskSuite) randomTaskQueueInfo(
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

func (s *TaskQueueTaskSuite) randomTask(
	taskID int64,
) *persistencespb.AllocatedTaskInfo {
	now := timestamp.TimeNowPtrUtc()
	return &persistencespb.AllocatedTaskInfo{
		TaskId: taskID,
		Data: &persistencespb.TaskInfo{
			NamespaceId:      s.namespaceID,
			WorkflowId:       uuid.New().String(),
			RunId:            uuid.New().String(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       now,
			ExpiryTime:       timestamp.TimePtr(now.Add(s.taskTTL)),
			Clock: &clockspb.VectorClock{
				ClusterId: rand.Int63(),
				ShardId:   rand.Int31(),
				Clock:     rand.Int63(),
			},
		},
	}
}
