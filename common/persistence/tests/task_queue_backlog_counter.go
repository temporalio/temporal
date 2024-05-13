// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	clockspb "go.temporal.io/server/api/clock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
)

type (
	TaskQueueBacklogCounterSuite struct {
		suite.Suite
		*require.Assertions
		protorequire.ProtoAssertions

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

func NewTaskQueueBacklogCounterSuite(
	t *testing.T,
	taskManager p.TaskStore,
	logger log.Logger,
) *TaskQueueBacklogCounterSuite {
	return &TaskQueueBacklogCounterSuite{
		Assertions: require.New(t),
		logger:     logger,
		taskManager: p.NewTaskManager(
			taskManager,
			serialization.NewSerializer()),
	}
}

func (s *TaskQueueBacklogCounterSuite) SetupSuite()    {}
func (s *TaskQueueBacklogCounterSuite) TearDownSuite() {}

func (s *TaskQueueBacklogCounterSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.ctx, s.cancel = context.WithTimeout(context.Background(), 30*time.Second*debug.TimeoutMultiplier)

	s.stickyTTL = time.Second * 10
	s.taskTTL = time.Second * 16
	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TaskQueueType(rand.Int31n(
		int32(len(enumspb.TaskQueueType_name)) + 1),
	)
}

func (s *TaskQueueBacklogCounterSuite) TearDownTest() {
	s.cancel()
}

// Helpers below

func (s *TaskQueueBacklogCounterSuite) createTaskQueue(
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

func (s *TaskQueueBacklogCounterSuite) randomTaskQueueInfo(
	taskQueueKind enumspb.TaskQueueKind,
) *persistencespb.TaskQueueInfo {
	now := time.Now().UTC()
	var expiryTime *timestamppb.Timestamp
	if taskQueueKind == enumspb.TASK_QUEUE_KIND_STICKY {
		expiryTime = timestamppb.New(now.Add(s.stickyTTL))
	}

	return &persistencespb.TaskQueueInfo{
		NamespaceId:             s.namespaceID,
		Name:                    s.taskQueueName,
		TaskType:                s.taskQueueType,
		Kind:                    taskQueueKind,
		AckLevel:                rand.Int63(),
		ExpiryTime:              expiryTime,
		LastUpdateTime:          timestamppb.New(now),
		ApproximateBacklogCount: 0,
	}
}

func (s *TaskQueueBacklogCounterSuite) randomTask(
	taskID int64,
) *persistencespb.AllocatedTaskInfo {
	now := time.Now().UTC()
	return &persistencespb.AllocatedTaskInfo{
		TaskId: taskID,
		Data: &persistencespb.TaskInfo{
			NamespaceId:      s.namespaceID,
			WorkflowId:       uuid.New().String(),
			RunId:            uuid.New().String(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       timestamppb.New(now),
			ExpiryTime:       timestamppb.New(now.Add(s.taskTTL)),
			Clock: &clockspb.VectorClock{
				ClusterId: rand.Int63(),
				ShardId:   rand.Int31(),
				Clock:     rand.Int63(),
			},
		},
	}
}

func (s *TaskQueueBacklogCounterSuite) ValidateBacklogCounterWithDB(
	rangeID int64,
	taskQueueInfo *persistencespb.TaskQueueInfo,
	expectedBacklogCounter int64,
) {
	resp, err := s.taskManager.GetTaskQueue(s.ctx, &p.GetTaskQueueRequest{
		NamespaceID: taskQueueInfo.NamespaceId,
		TaskQueue:   taskQueueInfo.Name,
		TaskType:    taskQueueInfo.TaskType,
	})
	s.NoError(err)

	s.Equal(rangeID, resp.RangeID)
	s.Equal(expectedBacklogCounter, resp.TaskQueueInfo.ApproximateBacklogCount)
}

func (s *TaskQueueBacklogCounterSuite) CreateTasksValidateBacklogCounter(numCreateBatch int, createBatchSize int) {
	numTasks := int64(createBatchSize * numCreateBatch)
	minTaskID := rand.Int63()

	rangeID := rand.Int63()
	taskQueue := s.createTaskQueue(rangeID)

	// Adding a task shall increase the backlog; manually increasing it here since
	// cassandra will persist backlog count while sql will not
	taskQueue.ApproximateBacklogCount = numTasks
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

	// asserting with db that the backlog counter was one
	s.ValidateBacklogCounterWithDB(rangeID, taskQueue, numTasks)
}

// Tests

func (s *TaskQueueBacklogCounterSuite) TestCreateSingleTaskValidateCounter() {
	s.CreateTasksValidateBacklogCounter(1, 1)

}

func (s *TaskQueueBacklogCounterSuite) TestCreateMultipleTaskValidateCounter() {
	s.CreateTasksValidateBacklogCounter(32, 32)
}
