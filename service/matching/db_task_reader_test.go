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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/shuffle"
)

type (
	dbTaskReaderSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller
		taskStore  *persistence.MockTaskManager

		namespaceID   string
		taskQueueName string
		taskQueueType enumspb.TaskQueueType
		ackedTaskID   int64
		maxTaskID     int64

		taskTracker *dbTaskReaderImpl
	}
)

func TestDBTaskReaderSuite(t *testing.T) {
	s := new(dbTaskReaderSuite)
	suite.Run(t, s)
}

func (s *dbTaskReaderSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *dbTaskReaderSuite) TearDownSuite() {

}

func (s *dbTaskReaderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.taskStore = persistence.NewMockTaskManager(s.controller)

	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TASK_QUEUE_TYPE_ACTIVITY
	s.ackedTaskID = rand.Int63()
	s.maxTaskID = s.ackedTaskID + 1000

	s.taskTracker = newDBTaskReader(
		persistence.TaskQueueKey{
			NamespaceID:   s.namespaceID,
			TaskQueueName: s.taskQueueName,
			TaskQueueType: s.taskQueueType,
		},
		s.taskStore,
		s.ackedTaskID,
		log.NewTestLogger(),
	)
}

func (s *dbTaskReaderSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *dbTaskReaderSuite) TestIteration_Error() {
	s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		PageSize:           dbTaskReaderPageSize,
		InclusiveMinTaskID: s.ackedTaskID + 1,
		ExclusiveMaxTaskID: s.maxTaskID + 1,
		NextPageToken:      nil,
	}).Return(nil, serviceerror.NewInternal("random error"))

	iter := s.taskTracker.taskIterator(context.Background(), s.maxTaskID)
	for iter.HasNext() {
		_, err := iter.Next()
		s.Error(err)
	}

	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)
	s.Equal(s.ackedTaskID, s.taskTracker.loadedTaskID)
	s.Equal(map[int64]bool{}, s.taskTracker.tasks)
}

func (s *dbTaskReaderSuite) TestIteration_ErrorRetry() {
	taskID1 := s.ackedTaskID + 1
	tasks1 := []*persistencespb.AllocatedTaskInfo{
		{TaskId: taskID1},
	}
	token := shuffle.Bytes([]byte("random page token"))
	taskID2 := s.ackedTaskID + 3
	tasks2 := []*persistencespb.AllocatedTaskInfo{
		{TaskId: taskID2},
	}
	gomock.InOrder(
		s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
			NamespaceID:        s.namespaceID,
			TaskQueue:          s.taskQueueName,
			TaskType:           s.taskQueueType,
			PageSize:           dbTaskReaderPageSize,
			InclusiveMinTaskID: s.ackedTaskID + 1,
			ExclusiveMaxTaskID: s.maxTaskID + 1,
			NextPageToken:      nil,
		}).Return(&persistence.GetTasksResponse{
			Tasks:         tasks1,
			NextPageToken: token,
		}, nil),
		s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
			NamespaceID:        s.namespaceID,
			TaskQueue:          s.taskQueueName,
			TaskType:           s.taskQueueType,
			PageSize:           dbTaskReaderPageSize,
			InclusiveMinTaskID: s.ackedTaskID + 1,
			ExclusiveMaxTaskID: s.maxTaskID + 1,
			NextPageToken:      token,
		}).Return(nil, serviceerror.NewInternal("some random error")),
		s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
			NamespaceID:        s.namespaceID,
			TaskQueue:          s.taskQueueName,
			TaskType:           s.taskQueueType,
			PageSize:           dbTaskReaderPageSize,
			InclusiveMinTaskID: taskID1 + 1,
			ExclusiveMaxTaskID: s.maxTaskID + 1,
			NextPageToken:      nil,
		}).Return(&persistence.GetTasksResponse{
			Tasks:         tasks2,
			NextPageToken: nil,
		}, nil),
	)

	iter := s.taskTracker.taskIterator(context.Background(), s.maxTaskID)
	var actualTasks []*persistencespb.AllocatedTaskInfo
	for iter.HasNext() {
		item, err := iter.Next()
		if err != nil {
			break
		}
		actualTasks = append(actualTasks, item)
	}
	s.Equal(tasks1, actualTasks)
	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)
	s.Equal(taskID1, s.taskTracker.loadedTaskID)
	s.Equal(map[int64]bool{
		taskID1: false,
	}, s.taskTracker.tasks)

	iter = s.taskTracker.taskIterator(context.Background(), s.maxTaskID)
	actualTasks = nil
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		actualTasks = append(actualTasks, item)
	}
	s.Equal(tasks2, actualTasks)
	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)
	s.Equal(s.maxTaskID, s.taskTracker.loadedTaskID)
	s.Equal(map[int64]bool{
		taskID1:     false,
		taskID2:     false,
		s.maxTaskID: true,
	}, s.taskTracker.tasks)
}

func (s *dbTaskReaderSuite) TestIteration_TwoIter() {
	taskID1 := s.ackedTaskID + 1
	taskID2 := s.ackedTaskID + 3
	tasks1 := []*persistencespb.AllocatedTaskInfo{
		{TaskId: taskID1},
		{TaskId: taskID2},
	}
	taskID3 := s.ackedTaskID + 6
	taskID4 := s.ackedTaskID + 10
	tasks2 := []*persistencespb.AllocatedTaskInfo{
		{TaskId: taskID3},
		{TaskId: taskID4},
	}
	maxTaskID1 := taskID3 - 1
	maxTaskID2 := s.maxTaskID

	s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		PageSize:           dbTaskReaderPageSize,
		InclusiveMinTaskID: s.ackedTaskID + 1,
		ExclusiveMaxTaskID: maxTaskID1 + 1,
		NextPageToken:      nil,
	}).Return(&persistence.GetTasksResponse{
		Tasks:         tasks1,
		NextPageToken: nil,
	}, nil)

	iter := s.taskTracker.taskIterator(context.Background(), maxTaskID1)
	var actualTasks []*persistencespb.AllocatedTaskInfo
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		actualTasks = append(actualTasks, item)
	}
	s.Equal(tasks1, actualTasks)

	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)
	s.Equal(maxTaskID1, s.taskTracker.loadedTaskID)
	s.Equal(map[int64]bool{
		taskID1:    false,
		taskID2:    false,
		maxTaskID1: true,
	}, s.taskTracker.tasks)

	s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		PageSize:           dbTaskReaderPageSize,
		InclusiveMinTaskID: s.taskTracker.loadedTaskID + 1,
		ExclusiveMaxTaskID: maxTaskID2 + 1,
		NextPageToken:      nil,
	}).Return(&persistence.GetTasksResponse{
		Tasks:         tasks2,
		NextPageToken: nil,
	}, nil)

	iter = s.taskTracker.taskIterator(context.Background(), maxTaskID2)
	actualTasks = nil
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		actualTasks = append(actualTasks, item)
	}
	s.Equal(tasks2, actualTasks)

	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)
	s.Equal(s.maxTaskID, s.taskTracker.loadedTaskID)
	s.Equal(map[int64]bool{
		taskID1:    false,
		taskID2:    false,
		maxTaskID1: true,
		taskID3:    false,
		taskID4:    false,
		maxTaskID2: true,
	}, s.taskTracker.tasks)
}

func (s *dbTaskReaderSuite) TestIteration_Pagination() {
	taskID1 := s.ackedTaskID + 1
	taskID2 := s.ackedTaskID + 3
	taskID3 := s.ackedTaskID + 6
	taskID4 := s.ackedTaskID + 10
	tasks1 := []*persistencespb.AllocatedTaskInfo{
		{TaskId: taskID1},
		{TaskId: taskID2},
	}
	token := shuffle.Bytes([]byte("random page token"))
	tasks2 := []*persistencespb.AllocatedTaskInfo{
		{TaskId: taskID3},
		{TaskId: taskID4},
	}

	gomock.InOrder(
		s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
			NamespaceID:        s.namespaceID,
			TaskQueue:          s.taskQueueName,
			TaskType:           s.taskQueueType,
			PageSize:           dbTaskReaderPageSize,
			InclusiveMinTaskID: s.ackedTaskID + 1,
			ExclusiveMaxTaskID: s.maxTaskID + 1,
			NextPageToken:      nil,
		}).Return(&persistence.GetTasksResponse{
			Tasks:         tasks1,
			NextPageToken: token,
		}, nil),
		s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
			NamespaceID:        s.namespaceID,
			TaskQueue:          s.taskQueueName,
			TaskType:           s.taskQueueType,
			PageSize:           dbTaskReaderPageSize,
			InclusiveMinTaskID: s.ackedTaskID + 1,
			ExclusiveMaxTaskID: s.maxTaskID + 1,
			NextPageToken:      token,
		}).Return(&persistence.GetTasksResponse{
			Tasks:         tasks2,
			NextPageToken: nil,
		}, nil),
	)

	iter := s.taskTracker.taskIterator(context.Background(), s.maxTaskID)
	var actualTasks []*persistencespb.AllocatedTaskInfo
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		actualTasks = append(actualTasks, item)
	}
	s.Equal(append(tasks1, tasks2...), actualTasks)

	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)
	s.Equal(s.maxTaskID, s.taskTracker.loadedTaskID)
	s.Equal(map[int64]bool{
		taskID1:     false,
		taskID2:     false,
		taskID3:     false,
		taskID4:     false,
		s.maxTaskID: true,
	}, s.taskTracker.tasks)
}

func (s *dbTaskReaderSuite) TestIteration_MaxTaskID_Exists() {
	taskID1 := s.ackedTaskID + 1
	taskID2 := s.ackedTaskID + 3
	taskID3 := s.maxTaskID
	tasks := []*persistencespb.AllocatedTaskInfo{
		{TaskId: taskID1},
		{TaskId: taskID2},
		{TaskId: taskID3},
	}

	s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		PageSize:           dbTaskReaderPageSize,
		InclusiveMinTaskID: s.ackedTaskID + 1,
		ExclusiveMaxTaskID: s.maxTaskID + 1,
		NextPageToken:      nil,
	}).Return(&persistence.GetTasksResponse{
		Tasks:         tasks,
		NextPageToken: nil,
	}, nil)

	iter := s.taskTracker.taskIterator(context.Background(), s.maxTaskID)
	var actualTasks []*persistencespb.AllocatedTaskInfo
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		actualTasks = append(actualTasks, item)
	}
	s.Equal(tasks, actualTasks)

	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)
	s.Equal(s.maxTaskID, s.taskTracker.loadedTaskID)
	s.Equal(map[int64]bool{
		taskID1: false,
		taskID2: false,
		taskID3: false,
	}, s.taskTracker.tasks)
}

func (s *dbTaskReaderSuite) TestIteration_MaxTaskID_Missing() {
	taskID1 := s.ackedTaskID + 1
	taskID2 := s.ackedTaskID + 3
	tasks := []*persistencespb.AllocatedTaskInfo{
		{TaskId: taskID1},
		{TaskId: taskID2},
	}

	s.taskStore.EXPECT().GetTasks(gomock.Any(), &persistence.GetTasksRequest{
		NamespaceID:        s.namespaceID,
		TaskQueue:          s.taskQueueName,
		TaskType:           s.taskQueueType,
		PageSize:           dbTaskReaderPageSize,
		InclusiveMinTaskID: s.ackedTaskID + 1,
		ExclusiveMaxTaskID: s.maxTaskID + 1,
		NextPageToken:      nil,
	}).Return(&persistence.GetTasksResponse{
		Tasks:         tasks,
		NextPageToken: nil,
	}, nil)

	iter := s.taskTracker.taskIterator(context.Background(), s.maxTaskID)
	var actualTasks []*persistencespb.AllocatedTaskInfo
	for iter.HasNext() {
		item, err := iter.Next()
		s.NoError(err)
		actualTasks = append(actualTasks, item)
	}
	s.Equal(tasks, actualTasks)

	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)
	s.Equal(s.maxTaskID, s.taskTracker.loadedTaskID)
	s.Equal(map[int64]bool{
		taskID1:     false,
		taskID2:     false,
		s.maxTaskID: true,
	}, s.taskTracker.tasks)
}

func (s *dbTaskReaderSuite) TestAck_Exist() {
	taskID := s.ackedTaskID + 12
	s.taskTracker.tasks[taskID] = false

	s.taskTracker.ackTask(taskID)
	s.True(s.taskTracker.tasks[taskID])
}

func (s *dbTaskReaderSuite) TestAck_NotExist() {
	taskID := s.ackedTaskID + 14

	s.taskTracker.ackTask(taskID)
	_, ok := s.taskTracker.tasks[taskID]
	s.False(ok)
}

func (s *dbTaskReaderSuite) TestMoveAckedTaskID() {
	taskID1 := s.ackedTaskID + 1
	taskID2 := s.ackedTaskID + 3
	taskID3 := s.ackedTaskID + 6
	taskID4 := s.ackedTaskID + 10

	s.taskTracker.tasks = map[int64]bool{
		taskID1: false,
		taskID2: false,
		taskID3: false,
		taskID4: false,
	}
	s.taskTracker.loadedTaskID = taskID4

	s.Equal(s.ackedTaskID, s.taskTracker.moveAckedTaskID())
	s.Equal(s.ackedTaskID, s.taskTracker.ackedTaskID)

	s.taskTracker.ackTask(taskID1)
	s.Equal(taskID1, s.taskTracker.moveAckedTaskID())
	s.Equal(taskID1, s.taskTracker.ackedTaskID)
	s.Equal(map[int64]bool{
		taskID2: false,
		taskID3: false,
		taskID4: false,
	}, s.taskTracker.tasks)

	s.taskTracker.ackTask(taskID3)
	s.Equal(taskID1, s.taskTracker.moveAckedTaskID())
	s.Equal(taskID1, s.taskTracker.ackedTaskID)
	s.Equal(map[int64]bool{
		taskID2: false,
		taskID3: true,
		taskID4: false,
	}, s.taskTracker.tasks)

	s.taskTracker.ackTask(taskID2)
	s.Equal(taskID3, s.taskTracker.moveAckedTaskID())
	s.Equal(taskID3, s.taskTracker.ackedTaskID)
	s.Equal(map[int64]bool{
		taskID4: false,
	}, s.taskTracker.tasks)

	s.taskTracker.ackTask(taskID4)
	s.Equal(taskID4, s.taskTracker.moveAckedTaskID())
	s.Equal(taskID4, s.taskTracker.ackedTaskID)
	s.Equal(map[int64]bool{}, s.taskTracker.tasks)
}
