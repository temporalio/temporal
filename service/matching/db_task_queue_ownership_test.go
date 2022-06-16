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
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	dbTaskOwnershipSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller
		taskStore  *persistence.MockTaskManager
		timeSource *clock.EventTimeSource

		now             time.Time
		taskIDRangeSize int64
		namespaceID     string
		taskQueueName   string
		taskQueueType   enumspb.TaskQueueType
		taskQueueKind   enumspb.TaskQueueKind

		taskOwnership *dbTaskQueueOwnershipImpl
	}
)

func TestDBTaskOwnershipSuite(t *testing.T) {
	s := new(dbTaskOwnershipSuite)
	suite.Run(t, s)
}

func (s *dbTaskOwnershipSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *dbTaskOwnershipSuite) TearDownSuite() {

}

func (s *dbTaskOwnershipSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.taskStore = persistence.NewMockTaskManager(s.controller)
	s.timeSource = clock.NewEventTimeSource()

	s.now = time.Now().UTC()
	s.taskIDRangeSize = 10
	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TASK_QUEUE_TYPE_ACTIVITY
	s.taskQueueKind = enumspb.TaskQueueKind(
		rand.Int31n(int32(len(enumspb.TaskQueueKind_name))-1) + 1,
	)

	s.taskOwnership = newDBTaskQueueOwnership(
		persistence.TaskQueueKey{
			NamespaceID:   s.namespaceID,
			TaskQueueName: s.taskQueueName,
			TaskQueueType: s.taskQueueType,
		},
		s.taskQueueKind,
		s.taskIDRangeSize,
		s.taskStore,
		log.NewTestLogger(),
	)
	s.timeSource.Update(s.now)
	s.taskOwnership.timeSource = s.timeSource
}

func (s *dbTaskOwnershipSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *dbTaskOwnershipSuite) TestTaskOwnership_Create_Success() {
	s.taskStore.EXPECT().GetTaskQueue(gomock.Any(), &persistence.GetTaskQueueRequest{
		NamespaceID: s.namespaceID,
		TaskQueue:   s.taskQueueName,
		TaskType:    s.taskQueueType,
	}).Return(nil, serviceerror.NewNotFound("random error message"))
	s.taskStore.EXPECT().CreateTaskQueue(gomock.Any(), &persistence.CreateTaskQueueRequest{
		RangeID: dbTaskInitialRangeID,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId:    s.namespaceID,
			Name:           s.taskQueueName,
			TaskType:       s.taskQueueType,
			Kind:           s.taskQueueKind,
			AckLevel:       0,
			ExpiryTime:     s.taskOwnership.expiryTime(),
			LastUpdateTime: timestamp.TimePtr(s.now),
		},
	}).Return(&persistence.CreateTaskQueueResponse{}, nil)

	minTaskID, maxTaskID := rangeIDToTaskIDRange(dbTaskInitialRangeID, s.taskIDRangeSize)
	err := s.taskOwnership.takeTaskQueueOwnership(context.Background())
	s.NoError(err)
	s.Equal(s.now, *s.taskOwnership.stateLastUpdateTime)
	s.Equal(dbTaskQueueOwnershipState{
		rangeID:             dbTaskInitialRangeID,
		ackedTaskID:         0,
		lastAllocatedTaskID: 0,
		minTaskIDExclusive:  minTaskID,
		maxTaskIDInclusive:  maxTaskID,
	}, *s.taskOwnership.ownershipState)
	select {
	case <-s.taskOwnership.getShutdownChan():
		s.Fail("task ownership lost")
	default:
		// noop
	}
}

func (s *dbTaskOwnershipSuite) TestTaskOwnership_Create_Failed() {
	s.taskStore.EXPECT().GetTaskQueue(gomock.Any(), &persistence.GetTaskQueueRequest{
		NamespaceID: s.namespaceID,
		TaskQueue:   s.taskQueueName,
		TaskType:    s.taskQueueType,
	}).Return(nil, serviceerror.NewNotFound("random error message"))
	s.taskStore.EXPECT().CreateTaskQueue(gomock.Any(), &persistence.CreateTaskQueueRequest{
		RangeID: dbTaskInitialRangeID,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId:    s.namespaceID,
			Name:           s.taskQueueName,
			TaskType:       s.taskQueueType,
			Kind:           s.taskQueueKind,
			AckLevel:       0,
			ExpiryTime:     s.taskOwnership.expiryTime(),
			LastUpdateTime: timestamp.TimePtr(s.now),
		},
	}).Return(nil, &persistence.ConditionFailedError{})

	err := s.taskOwnership.takeTaskQueueOwnership(context.Background())
	s.Error(err)
	s.Nil(s.taskOwnership.stateLastUpdateTime)
	s.Nil(s.taskOwnership.ownershipState)
	<-s.taskOwnership.getShutdownChan()
}

func (s *dbTaskOwnershipSuite) TestTaskOwnership_Update_Success() {
	rangeID := rand.Int63()
	minTaskID, maxTaskID := rangeIDToTaskIDRange(rangeID, s.taskIDRangeSize)
	ackedTaskID := minTaskID + rand.Int63n(maxTaskID-minTaskID)
	taskQueueInfo := s.randomTaskQueue(ackedTaskID)
	s.taskStore.EXPECT().GetTaskQueue(gomock.Any(), &persistence.GetTaskQueueRequest{
		NamespaceID: s.namespaceID,
		TaskQueue:   s.taskQueueName,
		TaskType:    s.taskQueueType,
	}).Return(&persistence.GetTaskQueueResponse{
		RangeID:       rangeID,
		TaskQueueInfo: taskQueueInfo,
	}, nil)
	s.taskStore.EXPECT().UpdateTaskQueue(gomock.Any(), &persistence.UpdateTaskQueueRequest{
		PrevRangeID: rangeID,
		RangeID:     rangeID + 1,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId:    s.namespaceID,
			Name:           s.taskQueueName,
			TaskType:       s.taskQueueType,
			Kind:           s.taskQueueKind,
			AckLevel:       ackedTaskID,
			ExpiryTime:     s.taskOwnership.expiryTime(),
			LastUpdateTime: timestamp.TimePtr(s.now),
		},
	}).Return(&persistence.UpdateTaskQueueResponse{}, nil)

	minTaskID, maxTaskID = rangeIDToTaskIDRange(rangeID+1, s.taskIDRangeSize)
	err := s.taskOwnership.takeTaskQueueOwnership(context.Background())
	s.NoError(err)
	s.Equal(s.now, *s.taskOwnership.stateLastUpdateTime)
	s.Equal(dbTaskQueueOwnershipState{
		rangeID:             rangeID + 1,
		ackedTaskID:         ackedTaskID,
		lastAllocatedTaskID: minTaskID,
		minTaskIDExclusive:  minTaskID,
		maxTaskIDInclusive:  maxTaskID,
	}, *s.taskOwnership.ownershipState)
	select {
	case <-s.taskOwnership.getShutdownChan():
		s.Fail("task ownership lost")
	default:
		// noop
	}
}

func (s *dbTaskOwnershipSuite) TestTaskOwnership_Update_Failed() {
	rangeID := rand.Int63()
	minTaskID, maxTaskID := rangeIDToTaskIDRange(rangeID, s.taskIDRangeSize)
	ackedTaskID := minTaskID + rand.Int63n(maxTaskID-minTaskID)
	taskQueueInfo := s.randomTaskQueue(ackedTaskID)
	s.taskStore.EXPECT().GetTaskQueue(gomock.Any(), &persistence.GetTaskQueueRequest{
		NamespaceID: s.namespaceID,
		TaskQueue:   s.taskQueueName,
		TaskType:    s.taskQueueType,
	}).Return(&persistence.GetTaskQueueResponse{
		RangeID:       rangeID,
		TaskQueueInfo: taskQueueInfo,
	}, nil)
	s.taskStore.EXPECT().UpdateTaskQueue(gomock.Any(), &persistence.UpdateTaskQueueRequest{
		PrevRangeID: rangeID,
		RangeID:     rangeID + 1,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId:    s.namespaceID,
			Name:           s.taskQueueName,
			TaskType:       s.taskQueueType,
			Kind:           s.taskQueueKind,
			AckLevel:       ackedTaskID,
			ExpiryTime:     s.taskOwnership.expiryTime(),
			LastUpdateTime: timestamp.TimePtr(s.now),
		},
	}).Return(nil, &persistence.ConditionFailedError{})

	err := s.taskOwnership.takeTaskQueueOwnership(context.Background())
	s.Error(err)
	s.Nil(s.taskOwnership.stateLastUpdateTime)
	s.Nil(s.taskOwnership.ownershipState)
	<-s.taskOwnership.getShutdownChan()
}

func (s *dbTaskOwnershipSuite) TestFlushTasks_Success() {
	ownershipState := s.prepareTaskQueueOwnership(rand.Int63())

	task1 := &persistencespb.AllocatedTaskInfo{
		Data:   s.randomTask(),
		TaskId: s.taskOwnership.getLastAllocatedTaskID() + 1,
	}
	task2 := &persistencespb.AllocatedTaskInfo{
		Data:   s.randomTask(),
		TaskId: s.taskOwnership.getLastAllocatedTaskID() + 2,
	}

	s.taskStore.EXPECT().CreateTasks(gomock.Any(), &persistence.CreateTasksRequest{
		TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
			Data:    s.taskOwnership.taskQueueInfoLocked(),
			RangeID: ownershipState.rangeID,
		},
		Tasks: []*persistencespb.AllocatedTaskInfo{task1, task2},
	}).Return(&persistence.CreateTasksResponse{}, nil)

	err := s.taskOwnership.flushTasks(context.Background(), task1.Data, task2.Data)
	s.NoError(err)
	s.Equal(dbTaskQueueOwnershipState{
		rangeID:             ownershipState.rangeID,
		ackedTaskID:         ownershipState.ackedTaskID,
		lastAllocatedTaskID: task2.TaskId,
		minTaskIDExclusive:  ownershipState.minTaskIDExclusive,
		maxTaskIDInclusive:  ownershipState.maxTaskIDInclusive,
	}, *s.taskOwnership.ownershipState)
	select {
	case <-s.taskOwnership.getShutdownChan():
		s.Fail("task ownership lost")
	default:
		// noop
	}
}

func (s *dbTaskOwnershipSuite) TestFlushTasks_Failed() {
	ownershipState := s.prepareTaskQueueOwnership(rand.Int63())

	task1 := &persistencespb.AllocatedTaskInfo{
		Data:   s.randomTask(),
		TaskId: s.taskOwnership.getLastAllocatedTaskID() + 1,
	}
	task2 := &persistencespb.AllocatedTaskInfo{
		Data:   s.randomTask(),
		TaskId: s.taskOwnership.getLastAllocatedTaskID() + 2,
	}

	s.taskStore.EXPECT().CreateTasks(gomock.Any(), &persistence.CreateTasksRequest{
		TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
			Data:    s.taskOwnership.taskQueueInfoLocked(),
			RangeID: ownershipState.rangeID,
		},
		Tasks: []*persistencespb.AllocatedTaskInfo{task1, task2},
	}).Return(nil, serviceerror.NewUnavailable("random error"))

	err := s.taskOwnership.flushTasks(context.Background(), task1.Data, task2.Data)
	s.Error(err)
	s.Equal(dbTaskQueueOwnershipState{
		rangeID:             ownershipState.rangeID,
		ackedTaskID:         ownershipState.ackedTaskID,
		lastAllocatedTaskID: task2.TaskId,
		minTaskIDExclusive:  ownershipState.minTaskIDExclusive,
		maxTaskIDInclusive:  ownershipState.maxTaskIDInclusive,
	}, *s.taskOwnership.ownershipState)
	select {
	case <-s.taskOwnership.getShutdownChan():
		s.Fail("task ownership lost")
	default:
		// noop
	}
}

func (s *dbTaskOwnershipSuite) TestFlushTasks_OwnershipLost() {
	ownershipState := s.prepareTaskQueueOwnership(rand.Int63())

	task1 := &persistencespb.AllocatedTaskInfo{
		Data:   s.randomTask(),
		TaskId: s.taskOwnership.getLastAllocatedTaskID() + 1,
	}
	task2 := &persistencespb.AllocatedTaskInfo{
		Data:   s.randomTask(),
		TaskId: s.taskOwnership.getLastAllocatedTaskID() + 2,
	}

	s.taskStore.EXPECT().CreateTasks(gomock.Any(), &persistence.CreateTasksRequest{
		TaskQueueInfo: &persistence.PersistedTaskQueueInfo{
			Data:    s.taskOwnership.taskQueueInfoLocked(),
			RangeID: ownershipState.rangeID,
		},
		Tasks: []*persistencespb.AllocatedTaskInfo{task1, task2},
	}).Return(nil, &persistence.ConditionFailedError{})

	err := s.taskOwnership.flushTasks(context.Background(), task1.Data, task2.Data)
	s.Error(err)
	s.Nil(s.taskOwnership.ownershipState)
	<-s.taskOwnership.getShutdownChan()
}

func (s *dbTaskOwnershipSuite) TestGenerateTaskID_WithinRange() {
	ownershipState := s.prepareTaskQueueOwnership(rand.Int63())

	var expectedTaskIDs []int64
	for i := ownershipState.minTaskIDExclusive + 1; i <= ownershipState.maxTaskIDInclusive; i++ {
		expectedTaskIDs = append(expectedTaskIDs, i)
	}

	var actualTaskIDs []int64
	for i := 0; i < int(s.taskIDRangeSize); i++ {
		taskIDs, err := s.taskOwnership.generatedTaskIDsLocked(context.Background(), 1)
		s.NoError(err)
		s.Equal(1, len(taskIDs))
		actualTaskIDs = append(actualTaskIDs, taskIDs[0])
	}

	s.Equal(expectedTaskIDs, actualTaskIDs)
	s.Equal(dbTaskQueueOwnershipState{
		rangeID:             ownershipState.rangeID,
		ackedTaskID:         ownershipState.ackedTaskID,
		lastAllocatedTaskID: ownershipState.maxTaskIDInclusive,
		minTaskIDExclusive:  ownershipState.minTaskIDExclusive,
		maxTaskIDInclusive:  ownershipState.maxTaskIDInclusive,
	}, *s.taskOwnership.ownershipState)
}

func (s *dbTaskOwnershipSuite) TestGenerateTaskID_OutOfRange() {
	prevOwnershipState := s.prepareTaskQueueOwnership(rand.Int63())
	for i := 0; i < int(s.taskIDRangeSize)-1; i++ {
		_, err := s.taskOwnership.generatedTaskIDsLocked(context.Background(), 1)
		s.NoError(err)
	}

	s.taskStore.EXPECT().UpdateTaskQueue(gomock.Any(), &persistence.UpdateTaskQueueRequest{
		PrevRangeID: prevOwnershipState.rangeID,
		RangeID:     prevOwnershipState.rangeID + 1,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId:    s.namespaceID,
			Name:           s.taskQueueName,
			TaskType:       s.taskQueueType,
			Kind:           s.taskQueueKind,
			AckLevel:       prevOwnershipState.ackedTaskID,
			ExpiryTime:     s.taskOwnership.expiryTime(),
			LastUpdateTime: timestamp.TimePtr(s.now),
		},
	}).Return(&persistence.UpdateTaskQueueResponse{}, nil)

	minTaskID, maxTaskID := rangeIDToTaskIDRange(prevOwnershipState.rangeID+1, s.taskIDRangeSize)
	expectedTaskIDs := []int64{minTaskID + 1, minTaskID + 2}
	actualTaskIDs, err := s.taskOwnership.generatedTaskIDsLocked(context.Background(), 2)
	s.NoError(err)
	s.Equal(expectedTaskIDs, actualTaskIDs)
	s.Equal(dbTaskQueueOwnershipState{
		rangeID:             prevOwnershipState.rangeID + 1,
		ackedTaskID:         prevOwnershipState.ackedTaskID,
		lastAllocatedTaskID: expectedTaskIDs[len(expectedTaskIDs)-1],
		minTaskIDExclusive:  minTaskID,
		maxTaskIDInclusive:  maxTaskID,
	}, *s.taskOwnership.ownershipState)
}

func (s *dbTaskOwnershipSuite) prepareTaskQueueOwnership(
	targetRangeID int64,
) dbTaskQueueOwnershipState {
	minTaskID, maxTaskID := rangeIDToTaskIDRange(targetRangeID-1, s.taskIDRangeSize)
	ackedTaskID := minTaskID + rand.Int63n(maxTaskID-minTaskID)
	taskQueueInfo := s.randomTaskQueue(ackedTaskID)
	s.taskStore.EXPECT().GetTaskQueue(gomock.Any(), &persistence.GetTaskQueueRequest{
		NamespaceID: s.namespaceID,
		TaskQueue:   s.taskQueueName,
		TaskType:    s.taskQueueType,
	}).Return(&persistence.GetTaskQueueResponse{
		RangeID:       targetRangeID - 1,
		TaskQueueInfo: taskQueueInfo,
	}, nil)
	s.taskStore.EXPECT().UpdateTaskQueue(gomock.Any(), &persistence.UpdateTaskQueueRequest{
		PrevRangeID: targetRangeID - 1,
		RangeID:     targetRangeID,
		TaskQueueInfo: &persistencespb.TaskQueueInfo{
			NamespaceId:    s.namespaceID,
			Name:           s.taskQueueName,
			TaskType:       s.taskQueueType,
			Kind:           s.taskQueueKind,
			AckLevel:       ackedTaskID,
			ExpiryTime:     s.taskOwnership.expiryTime(),
			LastUpdateTime: timestamp.TimePtr(s.now),
		},
	}).Return(&persistence.UpdateTaskQueueResponse{}, nil)

	err := s.taskOwnership.takeTaskQueueOwnership(context.Background())
	s.NoError(err)
	return *s.taskOwnership.ownershipState
}

func (s *dbTaskOwnershipSuite) randomTaskQueue(
	ackedTaskID int64,
) *persistencespb.TaskQueueInfo {
	return &persistencespb.TaskQueueInfo{
		NamespaceId:    s.namespaceID,
		Name:           s.taskQueueName,
		TaskType:       s.taskQueueType,
		Kind:           s.taskQueueKind,
		AckLevel:       ackedTaskID,
		ExpiryTime:     timestamp.TimePtr(time.Unix(0, rand.Int63())),
		LastUpdateTime: timestamp.TimePtr(time.Unix(0, rand.Int63())),
	}
}

func (s *dbTaskOwnershipSuite) randomTask() *persistencespb.TaskInfo {
	return &persistencespb.TaskInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       uuid.New().String(),
		RunId:            uuid.New().String(),
		ScheduledEventId: rand.Int63(),
		CreateTime:       timestamp.TimePtr(time.Unix(0, rand.Int63())),
		ExpiryTime:       timestamp.TimePtr(time.Unix(0, rand.Int63())),
	}
}
