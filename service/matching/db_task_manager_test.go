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
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	dbTaskManagerSuite struct {
		*require.Assertions
		suite.Suite

		controller          *gomock.Controller
		taskQueueOwnership  *MockdbTaskQueueOwnership
		taskWriter          *MockdbTaskWriter
		taskReader          *MockdbTaskReader
		store               *persistence.MockTaskManager
		ackedTaskID         int64
		lastAllocatedTaskID int64
		dispatchTaskFn      func(context.Context, *internalTask) error

		namespaceID     string
		taskQueueName   string
		taskQueueType   enumspb.TaskQueueType
		taskQueueKind   enumspb.TaskQueueKind
		taskIDRangeSize int64

		dbTaskManager *dbTaskManager
	}
)

func TestDBTaskManagerSuite(t *testing.T) {
	s := new(dbTaskManagerSuite)
	suite.Run(t, s)
}

func (s *dbTaskManagerSuite) SetupSuite() {
	rand.Seed(time.Now().UnixNano())
}

func (s *dbTaskManagerSuite) TearDownSuite() {

}

func (s *dbTaskManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	logger := log.NewTestLogger()
	s.controller = gomock.NewController(s.T())
	s.taskQueueOwnership = NewMockdbTaskQueueOwnership(s.controller)
	s.taskWriter = NewMockdbTaskWriter(s.controller)
	s.taskReader = NewMockdbTaskReader(s.controller)
	s.store = persistence.NewMockTaskManager(s.controller)
	s.ackedTaskID = rand.Int63()
	s.lastAllocatedTaskID = s.ackedTaskID + 100
	s.dispatchTaskFn = func(context.Context, *internalTask) error {
		panic("unexpected call to dispatch function")
	}

	s.namespaceID = uuid.New().String()
	s.taskQueueName = uuid.New().String()
	s.taskQueueType = enumspb.TASK_QUEUE_TYPE_WORKFLOW
	s.taskQueueKind = enumspb.TASK_QUEUE_KIND_STICKY
	s.taskIDRangeSize = rand.Int63()

	s.dbTaskManager = newDBTaskManager(
		persistence.TaskQueueKey{
			NamespaceID:   s.namespaceID,
			TaskQueueName: s.taskQueueName,
			TaskQueueType: s.taskQueueType,
		},
		s.taskQueueKind,
		s.taskIDRangeSize,
		s.dispatchTaskFn,
		s.store,
		logger,
	)
	s.dbTaskManager.taskQueueOwnershipProvider = func() dbTaskQueueOwnership {
		return s.taskQueueOwnership
	}
	s.dbTaskManager.taskReaderProvider = func(_ dbTaskQueueOwnership) dbTaskReader {
		return s.taskReader
	}
	s.dbTaskManager.taskWriterProvider = func(_ dbTaskQueueOwnership) dbTaskWriter {
		return s.taskWriter
	}
}

func (s *dbTaskManagerSuite) TearDownTest() {
	s.dbTaskManager.Stop()
	s.controller.Finish()
}

func (s *dbTaskManagerSuite) TestAcquireOwnership_Success() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID).AnyTimes()

	err := s.dbTaskManager.acquireOwnership(context.Background())
	s.NoError(err)
	s.NotNil(s.dbTaskManager.taskWriter)
	s.NotNil(s.dbTaskManager.taskReader)
	s.Equal(s.ackedTaskID, s.dbTaskManager.maxDeletedTaskIDInclusive)
}

func (s *dbTaskManagerSuite) TestAcquireOwnership_Failed() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(serviceerror.NewUnavailable("some random error"))

	err := s.dbTaskManager.acquireOwnership(context.Background())
	s.Error(err)
	s.Nil(s.dbTaskManager.taskWriter)
	s.Nil(s.dbTaskManager.taskReader)
	s.Equal(int64(0), s.dbTaskManager.maxDeletedTaskIDInclusive)
}

func (s *dbTaskManagerSuite) TestStart_Success() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID).AnyTimes()
	s.taskQueueOwnership.EXPECT().getLastAllocatedTaskID().Return(s.lastAllocatedTaskID).AnyTimes()
	s.taskQueueOwnership.EXPECT().getShutdownChan().Return(nil).AnyTimes()
	s.taskReader.EXPECT().taskIterator(gomock.Any(), s.lastAllocatedTaskID).Return(collection.NewPagingIterator(
		func(paginationToken []byte) ([]*persistencespb.AllocatedTaskInfo, []byte, error) {
			return nil, nil, nil
		},
	)).AnyTimes()
	s.taskWriter.EXPECT().notifyFlushChan().Return(nil).AnyTimes()

	s.dbTaskManager.Start()
	<-s.dbTaskManager.startupChan
	s.False(s.dbTaskManager.isStopped())
}

func (s *dbTaskManagerSuite) TestStart_ErrorThenSuccess() {
	gomock.InOrder(
		s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(serviceerror.NewUnavailable("some random error")),
		s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil),
	)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID).AnyTimes()
	s.taskQueueOwnership.EXPECT().getLastAllocatedTaskID().Return(s.lastAllocatedTaskID).AnyTimes()
	s.taskQueueOwnership.EXPECT().getShutdownChan().Return(nil).AnyTimes()
	s.taskReader.EXPECT().taskIterator(gomock.Any(), s.lastAllocatedTaskID).Return(collection.NewPagingIterator(
		func(paginationToken []byte) ([]*persistencespb.AllocatedTaskInfo, []byte, error) {
			return nil, nil, nil
		},
	)).AnyTimes()
	s.taskWriter.EXPECT().notifyFlushChan().Return(nil).AnyTimes()

	s.dbTaskManager.Start()
	<-s.dbTaskManager.startupChan
	s.False(s.dbTaskManager.isStopped())
}

func (s *dbTaskManagerSuite) TestStart_Error() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(&persistence.ConditionFailedError{})

	s.dbTaskManager.Start()
	<-s.dbTaskManager.startupChan
	s.True(s.dbTaskManager.isStopped())
}

func (s *dbTaskManagerSuite) TestBufferAndWriteTask_NotReady() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(serviceerror.NewUnavailable("some random error")).AnyTimes()
	s.dbTaskManager.Start()

	taskInfo := &persistencespb.TaskInfo{}
	fut := s.dbTaskManager.BufferAndWriteTask(taskInfo)
	_, err := fut.Get(context.Background())
	s.Equal(errDBTaskManagerNotReady, err)
}

func (s *dbTaskManagerSuite) TestBufferAndWriteTask_Ready() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID).AnyTimes()
	s.taskQueueOwnership.EXPECT().getLastAllocatedTaskID().Return(s.lastAllocatedTaskID).AnyTimes()
	s.taskQueueOwnership.EXPECT().getShutdownChan().Return(nil).AnyTimes()
	s.taskReader.EXPECT().taskIterator(gomock.Any(), s.lastAllocatedTaskID).Return(collection.NewPagingIterator(
		func(paginationToken []byte) ([]*persistencespb.AllocatedTaskInfo, []byte, error) {
			return nil, nil, nil
		},
	)).AnyTimes()
	s.taskWriter.EXPECT().notifyFlushChan().Return(nil).AnyTimes()
	s.dbTaskManager.Start()
	<-s.dbTaskManager.startupChan

	taskInfo := &persistencespb.TaskInfo{}
	taskWriterErr := serviceerror.NewInternal("random error")
	s.taskWriter.EXPECT().appendTask(taskInfo).Return(
		future.NewReadyFuture[struct{}](struct{}{}, taskWriterErr),
	)
	fut := s.dbTaskManager.BufferAndWriteTask(taskInfo)
	_, err := fut.Get(context.Background())
	s.Equal(taskWriterErr, err)
}

func (s *dbTaskManagerSuite) TestReadAndDispatchTasks_ReadSuccess_Expired() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID)
	err := s.dbTaskManager.acquireOwnership(context.Background())
	s.NoError(err)

	// make sure no signal exists in dispatch chan
	select {
	case <-s.dbTaskManager.dispatchChan:
	default:
	}

	allocatedTaskInfo := &persistencespb.AllocatedTaskInfo{
		TaskId: s.lastAllocatedTaskID + 100,
		Data: &persistencespb.TaskInfo{
			NamespaceId:      uuid.New().String(),
			WorkflowId:       uuid.New().String(),
			RunId:            uuid.New().String(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       timestamp.TimePtr(time.Now().UTC()),
			ExpiryTime:       timestamp.TimePtr(time.Now().UTC().Add(-time.Minute)),
		},
	}
	s.taskQueueOwnership.EXPECT().getLastAllocatedTaskID().Return(s.lastAllocatedTaskID)
	s.taskReader.EXPECT().taskIterator(gomock.Any(), s.lastAllocatedTaskID).Return(collection.NewPagingIterator(
		func(paginationToken []byte) ([]*persistencespb.AllocatedTaskInfo, []byte, error) {
			return []*persistencespb.AllocatedTaskInfo{allocatedTaskInfo}, nil, nil
		},
	))
	s.taskReader.EXPECT().ackTask(allocatedTaskInfo.TaskId)

	s.dbTaskManager.readAndDispatchTasks(context.Background())
}

func (s *dbTaskManagerSuite) TestReadAndDispatchTasks_ReadSuccess_Dispatch() {
	var dispatchedTasks []*persistencespb.AllocatedTaskInfo
	s.dbTaskManager.dispatchTaskFn = func(_ context.Context, task *internalTask) error {
		dispatchedTasks = append(dispatchedTasks, task.event.AllocatedTaskInfo)
		return nil
	}
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID)
	err := s.dbTaskManager.acquireOwnership(context.Background())
	s.NoError(err)

	// make sure no signal exists in dispatch chan
	select {
	case <-s.dbTaskManager.dispatchChan:
	default:
	}

	allocatedTaskInfo := &persistencespb.AllocatedTaskInfo{
		TaskId: s.lastAllocatedTaskID + 100,
		Data: &persistencespb.TaskInfo{
			NamespaceId:      uuid.New().String(),
			WorkflowId:       uuid.New().String(),
			RunId:            uuid.New().String(),
			ScheduledEventId: rand.Int63(),
			CreateTime:       timestamp.TimePtr(time.Now().UTC()),
			ExpiryTime:       timestamp.TimePtr(time.Unix(0, 0)),
		},
	}
	s.taskQueueOwnership.EXPECT().getLastAllocatedTaskID().Return(s.lastAllocatedTaskID)
	s.taskReader.EXPECT().taskIterator(gomock.Any(), s.lastAllocatedTaskID).Return(collection.NewPagingIterator(
		func(paginationToken []byte) ([]*persistencespb.AllocatedTaskInfo, []byte, error) {
			return []*persistencespb.AllocatedTaskInfo{allocatedTaskInfo}, nil, nil
		},
	))

	s.dbTaskManager.readAndDispatchTasks(context.Background())
	s.Equal([]*persistencespb.AllocatedTaskInfo{allocatedTaskInfo}, dispatchedTasks)
}

func (s *dbTaskManagerSuite) TestReadAndDispatchTasks_ReadFailure() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID)
	err := s.dbTaskManager.acquireOwnership(context.Background())
	s.NoError(err)

	// make sure no signal exists in dispatch chan
	select {
	case <-s.dbTaskManager.dispatchChan:
	default:
	}

	s.taskQueueOwnership.EXPECT().getLastAllocatedTaskID().Return(s.lastAllocatedTaskID)
	s.taskReader.EXPECT().taskIterator(gomock.Any(), s.lastAllocatedTaskID).Return(collection.NewPagingIterator(
		func(paginationToken []byte) ([]*persistencespb.AllocatedTaskInfo, []byte, error) {
			return nil, nil, serviceerror.NewUnavailable("random error")
		},
	))

	s.dbTaskManager.readAndDispatchTasks(context.Background())
	select {
	case <-s.dbTaskManager.dispatchChan:
		// noop
	default:
		s.Fail("dispatch channel should contain one signal")
	}
}

func (s *dbTaskManagerSuite) TestUpdateAckTaskID() {
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID)
	err := s.dbTaskManager.acquireOwnership(context.Background())
	s.NoError(err)

	ackedTaskID := rand.Int63()
	s.taskReader.EXPECT().moveAckedTaskID().Return(ackedTaskID)
	s.taskQueueOwnership.EXPECT().updateAckedTaskID(ackedTaskID)

	s.dbTaskManager.updateAckTaskID()
}

func (s *dbTaskManagerSuite) TestDeleteAckedTasks_Success() {
	maxDeletedTaskIDInclusive := s.ackedTaskID - 100
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID).AnyTimes()
	err := s.dbTaskManager.acquireOwnership(context.Background())
	s.NoError(err)
	s.dbTaskManager.maxDeletedTaskIDInclusive = maxDeletedTaskIDInclusive

	s.store.EXPECT().CompleteTasksLessThan(gomock.Any(), &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        s.namespaceID,
		TaskQueueName:      s.taskQueueName,
		TaskType:           s.taskQueueType,
		ExclusiveMaxTaskID: s.ackedTaskID + 1,
		Limit:              100000,
	}).Return(0, nil)

	s.dbTaskManager.deleteAckedTasks(context.Background())
	s.Equal(s.ackedTaskID, s.dbTaskManager.maxDeletedTaskIDInclusive)
}

func (s *dbTaskManagerSuite) TestDeleteAckedTasks_Failed() {
	maxDeletedTaskIDInclusive := s.ackedTaskID - 100
	s.taskQueueOwnership.EXPECT().takeTaskQueueOwnership(gomock.Any()).Return(nil)
	s.taskQueueOwnership.EXPECT().getAckedTaskID().Return(s.ackedTaskID).AnyTimes()
	err := s.dbTaskManager.acquireOwnership(context.Background())
	s.NoError(err)
	s.dbTaskManager.maxDeletedTaskIDInclusive = maxDeletedTaskIDInclusive

	s.store.EXPECT().CompleteTasksLessThan(gomock.Any(), &persistence.CompleteTasksLessThanRequest{
		NamespaceID:        s.namespaceID,
		TaskQueueName:      s.taskQueueName,
		TaskType:           s.taskQueueType,
		ExclusiveMaxTaskID: s.ackedTaskID + 1,
		Limit:              100000,
	}).Return(0, serviceerror.NewUnavailable("random error"))

	s.dbTaskManager.deleteAckedTasks(context.Background())
	s.Equal(maxDeletedTaskIDInclusive, s.dbTaskManager.maxDeletedTaskIDInclusive)
}

// TODO @wxing1292 add necessary tests
//  once there is concensus about whether to keep the `task move to end` behavior
func (s *dbTaskManagerSuite) TestFinishTask_Success() {}

func (s *dbTaskManagerSuite) TestFinishTask_Error() {}
