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

package persistence

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

const initialRangeID = 1 // Id of the first range of a new task queue

type taskManagerImpl struct {
	taskStore  TaskStore
	serializer serialization.Serializer
}

// NewTaskManager creates a new instance of TaskManager
func NewTaskManager(store TaskStore) TaskManager {
	return &taskManagerImpl{
		taskStore:  store,
		serializer: serialization.NewSerializer(),
	}
}

func (m *taskManagerImpl) Close() {
	m.taskStore.Close()
}

func (m *taskManagerImpl) GetName() string {
	return m.taskStore.GetName()
}

func (m *taskManagerImpl) LeaseTaskQueue(request *LeaseTaskQueueRequest) (*LeaseTaskQueueResponse, error) {
	if len(request.TaskQueue) == 0 {
		return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue requires non empty task queue"))
	}

	taskQueue, err := m.taskStore.GetTaskQueue(&InternalGetTaskQueueRequest{
		NamespaceID: request.NamespaceID,
		TaskQueue:   request.TaskQueue,
		TaskType:    request.TaskType,
	})

	switch err.(type) {
	case nil:
		// If request.RangeID is > 0, we are trying to renew an existing lease on the task queue.
		// If request.RangeID=0, we are trying to steal the task queue from its current owner.
		if request.RangeID > 0 && request.RangeID != taskQueue.RangeID {
			return nil, &ConditionFailedError{
				Msg: fmt.Sprintf("LeaseTaskQueue: renew failed: taskQueue:%v, taskQueueType:%v, haveRangeID:%v, gotRangeID:%v",
					request.TaskQueue, request.TaskType, request.RangeID, taskQueue.RangeID),
			}
		}
		taskQueueInfo, err := m.serializer.TaskQueueInfoFromBlob(taskQueue.TaskQueueInfo)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("LeaseTaskQueue operation failed during serialization. TaskQueue: %v, TaskType: %v, Error: %v", request.TaskQueue, request.TaskType, err))
		}

		taskQueueInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
		taskQueueInfoBlob, err := m.serializer.TaskQueueInfoToBlob(taskQueueInfo, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		err = m.taskStore.ExtendLease(&InternalLeaseTaskQueueRequest{
			NamespaceID:   request.NamespaceID,
			TaskQueue:     request.TaskQueue,
			TaskType:      request.TaskType,
			RangeID:       taskQueue.RangeID,
			TaskQueueInfo: taskQueueInfoBlob,
		})
		if err != nil {
			return nil, err
		}

		return &LeaseTaskQueueResponse{
			TaskQueueInfo: &PersistedTaskQueueInfo{
				Data:    taskQueueInfo,
				RangeID: taskQueue.RangeID + 1,
			},
		}, nil

	case *serviceerror.NotFound:
		// First time task queue is used
		taskQueueInfo := &persistencespb.TaskQueueInfo{
			NamespaceId:    request.NamespaceID,
			Name:           request.TaskQueue,
			TaskType:       request.TaskType,
			Kind:           request.TaskQueueKind,
			AckLevel:       0,
			ExpiryTime:     nil,
			LastUpdateTime: timestamp.TimeNowPtrUtc(),
		}
		taskQueueInfoBlob, err := m.serializer.TaskQueueInfoToBlob(taskQueueInfo, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}

		if err := m.taskStore.CreateTaskQueue(&InternalCreateTaskQueueRequest{
			NamespaceID:   request.NamespaceID,
			TaskQueue:     request.TaskQueue,
			TaskType:      request.TaskType,
			RangeID:       initialRangeID,
			TaskQueueInfo: taskQueueInfoBlob,
		}); err != nil {
			return nil, err
		}

		// return newly created TaskQueueInfo
		return &LeaseTaskQueueResponse{
			TaskQueueInfo: &PersistedTaskQueueInfo{
				Data:    taskQueueInfo,
				RangeID: initialRangeID,
			},
		}, nil
	default:
		// failed
		return nil, err
	}
}

// TODO: below part will be refactored in separate PR.
func (m *taskManagerImpl) UpdateTaskQueue(request *UpdateTaskQueueRequest) (*UpdateTaskQueueResponse, error) {
	return m.taskStore.UpdateTaskQueue(request)
}

func (m *taskManagerImpl) ListTaskQueue(request *ListTaskQueueRequest) (*ListTaskQueueResponse, error) {
	return m.taskStore.ListTaskQueue(request)
}

func (m *taskManagerImpl) DeleteTaskQueue(request *DeleteTaskQueueRequest) error {
	return m.taskStore.DeleteTaskQueue(request)
}

func (m *taskManagerImpl) CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error) {
	return m.taskStore.CreateTasks(request)
}

func (m *taskManagerImpl) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	return m.taskStore.GetTasks(request)
}

func (m *taskManagerImpl) CompleteTask(request *CompleteTaskRequest) error {
	return m.taskStore.CompleteTask(request)
}

func (m *taskManagerImpl) CompleteTasksLessThan(request *CompleteTasksLessThanRequest) (int, error) {
	return m.taskStore.CompleteTasksLessThan(request)
}
