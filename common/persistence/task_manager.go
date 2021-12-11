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

func (m *taskManagerImpl) CreateTaskQueue(request *CreateTaskQueueRequest) (*CreateTaskQueueResponse, error) {
	taskQueueInfo := request.TaskQueueInfo
	if taskQueueInfo.LastUpdateTime == nil {
		panic("CreateTaskQueue encountered LastUpdateTime not set")
	}
	if taskQueueInfo.ExpiryTime == nil && taskQueueInfo.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		panic("CreateTaskQueue encountered ExpiryTime not set for sticky task queue")
	}
	taskQueueInfoBlob, err := m.serializer.TaskQueueInfoToBlob(taskQueueInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	internalRequest := &InternalCreateTaskQueueRequest{
		NamespaceID:   request.TaskQueueInfo.GetNamespaceId(),
		TaskQueue:     request.TaskQueueInfo.GetName(),
		TaskType:      request.TaskQueueInfo.GetTaskType(),
		TaskQueueKind: request.TaskQueueInfo.GetKind(),
		RangeID:       request.RangeID,
		ExpiryTime:    taskQueueInfo.ExpiryTime,
		TaskQueueInfo: taskQueueInfoBlob,
	}
	if err := m.taskStore.CreateTaskQueue(internalRequest); err != nil {
		return nil, err
	}
	return &CreateTaskQueueResponse{}, nil
}

func (m *taskManagerImpl) UpdateTaskQueue(request *UpdateTaskQueueRequest) (*UpdateTaskQueueResponse, error) {
	taskQueueInfo := request.TaskQueueInfo
	if taskQueueInfo.LastUpdateTime == nil {
		panic("UpdateTaskQueue encountered LastUpdateTime not set")
	}
	if taskQueueInfo.ExpiryTime == nil && taskQueueInfo.GetKind() == enumspb.TASK_QUEUE_KIND_STICKY {
		panic("UpdateTaskQueue encountered ExpiryTime not set for sticky task queue")
	}
	taskQueueInfoBlob, err := m.serializer.TaskQueueInfoToBlob(taskQueueInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	internalRequest := &InternalUpdateTaskQueueRequest{
		NamespaceID:   request.TaskQueueInfo.GetNamespaceId(),
		TaskQueue:     request.TaskQueueInfo.GetName(),
		TaskType:      request.TaskQueueInfo.GetTaskType(),
		RangeID:       request.RangeID,
		TaskQueueInfo: taskQueueInfoBlob,

		TaskQueueKind: request.TaskQueueInfo.GetKind(),
		ExpiryTime:    taskQueueInfo.ExpiryTime,

		PrevRangeID: request.PrevRangeID,
	}
	return m.taskStore.UpdateTaskQueue(internalRequest)
}

func (m *taskManagerImpl) GetTaskQueue(request *GetTaskQueueRequest) (*GetTaskQueueResponse, error) {
	response, err := m.taskStore.GetTaskQueue(&InternalGetTaskQueueRequest{
		NamespaceID: request.NamespaceID,
		TaskQueue:   request.TaskQueue,
		TaskType:    request.TaskType,
	})
	if err != nil {
		return nil, err
	}

	taskQueueInfo, err := m.serializer.TaskQueueInfoFromBlob(response.TaskQueueInfo)
	if err != nil {
		return nil, err
	}
	return &GetTaskQueueResponse{
		TaskQueueInfo: taskQueueInfo,
		RangeID:       response.RangeID,
	}, nil
}

func (m *taskManagerImpl) ListTaskQueue(request *ListTaskQueueRequest) (*ListTaskQueueResponse, error) {
	internalResp, err := m.taskStore.ListTaskQueue(request)
	if err != nil {
		return nil, err
	}
	taskQueues := make([]*PersistedTaskQueueInfo, len(internalResp.Items))
	for i, item := range internalResp.Items {
		tqi, err := m.serializer.TaskQueueInfoFromBlob(item.TaskQueue)
		if err != nil {
			return nil, err
		}
		taskQueues[i] = &PersistedTaskQueueInfo{
			Data:    tqi,
			RangeID: item.RangeID,
		}

	}
	return &ListTaskQueueResponse{
		Items:         taskQueues,
		NextPageToken: internalResp.NextPageToken,
	}, nil
}

func (m *taskManagerImpl) DeleteTaskQueue(request *DeleteTaskQueueRequest) error {
	return m.taskStore.DeleteTaskQueue(request)
}

func (m *taskManagerImpl) CreateTasks(request *CreateTasksRequest) (*CreateTasksResponse, error) {
	taskQueueInfo := request.TaskQueueInfo.Data
	taskQueueInfo.LastUpdateTime = timestamp.TimeNowPtrUtc()
	taskQueueInfoBlob, err := m.serializer.TaskQueueInfoToBlob(taskQueueInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	tasks := make([]*InternalCreateTask, len(request.Tasks))
	for i, task := range request.Tasks {
		taskBlob, err := m.serializer.TaskInfoToBlob(task, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf("CreateTasks operation failed during serialization. Error : %v", err))
		}
		tasks[i] = &InternalCreateTask{
			TaskId:     task.GetTaskId(),
			ExpiryTime: task.Data.ExpiryTime,
			Task:       taskBlob,
		}
	}
	internalRequest := &InternalCreateTasksRequest{
		NamespaceID:   request.TaskQueueInfo.Data.GetNamespaceId(),
		TaskQueue:     request.TaskQueueInfo.Data.GetName(),
		TaskType:      request.TaskQueueInfo.Data.GetTaskType(),
		RangeID:       request.TaskQueueInfo.RangeID,
		TaskQueueInfo: taskQueueInfoBlob,
		Tasks:         tasks,
	}
	return m.taskStore.CreateTasks(internalRequest)
}

func (m *taskManagerImpl) GetTasks(request *GetTasksRequest) (*GetTasksResponse, error) {
	if request.MinTaskIDExclusive >= request.MaxTaskIDInclusive {
		return &GetTasksResponse{}, nil
	}

	internalResp, err := m.taskStore.GetTasks(request)
	if err != nil {
		return nil, err
	}
	tasks := make([]*persistencespb.AllocatedTaskInfo, len(internalResp.Tasks))
	for i, taskBlob := range internalResp.Tasks {
		task, err := m.serializer.TaskInfoFromBlob(taskBlob)
		if err != nil {
			return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetTasks failed to deserialize task: %s", err.Error()))
		}
		tasks[i] = task
	}
	return &GetTasksResponse{Tasks: tasks, NextPageToken: internalResp.NextPageToken}, nil
}

func (m *taskManagerImpl) CompleteTask(request *CompleteTaskRequest) error {
	return m.taskStore.CompleteTask(request)
}

func (m *taskManagerImpl) CompleteTasksLessThan(request *CompleteTasksLessThanRequest) (int, error) {
	return m.taskStore.CompleteTasksLessThan(request)
}
