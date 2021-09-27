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

package serialization

import (
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	TaskSerializer struct {
		namespaceID string
		workflowID  string
		runID       string
	}
)

func NewTaskSerializer(
	namespaceID string,
	workflowID string,
	runID string,
) *TaskSerializer {
	return &TaskSerializer{
		namespaceID: namespaceID,
		workflowID:  workflowID,
		runID:       runID,
	}
}

func (s *TaskSerializer) SerializeTransferTasks(
	tasks []definition.Task,
) ([]commonpb.DataBlob, error) {
	blobs := make([]commonpb.DataBlob, len(tasks))
	for index, task := range tasks {
		var transferTask *persistencespb.TransferTaskInfo
		switch task.GetType() {
		case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
			transferTask = s.activityTaskToProto(task.(*definition.ActivityTask))

		case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
			transferTask = s.workflowTaskToProto(task.(*definition.WorkflowTask))

		case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
			transferTask = s.requestCancelTaskToProto(task.(*definition.CancelExecutionTask))

		case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
			transferTask = s.signalTaskToProto(task.(*definition.SignalExecutionTask))

		case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
			transferTask = s.childWorkflowTaskToProto(task.(*definition.StartChildExecutionTask))

		case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
			transferTask = s.closeTaskToProto(task.(*definition.CloseExecutionTask))

		case enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
			transferTask = s.resetTaskToProto(task.(*definition.ResetWorkflowTask))

		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknow transfer type: %v", task.GetType()))
		}

		blob, err := TransferTaskInfoToBlob(transferTask)
		if err != nil {
			return nil, err
		}
		blobs[index] = blob
	}
	return blobs, nil
}

func (s *TaskSerializer) DeserializeTransferTasks(
	blobs []commonpb.DataBlob,
) ([]definition.Task, error) {
	tasks := make([]definition.Task, len(blobs))
	for index, blob := range blobs {
		transferTask, err := TransferTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
		if err != nil {
			return nil, err
		}
		var task definition.Task
		switch transferTask.TaskType {
		case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
			task = s.activityTaskFromProto(transferTask)

		case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
			task = s.workflowTaskFromProto(transferTask)

		case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
			task = s.requestCancelTaskFromProto(transferTask)

		case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
			task = s.signalTaskFromProto(transferTask)

		case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
			task = s.childWorkflowTaskFromProto(transferTask)

		case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
			task = s.closeTaskFromProto(transferTask)

		case enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
			task = s.resetTaskFromProto(transferTask)

		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknow transfer type: %v", transferTask.TaskType))
		}

		tasks[index] = task
	}
	return tasks, nil
}

func (s *TaskSerializer) activityTaskToProto(
	activityTask *definition.ActivityTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             s.namespaceID,
		WorkflowId:              s.workflowID,
		RunId:                   s.runID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		TargetNamespaceId:       activityTask.NamespaceID,
		TargetWorkflowId:        "",
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               activityTask.TaskQueue,
		ScheduleId:              activityTask.ScheduleID,
		Version:                 activityTask.Version,
		TaskId:                  activityTask.TaskID,
		VisibilityTime:          timestamp.TimePtr(activityTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) activityTaskFromProto(
	activityTask *persistencespb.TransferTaskInfo,
) *definition.ActivityTask {
	return &definition.ActivityTask{
		VisibilityTimestamp: *activityTask.VisibilityTime,
		TaskID:              activityTask.TaskId,
		NamespaceID:         activityTask.NamespaceId,
		TaskQueue:           activityTask.TaskQueue,
		ScheduleID:          activityTask.ScheduleId,
		Version:             activityTask.Version,
	}
}

func (s *TaskSerializer) workflowTaskToProto(
	workflowTask *definition.WorkflowTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             s.namespaceID,
		WorkflowId:              s.workflowID,
		RunId:                   s.runID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		TargetNamespaceId:       workflowTask.NamespaceID,
		TargetWorkflowId:        "",
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               workflowTask.TaskQueue,
		ScheduleId:              workflowTask.ScheduleID,
		Version:                 workflowTask.Version,
		TaskId:                  workflowTask.TaskID,
		VisibilityTime:          timestamp.TimePtr(workflowTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) workflowTaskFromProto(
	workflowTask *persistencespb.TransferTaskInfo,
) *definition.WorkflowTask {
	return &definition.WorkflowTask{
		VisibilityTimestamp: *workflowTask.VisibilityTime,
		TaskID:              workflowTask.TaskId,
		NamespaceID:         workflowTask.NamespaceId,
		TaskQueue:           workflowTask.TaskQueue,
		ScheduleID:          workflowTask.ScheduleId,
		Version:             workflowTask.Version,
	}
}

func (s *TaskSerializer) requestCancelTaskToProto(
	requestCancelTask *definition.CancelExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             s.namespaceID,
		WorkflowId:              s.workflowID,
		RunId:                   s.runID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION,
		TargetNamespaceId:       requestCancelTask.TargetNamespaceID,
		TargetWorkflowId:        requestCancelTask.TargetWorkflowID,
		TargetRunId:             requestCancelTask.TargetRunID,
		TargetChildWorkflowOnly: requestCancelTask.TargetChildWorkflowOnly,
		TaskQueue:               "",
		ScheduleId:              requestCancelTask.InitiatedID,
		Version:                 requestCancelTask.Version,
		TaskId:                  requestCancelTask.TaskID,
		VisibilityTime:          timestamp.TimePtr(requestCancelTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) requestCancelTaskFromProto(
	requestCancelTask *persistencespb.TransferTaskInfo,
) *definition.CancelExecutionTask {
	return &definition.CancelExecutionTask{
		VisibilityTimestamp:     *requestCancelTask.VisibilityTime,
		TaskID:                  requestCancelTask.TaskId,
		TargetNamespaceID:       requestCancelTask.TargetNamespaceId,
		TargetWorkflowID:        requestCancelTask.TargetWorkflowId,
		TargetRunID:             requestCancelTask.TargetRunId,
		TargetChildWorkflowOnly: requestCancelTask.TargetChildWorkflowOnly,
		InitiatedID:             requestCancelTask.ScheduleId,
		Version:                 requestCancelTask.Version,
	}
}

func (s *TaskSerializer) signalTaskToProto(
	signalTask *definition.SignalExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             s.namespaceID,
		WorkflowId:              s.workflowID,
		RunId:                   s.runID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION,
		TargetNamespaceId:       signalTask.TargetNamespaceID,
		TargetWorkflowId:        signalTask.TargetWorkflowID,
		TargetRunId:             signalTask.TargetRunID,
		TargetChildWorkflowOnly: signalTask.TargetChildWorkflowOnly,
		TaskQueue:               "",
		ScheduleId:              signalTask.InitiatedID,
		Version:                 signalTask.Version,
		TaskId:                  signalTask.TaskID,
		VisibilityTime:          timestamp.TimePtr(signalTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) signalTaskFromProto(
	signalTask *persistencespb.TransferTaskInfo,
) *definition.SignalExecutionTask {
	return &definition.SignalExecutionTask{
		VisibilityTimestamp:     *signalTask.VisibilityTime,
		TaskID:                  signalTask.TaskId,
		TargetNamespaceID:       signalTask.TargetNamespaceId,
		TargetWorkflowID:        signalTask.TargetWorkflowId,
		TargetRunID:             signalTask.TargetRunId,
		TargetChildWorkflowOnly: signalTask.TargetChildWorkflowOnly,
		InitiatedID:             signalTask.ScheduleId,
		Version:                 signalTask.Version,
	}
}

func (s *TaskSerializer) childWorkflowTaskToProto(
	childWorkflowTask *definition.StartChildExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             s.namespaceID,
		WorkflowId:              s.workflowID,
		RunId:                   s.runID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
		TargetNamespaceId:       childWorkflowTask.TargetNamespaceID,
		TargetWorkflowId:        childWorkflowTask.TargetWorkflowID,
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               "",
		ScheduleId:              childWorkflowTask.InitiatedID,
		Version:                 childWorkflowTask.Version,
		TaskId:                  childWorkflowTask.TaskID,
		VisibilityTime:          timestamp.TimePtr(childWorkflowTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) childWorkflowTaskFromProto(
	signalTask *persistencespb.TransferTaskInfo,
) *definition.StartChildExecutionTask {
	return &definition.StartChildExecutionTask{
		VisibilityTimestamp: *signalTask.VisibilityTime,
		TaskID:              signalTask.TaskId,
		TargetNamespaceID:   signalTask.TargetNamespaceId,
		TargetWorkflowID:    signalTask.TargetWorkflowId,
		InitiatedID:         signalTask.ScheduleId,
		Version:             signalTask.Version,
	}
}

func (s *TaskSerializer) closeTaskToProto(
	closeTask *definition.CloseExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             s.namespaceID,
		WorkflowId:              s.workflowID,
		RunId:                   s.runID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		TargetNamespaceId:       "",
		TargetWorkflowId:        "",
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               "",
		ScheduleId:              0,
		Version:                 closeTask.Version,
		TaskId:                  closeTask.TaskID,
		VisibilityTime:          timestamp.TimePtr(closeTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) closeTaskFromProto(
	closeTask *persistencespb.TransferTaskInfo,
) *definition.CloseExecutionTask {
	return &definition.CloseExecutionTask{
		VisibilityTimestamp: *closeTask.VisibilityTime,
		TaskID:              closeTask.TaskId,
		Version:             closeTask.Version,
	}
}

func (s *TaskSerializer) resetTaskToProto(
	resetTask *definition.ResetWorkflowTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             s.namespaceID,
		WorkflowId:              s.workflowID,
		RunId:                   s.runID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		TargetNamespaceId:       "",
		TargetWorkflowId:        "",
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               "",
		ScheduleId:              0,
		Version:                 resetTask.Version,
		TaskId:                  resetTask.TaskID,
		VisibilityTime:          timestamp.TimePtr(resetTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) resetTaskFromProto(
	resetTask *persistencespb.TransferTaskInfo,
) *definition.ResetWorkflowTask {
	return &definition.ResetWorkflowTask{
		VisibilityTimestamp: *resetTask.VisibilityTime,
		TaskID:              resetTask.TaskId,
		Version:             resetTask.Version,
	}
}
