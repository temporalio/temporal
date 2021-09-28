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
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/tasks"
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
	taskSlice []tasks.Task,
) ([]commonpb.DataBlob, error) {
	blobSlice := make([]commonpb.DataBlob, len(taskSlice))
	for index, task := range taskSlice {
		var transferTask *persistencespb.TransferTaskInfo
		switch task.GetType() {
		case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
			transferTask = s.transferWorkflowTaskToProto(task.(*tasks.WorkflowTask))
		case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
			transferTask = s.transferActivityTaskToProto(task.(*tasks.ActivityTask))
		case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
			transferTask = s.transferRequestCancelTaskToProto(task.(*tasks.CancelExecutionTask))
		case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
			transferTask = s.transferSignalTaskToProto(task.(*tasks.SignalExecutionTask))
		case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
			transferTask = s.transferChildWorkflowTaskToProto(task.(*tasks.StartChildExecutionTask))
		case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
			transferTask = s.transferCloseTaskToProto(task.(*tasks.CloseExecutionTask))
		case enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
			transferTask = s.transferResetTaskToProto(task.(*tasks.ResetWorkflowTask))
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown transfer task type: %v", task.GetType()))
		}

		blob, err := TransferTaskInfoToBlob(transferTask)
		if err != nil {
			return nil, err
		}
		blobSlice[index] = blob
	}
	return blobSlice, nil
}

func (s *TaskSerializer) DeserializeTransferTasks(
	blobSlice []commonpb.DataBlob,
) ([]tasks.Task, error) {
	taskSlice := make([]tasks.Task, len(blobSlice))
	for index, blob := range blobSlice {
		transferTask, err := TransferTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
		if err != nil {
			return nil, err
		}
		var task tasks.Task
		switch transferTask.TaskType {
		case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
			task = s.transferWorkflowTaskFromProto(transferTask)
		case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
			task = s.transferActivityTaskFromProto(transferTask)
		case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
			task = s.transferRequestCancelTaskFromProto(transferTask)
		case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
			task = s.transferSignalTaskFromProto(transferTask)
		case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
			task = s.transferChildWorkflowTaskFromProto(transferTask)
		case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
			task = s.transferCloseTaskFromProto(transferTask)
		case enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
			task = s.transferResetTaskFromProto(transferTask)
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown transfer task type: %v", transferTask.TaskType))
		}

		taskSlice[index] = task
	}
	return taskSlice, nil
}

func (s *TaskSerializer) SerializeTimerTasks(
	taskSlice []tasks.Task,
) ([]commonpb.DataBlob, error) {
	blobSlice := make([]commonpb.DataBlob, len(taskSlice))
	for index, task := range taskSlice {
		var timerTask *persistencespb.TimerTaskInfo
		switch task.GetType() {
		case enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT:
			timerTask = s.timerWorkflowTaskToProto(task.(*tasks.WorkflowTaskTimeoutTask))
		case enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER:
			timerTask = s.timerWorkflowDelayTaskToProto(task.(*tasks.WorkflowBackoffTimerTask))
		case enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT:
			timerTask = s.timerActivityTaskToProto(task.(*tasks.ActivityTimeoutTask))
		case enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER:
			timerTask = s.timerActivityRetryTaskToProto(task.(*tasks.ActivityRetryTimerTask))
		case enumsspb.TASK_TYPE_USER_TIMER:
			timerTask = s.timerUserTaskToProto(task.(*tasks.UserTimerTask))
		case enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT:
			timerTask = s.timerWorkflowRunToProto(task.(*tasks.WorkflowTimeoutTask))
		case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT:
			timerTask = s.timerWorkflowCleanupTaskToProto(task.(*tasks.DeleteHistoryEventTask))
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown timer task type: %v", task.GetType()))
		}

		blob, err := TimerTaskInfoToBlob(timerTask)
		if err != nil {
			return nil, err
		}
		blobSlice[index] = blob
	}
	return blobSlice, nil
}

func (s *TaskSerializer) DeserializeTimerTasks(
	blobSlice []commonpb.DataBlob,
) ([]tasks.Task, error) {
	taskSlice := make([]tasks.Task, len(blobSlice))
	for index, blob := range blobSlice {
		timerTask, err := TimerTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
		if err != nil {
			return nil, err
		}
		var timer tasks.Task
		switch timerTask.TaskType {
		case enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT:
			timer = s.timerWorkflowTaskFromProto(timerTask)
		case enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER:
			timer = s.timerWorkflowDelayTaskFromProto(timerTask)
		case enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT:
			timer = s.timerActivityTaskFromProto(timerTask)
		case enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER:
			timer = s.timerActivityRetryTaskFromProto(timerTask)
		case enumsspb.TASK_TYPE_USER_TIMER:
			timer = s.timerUserTaskFromProto(timerTask)
		case enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT:
			timer = s.timerWorkflowRunFromProto(timerTask)
		case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT:
			timer = s.timerWorkflowCleanupTaskFromProto(timerTask)
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown timer task type: %v", timerTask.TaskType))
		}

		taskSlice[index] = timer
	}
	return taskSlice, nil
}

func (s *TaskSerializer) SerializeVisibilityTasks(
	taskSlice []tasks.Task,
) ([]commonpb.DataBlob, error) {
	blobSlice := make([]commonpb.DataBlob, len(taskSlice))
	for index, task := range taskSlice {
		var visibilityTask *persistencespb.VisibilityTaskInfo
		switch task.GetType() {
		case enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION:
			visibilityTask = s.visibilityStartTaskToProto(task.(*tasks.StartExecutionVisibilityTask))
		case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION:
			visibilityTask = s.visibilityUpsertTaskToProto(task.(*tasks.UpsertExecutionVisibilityTask))
		case enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION:
			visibilityTask = s.visibilityCloseTaskToProto(task.(*tasks.CloseExecutionVisibilityTask))
		case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
			visibilityTask = s.visibilityDeleteTaskToProto(task.(*tasks.DeleteExecutionVisibilityTask))
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown visibilit task type: %v", task.GetType()))
		}

		blob, err := VisibilityTaskInfoToBlob(visibilityTask)
		if err != nil {
			return nil, err
		}
		blobSlice[index] = blob
	}
	return blobSlice, nil
}

func (s *TaskSerializer) DeserializeVisibilityTasks(
	blobSlice []commonpb.DataBlob,
) ([]tasks.Task, error) {
	taskSlice := make([]tasks.Task, len(blobSlice))
	for index, blob := range blobSlice {
		visibilityTask, err := VisibilityTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
		if err != nil {
			return nil, err
		}
		var visibility tasks.Task
		switch visibilityTask.TaskType {
		case enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION:
			visibility = s.visibilityStartTaskFromProto(visibilityTask)
		case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION:
			visibility = s.visibilityUpsertTaskFromProto(visibilityTask)
		case enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION:
			visibility = s.visibilityCloseTaskFromProto(visibilityTask)
		case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
			visibility = s.visibilityDeleteTaskFromProto(visibilityTask)
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown visibility task type: %v", visibilityTask.TaskType))
		}

		taskSlice[index] = visibility
	}
	return taskSlice, nil
}

func (s *TaskSerializer) SerializeReplicationTasks(
	taskSlice []tasks.Task,
) ([]commonpb.DataBlob, error) {
	blobSlice := make([]commonpb.DataBlob, len(taskSlice))
	for index, task := range taskSlice {
		var replicationTask *persistencespb.ReplicationTaskInfo
		switch task.GetType() {
		case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
			replicationTask = s.replicationActivityTaskToProto(task.(*tasks.SyncActivityTask))
		case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
			replicationTask = s.replicationHistoryTaskToProto(task.(*tasks.HistoryReplicationTask))
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown repication task type: %v", task.GetType()))
		}

		blob, err := ReplicationTaskInfoToBlob(replicationTask)
		if err != nil {
			return nil, err
		}
		blobSlice[index] = blob
	}
	return blobSlice, nil
}

func (s *TaskSerializer) DeserializeReplicationTasks(
	blobSlice []commonpb.DataBlob,
) ([]tasks.Task, error) {
	taskSlice := make([]tasks.Task, len(blobSlice))
	for index, blob := range blobSlice {
		replicationTask, err := ReplicationTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
		if err != nil {
			return nil, err
		}
		var replication tasks.Task
		switch replicationTask.TaskType {
		case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
			replication = s.replicationActivityTaskFromProto(replicationTask)
		case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
			replication = s.replicationHistoryTaskFromProto(replicationTask)
		default:
			return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown replication task type: %v", replicationTask.TaskType))
		}

		taskSlice[index] = replication
	}
	return taskSlice, nil
}

func (s *TaskSerializer) transferActivityTaskToProto(
	activityTask *tasks.ActivityTask,
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

func (s *TaskSerializer) transferActivityTaskFromProto(
	activityTask *persistencespb.TransferTaskInfo,
) *tasks.ActivityTask {
	return &tasks.ActivityTask{
		VisibilityTimestamp: *activityTask.VisibilityTime,
		TaskID:              activityTask.TaskId,
		NamespaceID:         activityTask.TargetNamespaceId,
		TaskQueue:           activityTask.TaskQueue,
		ScheduleID:          activityTask.ScheduleId,
		Version:             activityTask.Version,
	}
}

func (s *TaskSerializer) transferWorkflowTaskToProto(
	workflowTask *tasks.WorkflowTask,
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

func (s *TaskSerializer) transferWorkflowTaskFromProto(
	workflowTask *persistencespb.TransferTaskInfo,
) *tasks.WorkflowTask {
	return &tasks.WorkflowTask{
		VisibilityTimestamp: *workflowTask.VisibilityTime,
		TaskID:              workflowTask.TaskId,
		NamespaceID:         workflowTask.TargetNamespaceId,
		TaskQueue:           workflowTask.TaskQueue,
		ScheduleID:          workflowTask.ScheduleId,
		Version:             workflowTask.Version,
	}
}

func (s *TaskSerializer) transferRequestCancelTaskToProto(
	requestCancelTask *tasks.CancelExecutionTask,
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

func (s *TaskSerializer) transferRequestCancelTaskFromProto(
	requestCancelTask *persistencespb.TransferTaskInfo,
) *tasks.CancelExecutionTask {
	return &tasks.CancelExecutionTask{
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

func (s *TaskSerializer) transferSignalTaskToProto(
	signalTask *tasks.SignalExecutionTask,
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

func (s *TaskSerializer) transferSignalTaskFromProto(
	signalTask *persistencespb.TransferTaskInfo,
) *tasks.SignalExecutionTask {
	return &tasks.SignalExecutionTask{
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

func (s *TaskSerializer) transferChildWorkflowTaskToProto(
	childWorkflowTask *tasks.StartChildExecutionTask,
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

func (s *TaskSerializer) transferChildWorkflowTaskFromProto(
	signalTask *persistencespb.TransferTaskInfo,
) *tasks.StartChildExecutionTask {
	return &tasks.StartChildExecutionTask{
		VisibilityTimestamp: *signalTask.VisibilityTime,
		TaskID:              signalTask.TaskId,
		TargetNamespaceID:   signalTask.TargetNamespaceId,
		TargetWorkflowID:    signalTask.TargetWorkflowId,
		InitiatedID:         signalTask.ScheduleId,
		Version:             signalTask.Version,
	}
}

func (s *TaskSerializer) transferCloseTaskToProto(
	closeTask *tasks.CloseExecutionTask,
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

func (s *TaskSerializer) transferCloseTaskFromProto(
	closeTask *persistencespb.TransferTaskInfo,
) *tasks.CloseExecutionTask {
	return &tasks.CloseExecutionTask{
		VisibilityTimestamp: *closeTask.VisibilityTime,
		TaskID:              closeTask.TaskId,
		Version:             closeTask.Version,
	}
}

func (s *TaskSerializer) transferResetTaskToProto(
	resetTask *tasks.ResetWorkflowTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             s.namespaceID,
		WorkflowId:              s.workflowID,
		RunId:                   s.runID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW,
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

func (s *TaskSerializer) transferResetTaskFromProto(
	resetTask *persistencespb.TransferTaskInfo,
) *tasks.ResetWorkflowTask {
	return &tasks.ResetWorkflowTask{
		VisibilityTimestamp: *resetTask.VisibilityTime,
		TaskID:              resetTask.TaskId,
		Version:             resetTask.Version,
	}
}

func (s *TaskSerializer) timerWorkflowTaskToProto(
	workflowTimer *tasks.WorkflowTaskTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         s.namespaceID,
		WorkflowId:          s.workflowID,
		RunId:               s.runID,
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT,
		TimeoutType:         workflowTimer.TimeoutType,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             workflowTimer.Version,
		ScheduleAttempt:     workflowTimer.ScheduleAttempt,
		EventId:             workflowTimer.EventID,
		TaskId:              workflowTimer.TaskID,
		VisibilityTime:      &workflowTimer.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) timerWorkflowTaskFromProto(
	workflowTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowTaskTimeoutTask {
	return &tasks.WorkflowTaskTimeoutTask{
		VisibilityTimestamp: *workflowTimer.VisibilityTime,
		TaskID:              workflowTimer.TaskId,
		EventID:             workflowTimer.EventId,
		ScheduleAttempt:     workflowTimer.ScheduleAttempt,
		TimeoutType:         workflowTimer.TimeoutType,
		Version:             workflowTimer.Version,
	}
}

func (s *TaskSerializer) timerWorkflowDelayTaskToProto(
	workflowDelayTimer *tasks.WorkflowBackoffTimerTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         s.namespaceID,
		WorkflowId:          s.workflowID,
		RunId:               s.runID,
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: workflowDelayTimer.WorkflowBackoffType,
		Version:             workflowDelayTimer.Version,
		ScheduleAttempt:     0,
		EventId:             0,
		TaskId:              workflowDelayTimer.TaskID,
		VisibilityTime:      &workflowDelayTimer.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) timerWorkflowDelayTaskFromProto(
	workflowDelayTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowBackoffTimerTask {
	return &tasks.WorkflowBackoffTimerTask{
		VisibilityTimestamp: *workflowDelayTimer.VisibilityTime,
		TaskID:              workflowDelayTimer.TaskId,
		Version:             workflowDelayTimer.Version,
		WorkflowBackoffType: workflowDelayTimer.WorkflowBackoffType,
	}
}

func (s *TaskSerializer) timerActivityTaskToProto(
	activityTimer *tasks.ActivityTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         s.namespaceID,
		WorkflowId:          s.workflowID,
		RunId:               s.runID,
		TaskType:            enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         activityTimer.TimeoutType,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             activityTimer.Version,
		ScheduleAttempt:     activityTimer.Attempt,
		EventId:             activityTimer.EventID,
		TaskId:              activityTimer.TaskID,
		VisibilityTime:      &activityTimer.VisibilityTimestamp,
	}
}
func (s *TaskSerializer) timerActivityTaskFromProto(
	activityTimer *persistencespb.TimerTaskInfo,
) *tasks.ActivityTimeoutTask {
	return &tasks.ActivityTimeoutTask{
		VisibilityTimestamp: *activityTimer.VisibilityTime,
		TaskID:              activityTimer.TaskId,
		EventID:             activityTimer.EventId,
		Attempt:             activityTimer.ScheduleAttempt,
		TimeoutType:         activityTimer.TimeoutType,
		Version:             activityTimer.Version,
	}
}

func (s *TaskSerializer) timerActivityRetryTaskToProto(
	activityRetryTimer *tasks.ActivityRetryTimerTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         s.namespaceID,
		WorkflowId:          s.workflowID,
		RunId:               s.runID,
		TaskType:            enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             activityRetryTimer.Version,
		ScheduleAttempt:     activityRetryTimer.Attempt,
		EventId:             activityRetryTimer.EventID,
		TaskId:              activityRetryTimer.TaskID,
		VisibilityTime:      &activityRetryTimer.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) timerActivityRetryTaskFromProto(
	activityRetryTimer *persistencespb.TimerTaskInfo,
) *tasks.ActivityRetryTimerTask {
	return &tasks.ActivityRetryTimerTask{
		VisibilityTimestamp: *activityRetryTimer.VisibilityTime,
		TaskID:              activityRetryTimer.TaskId,
		EventID:             activityRetryTimer.EventId,
		Version:             activityRetryTimer.Version,
		Attempt:             activityRetryTimer.ScheduleAttempt,
	}
}

func (s *TaskSerializer) timerUserTaskToProto(
	userTimer *tasks.UserTimerTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         s.namespaceID,
		WorkflowId:          s.workflowID,
		RunId:               s.runID,
		TaskType:            enumsspb.TASK_TYPE_USER_TIMER,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             userTimer.Version,
		ScheduleAttempt:     0,
		EventId:             userTimer.EventID,
		TaskId:              userTimer.TaskID,
		VisibilityTime:      &userTimer.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) timerUserTaskFromProto(
	userTimer *persistencespb.TimerTaskInfo,
) *tasks.UserTimerTask {
	return &tasks.UserTimerTask{
		VisibilityTimestamp: *userTimer.VisibilityTime,
		TaskID:              userTimer.TaskId,
		EventID:             userTimer.EventId,
		Version:             userTimer.Version,
	}
}

func (s *TaskSerializer) timerWorkflowRunToProto(
	workflowTimer *tasks.WorkflowTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         s.namespaceID,
		WorkflowId:          s.workflowID,
		RunId:               s.runID,
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             workflowTimer.Version,
		ScheduleAttempt:     0,
		EventId:             0,
		TaskId:              workflowTimer.TaskID,
		VisibilityTime:      &workflowTimer.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) timerWorkflowRunFromProto(
	workflowTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowTimeoutTask {
	return &tasks.WorkflowTimeoutTask{
		VisibilityTimestamp: *workflowTimer.VisibilityTime,
		TaskID:              workflowTimer.TaskId,
		Version:             workflowTimer.Version,
	}
}

func (s *TaskSerializer) timerWorkflowCleanupTaskToProto(
	workflowCleanupTimer *tasks.DeleteHistoryEventTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         s.namespaceID,
		WorkflowId:          s.workflowID,
		RunId:               s.runID,
		TaskType:            enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             workflowCleanupTimer.Version,
		ScheduleAttempt:     0,
		EventId:             0,
		TaskId:              workflowCleanupTimer.TaskID,
		VisibilityTime:      &workflowCleanupTimer.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) timerWorkflowCleanupTaskFromProto(
	workflowCleanupTimer *persistencespb.TimerTaskInfo,
) *tasks.DeleteHistoryEventTask {
	return &tasks.DeleteHistoryEventTask{
		VisibilityTimestamp: *workflowCleanupTimer.VisibilityTime,
		TaskID:              workflowCleanupTimer.TaskId,
		Version:             workflowCleanupTimer.Version,
	}
}

func (s *TaskSerializer) visibilityStartTaskToProto(
	startVisibilityTask *tasks.StartExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:    s.namespaceID,
		WorkflowId:     s.workflowID,
		RunId:          s.runID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION,
		Version:        startVisibilityTask.Version,
		TaskId:         startVisibilityTask.TaskID,
		VisibilityTime: &startVisibilityTask.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) visibilityStartTaskFromProto(
	startVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.StartExecutionVisibilityTask {
	return &tasks.StartExecutionVisibilityTask{
		VisibilityTimestamp: *startVisibilityTask.VisibilityTime,
		TaskID:              startVisibilityTask.TaskId,
		Version:             startVisibilityTask.Version,
	}
}

func (s *TaskSerializer) visibilityUpsertTaskToProto(
	upsertVisibilityTask *tasks.UpsertExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:    s.namespaceID,
		WorkflowId:     s.workflowID,
		RunId:          s.runID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
		Version:        upsertVisibilityTask.Version,
		TaskId:         upsertVisibilityTask.TaskID,
		VisibilityTime: &upsertVisibilityTask.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) visibilityUpsertTaskFromProto(
	upsertVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.UpsertExecutionVisibilityTask {
	return &tasks.UpsertExecutionVisibilityTask{
		VisibilityTimestamp: *upsertVisibilityTask.VisibilityTime,
		TaskID:              upsertVisibilityTask.TaskId,
		Version:             upsertVisibilityTask.Version,
	}
}

func (s *TaskSerializer) visibilityCloseTaskToProto(
	closetVisibilityTask *tasks.CloseExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:    s.namespaceID,
		WorkflowId:     s.workflowID,
		RunId:          s.runID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
		Version:        closetVisibilityTask.Version,
		TaskId:         closetVisibilityTask.TaskID,
		VisibilityTime: &closetVisibilityTask.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) visibilityCloseTaskFromProto(
	closeVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.CloseExecutionVisibilityTask {
	return &tasks.CloseExecutionVisibilityTask{
		VisibilityTimestamp: *closeVisibilityTask.VisibilityTime,
		TaskID:              closeVisibilityTask.TaskId,
		Version:             closeVisibilityTask.Version,
	}
}

func (s *TaskSerializer) visibilityDeleteTaskToProto(
	deleteVisibilityTask *tasks.DeleteExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:    s.namespaceID,
		WorkflowId:     s.workflowID,
		RunId:          s.runID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		Version:        deleteVisibilityTask.Version,
		TaskId:         deleteVisibilityTask.TaskID,
		VisibilityTime: &deleteVisibilityTask.VisibilityTimestamp,
	}
}

func (s *TaskSerializer) visibilityDeleteTaskFromProto(
	deleteVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.DeleteExecutionVisibilityTask {
	return &tasks.DeleteExecutionVisibilityTask{
		VisibilityTimestamp: *deleteVisibilityTask.VisibilityTime,
		TaskID:              deleteVisibilityTask.TaskId,
		Version:             deleteVisibilityTask.Version,
	}
}

func (s *TaskSerializer) replicationActivityTaskToProto(
	activityTask *tasks.SyncActivityTask,
) *persistencespb.ReplicationTaskInfo {
	return &persistencespb.ReplicationTaskInfo{
		NamespaceId:       s.namespaceID,
		WorkflowId:        s.workflowID,
		RunId:             s.runID,
		TaskType:          enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		TaskId:            activityTask.TaskID,
		Version:           activityTask.Version,
		ScheduledId:       activityTask.ScheduledID,
		FirstEventId:      0,
		NextEventId:       0,
		BranchToken:       nil,
		NewRunBranchToken: nil,
	}
}

func (s *TaskSerializer) replicationActivityTaskFromProto(
	activityTask *persistencespb.ReplicationTaskInfo,
) *tasks.SyncActivityTask {
	return &tasks.SyncActivityTask{
		VisibilityTimestamp: time.Time{}, // TODO add the missing attribute to proto definition
		Version:             activityTask.Version,
		TaskID:              activityTask.TaskId,
		ScheduledID:         activityTask.ScheduledId,
	}
}

func (s *TaskSerializer) replicationHistoryTaskToProto(
	historyTask *tasks.HistoryReplicationTask,
) *persistencespb.ReplicationTaskInfo {
	return &persistencespb.ReplicationTaskInfo{
		NamespaceId:       s.namespaceID,
		WorkflowId:        s.workflowID,
		RunId:             s.runID,
		TaskType:          enumsspb.TASK_TYPE_REPLICATION_HISTORY,
		TaskId:            historyTask.TaskID,
		Version:           historyTask.Version,
		ScheduledId:       0,
		FirstEventId:      historyTask.FirstEventID,
		NextEventId:       historyTask.NextEventID,
		BranchToken:       historyTask.BranchToken,
		NewRunBranchToken: historyTask.NewRunBranchToken,
	}
}

func (s *TaskSerializer) replicationHistoryTaskFromProto(
	historyTask *persistencespb.ReplicationTaskInfo,
) *tasks.HistoryReplicationTask {
	return &tasks.HistoryReplicationTask{
		VisibilityTimestamp: time.Time{}, // TODO add the missing attribute to proto definition
		TaskID:              historyTask.TaskId,
		FirstEventID:        historyTask.FirstEventId,
		NextEventID:         historyTask.NextEventId,
		Version:             historyTask.Version,
		BranchToken:         historyTask.BranchToken,
		NewRunBranchToken:   historyTask.NewRunBranchToken,
	}
}
