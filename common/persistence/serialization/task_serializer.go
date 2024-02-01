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
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
)

type (
	TaskSerializer struct {
	}
)

func NewTaskSerializer() *TaskSerializer {
	return &TaskSerializer{}
}

func (s *TaskSerializer) SerializeTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	category := task.GetCategory()
	switch category.ID() {
	case tasks.CategoryIDTransfer:
		return s.serializeTransferTask(task)
	case tasks.CategoryIDTimer:
		return s.serializeTimerTask(task)
	case tasks.CategoryIDVisibility:
		return s.serializeVisibilityTask(task)
	case tasks.CategoryIDReplication:
		return s.serializeReplicationTask(task)
	case tasks.CategoryIDArchival:
		return s.serializeArchivalTask(task)
	case tasks.CategoryIDCallback:
		return s.serializeCallbackTask(task)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown task category: %v", category))
	}
}

func (s *TaskSerializer) DeserializeTask(
	category tasks.Category,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	switch category.ID() {
	case tasks.CategoryIDTransfer:
		return s.deserializeTransferTasks(blob)
	case tasks.CategoryIDTimer:
		return s.deserializeTimerTasks(blob)
	case tasks.CategoryIDVisibility:
		return s.deserializeVisibilityTasks(blob)
	case tasks.CategoryIDReplication:
		return s.deserializeReplicationTasks(blob)
	case tasks.CategoryIDArchival:
		return s.deserializeArchivalTasks(blob)
	case tasks.CategoryIDCallback:
		return s.deserializeCallbackTask(blob)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown task category: %v", category))
	}
}

func (s *TaskSerializer) serializeTransferTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var transferTask *persistencespb.TransferTaskInfo
	switch task := task.(type) {
	case *tasks.WorkflowTask:
		transferTask = s.transferWorkflowTaskToProto(task)
	case *tasks.ActivityTask:
		transferTask = s.transferActivityTaskToProto(task)
	case *tasks.CancelExecutionTask:
		transferTask = s.transferRequestCancelTaskToProto(task)
	case *tasks.SignalExecutionTask:
		transferTask = s.transferSignalTaskToProto(task)
	case *tasks.StartChildExecutionTask:
		transferTask = s.transferChildWorkflowTaskToProto(task)
	case *tasks.CloseExecutionTask:
		transferTask = s.transferCloseTaskToProto(task)
	case *tasks.ResetWorkflowTask:
		transferTask = s.transferResetTaskToProto(task)
	case *tasks.DeleteExecutionTask:
		transferTask = s.transferDeleteExecutionTaskToProto(task)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown transfer task type: %v", task))
	}

	return TransferTaskInfoToBlob(transferTask)
}

func (s *TaskSerializer) deserializeTransferTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
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
	case enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION:
		task = s.transferDeleteExecutionTaskFromProto(transferTask)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown transfer task type: %v", transferTask.TaskType))
	}
	return task, nil
}

func (s *TaskSerializer) serializeTimerTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var timerTask *persistencespb.TimerTaskInfo
	switch task := task.(type) {
	case *tasks.WorkflowTaskTimeoutTask:
		timerTask = s.timerWorkflowTaskToProto(task)
	case *tasks.WorkflowBackoffTimerTask:
		timerTask = s.timerWorkflowDelayTaskToProto(task)
	case *tasks.ActivityTimeoutTask:
		timerTask = s.timerActivityTaskToProto(task)
	case *tasks.ActivityRetryTimerTask:
		timerTask = s.timerActivityRetryTaskToProto(task)
	case *tasks.UserTimerTask:
		timerTask = s.timerUserTaskToProto(task)
	case *tasks.WorkflowTimeoutTask:
		timerTask = s.timerWorkflowRunToProto(task)
	case *tasks.DeleteHistoryEventTask:
		timerTask = s.timerWorkflowCleanupTaskToProto(task)
	case *tasks.CallbackBackoffTask:
		timerTask = s.callbackBackoffTaskToProto(task)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown timer task type: %v", task))
	}

	return TimerTaskInfoToBlob(timerTask)
}

func (s *TaskSerializer) deserializeTimerTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
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
	case enumsspb.TASK_TYPE_CALLBACK_BACKOFF:
		timer = s.callbackBackoffTaskFromProto(timerTask)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown timer task type: %v", timerTask.TaskType))
	}
	return timer, nil
}

func (s *TaskSerializer) serializeVisibilityTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var visibilityTask *persistencespb.VisibilityTaskInfo
	switch task := task.(type) {
	case *tasks.StartExecutionVisibilityTask:
		visibilityTask = s.visibilityStartTaskToProto(task)
	case *tasks.UpsertExecutionVisibilityTask:
		visibilityTask = s.visibilityUpsertTaskToProto(task)
	case *tasks.CloseExecutionVisibilityTask:
		visibilityTask = s.visibilityCloseTaskToProto(task)
	case *tasks.DeleteExecutionVisibilityTask:
		visibilityTask = s.visibilityDeleteTaskToProto(task)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown visibility task type: %v", task))
	}

	return VisibilityTaskInfoToBlob(visibilityTask)
}

func (s *TaskSerializer) deserializeVisibilityTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
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
	return visibility, nil
}

func (s *TaskSerializer) serializeReplicationTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var replicationTask *persistencespb.ReplicationTaskInfo
	switch task := task.(type) {
	case *tasks.SyncActivityTask:
		replicationTask = s.replicationActivityTaskToProto(task)
	case *tasks.HistoryReplicationTask:
		replicationTask = s.replicationHistoryTaskToProto(task)
	case *tasks.SyncWorkflowStateTask:
		replicationTask = s.replicationSyncWorkflowStateTaskToProto(task)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown repication task type: %v", task))
	}

	return ReplicationTaskInfoToBlob(replicationTask)
}

func (s *TaskSerializer) deserializeReplicationTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	replicationTask, err := ReplicationTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
	if err != nil {
		return nil, err
	}
	return s.ParseReplicationTask(replicationTask)
}

func (s *TaskSerializer) ParseReplicationTask(replicationTask *persistencespb.ReplicationTaskInfo) (tasks.Task, error) {
	switch replicationTask.TaskType {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return s.replicationActivityTaskFromProto(replicationTask), nil
	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return s.replicationHistoryTaskFromProto(replicationTask), nil
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE:
		return s.replicationSyncWorkflowStateTaskFromProto(replicationTask), nil
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown replication task type: %v", replicationTask.TaskType))
	}
}

func (s *TaskSerializer) serializeArchivalTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var archivalTaskInfo *persistencespb.ArchivalTaskInfo
	switch task := task.(type) {
	case *tasks.ArchiveExecutionTask:
		archivalTaskInfo = s.archiveExecutionTaskToProto(task)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf(
			"Unknown archival task type while serializing: %v", task))
	}

	return ArchivalTaskInfoToBlob(archivalTaskInfo)
}

func (s *TaskSerializer) deserializeArchivalTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	archivalTask, err := ArchivalTaskInfoFromBlob(blob.Data, blob.EncodingType.String())
	if err != nil {
		return nil, err
	}
	var task tasks.Task
	switch archivalTask.TaskType {
	case enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION:
		task = s.archiveExecutionTaskFromProto(archivalTask)
	default:
		return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown archival task type while deserializing: %v", task))
	}
	return task, nil
}

func (s *TaskSerializer) transferActivityTaskToProto(
	activityTask *tasks.ActivityTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             activityTask.WorkflowKey.NamespaceID,
		WorkflowId:              activityTask.WorkflowKey.WorkflowID,
		RunId:                   activityTask.WorkflowKey.RunID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK,
		TargetNamespaceId:       activityTask.WorkflowKey.NamespaceID,
		TargetWorkflowId:        "",
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               activityTask.TaskQueue,
		ScheduledEventId:        activityTask.ScheduledEventID,
		Version:                 activityTask.Version,
		TaskId:                  activityTask.TaskID,
		VisibilityTime:          timestamppb.New(activityTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) transferActivityTaskFromProto(
	activityTask *persistencespb.TransferTaskInfo,
) *tasks.ActivityTask {
	return &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			activityTask.NamespaceId,
			activityTask.WorkflowId,
			activityTask.RunId,
		),
		VisibilityTimestamp: activityTask.VisibilityTime.AsTime(),
		TaskID:              activityTask.TaskId,
		TaskQueue:           activityTask.TaskQueue,
		ScheduledEventID:    activityTask.ScheduledEventId,
		Version:             activityTask.Version,
	}
}

func (s *TaskSerializer) transferWorkflowTaskToProto(
	workflowTask *tasks.WorkflowTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             workflowTask.WorkflowKey.NamespaceID,
		WorkflowId:              workflowTask.WorkflowKey.WorkflowID,
		RunId:                   workflowTask.WorkflowKey.RunID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK,
		TargetNamespaceId:       workflowTask.NamespaceID,
		TargetWorkflowId:        "",
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               workflowTask.TaskQueue,
		ScheduledEventId:        workflowTask.ScheduledEventID,
		Version:                 workflowTask.Version,
		TaskId:                  workflowTask.TaskID,
		VisibilityTime:          timestamppb.New(workflowTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) transferWorkflowTaskFromProto(
	workflowTask *persistencespb.TransferTaskInfo,
) *tasks.WorkflowTask {
	return &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowTask.NamespaceId,
			workflowTask.WorkflowId,
			workflowTask.RunId,
		),
		VisibilityTimestamp: workflowTask.VisibilityTime.AsTime(),
		TaskID:              workflowTask.TaskId,
		TaskQueue:           workflowTask.TaskQueue,
		ScheduledEventID:    workflowTask.ScheduledEventId,
		Version:             workflowTask.Version,
	}
}

func (s *TaskSerializer) transferRequestCancelTaskToProto(
	requestCancelTask *tasks.CancelExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             requestCancelTask.WorkflowKey.NamespaceID,
		WorkflowId:              requestCancelTask.WorkflowKey.WorkflowID,
		RunId:                   requestCancelTask.WorkflowKey.RunID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION,
		TargetNamespaceId:       requestCancelTask.TargetNamespaceID,
		TargetWorkflowId:        requestCancelTask.TargetWorkflowID,
		TargetRunId:             requestCancelTask.TargetRunID,
		TargetChildWorkflowOnly: requestCancelTask.TargetChildWorkflowOnly,
		TaskQueue:               "",
		ScheduledEventId:        requestCancelTask.InitiatedEventID,
		Version:                 requestCancelTask.Version,
		TaskId:                  requestCancelTask.TaskID,
		VisibilityTime:          timestamppb.New(requestCancelTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) transferRequestCancelTaskFromProto(
	requestCancelTask *persistencespb.TransferTaskInfo,
) *tasks.CancelExecutionTask {
	return &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			requestCancelTask.NamespaceId,
			requestCancelTask.WorkflowId,
			requestCancelTask.RunId,
		),
		VisibilityTimestamp:     requestCancelTask.VisibilityTime.AsTime(),
		TaskID:                  requestCancelTask.TaskId,
		TargetNamespaceID:       requestCancelTask.TargetNamespaceId,
		TargetWorkflowID:        requestCancelTask.TargetWorkflowId,
		TargetRunID:             requestCancelTask.TargetRunId,
		TargetChildWorkflowOnly: requestCancelTask.TargetChildWorkflowOnly,
		InitiatedEventID:        requestCancelTask.ScheduledEventId,
		Version:                 requestCancelTask.Version,
	}
}

func (s *TaskSerializer) transferSignalTaskToProto(
	signalTask *tasks.SignalExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             signalTask.WorkflowKey.NamespaceID,
		WorkflowId:              signalTask.WorkflowKey.WorkflowID,
		RunId:                   signalTask.WorkflowKey.RunID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION,
		TargetNamespaceId:       signalTask.TargetNamespaceID,
		TargetWorkflowId:        signalTask.TargetWorkflowID,
		TargetRunId:             signalTask.TargetRunID,
		TargetChildWorkflowOnly: signalTask.TargetChildWorkflowOnly,
		TaskQueue:               "",
		ScheduledEventId:        signalTask.InitiatedEventID,
		Version:                 signalTask.Version,
		TaskId:                  signalTask.TaskID,
		VisibilityTime:          timestamppb.New(signalTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) transferSignalTaskFromProto(
	signalTask *persistencespb.TransferTaskInfo,
) *tasks.SignalExecutionTask {
	return &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			signalTask.NamespaceId,
			signalTask.WorkflowId,
			signalTask.RunId,
		),
		VisibilityTimestamp:     signalTask.VisibilityTime.AsTime(),
		TaskID:                  signalTask.TaskId,
		TargetNamespaceID:       signalTask.TargetNamespaceId,
		TargetWorkflowID:        signalTask.TargetWorkflowId,
		TargetRunID:             signalTask.TargetRunId,
		TargetChildWorkflowOnly: signalTask.TargetChildWorkflowOnly,
		InitiatedEventID:        signalTask.ScheduledEventId,
		Version:                 signalTask.Version,
	}
}

func (s *TaskSerializer) transferChildWorkflowTaskToProto(
	childWorkflowTask *tasks.StartChildExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             childWorkflowTask.WorkflowKey.NamespaceID,
		WorkflowId:              childWorkflowTask.WorkflowKey.WorkflowID,
		RunId:                   childWorkflowTask.WorkflowKey.RunID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION,
		TargetNamespaceId:       childWorkflowTask.TargetNamespaceID,
		TargetWorkflowId:        childWorkflowTask.TargetWorkflowID,
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               "",
		ScheduledEventId:        childWorkflowTask.InitiatedEventID,
		Version:                 childWorkflowTask.Version,
		TaskId:                  childWorkflowTask.TaskID,
		VisibilityTime:          timestamppb.New(childWorkflowTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) transferChildWorkflowTaskFromProto(
	signalTask *persistencespb.TransferTaskInfo,
) *tasks.StartChildExecutionTask {
	return &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			signalTask.NamespaceId,
			signalTask.WorkflowId,
			signalTask.RunId,
		),
		VisibilityTimestamp: signalTask.VisibilityTime.AsTime(),
		TaskID:              signalTask.TaskId,
		TargetNamespaceID:   signalTask.TargetNamespaceId,
		TargetWorkflowID:    signalTask.TargetWorkflowId,
		InitiatedEventID:    signalTask.ScheduledEventId,
		Version:             signalTask.Version,
	}
}

func (s *TaskSerializer) transferCloseTaskToProto(
	closeTask *tasks.CloseExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             closeTask.WorkflowKey.NamespaceID,
		WorkflowId:              closeTask.WorkflowKey.WorkflowID,
		RunId:                   closeTask.WorkflowKey.RunID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION,
		TargetNamespaceId:       "",
		TargetWorkflowId:        "",
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               "",
		ScheduledEventId:        0,
		Version:                 closeTask.Version,
		TaskId:                  closeTask.TaskID,
		VisibilityTime:          timestamppb.New(closeTask.VisibilityTimestamp),
		DeleteAfterClose:        closeTask.DeleteAfterClose,
		TaskDetails: &persistencespb.TransferTaskInfo_CloseExecutionTaskDetails_{
			CloseExecutionTaskDetails: &persistencespb.TransferTaskInfo_CloseExecutionTaskDetails{
				// We set this to true even though it's no longer checked in case someone downgrades to a version that
				// still checks this field.
				CanSkipVisibilityArchival: true,
			},
		},
	}
}

func (s *TaskSerializer) transferCloseTaskFromProto(
	closeTask *persistencespb.TransferTaskInfo,
) *tasks.CloseExecutionTask {
	return &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			closeTask.NamespaceId,
			closeTask.WorkflowId,
			closeTask.RunId,
		),
		VisibilityTimestamp: closeTask.VisibilityTime.AsTime(),
		TaskID:              closeTask.TaskId,
		Version:             closeTask.Version,
		DeleteAfterClose:    closeTask.DeleteAfterClose,
		// Delete workflow task process stage is not persisted. It is only for in memory retries.
		DeleteProcessStage: tasks.DeleteWorkflowExecutionStageNone,
	}
}

func (s *TaskSerializer) transferResetTaskToProto(
	resetTask *tasks.ResetWorkflowTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:             resetTask.WorkflowKey.NamespaceID,
		WorkflowId:              resetTask.WorkflowKey.WorkflowID,
		RunId:                   resetTask.WorkflowKey.RunID,
		TaskType:                enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW,
		TargetNamespaceId:       "",
		TargetWorkflowId:        "",
		TargetRunId:             "",
		TargetChildWorkflowOnly: false,
		TaskQueue:               "",
		ScheduledEventId:        0,
		Version:                 resetTask.Version,
		TaskId:                  resetTask.TaskID,
		VisibilityTime:          timestamppb.New(resetTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) transferResetTaskFromProto(
	resetTask *persistencespb.TransferTaskInfo,
) *tasks.ResetWorkflowTask {
	return &tasks.ResetWorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			resetTask.NamespaceId,
			resetTask.WorkflowId,
			resetTask.RunId,
		),
		VisibilityTimestamp: resetTask.VisibilityTime.AsTime(),
		TaskID:              resetTask.TaskId,
		Version:             resetTask.Version,
	}
}

func (s *TaskSerializer) transferDeleteExecutionTaskToProto(
	deleteExecutionTask *tasks.DeleteExecutionTask,
) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:    deleteExecutionTask.WorkflowKey.NamespaceID,
		WorkflowId:     deleteExecutionTask.WorkflowKey.WorkflowID,
		RunId:          deleteExecutionTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		Version:        deleteExecutionTask.Version,
		TaskId:         deleteExecutionTask.TaskID,
		VisibilityTime: timestamppb.New(deleteExecutionTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) transferDeleteExecutionTaskFromProto(
	deleteExecutionTask *persistencespb.TransferTaskInfo,
) *tasks.DeleteExecutionTask {
	return &tasks.DeleteExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			deleteExecutionTask.NamespaceId,
			deleteExecutionTask.WorkflowId,
			deleteExecutionTask.RunId,
		),
		VisibilityTimestamp: deleteExecutionTask.VisibilityTime.AsTime(),
		TaskID:              deleteExecutionTask.TaskId,
		Version:             deleteExecutionTask.Version,
		// Delete workflow task process stage is not persisted. It is only for in memory retries.
		ProcessStage: tasks.DeleteWorkflowExecutionStageNone,
	}
}

func (s *TaskSerializer) timerWorkflowTaskToProto(
	workflowTimer *tasks.WorkflowTaskTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         workflowTimer.WorkflowKey.NamespaceID,
		WorkflowId:          workflowTimer.WorkflowKey.WorkflowID,
		RunId:               workflowTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT,
		TimeoutType:         workflowTimer.TimeoutType,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             workflowTimer.Version,
		ScheduleAttempt:     workflowTimer.ScheduleAttempt,
		EventId:             workflowTimer.EventID,
		TaskId:              workflowTimer.TaskID,
		VisibilityTime:      timestamppb.New(workflowTimer.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) timerWorkflowTaskFromProto(
	workflowTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowTaskTimeoutTask {
	return &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowTimer.NamespaceId,
			workflowTimer.WorkflowId,
			workflowTimer.RunId,
		),
		VisibilityTimestamp: workflowTimer.VisibilityTime.AsTime(),
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
		NamespaceId:         workflowDelayTimer.WorkflowKey.NamespaceID,
		WorkflowId:          workflowDelayTimer.WorkflowKey.WorkflowID,
		RunId:               workflowDelayTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: workflowDelayTimer.WorkflowBackoffType,
		Version:             workflowDelayTimer.Version,
		ScheduleAttempt:     0,
		EventId:             0,
		TaskId:              workflowDelayTimer.TaskID,
		VisibilityTime:      timestamppb.New(workflowDelayTimer.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) timerWorkflowDelayTaskFromProto(
	workflowDelayTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowBackoffTimerTask {
	return &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowDelayTimer.NamespaceId,
			workflowDelayTimer.WorkflowId,
			workflowDelayTimer.RunId,
		),
		VisibilityTimestamp: workflowDelayTimer.VisibilityTime.AsTime(),
		TaskID:              workflowDelayTimer.TaskId,
		Version:             workflowDelayTimer.Version,
		WorkflowBackoffType: workflowDelayTimer.WorkflowBackoffType,
	}
}

func (s *TaskSerializer) timerActivityTaskToProto(
	activityTimer *tasks.ActivityTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         activityTimer.WorkflowKey.NamespaceID,
		WorkflowId:          activityTimer.WorkflowKey.WorkflowID,
		RunId:               activityTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         activityTimer.TimeoutType,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             activityTimer.Version,
		ScheduleAttempt:     activityTimer.Attempt,
		EventId:             activityTimer.EventID,
		TaskId:              activityTimer.TaskID,
		VisibilityTime:      timestamppb.New(activityTimer.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) timerActivityTaskFromProto(
	activityTimer *persistencespb.TimerTaskInfo,
) *tasks.ActivityTimeoutTask {
	return &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			activityTimer.NamespaceId,
			activityTimer.WorkflowId,
			activityTimer.RunId,
		),
		VisibilityTimestamp: activityTimer.VisibilityTime.AsTime(),
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
		NamespaceId:         activityRetryTimer.WorkflowKey.NamespaceID,
		WorkflowId:          activityRetryTimer.WorkflowKey.WorkflowID,
		RunId:               activityRetryTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             activityRetryTimer.Version,
		ScheduleAttempt:     activityRetryTimer.Attempt,
		EventId:             activityRetryTimer.EventID,
		TaskId:              activityRetryTimer.TaskID,
		VisibilityTime:      timestamppb.New(activityRetryTimer.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) timerActivityRetryTaskFromProto(
	activityRetryTimer *persistencespb.TimerTaskInfo,
) *tasks.ActivityRetryTimerTask {
	return &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			activityRetryTimer.NamespaceId,
			activityRetryTimer.WorkflowId,
			activityRetryTimer.RunId,
		),
		VisibilityTimestamp: activityRetryTimer.VisibilityTime.AsTime(),
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
		NamespaceId:         userTimer.WorkflowKey.NamespaceID,
		WorkflowId:          userTimer.WorkflowKey.WorkflowID,
		RunId:               userTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_USER_TIMER,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             userTimer.Version,
		ScheduleAttempt:     0,
		EventId:             userTimer.EventID,
		TaskId:              userTimer.TaskID,
		VisibilityTime:      timestamppb.New(userTimer.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) timerUserTaskFromProto(
	userTimer *persistencespb.TimerTaskInfo,
) *tasks.UserTimerTask {
	return &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			userTimer.NamespaceId,
			userTimer.WorkflowId,
			userTimer.RunId,
		),
		VisibilityTimestamp: userTimer.VisibilityTime.AsTime(),
		TaskID:              userTimer.TaskId,
		EventID:             userTimer.EventId,
		Version:             userTimer.Version,
	}
}

func (s *TaskSerializer) timerWorkflowRunToProto(
	workflowTimer *tasks.WorkflowTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         workflowTimer.WorkflowKey.NamespaceID,
		WorkflowId:          workflowTimer.WorkflowKey.WorkflowID,
		RunId:               workflowTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             workflowTimer.Version,
		ScheduleAttempt:     0,
		EventId:             0,
		TaskId:              workflowTimer.TaskID,
		VisibilityTime:      timestamppb.New(workflowTimer.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) timerWorkflowRunFromProto(
	workflowTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowTimeoutTask {
	return &tasks.WorkflowTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowTimer.NamespaceId,
			workflowTimer.WorkflowId,
			workflowTimer.RunId,
		),
		VisibilityTimestamp: workflowTimer.VisibilityTime.AsTime(),
		TaskID:              workflowTimer.TaskId,
		Version:             workflowTimer.Version,
	}
}

func (s *TaskSerializer) timerWorkflowCleanupTaskToProto(
	workflowCleanupTimer *tasks.DeleteHistoryEventTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:         workflowCleanupTimer.WorkflowKey.NamespaceID,
		WorkflowId:          workflowCleanupTimer.WorkflowKey.WorkflowID,
		RunId:               workflowCleanupTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             workflowCleanupTimer.Version,
		ScheduleAttempt:     0,
		EventId:             0,
		TaskId:              workflowCleanupTimer.TaskID,
		VisibilityTime:      timestamppb.New(workflowCleanupTimer.VisibilityTimestamp),
		BranchToken:         workflowCleanupTimer.BranchToken,
		// We set this to true even though it's no longer checked in case someone downgrades to a version that still
		// checks this field.
		AlreadyArchived: true,
	}
}

func (s *TaskSerializer) timerWorkflowCleanupTaskFromProto(
	workflowCleanupTimer *persistencespb.TimerTaskInfo,
) *tasks.DeleteHistoryEventTask {
	return &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowCleanupTimer.NamespaceId,
			workflowCleanupTimer.WorkflowId,
			workflowCleanupTimer.RunId,
		),
		VisibilityTimestamp: workflowCleanupTimer.VisibilityTime.AsTime(),
		TaskID:              workflowCleanupTimer.TaskId,
		Version:             workflowCleanupTimer.Version,
		BranchToken:         workflowCleanupTimer.BranchToken,
		// Delete workflow task process stage is not persisted. It is only for in memory retries.
		ProcessStage: tasks.DeleteWorkflowExecutionStageNone,
	}
}

func (s *TaskSerializer) visibilityStartTaskToProto(
	startVisibilityTask *tasks.StartExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:    startVisibilityTask.WorkflowKey.NamespaceID,
		WorkflowId:     startVisibilityTask.WorkflowKey.WorkflowID,
		RunId:          startVisibilityTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION,
		Version:        startVisibilityTask.Version,
		TaskId:         startVisibilityTask.TaskID,
		VisibilityTime: timestamppb.New(startVisibilityTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) visibilityStartTaskFromProto(
	startVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.StartExecutionVisibilityTask {
	return &tasks.StartExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			startVisibilityTask.NamespaceId,
			startVisibilityTask.WorkflowId,
			startVisibilityTask.RunId,
		),
		VisibilityTimestamp: startVisibilityTask.VisibilityTime.AsTime(),
		TaskID:              startVisibilityTask.TaskId,
		Version:             startVisibilityTask.Version,
	}
}

func (s *TaskSerializer) visibilityUpsertTaskToProto(
	upsertVisibilityTask *tasks.UpsertExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:    upsertVisibilityTask.WorkflowKey.NamespaceID,
		WorkflowId:     upsertVisibilityTask.WorkflowKey.WorkflowID,
		RunId:          upsertVisibilityTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
		Version:        upsertVisibilityTask.Version,
		TaskId:         upsertVisibilityTask.TaskID,
		VisibilityTime: timestamppb.New(upsertVisibilityTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) visibilityUpsertTaskFromProto(
	upsertVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.UpsertExecutionVisibilityTask {
	return &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			upsertVisibilityTask.NamespaceId,
			upsertVisibilityTask.WorkflowId,
			upsertVisibilityTask.RunId,
		),
		VisibilityTimestamp: upsertVisibilityTask.VisibilityTime.AsTime(),
		TaskID:              upsertVisibilityTask.TaskId,
		Version:             upsertVisibilityTask.Version,
	}
}

func (s *TaskSerializer) visibilityCloseTaskToProto(
	closetVisibilityTask *tasks.CloseExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:    closetVisibilityTask.WorkflowKey.NamespaceID,
		WorkflowId:     closetVisibilityTask.WorkflowKey.WorkflowID,
		RunId:          closetVisibilityTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
		Version:        closetVisibilityTask.Version,
		TaskId:         closetVisibilityTask.TaskID,
		VisibilityTime: timestamppb.New(closetVisibilityTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) visibilityCloseTaskFromProto(
	closeVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.CloseExecutionVisibilityTask {
	return &tasks.CloseExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			closeVisibilityTask.NamespaceId,
			closeVisibilityTask.WorkflowId,
			closeVisibilityTask.RunId,
		),
		VisibilityTimestamp: closeVisibilityTask.VisibilityTime.AsTime(),
		TaskID:              closeVisibilityTask.TaskId,
		Version:             closeVisibilityTask.Version,
	}
}

func (s *TaskSerializer) visibilityDeleteTaskToProto(
	deleteVisibilityTask *tasks.DeleteExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:           deleteVisibilityTask.WorkflowKey.NamespaceID,
		WorkflowId:            deleteVisibilityTask.WorkflowKey.WorkflowID,
		RunId:                 deleteVisibilityTask.WorkflowKey.RunID,
		TaskType:              enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		Version:               deleteVisibilityTask.Version,
		TaskId:                deleteVisibilityTask.TaskID,
		VisibilityTime:        timestamppb.New(deleteVisibilityTask.VisibilityTimestamp),
		CloseVisibilityTaskId: deleteVisibilityTask.CloseExecutionVisibilityTaskID,
	}
}

func (s *TaskSerializer) visibilityDeleteTaskFromProto(
	deleteVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.DeleteExecutionVisibilityTask {
	return &tasks.DeleteExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			deleteVisibilityTask.NamespaceId,
			deleteVisibilityTask.WorkflowId,
			deleteVisibilityTask.RunId,
		),
		VisibilityTimestamp:            deleteVisibilityTask.VisibilityTime.AsTime(),
		TaskID:                         deleteVisibilityTask.TaskId,
		Version:                        deleteVisibilityTask.Version,
		CloseExecutionVisibilityTaskID: deleteVisibilityTask.CloseVisibilityTaskId,
	}
}

func (s *TaskSerializer) replicationActivityTaskToProto(
	activityTask *tasks.SyncActivityTask,
) *persistencespb.ReplicationTaskInfo {
	return &persistencespb.ReplicationTaskInfo{
		NamespaceId:       activityTask.WorkflowKey.NamespaceID,
		WorkflowId:        activityTask.WorkflowKey.WorkflowID,
		RunId:             activityTask.WorkflowKey.RunID,
		TaskType:          enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		TaskId:            activityTask.TaskID,
		Version:           activityTask.Version,
		ScheduledEventId:  activityTask.ScheduledEventID,
		FirstEventId:      0,
		NextEventId:       0,
		BranchToken:       nil,
		NewRunBranchToken: nil,
		VisibilityTime:    timestamppb.New(activityTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) replicationActivityTaskFromProto(
	activityTask *persistencespb.ReplicationTaskInfo,
) *tasks.SyncActivityTask {
	visibilityTimestamp := time.Unix(0, 0)
	if activityTask.VisibilityTime != nil {
		visibilityTimestamp = activityTask.VisibilityTime.AsTime()
	}
	return &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			activityTask.NamespaceId,
			activityTask.WorkflowId,
			activityTask.RunId,
		),
		VisibilityTimestamp: visibilityTimestamp,
		Version:             activityTask.Version,
		TaskID:              activityTask.TaskId,
		ScheduledEventID:    activityTask.ScheduledEventId,
	}
}

func (s *TaskSerializer) replicationHistoryTaskToProto(
	historyTask *tasks.HistoryReplicationTask,
) *persistencespb.ReplicationTaskInfo {
	return &persistencespb.ReplicationTaskInfo{
		NamespaceId:       historyTask.WorkflowKey.NamespaceID,
		WorkflowId:        historyTask.WorkflowKey.WorkflowID,
		RunId:             historyTask.WorkflowKey.RunID,
		TaskType:          enumsspb.TASK_TYPE_REPLICATION_HISTORY,
		TaskId:            historyTask.TaskID,
		Version:           historyTask.Version,
		ScheduledEventId:  0,
		FirstEventId:      historyTask.FirstEventID,
		NextEventId:       historyTask.NextEventID,
		BranchToken:       historyTask.BranchToken,
		NewRunBranchToken: historyTask.NewRunBranchToken,
		NewRunId:          historyTask.NewRunID,
		VisibilityTime:    timestamppb.New(historyTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) replicationHistoryTaskFromProto(
	historyTask *persistencespb.ReplicationTaskInfo,
) *tasks.HistoryReplicationTask {
	visibilityTimestamp := time.Unix(0, 0)
	if historyTask.VisibilityTime != nil {
		visibilityTimestamp = historyTask.VisibilityTime.AsTime()
	}
	return &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(
			historyTask.NamespaceId,
			historyTask.WorkflowId,
			historyTask.RunId,
		),
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              historyTask.TaskId,
		FirstEventID:        historyTask.FirstEventId,
		NextEventID:         historyTask.NextEventId,
		Version:             historyTask.Version,
		BranchToken:         historyTask.BranchToken,
		NewRunBranchToken:   historyTask.NewRunBranchToken,
		NewRunID:            historyTask.NewRunId,
	}
}

func (s *TaskSerializer) archiveExecutionTaskToProto(
	archiveExecutionTask *tasks.ArchiveExecutionTask,
) *persistencespb.ArchivalTaskInfo {
	return &persistencespb.ArchivalTaskInfo{
		NamespaceId:    archiveExecutionTask.WorkflowKey.NamespaceID,
		WorkflowId:     archiveExecutionTask.WorkflowKey.WorkflowID,
		RunId:          archiveExecutionTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION,
		TaskId:         archiveExecutionTask.TaskID,
		Version:        archiveExecutionTask.Version,
		VisibilityTime: timestamppb.New(archiveExecutionTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) archiveExecutionTaskFromProto(
	archivalTaskInfo *persistencespb.ArchivalTaskInfo,
) *tasks.ArchiveExecutionTask {
	visibilityTimestamp := time.Unix(0, 0)
	if archivalTaskInfo.VisibilityTime != nil {
		visibilityTimestamp = archivalTaskInfo.VisibilityTime.AsTime()
	}
	return &tasks.ArchiveExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			archivalTaskInfo.NamespaceId,
			archivalTaskInfo.WorkflowId,
			archivalTaskInfo.RunId,
		),
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              archivalTaskInfo.TaskId,
		Version:             archivalTaskInfo.Version,
	}
}

func (s *TaskSerializer) replicationSyncWorkflowStateTaskToProto(
	syncWorkflowStateTask *tasks.SyncWorkflowStateTask,
) *persistencespb.ReplicationTaskInfo {
	return &persistencespb.ReplicationTaskInfo{
		NamespaceId:    syncWorkflowStateTask.WorkflowKey.NamespaceID,
		WorkflowId:     syncWorkflowStateTask.WorkflowKey.WorkflowID,
		RunId:          syncWorkflowStateTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
		TaskId:         syncWorkflowStateTask.TaskID,
		Version:        syncWorkflowStateTask.Version,
		VisibilityTime: timestamppb.New(syncWorkflowStateTask.VisibilityTimestamp),
	}
}

func (s *TaskSerializer) replicationSyncWorkflowStateTaskFromProto(
	syncWorkflowStateTask *persistencespb.ReplicationTaskInfo,
) *tasks.SyncWorkflowStateTask {
	visibilityTimestamp := time.Unix(0, 0)
	if syncWorkflowStateTask.VisibilityTime != nil {
		visibilityTimestamp = syncWorkflowStateTask.VisibilityTime.AsTime()
	}
	return &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.NewWorkflowKey(
			syncWorkflowStateTask.NamespaceId,
			syncWorkflowStateTask.WorkflowId,
			syncWorkflowStateTask.RunId,
		),
		VisibilityTimestamp: visibilityTimestamp,
		Version:             syncWorkflowStateTask.Version,
		TaskID:              syncWorkflowStateTask.TaskId,
	}
}

func (s *TaskSerializer) serializeCallbackTask(task tasks.Task) (*commonpb.DataBlob, error) {
	if task, ok := task.(*tasks.CallbackTask); ok {
		return proto3Encode(s.callbackTaskToProto(task))
	}
	return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown callback task type while serializing: %v", task))
}

func (s *TaskSerializer) callbackTaskToProto(task *tasks.CallbackTask) *persistencespb.CallbackTaskInfo {
	return &persistencespb.CallbackTaskInfo{
		NamespaceId:        task.WorkflowKey.NamespaceID,
		WorkflowId:         task.WorkflowKey.WorkflowID,
		RunId:              task.WorkflowKey.RunID,
		TaskType:           enumsspb.TASK_TYPE_CALLBACK,
		Version:            task.Version,
		TaskId:             task.TaskID,
		VisibilityTime:     timestamppb.New(task.VisibilityTimestamp),
		CallbackId:         task.CallbackID,
		DestinationAddress: task.DestinationAddress,
		ScheduleAttempt:    task.Attempt,
	}
}

func (s *TaskSerializer) deserializeCallbackTask(blob *commonpb.DataBlob) (*tasks.CallbackTask, error) {
	task := &persistencespb.CallbackTaskInfo{}
	if err := proto3Decode(blob.Data, blob.EncodingType.String(), task); err != nil {
		return nil, err
	}
	if task.TaskType == enumsspb.TASK_TYPE_CALLBACK {
		return s.callbackTaskFromProto(task), nil
	}
	return nil, serviceerror.NewInternal(fmt.Sprintf("Unknown callback task type while deserializing: %v", task))
}

func (s *TaskSerializer) callbackTaskFromProto(task *persistencespb.CallbackTaskInfo) *tasks.CallbackTask {
	return &tasks.CallbackTask{
		WorkflowKey: definition.NewWorkflowKey(
			task.NamespaceId,
			task.WorkflowId,
			task.RunId,
		),
		VisibilityTimestamp: task.GetVisibilityTime().AsTime(),
		Version:             task.Version,
		TaskID:              task.TaskId,
		CallbackID:          task.CallbackId,
		DestinationAddress:  task.DestinationAddress,
		Attempt:             task.ScheduleAttempt,
	}
}

func (s *TaskSerializer) callbackBackoffTaskToProto(task *tasks.CallbackBackoffTask) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:     task.WorkflowKey.NamespaceID,
		WorkflowId:      task.WorkflowKey.WorkflowID,
		RunId:           task.WorkflowKey.RunID,
		TaskType:        enumsspb.TASK_TYPE_CALLBACK_BACKOFF,
		Version:         task.Version,
		TaskId:          task.TaskID,
		VisibilityTime:  timestamppb.New(task.VisibilityTimestamp),
		CallbackId:      task.CallbackID,
		ScheduleAttempt: task.Attempt,
	}
}

func (s *TaskSerializer) callbackBackoffTaskFromProto(task *persistencespb.TimerTaskInfo) *tasks.CallbackBackoffTask {
	return &tasks.CallbackBackoffTask{
		WorkflowKey: definition.NewWorkflowKey(
			task.NamespaceId,
			task.WorkflowId,
			task.RunId,
		),
		VisibilityTimestamp: task.GetVisibilityTime().AsTime(),
		Version:             task.Version,
		TaskID:              task.TaskId,
		CallbackID:          task.CallbackId,
		Attempt:             task.ScheduleAttempt,
	}
}
