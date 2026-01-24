package serialization

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func serializeTransferTask(
	encoder Encoder,
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var transferTask *persistencespb.TransferTaskInfo
	switch task := task.(type) {
	case *tasks.WorkflowTask:
		transferTask = transferWorkflowTaskToProto(task)
	case *tasks.ActivityTask:
		transferTask = transferActivityTaskToProto(task)
	case *tasks.CancelExecutionTask:
		transferTask = transferRequestCancelTaskToProto(task)
	case *tasks.SignalExecutionTask:
		transferTask = transferSignalTaskToProto(task)
	case *tasks.StartChildExecutionTask:
		transferTask = transferChildWorkflowTaskToProto(task)
	case *tasks.CloseExecutionTask:
		transferTask = transferCloseTaskToProto(task)
	case *tasks.ResetWorkflowTask:
		transferTask = transferResetTaskToProto(task)
	case *tasks.DeleteExecutionTask:
		transferTask = transferDeleteExecutionTaskToProto(task)
	case *tasks.ChasmTask:
		transferTask = transferChasmTaskToProto(task)
	default:
		return nil, serviceerror.NewInternalf("Unknown transfer task type: %v", task)
	}
	return encoder.TransferTaskInfoToBlob(transferTask)
}

func transferChasmTaskToProto(task *tasks.ChasmTask) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
		NamespaceId:    task.WorkflowKey.NamespaceID,
		WorkflowId:     task.WorkflowKey.WorkflowID,
		RunId:          task.WorkflowKey.RunID,
		TaskId:         task.TaskID,
		TaskType:       task.GetType(),
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		ChasmTaskInfo:  proto.ValueOrDefault(task.Info),
	}.Build()
}

func deserializeTransferTask(
	decoder Decoder,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	transferTask, err := decoder.TransferTaskInfoFromBlob(blob)
	if err != nil {
		return nil, err
	}
	var task tasks.Task
	switch transferTask.GetTaskType() {
	case enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK:
		task = transferWorkflowTaskFromProto(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK:
		task = transferActivityTaskFromProto(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION:
		task = transferRequestCancelTaskFromProto(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION:
		task = transferSignalTaskFromProto(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION:
		task = transferChildWorkflowTaskFromProto(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION:
		task = transferCloseTaskFromProto(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW:
		task = transferResetTaskFromProto(transferTask)
	case enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION:
		task = transferDeleteExecutionTaskFromProto(transferTask)
	case enumsspb.TASK_TYPE_CHASM:
		task = transferChasmTaskFromProto(transferTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown transfer task type: %v", transferTask.GetTaskType())
	}
	return task, nil
}

func transferChasmTaskFromProto(task *persistencespb.TransferTaskInfo) tasks.Task {
	return &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			task.GetNamespaceId(),
			task.GetWorkflowId(),
			task.GetRunId(),
		),
		VisibilityTimestamp: task.GetVisibilityTime().AsTime(),
		TaskID:              task.GetTaskId(),
		Category:            tasks.CategoryTransfer,
		Info:                task.GetChasmTaskInfo(),
	}
}

func serializeTimerTask(
	encoder Encoder,
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var timerTask *persistencespb.TimerTaskInfo
	switch task := task.(type) {
	case *tasks.WorkflowTaskTimeoutTask:
		timerTask = timerWorkflowTaskToProto(task)
	case *tasks.WorkflowBackoffTimerTask:
		timerTask = timerWorkflowDelayTaskToProto(task)
	case *tasks.ActivityTimeoutTask:
		timerTask = timerActivityTaskToProto(task)
	case *tasks.ActivityRetryTimerTask:
		timerTask = timerActivityRetryTaskToProto(task)
	case *tasks.UserTimerTask:
		timerTask = timerUserTaskToProto(task)
	case *tasks.WorkflowRunTimeoutTask:
		timerTask = timerWorkflowRunToProto(task)
	case *tasks.WorkflowExecutionTimeoutTask:
		timerTask = timerWorkflowExecutionToProto(task)
	case *tasks.DeleteHistoryEventTask:
		timerTask = timerWorkflowCleanupTaskToProto(task)
	case *tasks.StateMachineTimerTask:
		timerTask = stateMachineTimerTaskToProto(task)
	case *tasks.ChasmTask:
		timerTask = timerChasmTaskToProto(task)
	case *tasks.ChasmTaskPure:
		timerTask = timerChasmPureTaskToProto(task)
	default:
		return nil, serviceerror.NewInternalf("Unknown timer task type: %v", task)
	}
	return encoder.TimerTaskInfoToBlob(timerTask)
}

func timerChasmPureTaskToProto(task *tasks.ChasmTaskPure) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
		NamespaceId:    task.NamespaceID,
		WorkflowId:     task.WorkflowID,
		RunId:          task.RunID,
		TaskId:         task.TaskID,
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		TaskType:       task.GetType(),
		ChasmTaskInfo: persistencespb.ChasmTaskInfo_builder{
			ArchetypeId: task.ArchetypeID,
		}.Build(),
	}.Build()
}

func timerChasmTaskToProto(task *tasks.ChasmTask) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
		NamespaceId:    task.NamespaceID,
		WorkflowId:     task.WorkflowID,
		RunId:          task.RunID,
		TaskId:         task.TaskID,
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		TaskType:       task.GetType(),
		ChasmTaskInfo:  proto.ValueOrDefault(task.Info),
	}.Build()
}

func deserializeTimerTask(
	decoder Decoder,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	timerTask, err := decoder.TimerTaskInfoFromBlob(blob)
	if err != nil {
		return nil, err
	}
	var timer tasks.Task
	switch timerTask.GetTaskType() {
	case enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT:
		timer = timerWorkflowTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER:
		timer = timerWorkflowDelayTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT:
		timer = timerActivityTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_ACTIVITY_RETRY_TIMER:
		timer = timerActivityRetryTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_USER_TIMER:
		timer = timerUserTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT:
		timer = timerWorkflowRunFromProto(timerTask)
	case enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT:
		timer = timerWorkflowExecutionFromProto(timerTask)
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT:
		timer = timerWorkflowCleanupTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_STATE_MACHINE_TIMER:
		timer = stateMachineTimerTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_CHASM:
		timer = timerChasmTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_CHASM_PURE:
		timer = timerChasmPureTaskFromProto(timerTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown timer task type: %v", timerTask.GetTaskType())
	}
	return timer, nil
}

func timerChasmTaskFromProto(info *persistencespb.TimerTaskInfo) tasks.Task {
	return &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			info.GetNamespaceId(),
			info.GetWorkflowId(),
			info.GetRunId(),
		),
		VisibilityTimestamp: info.GetVisibilityTime().AsTime(),
		TaskID:              info.GetTaskId(),
		Category:            tasks.CategoryTimer,
		Info:                info.GetChasmTaskInfo(),
	}
}

func timerChasmPureTaskFromProto(info *persistencespb.TimerTaskInfo) tasks.Task {
	return &tasks.ChasmTaskPure{
		WorkflowKey: definition.NewWorkflowKey(
			info.GetNamespaceId(),
			info.GetWorkflowId(),
			info.GetRunId(),
		),
		VisibilityTimestamp: info.GetVisibilityTime().AsTime(),
		TaskID:              info.GetTaskId(),
		ArchetypeID:         info.GetChasmTaskInfo().GetArchetypeId(),
	}
}

func serializeVisibilityTask(
	encoder Encoder,
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var visibilityTask *persistencespb.VisibilityTaskInfo
	switch task := task.(type) {
	case *tasks.StartExecutionVisibilityTask:
		visibilityTask = visibilityStartTaskToProto(task)
	case *tasks.UpsertExecutionVisibilityTask:
		visibilityTask = visibilityUpsertTaskToProto(task)
	case *tasks.CloseExecutionVisibilityTask:
		visibilityTask = visibilityCloseTaskToProto(task)
	case *tasks.DeleteExecutionVisibilityTask:
		visibilityTask = visibilityDeleteTaskToProto(task)
	case *tasks.ChasmTask:
		visibilityTask = visibilityChasmTaskToProto(task)
	default:
		return nil, serviceerror.NewInternalf("Unknown visibility task type: %v", task)
	}
	return encoder.VisibilityTaskInfoToBlob(visibilityTask)
}

func deserializeVisibilityTask(
	decoder Decoder,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	visibilityTask, err := decoder.VisibilityTaskInfoFromBlob(blob)
	if err != nil {
		return nil, err
	}
	var visibility tasks.Task
	switch visibilityTask.GetTaskType() {
	case enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION:
		visibility = visibilityStartTaskFromProto(visibilityTask)
	case enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION:
		visibility = visibilityUpsertTaskFromProto(visibilityTask)
	case enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION:
		visibility = visibilityCloseTaskFromProto(visibilityTask)
	case enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION:
		visibility = visibilityDeleteTaskFromProto(visibilityTask)
	case enumsspb.TASK_TYPE_CHASM:
		visibility = visibilityChasmTaskFromProto(visibilityTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown visibility task type: %v", visibilityTask.GetTaskType())
	}
	return visibility, nil
}

func serializeReplicationTask(
	encoder Encoder,
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	replicationTask, err := encoder.SerializeReplicationTask(task)
	if err != nil {
		return nil, err
	}

	return encoder.ReplicationTaskInfoToBlob(replicationTask)
}

func deserializeReplicationTask(
	decoder Decoder,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	replicationTask, err := decoder.ReplicationTaskInfoFromBlob(blob)
	if err != nil {
		return nil, err
	}
	return decoder.DeserializeReplicationTask(replicationTask)
}

func (t *serializerImpl) DeserializeReplicationTask(replicationTask *persistencespb.ReplicationTaskInfo) (tasks.Task, error) {
	switch replicationTask.GetTaskType() {
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY:
		return replicationActivityTaskFromProto(replicationTask), nil
	case enumsspb.TASK_TYPE_REPLICATION_HISTORY:
		return replicationHistoryTaskFromProto(replicationTask), nil
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE:
		return replicationSyncWorkflowStateTaskFromProto(replicationTask), nil
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM:
		return replicationSyncHSMTaskFromProto(replicationTask), nil
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION:
		return replicationSyncVersionedTransitionTaskFromProto(replicationTask, t)
	default:
		return nil, serviceerror.NewInternalf("Unknown replication task type: %v", replicationTask.GetTaskType())
	}
}

func (t *serializerImpl) SerializeReplicationTask(task tasks.Task) (*persistencespb.ReplicationTaskInfo, error) {
	switch task := task.(type) {
	case *tasks.SyncActivityTask:
		return replicationActivityTaskToProto(task), nil
	case *tasks.HistoryReplicationTask:
		return replicationHistoryTaskToProto(task), nil
	case *tasks.SyncWorkflowStateTask:
		return replicationSyncWorkflowStateTaskToProto(task), nil
	case *tasks.SyncHSMTask:
		return replicationSyncHSMTaskToProto(task), nil
	case *tasks.SyncVersionedTransitionTask:
		return replicationSyncVersionedTransitionTaskToProto(task, t)
	default:
		return nil, serviceerror.NewInternalf("Unknown repication task type: %v", task)
	}
}

func serializeArchivalTask(
	encoder Encoder,
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var archivalTaskInfo *persistencespb.ArchivalTaskInfo
	switch task := task.(type) {
	case *tasks.ArchiveExecutionTask:
		archivalTaskInfo = archiveExecutionTaskToProto(task)
	default:
		return nil, serviceerror.NewInternalf(
			"Unknown archival task type while serializing: %v", task)
	}

	return encoder.ArchivalTaskInfoToBlob(archivalTaskInfo)
}

func deserializeArchivalTask(
	decoder Decoder,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	archivalTask, err := decoder.ArchivalTaskInfoFromBlob(blob)
	if err != nil {
		return nil, err
	}
	var task tasks.Task
	switch archivalTask.GetTaskType() {
	case enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION:
		task = archiveExecutionTaskFromProto(archivalTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown archival task type while deserializing: %v", task)
	}
	return task, nil
}

func transferActivityTaskToProto(
	activityTask *tasks.ActivityTask,
) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
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
		Stamp:                   activityTask.Stamp,
	}.Build()
}

func transferActivityTaskFromProto(
	activityTask *persistencespb.TransferTaskInfo,
) *tasks.ActivityTask {
	return &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			activityTask.GetNamespaceId(),
			activityTask.GetWorkflowId(),
			activityTask.GetRunId(),
		),
		VisibilityTimestamp: activityTask.GetVisibilityTime().AsTime(),
		TaskID:              activityTask.GetTaskId(),
		TaskQueue:           activityTask.GetTaskQueue(),
		ScheduledEventID:    activityTask.GetScheduledEventId(),
		Version:             activityTask.GetVersion(),
		Stamp:               activityTask.GetStamp(),
	}
}

func transferWorkflowTaskToProto(
	workflowTask *tasks.WorkflowTask,
) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
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
		Stamp:                   workflowTask.Stamp,
	}.Build()
}

func transferWorkflowTaskFromProto(
	workflowTask *persistencespb.TransferTaskInfo,
) *tasks.WorkflowTask {
	return &tasks.WorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowTask.GetNamespaceId(),
			workflowTask.GetWorkflowId(),
			workflowTask.GetRunId(),
		),
		VisibilityTimestamp: workflowTask.GetVisibilityTime().AsTime(),
		TaskID:              workflowTask.GetTaskId(),
		TaskQueue:           workflowTask.GetTaskQueue(),
		ScheduledEventID:    workflowTask.GetScheduledEventId(),
		Version:             workflowTask.GetVersion(),
		Stamp:               workflowTask.GetStamp(),
	}
}

func transferRequestCancelTaskToProto(
	requestCancelTask *tasks.CancelExecutionTask,
) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
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
	}.Build()
}

func transferRequestCancelTaskFromProto(
	requestCancelTask *persistencespb.TransferTaskInfo,
) *tasks.CancelExecutionTask {
	return &tasks.CancelExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			requestCancelTask.GetNamespaceId(),
			requestCancelTask.GetWorkflowId(),
			requestCancelTask.GetRunId(),
		),
		VisibilityTimestamp:     requestCancelTask.GetVisibilityTime().AsTime(),
		TaskID:                  requestCancelTask.GetTaskId(),
		TargetNamespaceID:       requestCancelTask.GetTargetNamespaceId(),
		TargetWorkflowID:        requestCancelTask.GetTargetWorkflowId(),
		TargetRunID:             requestCancelTask.GetTargetRunId(),
		TargetChildWorkflowOnly: requestCancelTask.GetTargetChildWorkflowOnly(),
		InitiatedEventID:        requestCancelTask.GetScheduledEventId(),
		Version:                 requestCancelTask.GetVersion(),
	}
}

func transferSignalTaskToProto(
	signalTask *tasks.SignalExecutionTask,
) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
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
	}.Build()
}

func transferSignalTaskFromProto(
	signalTask *persistencespb.TransferTaskInfo,
) *tasks.SignalExecutionTask {
	return &tasks.SignalExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			signalTask.GetNamespaceId(),
			signalTask.GetWorkflowId(),
			signalTask.GetRunId(),
		),
		VisibilityTimestamp:     signalTask.GetVisibilityTime().AsTime(),
		TaskID:                  signalTask.GetTaskId(),
		TargetNamespaceID:       signalTask.GetTargetNamespaceId(),
		TargetWorkflowID:        signalTask.GetTargetWorkflowId(),
		TargetRunID:             signalTask.GetTargetRunId(),
		TargetChildWorkflowOnly: signalTask.GetTargetChildWorkflowOnly(),
		InitiatedEventID:        signalTask.GetScheduledEventId(),
		Version:                 signalTask.GetVersion(),
	}
}

func transferChildWorkflowTaskToProto(
	childWorkflowTask *tasks.StartChildExecutionTask,
) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
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
	}.Build()
}

func transferChildWorkflowTaskFromProto(
	signalTask *persistencespb.TransferTaskInfo,
) *tasks.StartChildExecutionTask {
	return &tasks.StartChildExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			signalTask.GetNamespaceId(),
			signalTask.GetWorkflowId(),
			signalTask.GetRunId(),
		),
		VisibilityTimestamp: signalTask.GetVisibilityTime().AsTime(),
		TaskID:              signalTask.GetTaskId(),
		TargetNamespaceID:   signalTask.GetTargetNamespaceId(),
		TargetWorkflowID:    signalTask.GetTargetWorkflowId(),
		InitiatedEventID:    signalTask.GetScheduledEventId(),
		Version:             signalTask.GetVersion(),
	}
}

func transferCloseTaskToProto(
	closeTask *tasks.CloseExecutionTask,
) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
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
		CloseExecutionTaskDetails: persistencespb.TransferTaskInfo_CloseExecutionTaskDetails_builder{
			// We set this to true even though it's no longer checked in case someone downgrades to a version that
			// still checks this field.
			CanSkipVisibilityArchival: true,
		}.Build(),
	}.Build()
}

func transferCloseTaskFromProto(
	closeTask *persistencespb.TransferTaskInfo,
) *tasks.CloseExecutionTask {
	return &tasks.CloseExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			closeTask.GetNamespaceId(),
			closeTask.GetWorkflowId(),
			closeTask.GetRunId(),
		),
		VisibilityTimestamp: closeTask.GetVisibilityTime().AsTime(),
		TaskID:              closeTask.GetTaskId(),
		Version:             closeTask.GetVersion(),
		DeleteAfterClose:    closeTask.GetDeleteAfterClose(),
		// Delete workflow task process stage is not persisted. It is only for in memory retries.
		DeleteProcessStage: tasks.DeleteWorkflowExecutionStageNone,
	}
}

func transferResetTaskToProto(
	resetTask *tasks.ResetWorkflowTask,
) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
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
	}.Build()
}

func transferResetTaskFromProto(
	resetTask *persistencespb.TransferTaskInfo,
) *tasks.ResetWorkflowTask {
	return &tasks.ResetWorkflowTask{
		WorkflowKey: definition.NewWorkflowKey(
			resetTask.GetNamespaceId(),
			resetTask.GetWorkflowId(),
			resetTask.GetRunId(),
		),
		VisibilityTimestamp: resetTask.GetVisibilityTime().AsTime(),
		TaskID:              resetTask.GetTaskId(),
		Version:             resetTask.GetVersion(),
	}
}

func transferDeleteExecutionTaskToProto(
	deleteExecutionTask *tasks.DeleteExecutionTask,
) *persistencespb.TransferTaskInfo {
	return persistencespb.TransferTaskInfo_builder{
		NamespaceId:    deleteExecutionTask.WorkflowKey.NamespaceID,
		WorkflowId:     deleteExecutionTask.WorkflowKey.WorkflowID,
		RunId:          deleteExecutionTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION,
		TaskId:         deleteExecutionTask.TaskID,
		VisibilityTime: timestamppb.New(deleteExecutionTask.VisibilityTimestamp),
		ChasmTaskInfo: persistencespb.ChasmTaskInfo_builder{
			ArchetypeId: deleteExecutionTask.ArchetypeID,
		}.Build(),
	}.Build()
}

func transferDeleteExecutionTaskFromProto(
	deleteExecutionTask *persistencespb.TransferTaskInfo,
) *tasks.DeleteExecutionTask {
	return &tasks.DeleteExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			deleteExecutionTask.GetNamespaceId(),
			deleteExecutionTask.GetWorkflowId(),
			deleteExecutionTask.GetRunId(),
		),
		VisibilityTimestamp: deleteExecutionTask.GetVisibilityTime().AsTime(),
		TaskID:              deleteExecutionTask.GetTaskId(),
		ArchetypeID:         deleteExecutionTask.GetChasmTaskInfo().GetArchetypeId(),
		// Delete workflow task process stage is not persisted. It is only for in memory retries.
		ProcessStage: tasks.DeleteWorkflowExecutionStageNone,
	}
}

func timerWorkflowTaskToProto(
	workflowTimer *tasks.WorkflowTaskTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
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
		Stamp:               workflowTimer.Stamp,
	}.Build()
}

func timerWorkflowTaskFromProto(
	workflowTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowTaskTimeoutTask {
	return &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowTimer.GetNamespaceId(),
			workflowTimer.GetWorkflowId(),
			workflowTimer.GetRunId(),
		),
		VisibilityTimestamp: workflowTimer.GetVisibilityTime().AsTime(),
		TaskID:              workflowTimer.GetTaskId(),
		EventID:             workflowTimer.GetEventId(),
		ScheduleAttempt:     workflowTimer.GetScheduleAttempt(),
		TimeoutType:         workflowTimer.GetTimeoutType(),
		Version:             workflowTimer.GetVersion(),
		Stamp:               workflowTimer.GetStamp(),
	}
}

func timerWorkflowDelayTaskToProto(
	workflowDelayTimer *tasks.WorkflowBackoffTimerTask,
) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
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
	}.Build()
}

func timerWorkflowDelayTaskFromProto(
	workflowDelayTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowBackoffTimerTask {
	return &tasks.WorkflowBackoffTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowDelayTimer.GetNamespaceId(),
			workflowDelayTimer.GetWorkflowId(),
			workflowDelayTimer.GetRunId(),
		),
		VisibilityTimestamp: workflowDelayTimer.GetVisibilityTime().AsTime(),
		TaskID:              workflowDelayTimer.GetTaskId(),
		Version:             workflowDelayTimer.GetVersion(),
		WorkflowBackoffType: workflowDelayTimer.GetWorkflowBackoffType(),
	}
}

func timerActivityTaskToProto(
	activityTimer *tasks.ActivityTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
		NamespaceId:         activityTimer.WorkflowKey.NamespaceID,
		WorkflowId:          activityTimer.WorkflowKey.WorkflowID,
		RunId:               activityTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT,
		TimeoutType:         activityTimer.TimeoutType,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		ScheduleAttempt:     activityTimer.Attempt,
		EventId:             activityTimer.EventID,
		TaskId:              activityTimer.TaskID,
		VisibilityTime:      timestamppb.New(activityTimer.VisibilityTimestamp),
		Stamp:               activityTimer.Stamp,
	}.Build()
}

func timerActivityTaskFromProto(
	activityTimer *persistencespb.TimerTaskInfo,
) *tasks.ActivityTimeoutTask {
	return &tasks.ActivityTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			activityTimer.GetNamespaceId(),
			activityTimer.GetWorkflowId(),
			activityTimer.GetRunId(),
		),
		VisibilityTimestamp: activityTimer.GetVisibilityTime().AsTime(),
		TaskID:              activityTimer.GetTaskId(),
		EventID:             activityTimer.GetEventId(),
		Attempt:             activityTimer.GetScheduleAttempt(),
		TimeoutType:         activityTimer.GetTimeoutType(),
		Stamp:               activityTimer.GetStamp(),
	}
}

func timerActivityRetryTaskToProto(
	activityRetryTimer *tasks.ActivityRetryTimerTask,
) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
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
		Stamp:               activityRetryTimer.Stamp,
	}.Build()
}

func timerActivityRetryTaskFromProto(
	activityRetryTimer *persistencespb.TimerTaskInfo,
) *tasks.ActivityRetryTimerTask {
	return &tasks.ActivityRetryTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			activityRetryTimer.GetNamespaceId(),
			activityRetryTimer.GetWorkflowId(),
			activityRetryTimer.GetRunId(),
		),
		VisibilityTimestamp: activityRetryTimer.GetVisibilityTime().AsTime(),
		TaskID:              activityRetryTimer.GetTaskId(),
		EventID:             activityRetryTimer.GetEventId(),
		Version:             activityRetryTimer.GetVersion(),
		Attempt:             activityRetryTimer.GetScheduleAttempt(),
		Stamp:               activityRetryTimer.GetStamp(),
	}
}

func timerUserTaskToProto(
	userTimer *tasks.UserTimerTask,
) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
		NamespaceId:         userTimer.WorkflowKey.NamespaceID,
		WorkflowId:          userTimer.WorkflowKey.WorkflowID,
		RunId:               userTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_USER_TIMER,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		ScheduleAttempt:     0,
		EventId:             userTimer.EventID,
		TaskId:              userTimer.TaskID,
		VisibilityTime:      timestamppb.New(userTimer.VisibilityTimestamp),
	}.Build()
}

func timerUserTaskFromProto(
	userTimer *persistencespb.TimerTaskInfo,
) *tasks.UserTimerTask {
	return &tasks.UserTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			userTimer.GetNamespaceId(),
			userTimer.GetWorkflowId(),
			userTimer.GetRunId(),
		),
		VisibilityTimestamp: userTimer.GetVisibilityTime().AsTime(),
		TaskID:              userTimer.GetTaskId(),
		EventID:             userTimer.GetEventId(),
	}
}

func timerWorkflowRunToProto(
	workflowRunTimer *tasks.WorkflowRunTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
		NamespaceId:         workflowRunTimer.WorkflowKey.NamespaceID,
		WorkflowId:          workflowRunTimer.WorkflowKey.WorkflowID,
		RunId:               workflowRunTimer.WorkflowKey.RunID,
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             workflowRunTimer.Version,
		ScheduleAttempt:     0,
		EventId:             0,
		TaskId:              workflowRunTimer.TaskID,
		VisibilityTime:      timestamppb.New(workflowRunTimer.VisibilityTimestamp),
	}.Build()
}

func timerWorkflowExecutionToProto(
	workflowExecutionTimer *tasks.WorkflowExecutionTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
		NamespaceId:         workflowExecutionTimer.GetNamespaceID(),
		WorkflowId:          workflowExecutionTimer.GetWorkflowID(),
		RunId:               workflowExecutionTimer.GetRunID(),
		FirstRunId:          workflowExecutionTimer.FirstRunID,
		TaskType:            enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT,
		TimeoutType:         enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		WorkflowBackoffType: enumsspb.WORKFLOW_BACKOFF_TYPE_UNSPECIFIED,
		Version:             common.EmptyVersion,
		ScheduleAttempt:     0,
		EventId:             0,
		TaskId:              workflowExecutionTimer.TaskID,
		VisibilityTime:      timestamppb.New(workflowExecutionTimer.VisibilityTimestamp),
	}.Build()
}

func timerWorkflowRunFromProto(
	workflowRunTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowRunTimeoutTask {
	return &tasks.WorkflowRunTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowRunTimer.GetNamespaceId(),
			workflowRunTimer.GetWorkflowId(),
			workflowRunTimer.GetRunId(),
		),
		VisibilityTimestamp: workflowRunTimer.GetVisibilityTime().AsTime(),
		TaskID:              workflowRunTimer.GetTaskId(),
		Version:             workflowRunTimer.GetVersion(),
	}
}

func timerWorkflowExecutionFromProto(
	workflowExecutionTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowExecutionTimeoutTask {
	return &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         workflowExecutionTimer.GetNamespaceId(),
		WorkflowID:          workflowExecutionTimer.GetWorkflowId(),
		FirstRunID:          workflowExecutionTimer.GetFirstRunId(),
		VisibilityTimestamp: workflowExecutionTimer.GetVisibilityTime().AsTime(),
		TaskID:              workflowExecutionTimer.GetTaskId(),
	}
}

func timerWorkflowCleanupTaskToProto(
	workflowCleanupTimer *tasks.DeleteHistoryEventTask,
) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
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
		ChasmTaskInfo: persistencespb.ChasmTaskInfo_builder{
			ArchetypeId: workflowCleanupTimer.ArchetypeID,
		}.Build(),
	}.Build()
}

func stateMachineTimerTaskToProto(task *tasks.StateMachineTimerTask) *persistencespb.TimerTaskInfo {
	return persistencespb.TimerTaskInfo_builder{
		NamespaceId:    task.NamespaceID,
		WorkflowId:     task.WorkflowID,
		RunId:          task.RunID,
		TaskId:         task.TaskID,
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		Version:        task.Version,
		TaskType:       task.GetType(),
	}.Build()
}

func timerWorkflowCleanupTaskFromProto(
	workflowCleanupTimer *persistencespb.TimerTaskInfo,
) *tasks.DeleteHistoryEventTask {
	return &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowCleanupTimer.GetNamespaceId(),
			workflowCleanupTimer.GetWorkflowId(),
			workflowCleanupTimer.GetRunId(),
		),
		VisibilityTimestamp: workflowCleanupTimer.GetVisibilityTime().AsTime(),
		TaskID:              workflowCleanupTimer.GetTaskId(),
		Version:             workflowCleanupTimer.GetVersion(),
		BranchToken:         workflowCleanupTimer.GetBranchToken(),
		ArchetypeID:         workflowCleanupTimer.GetChasmTaskInfo().GetArchetypeId(),
		// Delete workflow task process stage is not persisted. It is only for in memory retries.
		ProcessStage: tasks.DeleteWorkflowExecutionStageNone,
	}
}

func stateMachineTimerTaskFromProto(info *persistencespb.TimerTaskInfo) *tasks.StateMachineTimerTask {
	return &tasks.StateMachineTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			info.GetNamespaceId(),
			info.GetWorkflowId(),
			info.GetRunId(),
		),
		VisibilityTimestamp: info.GetVisibilityTime().AsTime(),
		TaskID:              info.GetTaskId(),
		Version:             info.GetVersion(),
	}
}

func visibilityStartTaskToProto(
	startVisibilityTask *tasks.StartExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return persistencespb.VisibilityTaskInfo_builder{
		NamespaceId:    startVisibilityTask.WorkflowKey.NamespaceID,
		WorkflowId:     startVisibilityTask.WorkflowKey.WorkflowID,
		RunId:          startVisibilityTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION,
		Version:        startVisibilityTask.Version,
		TaskId:         startVisibilityTask.TaskID,
		VisibilityTime: timestamppb.New(startVisibilityTask.VisibilityTimestamp),
	}.Build()
}

func visibilityStartTaskFromProto(
	startVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.StartExecutionVisibilityTask {
	return &tasks.StartExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			startVisibilityTask.GetNamespaceId(),
			startVisibilityTask.GetWorkflowId(),
			startVisibilityTask.GetRunId(),
		),
		VisibilityTimestamp: startVisibilityTask.GetVisibilityTime().AsTime(),
		TaskID:              startVisibilityTask.GetTaskId(),
		Version:             startVisibilityTask.GetVersion(),
	}
}

func visibilityUpsertTaskToProto(
	upsertVisibilityTask *tasks.UpsertExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return persistencespb.VisibilityTaskInfo_builder{
		NamespaceId:    upsertVisibilityTask.WorkflowKey.NamespaceID,
		WorkflowId:     upsertVisibilityTask.WorkflowKey.WorkflowID,
		RunId:          upsertVisibilityTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_UPSERT_EXECUTION,
		TaskId:         upsertVisibilityTask.TaskID,
		VisibilityTime: timestamppb.New(upsertVisibilityTask.VisibilityTimestamp),
	}.Build()
}

func visibilityUpsertTaskFromProto(
	upsertVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.UpsertExecutionVisibilityTask {
	return &tasks.UpsertExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			upsertVisibilityTask.GetNamespaceId(),
			upsertVisibilityTask.GetWorkflowId(),
			upsertVisibilityTask.GetRunId(),
		),
		VisibilityTimestamp: upsertVisibilityTask.GetVisibilityTime().AsTime(),
		TaskID:              upsertVisibilityTask.GetTaskId(),
	}
}

func visibilityCloseTaskToProto(
	closetVisibilityTask *tasks.CloseExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return persistencespb.VisibilityTaskInfo_builder{
		NamespaceId:    closetVisibilityTask.WorkflowKey.NamespaceID,
		WorkflowId:     closetVisibilityTask.WorkflowKey.WorkflowID,
		RunId:          closetVisibilityTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION,
		Version:        closetVisibilityTask.Version,
		TaskId:         closetVisibilityTask.TaskID,
		VisibilityTime: timestamppb.New(closetVisibilityTask.VisibilityTimestamp),
	}.Build()
}

func visibilityCloseTaskFromProto(
	closeVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.CloseExecutionVisibilityTask {
	return &tasks.CloseExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			closeVisibilityTask.GetNamespaceId(),
			closeVisibilityTask.GetWorkflowId(),
			closeVisibilityTask.GetRunId(),
		),
		VisibilityTimestamp: closeVisibilityTask.GetVisibilityTime().AsTime(),
		TaskID:              closeVisibilityTask.GetTaskId(),
		Version:             closeVisibilityTask.GetVersion(),
	}
}

func visibilityDeleteTaskToProto(
	deleteVisibilityTask *tasks.DeleteExecutionVisibilityTask,
) *persistencespb.VisibilityTaskInfo {
	return persistencespb.VisibilityTaskInfo_builder{
		NamespaceId:           deleteVisibilityTask.WorkflowKey.NamespaceID,
		WorkflowId:            deleteVisibilityTask.WorkflowKey.WorkflowID,
		RunId:                 deleteVisibilityTask.WorkflowKey.RunID,
		TaskType:              enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		TaskId:                deleteVisibilityTask.TaskID,
		VisibilityTime:        timestamppb.New(deleteVisibilityTask.VisibilityTimestamp),
		CloseVisibilityTaskId: deleteVisibilityTask.CloseExecutionVisibilityTaskID,
		CloseTime:             timestamppb.New(deleteVisibilityTask.CloseTime),
		ChasmTaskInfo: persistencespb.ChasmTaskInfo_builder{
			ArchetypeId: deleteVisibilityTask.ArchetypeID,
		}.Build(),
	}.Build()
}

func visibilityDeleteTaskFromProto(
	deleteVisibilityTask *persistencespb.VisibilityTaskInfo,
) *tasks.DeleteExecutionVisibilityTask {
	return &tasks.DeleteExecutionVisibilityTask{
		WorkflowKey: definition.NewWorkflowKey(
			deleteVisibilityTask.GetNamespaceId(),
			deleteVisibilityTask.GetWorkflowId(),
			deleteVisibilityTask.GetRunId(),
		),
		VisibilityTimestamp:            deleteVisibilityTask.GetVisibilityTime().AsTime(),
		TaskID:                         deleteVisibilityTask.GetTaskId(),
		ArchetypeID:                    deleteVisibilityTask.GetChasmTaskInfo().GetArchetypeId(),
		CloseExecutionVisibilityTaskID: deleteVisibilityTask.GetCloseVisibilityTaskId(),
		CloseTime:                      deleteVisibilityTask.GetCloseTime().AsTime(),
	}
}

func visibilityChasmTaskToProto(task *tasks.ChasmTask) *persistencespb.VisibilityTaskInfo {
	return persistencespb.VisibilityTaskInfo_builder{
		NamespaceId:    task.WorkflowKey.NamespaceID,
		WorkflowId:     task.WorkflowKey.WorkflowID,
		RunId:          task.WorkflowKey.RunID,
		TaskId:         task.TaskID,
		TaskType:       task.GetType(),
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		ChasmTaskInfo:  proto.ValueOrDefault(task.Info),
	}.Build()
}

func visibilityChasmTaskFromProto(task *persistencespb.VisibilityTaskInfo) tasks.Task {
	return &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			task.GetNamespaceId(),
			task.GetWorkflowId(),
			task.GetRunId(),
		),
		VisibilityTimestamp: task.GetVisibilityTime().AsTime(),
		TaskID:              task.GetTaskId(),
		Category:            tasks.CategoryVisibility,
		Info:                task.GetChasmTaskInfo(),
	}
}

func replicationActivityTaskToProto(
	activityTask *tasks.SyncActivityTask,
) *persistencespb.ReplicationTaskInfo {
	return persistencespb.ReplicationTaskInfo_builder{
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
		TargetClusters:    activityTask.TargetClusters,
	}.Build()
}

func replicationActivityTaskFromProto(
	activityTask *persistencespb.ReplicationTaskInfo,
) *tasks.SyncActivityTask {
	visibilityTimestamp := time.Unix(0, 0)
	if activityTask.HasVisibilityTime() {
		visibilityTimestamp = activityTask.GetVisibilityTime().AsTime()
	}
	return &tasks.SyncActivityTask{
		WorkflowKey: definition.NewWorkflowKey(
			activityTask.GetNamespaceId(),
			activityTask.GetWorkflowId(),
			activityTask.GetRunId(),
		),
		VisibilityTimestamp: visibilityTimestamp,
		Version:             activityTask.GetVersion(),
		TaskID:              activityTask.GetTaskId(),
		ScheduledEventID:    activityTask.GetScheduledEventId(),
		TargetClusters:      activityTask.GetTargetClusters(),
	}
}

func replicationHistoryTaskToProto(
	historyTask *tasks.HistoryReplicationTask,
) *persistencespb.ReplicationTaskInfo {
	return persistencespb.ReplicationTaskInfo_builder{
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
		TargetClusters:    historyTask.TargetClusters,
	}.Build()
}

func replicationHistoryTaskFromProto(
	historyTask *persistencespb.ReplicationTaskInfo,
) *tasks.HistoryReplicationTask {
	visibilityTimestamp := time.Unix(0, 0)
	if historyTask.HasVisibilityTime() {
		visibilityTimestamp = historyTask.GetVisibilityTime().AsTime()
	}
	return &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(
			historyTask.GetNamespaceId(),
			historyTask.GetWorkflowId(),
			historyTask.GetRunId(),
		),
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              historyTask.GetTaskId(),
		FirstEventID:        historyTask.GetFirstEventId(),
		NextEventID:         historyTask.GetNextEventId(),
		Version:             historyTask.GetVersion(),
		BranchToken:         historyTask.GetBranchToken(),
		NewRunBranchToken:   historyTask.GetNewRunBranchToken(),
		NewRunID:            historyTask.GetNewRunId(),
		TargetClusters:      historyTask.GetTargetClusters(),
	}
}

func archiveExecutionTaskToProto(
	archiveExecutionTask *tasks.ArchiveExecutionTask,
) *persistencespb.ArchivalTaskInfo {
	return persistencespb.ArchivalTaskInfo_builder{
		NamespaceId:    archiveExecutionTask.WorkflowKey.NamespaceID,
		WorkflowId:     archiveExecutionTask.WorkflowKey.WorkflowID,
		RunId:          archiveExecutionTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION,
		TaskId:         archiveExecutionTask.TaskID,
		Version:        archiveExecutionTask.Version,
		VisibilityTime: timestamppb.New(archiveExecutionTask.VisibilityTimestamp),
	}.Build()
}

func archiveExecutionTaskFromProto(
	archivalTaskInfo *persistencespb.ArchivalTaskInfo,
) *tasks.ArchiveExecutionTask {
	visibilityTimestamp := time.Unix(0, 0)
	if archivalTaskInfo.HasVisibilityTime() {
		visibilityTimestamp = archivalTaskInfo.GetVisibilityTime().AsTime()
	}
	return &tasks.ArchiveExecutionTask{
		WorkflowKey: definition.NewWorkflowKey(
			archivalTaskInfo.GetNamespaceId(),
			archivalTaskInfo.GetWorkflowId(),
			archivalTaskInfo.GetRunId(),
		),
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              archivalTaskInfo.GetTaskId(),
		Version:             archivalTaskInfo.GetVersion(),
	}
}

func replicationSyncWorkflowStateTaskToProto(
	syncWorkflowStateTask *tasks.SyncWorkflowStateTask,
) *persistencespb.ReplicationTaskInfo {
	return persistencespb.ReplicationTaskInfo_builder{
		NamespaceId:        syncWorkflowStateTask.NamespaceID,
		WorkflowId:         syncWorkflowStateTask.WorkflowID,
		RunId:              syncWorkflowStateTask.RunID,
		TaskType:           enumsspb.TASK_TYPE_REPLICATION_SYNC_WORKFLOW_STATE,
		TaskId:             syncWorkflowStateTask.TaskID,
		Version:            syncWorkflowStateTask.Version,
		VisibilityTime:     timestamppb.New(syncWorkflowStateTask.VisibilityTimestamp),
		Priority:           syncWorkflowStateTask.Priority,
		TargetClusters:     syncWorkflowStateTask.TargetClusters,
		IsForceReplication: syncWorkflowStateTask.IsForceReplication,
	}.Build()
}

func replicationSyncWorkflowStateTaskFromProto(
	syncWorkflowStateTask *persistencespb.ReplicationTaskInfo,
) *tasks.SyncWorkflowStateTask {
	visibilityTimestamp := time.Unix(0, 0)
	if syncWorkflowStateTask.HasVisibilityTime() {
		visibilityTimestamp = syncWorkflowStateTask.GetVisibilityTime().AsTime()
	}
	return &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.NewWorkflowKey(
			syncWorkflowStateTask.GetNamespaceId(),
			syncWorkflowStateTask.GetWorkflowId(),
			syncWorkflowStateTask.GetRunId(),
		),
		VisibilityTimestamp: visibilityTimestamp,
		Version:             syncWorkflowStateTask.GetVersion(),
		TaskID:              syncWorkflowStateTask.GetTaskId(),
		Priority:            syncWorkflowStateTask.GetPriority(),
		TargetClusters:      syncWorkflowStateTask.GetTargetClusters(),
		IsForceReplication:  syncWorkflowStateTask.GetIsForceReplication(),
	}
}

func replicationSyncHSMTaskToProto(
	syncHSMTask *tasks.SyncHSMTask,
) *persistencespb.ReplicationTaskInfo {
	return persistencespb.ReplicationTaskInfo_builder{
		NamespaceId:    syncHSMTask.WorkflowKey.NamespaceID,
		WorkflowId:     syncHSMTask.WorkflowKey.WorkflowID,
		RunId:          syncHSMTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM,
		TaskId:         syncHSMTask.TaskID,
		VisibilityTime: timestamppb.New(syncHSMTask.VisibilityTimestamp),
		TargetClusters: syncHSMTask.TargetClusters,
	}.Build()
}

func replicationSyncHSMTaskFromProto(
	syncHSMTask *persistencespb.ReplicationTaskInfo,
) *tasks.SyncHSMTask {
	visibilityTimestamp := time.Unix(0, 0)
	if syncHSMTask.HasVisibilityTime() {
		visibilityTimestamp = syncHSMTask.GetVisibilityTime().AsTime()
	}
	return &tasks.SyncHSMTask{
		WorkflowKey: definition.NewWorkflowKey(
			syncHSMTask.GetNamespaceId(),
			syncHSMTask.GetWorkflowId(),
			syncHSMTask.GetRunId(),
		),
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              syncHSMTask.GetTaskId(),
		TargetClusters:      syncHSMTask.GetTargetClusters(),
	}
}

func replicationSyncVersionedTransitionTaskToProto(
	syncVersionedTransitionTask *tasks.SyncVersionedTransitionTask,
	encoder Encoder,
) (*persistencespb.ReplicationTaskInfo, error) {
	taskInfoEquivalents := make([]*persistencespb.ReplicationTaskInfo, 0, len(syncVersionedTransitionTask.TaskEquivalents))
	for _, task := range syncVersionedTransitionTask.TaskEquivalents {
		taskInfoEquivalent, err := encoder.SerializeReplicationTask(task)
		if err != nil {
			return nil, err
		}
		taskInfoEquivalents = append(taskInfoEquivalents, taskInfoEquivalent)
	}

	return persistencespb.ReplicationTaskInfo_builder{
		NamespaceId:            syncVersionedTransitionTask.WorkflowKey.NamespaceID,
		WorkflowId:             syncVersionedTransitionTask.WorkflowKey.WorkflowID,
		RunId:                  syncVersionedTransitionTask.WorkflowKey.RunID,
		TaskType:               enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION,
		TaskId:                 syncVersionedTransitionTask.TaskID,
		VisibilityTime:         timestamppb.New(syncVersionedTransitionTask.VisibilityTimestamp),
		ArchetypeId:            syncVersionedTransitionTask.ArchetypeID,
		VersionedTransition:    syncVersionedTransitionTask.VersionedTransition,
		FirstEventId:           syncVersionedTransitionTask.FirstEventID,
		Version:                syncVersionedTransitionTask.FirstEventVersion,
		NextEventId:            syncVersionedTransitionTask.NextEventID,
		NewRunId:               syncVersionedTransitionTask.NewRunID,
		LastVersionHistoryItem: syncVersionedTransitionTask.LastVersionHistoryItem,
		IsFirstTask:            syncVersionedTransitionTask.IsFirstTask,
		TargetClusters:         syncVersionedTransitionTask.TargetClusters,
		IsForceReplication:     syncVersionedTransitionTask.IsForceReplication,
		TaskEquivalents:        taskInfoEquivalents,
	}.Build(), nil
}

func replicationSyncVersionedTransitionTaskFromProto(
	syncVersionedTransitionTask *persistencespb.ReplicationTaskInfo,
	decoder Decoder,
) (*tasks.SyncVersionedTransitionTask, error) {

	taskEquivalents := make([]tasks.Task, 0, len(syncVersionedTransitionTask.GetTaskEquivalents()))
	for _, taskInfoEquivalent := range syncVersionedTransitionTask.GetTaskEquivalents() {
		taskEquivalent, err := decoder.DeserializeReplicationTask(taskInfoEquivalent)
		if err != nil {
			return nil, err
		}
		taskEquivalents = append(taskEquivalents, taskEquivalent)
	}

	visibilityTimestamp := time.Unix(0, 0)
	if syncVersionedTransitionTask.HasVisibilityTime() {
		visibilityTimestamp = syncVersionedTransitionTask.GetVisibilityTime().AsTime()
	}
	return &tasks.SyncVersionedTransitionTask{
		WorkflowKey: definition.NewWorkflowKey(
			syncVersionedTransitionTask.GetNamespaceId(),
			syncVersionedTransitionTask.GetWorkflowId(),
			syncVersionedTransitionTask.GetRunId(),
		),
		VisibilityTimestamp:    visibilityTimestamp,
		TaskID:                 syncVersionedTransitionTask.GetTaskId(),
		ArchetypeID:            syncVersionedTransitionTask.GetArchetypeId(),
		FirstEventID:           syncVersionedTransitionTask.GetFirstEventId(),
		FirstEventVersion:      syncVersionedTransitionTask.GetVersion(),
		NextEventID:            syncVersionedTransitionTask.GetNextEventId(),
		NewRunID:               syncVersionedTransitionTask.GetNewRunId(),
		VersionedTransition:    syncVersionedTransitionTask.GetVersionedTransition(),
		LastVersionHistoryItem: syncVersionedTransitionTask.GetLastVersionHistoryItem(),
		TaskEquivalents:        taskEquivalents,
		IsFirstTask:            syncVersionedTransitionTask.GetIsFirstTask(),
		TargetClusters:         syncVersionedTransitionTask.GetTargetClusters(),
		IsForceReplication:     syncVersionedTransitionTask.GetIsForceReplication(),
	}, nil
}

func serializeOutboundTask(
	encoder Encoder,
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	var outboundTaskInfo *persistencespb.OutboundTaskInfo
	switch task := task.(type) {
	case *tasks.StateMachineOutboundTask:
		outboundTaskInfo = persistencespb.OutboundTaskInfo_builder{
			NamespaceId:      task.NamespaceID,
			WorkflowId:       task.WorkflowID,
			RunId:            task.RunID,
			TaskId:           task.TaskID,
			TaskType:         task.GetType(),
			Destination:      task.Destination,
			VisibilityTime:   timestamppb.New(task.VisibilityTimestamp),
			StateMachineInfo: proto.ValueOrDefault(task.Info),
		}.Build()
	case *tasks.ChasmTask:
		outboundTaskInfo = persistencespb.OutboundTaskInfo_builder{
			NamespaceId:    task.NamespaceID,
			WorkflowId:     task.WorkflowID,
			RunId:          task.RunID,
			TaskId:         task.TaskID,
			TaskType:       task.GetType(),
			Destination:    task.Destination,
			VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
			ChasmTaskInfo:  proto.ValueOrDefault(task.Info),
		}.Build()
	default:
		return nil, serviceerror.NewInternalf("unknown outbound task type while serializing: %v", task)
	}
	return encoder.OutboundTaskInfoToBlob(outboundTaskInfo)
}

func deserializeOutboundTask(
	decoder Decoder,
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	info, err := decoder.OutboundTaskInfoFromBlob(blob)
	if err != nil {
		return nil, err
	}
	switch info.GetTaskType() {
	case enumsspb.TASK_TYPE_STATE_MACHINE_OUTBOUND:
		return &tasks.StateMachineOutboundTask{
			StateMachineTask: tasks.StateMachineTask{
				WorkflowKey: definition.NewWorkflowKey(
					info.GetNamespaceId(),
					info.GetWorkflowId(),
					info.GetRunId(),
				),
				VisibilityTimestamp: info.GetVisibilityTime().AsTime(),
				TaskID:              info.GetTaskId(),
				Info:                info.GetStateMachineInfo(),
			},
			Destination: info.GetDestination(),
		}, nil
	case enumsspb.TASK_TYPE_CHASM:
		return &tasks.ChasmTask{
			WorkflowKey: definition.NewWorkflowKey(
				info.GetNamespaceId(),
				info.GetWorkflowId(),
				info.GetRunId(),
			),
			VisibilityTimestamp: info.GetVisibilityTime().AsTime(),
			TaskID:              info.GetTaskId(),
			Category:            tasks.CategoryOutbound,
			Info:                info.GetChasmTaskInfo(),
			Destination:         info.GetDestination(),
		}, nil
	default:
		return nil, serviceerror.NewInternalf("unknown outbound task type while deserializing: %v", info)
	}
}
