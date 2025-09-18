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
	"google.golang.org/protobuf/types/known/timestamppb"
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
	case tasks.CategoryIDOutbound:
		return s.serializeOutboundTask(task)
	default:
		return nil, serviceerror.NewInternalf("Unknown task category: %v", category)
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
	case tasks.CategoryIDOutbound:
		return s.deserializeOutboundTask(blob)
	default:
		return nil, serviceerror.NewInternalf("Unknown task category: %v", category)
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
	case *tasks.ChasmTask:
		transferTask = s.transferChasmTaskToProto(task)
	default:
		return nil, serviceerror.NewInternalf("Unknown transfer task type: %v", task)
	}

	return TransferTaskInfoToBlob(transferTask)
}

func (s *TaskSerializer) transferChasmTaskToProto(task *tasks.ChasmTask) *persistencespb.TransferTaskInfo {
	return &persistencespb.TransferTaskInfo{
		NamespaceId:    task.WorkflowKey.NamespaceID,
		WorkflowId:     task.WorkflowKey.WorkflowID,
		RunId:          task.WorkflowKey.RunID,
		TaskId:         task.TaskID,
		TaskType:       task.GetType(),
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		TaskDetails: &persistencespb.TransferTaskInfo_ChasmTaskInfo{
			ChasmTaskInfo: task.Info,
		},
	}
}

func (s *TaskSerializer) deserializeTransferTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	transferTask, err := TransferTaskInfoFromBlob(blob)
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
	case enumsspb.TASK_TYPE_CHASM:
		task = s.transferChasmTaskFromProto(transferTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown transfer task type: %v", transferTask.TaskType)
	}
	return task, nil
}

func (s *TaskSerializer) transferChasmTaskFromProto(task *persistencespb.TransferTaskInfo) tasks.Task {
	return &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			task.NamespaceId,
			task.WorkflowId,
			task.RunId,
		),
		VisibilityTimestamp: task.VisibilityTime.AsTime(),
		TaskID:              task.TaskId,
		Category:            tasks.CategoryTransfer,
		Info:                task.GetChasmTaskInfo(),
	}
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
	case *tasks.WorkflowRunTimeoutTask:
		timerTask = s.timerWorkflowRunToProto(task)
	case *tasks.WorkflowExecutionTimeoutTask:
		timerTask = s.timerWorkflowExecutionToProto(task)
	case *tasks.DeleteHistoryEventTask:
		timerTask = s.timerWorkflowCleanupTaskToProto(task)
	case *tasks.StateMachineTimerTask:
		timerTask = s.stateMachineTimerTaskToProto(task)
	case *tasks.ChasmTask:
		timerTask = s.timerChasmTaskToProto(task)
	case *tasks.ChasmTaskPure:
		timerTask = s.timerChasmPureTaskToProto(task)
	default:
		return nil, serviceerror.NewInternalf("Unknown timer task type: %v", task)
	}
	return TimerTaskInfoToBlob(timerTask)
}

func (s *TaskSerializer) timerChasmPureTaskToProto(task *tasks.ChasmTaskPure) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:    task.NamespaceID,
		WorkflowId:     task.WorkflowID,
		RunId:          task.RunID,
		TaskId:         task.TaskID,
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		TaskType:       task.GetType(),
	}
}

func (s *TaskSerializer) timerChasmTaskToProto(task *tasks.ChasmTask) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:    task.NamespaceID,
		WorkflowId:     task.WorkflowID,
		RunId:          task.RunID,
		TaskId:         task.TaskID,
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		TaskType:       task.GetType(),
		TaskDetails: &persistencespb.TimerTaskInfo_ChasmTaskInfo{
			ChasmTaskInfo: task.Info,
		},
	}
}

func (s *TaskSerializer) deserializeTimerTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	timerTask, err := TimerTaskInfoFromBlob(blob)
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
	case enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT:
		timer = s.timerWorkflowExecutionFromProto(timerTask)
	case enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT:
		timer = s.timerWorkflowCleanupTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_STATE_MACHINE_TIMER:
		timer = s.stateMachineTimerTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_CHASM:
		timer = s.timerChasmTaskFromProto(timerTask)
	case enumsspb.TASK_TYPE_CHASM_PURE:
		timer = s.timerChasmPureTaskFromProto(timerTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown timer task type: %v", timerTask.TaskType)
	}
	return timer, nil
}

func (s *TaskSerializer) timerChasmTaskFromProto(info *persistencespb.TimerTaskInfo) tasks.Task {
	return &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			info.NamespaceId,
			info.WorkflowId,
			info.RunId,
		),
		VisibilityTimestamp: info.VisibilityTime.AsTime(),
		TaskID:              info.TaskId,
		Category:            tasks.CategoryTimer,
		Info:                info.GetChasmTaskInfo(),
	}
}

func (s *TaskSerializer) timerChasmPureTaskFromProto(info *persistencespb.TimerTaskInfo) tasks.Task {
	return &tasks.ChasmTaskPure{
		WorkflowKey: definition.NewWorkflowKey(
			info.NamespaceId,
			info.WorkflowId,
			info.RunId,
		),
		VisibilityTimestamp: info.VisibilityTime.AsTime(),
		TaskID:              info.TaskId,
		Category:            tasks.CategoryTimer,
	}
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
	case *tasks.ChasmTask:
		visibilityTask = s.visibilityChasmTaskToProto(task)
	default:
		return nil, serviceerror.NewInternalf("Unknown visibility task type: %v", task)
	}

	return VisibilityTaskInfoToBlob(visibilityTask)
}

func (s *TaskSerializer) deserializeVisibilityTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	visibilityTask, err := VisibilityTaskInfoFromBlob(blob)
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
	case enumsspb.TASK_TYPE_CHASM:
		visibility = s.visibilityChasmTaskFromProto(visibilityTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown visibility task type: %v", visibilityTask.TaskType)
	}
	return visibility, nil
}

func (s *TaskSerializer) serializeReplicationTask(
	task tasks.Task,
) (*commonpb.DataBlob, error) {
	replicationTask, err := s.ParseReplicationTaskInfo(task)
	if err != nil {
		return nil, err
	}

	return ReplicationTaskInfoToBlob(replicationTask)
}

func (s *TaskSerializer) deserializeReplicationTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	replicationTask, err := ReplicationTaskInfoFromBlob(blob)
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
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM:
		return s.replicationSyncHSMTaskFromProto(replicationTask), nil
	case enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION:
		return s.replicationSyncVersionedTransitionTaskFromProto(replicationTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown replication task type: %v", replicationTask.TaskType)
	}
}

func (s *TaskSerializer) ParseReplicationTaskInfo(task tasks.Task) (*persistencespb.ReplicationTaskInfo, error) {
	switch task := task.(type) {
	case *tasks.SyncActivityTask:
		return s.replicationActivityTaskToProto(task), nil
	case *tasks.HistoryReplicationTask:
		return s.replicationHistoryTaskToProto(task), nil
	case *tasks.SyncWorkflowStateTask:
		return s.replicationSyncWorkflowStateTaskToProto(task), nil
	case *tasks.SyncHSMTask:
		return s.replicationSyncHSMTaskToProto(task), nil
	case *tasks.SyncVersionedTransitionTask:
		return s.replicationSyncVersionedTransitionTaskToProto(task)
	default:
		return nil, serviceerror.NewInternalf("Unknown repication task type: %v", task)
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
		return nil, serviceerror.NewInternalf(
			"Unknown archival task type while serializing: %v", task)
	}

	return ArchivalTaskInfoToBlob(archivalTaskInfo)
}

func (s *TaskSerializer) deserializeArchivalTasks(
	blob *commonpb.DataBlob,
) (tasks.Task, error) {
	archivalTask, err := ArchivalTaskInfoFromBlob(blob)
	if err != nil {
		return nil, err
	}
	var task tasks.Task
	switch archivalTask.TaskType {
	case enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION:
		task = s.archiveExecutionTaskFromProto(archivalTask)
	default:
		return nil, serviceerror.NewInternalf("Unknown archival task type while deserializing: %v", task)
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
		Stamp:                   activityTask.Stamp,
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
		Stamp:               activityTask.Stamp,
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
		Stamp:                   workflowTask.Stamp,
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
		Stamp:               workflowTask.Stamp,
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
		Stamp:               workflowTimer.Stamp,
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
		Stamp:               workflowTimer.Stamp,
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
		ScheduleAttempt:     activityTimer.Attempt,
		EventId:             activityTimer.EventID,
		TaskId:              activityTimer.TaskID,
		VisibilityTime:      timestamppb.New(activityTimer.VisibilityTimestamp),
		Stamp:               activityTimer.Stamp,
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
		Stamp:               activityTimer.Stamp,
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
		Stamp:               activityRetryTimer.Stamp,
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
		Stamp:               activityRetryTimer.Stamp,
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
	}
}

func (s *TaskSerializer) timerWorkflowRunToProto(
	workflowRunTimer *tasks.WorkflowRunTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
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
	}
}

func (s *TaskSerializer) timerWorkflowExecutionToProto(
	workflowExecutionTimer *tasks.WorkflowExecutionTimeoutTask,
) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
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
	}
}

func (s *TaskSerializer) timerWorkflowRunFromProto(
	workflowRunTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowRunTimeoutTask {
	return &tasks.WorkflowRunTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(
			workflowRunTimer.NamespaceId,
			workflowRunTimer.WorkflowId,
			workflowRunTimer.RunId,
		),
		VisibilityTimestamp: workflowRunTimer.VisibilityTime.AsTime(),
		TaskID:              workflowRunTimer.TaskId,
		Version:             workflowRunTimer.Version,
	}
}

func (s *TaskSerializer) timerWorkflowExecutionFromProto(
	workflowExecutionTimer *persistencespb.TimerTaskInfo,
) *tasks.WorkflowExecutionTimeoutTask {
	return &tasks.WorkflowExecutionTimeoutTask{
		NamespaceID:         workflowExecutionTimer.NamespaceId,
		WorkflowID:          workflowExecutionTimer.WorkflowId,
		FirstRunID:          workflowExecutionTimer.FirstRunId,
		VisibilityTimestamp: workflowExecutionTimer.VisibilityTime.AsTime(),
		TaskID:              workflowExecutionTimer.TaskId,
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

func (s *TaskSerializer) stateMachineTimerTaskToProto(task *tasks.StateMachineTimerTask) *persistencespb.TimerTaskInfo {
	return &persistencespb.TimerTaskInfo{
		NamespaceId:    task.NamespaceID,
		WorkflowId:     task.WorkflowID,
		RunId:          task.RunID,
		TaskId:         task.TaskID,
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		Version:        task.Version,
		TaskType:       task.GetType(),
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

func (s *TaskSerializer) stateMachineTimerTaskFromProto(info *persistencespb.TimerTaskInfo) *tasks.StateMachineTimerTask {
	return &tasks.StateMachineTimerTask{
		WorkflowKey: definition.NewWorkflowKey(
			info.NamespaceId,
			info.WorkflowId,
			info.RunId,
		),
		VisibilityTimestamp: info.VisibilityTime.AsTime(),
		TaskID:              info.TaskId,
		Version:             info.Version,
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
		TaskId:                deleteVisibilityTask.TaskID,
		VisibilityTime:        timestamppb.New(deleteVisibilityTask.VisibilityTimestamp),
		CloseVisibilityTaskId: deleteVisibilityTask.CloseExecutionVisibilityTaskID,
		CloseTime:             timestamppb.New(deleteVisibilityTask.CloseTime),
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
		CloseExecutionVisibilityTaskID: deleteVisibilityTask.CloseVisibilityTaskId,
		CloseTime:                      deleteVisibilityTask.CloseTime.AsTime(),
	}
}

func (s *TaskSerializer) visibilityChasmTaskToProto(task *tasks.ChasmTask) *persistencespb.VisibilityTaskInfo {
	return &persistencespb.VisibilityTaskInfo{
		NamespaceId:    task.WorkflowKey.NamespaceID,
		WorkflowId:     task.WorkflowKey.WorkflowID,
		RunId:          task.WorkflowKey.RunID,
		TaskId:         task.TaskID,
		TaskType:       task.GetType(),
		VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
		TaskDetails: &persistencespb.VisibilityTaskInfo_ChasmTaskInfo{
			ChasmTaskInfo: task.Info,
		},
	}
}

func (s *TaskSerializer) visibilityChasmTaskFromProto(task *persistencespb.VisibilityTaskInfo) tasks.Task {
	return &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(
			task.NamespaceId,
			task.WorkflowId,
			task.RunId,
		),
		VisibilityTimestamp: task.VisibilityTime.AsTime(),
		TaskID:              task.TaskId,
		Category:            tasks.CategoryVisibility,
		Info:                task.GetChasmTaskInfo(),
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
		TargetClusters:    activityTask.TargetClusters,
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
		TargetClusters:      activityTask.TargetClusters,
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
		TargetClusters:    historyTask.TargetClusters,
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
		TargetClusters:      historyTask.TargetClusters,
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
		Priority:       syncWorkflowStateTask.Priority,
		TargetClusters: syncWorkflowStateTask.TargetClusters,
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
		Priority:            syncWorkflowStateTask.Priority,
		TargetClusters:      syncWorkflowStateTask.TargetClusters,
	}
}

func (s *TaskSerializer) replicationSyncHSMTaskToProto(
	syncHSMTask *tasks.SyncHSMTask,
) *persistencespb.ReplicationTaskInfo {
	return &persistencespb.ReplicationTaskInfo{
		NamespaceId:    syncHSMTask.WorkflowKey.NamespaceID,
		WorkflowId:     syncHSMTask.WorkflowKey.WorkflowID,
		RunId:          syncHSMTask.WorkflowKey.RunID,
		TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM,
		TaskId:         syncHSMTask.TaskID,
		VisibilityTime: timestamppb.New(syncHSMTask.VisibilityTimestamp),
		TargetClusters: syncHSMTask.TargetClusters,
	}
}

func (s *TaskSerializer) replicationSyncHSMTaskFromProto(
	syncHSMTask *persistencespb.ReplicationTaskInfo,
) *tasks.SyncHSMTask {
	visibilityTimestamp := time.Unix(0, 0)
	if syncHSMTask.VisibilityTime != nil {
		visibilityTimestamp = syncHSMTask.VisibilityTime.AsTime()
	}
	return &tasks.SyncHSMTask{
		WorkflowKey: definition.NewWorkflowKey(
			syncHSMTask.NamespaceId,
			syncHSMTask.WorkflowId,
			syncHSMTask.RunId,
		),
		VisibilityTimestamp: visibilityTimestamp,
		TaskID:              syncHSMTask.TaskId,
		TargetClusters:      syncHSMTask.TargetClusters,
	}
}

func (s *TaskSerializer) replicationSyncVersionedTransitionTaskToProto(
	syncVersionedTransitionTask *tasks.SyncVersionedTransitionTask,
) (*persistencespb.ReplicationTaskInfo, error) {
	taskInfoEquivalents := make([]*persistencespb.ReplicationTaskInfo, 0, len(syncVersionedTransitionTask.TaskEquivalents))
	for _, task := range syncVersionedTransitionTask.TaskEquivalents {
		taskInfoEquivalent, err := s.ParseReplicationTaskInfo(task)
		if err != nil {
			return nil, err
		}
		taskInfoEquivalents = append(taskInfoEquivalents, taskInfoEquivalent)
	}

	return &persistencespb.ReplicationTaskInfo{
		NamespaceId:            syncVersionedTransitionTask.WorkflowKey.NamespaceID,
		WorkflowId:             syncVersionedTransitionTask.WorkflowKey.WorkflowID,
		RunId:                  syncVersionedTransitionTask.WorkflowKey.RunID,
		TaskType:               enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION,
		TaskId:                 syncVersionedTransitionTask.TaskID,
		VisibilityTime:         timestamppb.New(syncVersionedTransitionTask.VisibilityTimestamp),
		VersionedTransition:    syncVersionedTransitionTask.VersionedTransition,
		FirstEventId:           syncVersionedTransitionTask.FirstEventID,
		Version:                syncVersionedTransitionTask.FirstEventVersion,
		NextEventId:            syncVersionedTransitionTask.NextEventID,
		NewRunId:               syncVersionedTransitionTask.NewRunID,
		LastVersionHistoryItem: syncVersionedTransitionTask.LastVersionHistoryItem,
		IsFirstTask:            syncVersionedTransitionTask.IsFirstTask,
		TargetClusters:         syncVersionedTransitionTask.TargetClusters,
		TaskEquivalents:        taskInfoEquivalents,
	}, nil
}

func (s *TaskSerializer) replicationSyncVersionedTransitionTaskFromProto(
	syncVersionedTransitionTask *persistencespb.ReplicationTaskInfo,
) (*tasks.SyncVersionedTransitionTask, error) {

	taskEquivalents := make([]tasks.Task, 0, len(syncVersionedTransitionTask.TaskEquivalents))
	for _, taskInfoEquivalent := range syncVersionedTransitionTask.TaskEquivalents {
		taskEquivalent, err := s.ParseReplicationTask(taskInfoEquivalent)
		if err != nil {
			return nil, err
		}
		taskEquivalents = append(taskEquivalents, taskEquivalent)
	}

	visibilityTimestamp := time.Unix(0, 0)
	if syncVersionedTransitionTask.VisibilityTime != nil {
		visibilityTimestamp = syncVersionedTransitionTask.VisibilityTime.AsTime()
	}
	return &tasks.SyncVersionedTransitionTask{
		WorkflowKey: definition.NewWorkflowKey(
			syncVersionedTransitionTask.NamespaceId,
			syncVersionedTransitionTask.WorkflowId,
			syncVersionedTransitionTask.RunId,
		),
		VisibilityTimestamp:    visibilityTimestamp,
		TaskID:                 syncVersionedTransitionTask.TaskId,
		FirstEventID:           syncVersionedTransitionTask.FirstEventId,
		FirstEventVersion:      syncVersionedTransitionTask.Version,
		NextEventID:            syncVersionedTransitionTask.NextEventId,
		NewRunID:               syncVersionedTransitionTask.NewRunId,
		VersionedTransition:    syncVersionedTransitionTask.VersionedTransition,
		LastVersionHistoryItem: syncVersionedTransitionTask.LastVersionHistoryItem,
		TaskEquivalents:        taskEquivalents,
		IsFirstTask:            syncVersionedTransitionTask.IsFirstTask,
		TargetClusters:         syncVersionedTransitionTask.TargetClusters,
	}, nil
}

func (s *TaskSerializer) serializeOutboundTask(task tasks.Task) (*commonpb.DataBlob, error) {
	switch task := task.(type) {
	case *tasks.StateMachineOutboundTask:
		return ProtoEncode(&persistencespb.OutboundTaskInfo{
			NamespaceId:    task.NamespaceID,
			WorkflowId:     task.WorkflowID,
			RunId:          task.RunID,
			TaskId:         task.TaskID,
			TaskType:       task.GetType(),
			Destination:    task.Destination,
			VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
			TaskDetails: &persistencespb.OutboundTaskInfo_StateMachineInfo{
				StateMachineInfo: task.Info,
			},
		})
	case *tasks.ChasmTask:
		return ProtoEncode(&persistencespb.OutboundTaskInfo{
			NamespaceId:    task.NamespaceID,
			WorkflowId:     task.WorkflowID,
			RunId:          task.RunID,
			TaskId:         task.TaskID,
			TaskType:       task.GetType(),
			Destination:    task.Destination,
			VisibilityTime: timestamppb.New(task.VisibilityTimestamp),
			TaskDetails: &persistencespb.OutboundTaskInfo_ChasmTaskInfo{
				ChasmTaskInfo: task.Info,
			}})
	default:
		return nil, serviceerror.NewInternalf("unknown outbound task type while serializing: %v", task)
	}
}

func (s *TaskSerializer) deserializeOutboundTask(blob *commonpb.DataBlob) (tasks.Task, error) {
	info := &persistencespb.OutboundTaskInfo{}
	if err := Decode(blob, info); err != nil {
		return nil, err
	}

	switch info.TaskType {
	case enumsspb.TASK_TYPE_STATE_MACHINE_OUTBOUND:
		return &tasks.StateMachineOutboundTask{
			StateMachineTask: tasks.StateMachineTask{
				WorkflowKey: definition.NewWorkflowKey(
					info.NamespaceId,
					info.WorkflowId,
					info.RunId,
				),
				VisibilityTimestamp: info.VisibilityTime.AsTime(),
				TaskID:              info.TaskId,
				Info:                info.GetStateMachineInfo(),
			},
			Destination: info.Destination,
		}, nil
	case enumsspb.TASK_TYPE_CHASM:
		return &tasks.ChasmTask{
			WorkflowKey: definition.NewWorkflowKey(
				info.NamespaceId,
				info.WorkflowId,
				info.RunId,
			),
			VisibilityTimestamp: info.VisibilityTime.AsTime(),
			TaskID:              info.TaskId,
			Category:            tasks.CategoryOutbound,
			Info:                info.GetChasmTaskInfo(),
			Destination:         info.Destination,
		}, nil
	default:
		return nil, serviceerror.NewInternalf("unknown outbound task type while deserializing: %v", info)
	}
}
