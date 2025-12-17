package replication

import (
	"context"
	"errors"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/consts"
)

type (
	ExecutableActivityStateTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		req *historyservice.SyncActivityRequest

		// following fields are used only for batching functionality
		batchable     bool
		activityInfos []*historyservice.ActivitySyncInfo
	}
)

var _ ctasks.Task = (*ExecutableActivityStateTask)(nil)
var _ TrackableExecutableTask = (*ExecutableActivityStateTask)(nil)
var _ BatchableTask = (*ExecutableActivityStateTask)(nil)

func NewExecutableActivityStateTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.SyncActivityTaskAttributes,
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
	replicationTask *replicationspb.ReplicationTask,
) *ExecutableActivityStateTask {
	return &ExecutableActivityStateTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.SyncActivityTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		),
		req: &historyservice.SyncActivityRequest{
			NamespaceId:                task.NamespaceId,
			WorkflowId:                 task.WorkflowId,
			RunId:                      task.RunId,
			Version:                    task.Version,
			ScheduledEventId:           task.ScheduledEventId,
			ScheduledTime:              task.ScheduledTime,
			StartedEventId:             task.StartedEventId,
			StartVersion:               task.StartVersion,
			StartedTime:                task.StartedTime,
			LastHeartbeatTime:          task.LastHeartbeatTime,
			Details:                    task.Details,
			Attempt:                    task.Attempt,
			LastFailure:                task.LastFailure,
			LastWorkerIdentity:         task.LastWorkerIdentity,
			LastStartedBuildId:         task.LastStartedBuildId,
			LastStartedRedirectCounter: task.LastStartedRedirectCounter,
			BaseExecutionInfo:          task.BaseExecutionInfo,
			VersionHistory:             task.VersionHistory,
			FirstScheduledTime:         task.FirstScheduledTime,
			LastAttemptCompleteTime:    task.LastAttemptCompleteTime,
			Stamp:                      task.Stamp,
			Paused:                     task.Paused,
			RetryInitialInterval:       task.RetryInitialInterval,
			RetryMaximumInterval:       task.RetryMaximumInterval,
			RetryMaximumAttempts:       task.RetryMaximumAttempts,
			RetryBackoffCoefficient:    task.RetryBackoffCoefficient,
		},

		batchable: true,
		activityInfos: append(make([]*historyservice.ActivitySyncInfo, 0, 1), &historyservice.ActivitySyncInfo{
			Version:                    task.Version,
			ScheduledEventId:           task.ScheduledEventId,
			ScheduledTime:              task.ScheduledTime,
			StartedEventId:             task.StartedEventId,
			StartVersion:               task.StartVersion,
			StartedTime:                task.StartedTime,
			LastHeartbeatTime:          task.LastHeartbeatTime,
			Details:                    task.Details,
			Attempt:                    task.Attempt,
			LastFailure:                task.LastFailure,
			LastWorkerIdentity:         task.LastWorkerIdentity,
			VersionHistory:             task.VersionHistory,
			LastStartedBuildId:         task.LastStartedBuildId,
			LastStartedRedirectCounter: task.LastStartedRedirectCounter,
			FirstScheduledTime:         task.FirstScheduledTime,
			LastAttemptCompleteTime:    task.LastAttemptCompleteTime,
			Stamp:                      task.Stamp,
			Paused:                     task.Paused,
			RetryInitialInterval:       task.RetryInitialInterval,
			RetryMaximumInterval:       task.RetryMaximumInterval,
			RetryMaximumAttempts:       task.RetryMaximumAttempts,
			RetryBackoffCoefficient:    task.RetryBackoffCoefficient,
		}),
	}
}

func (e *ExecutableActivityStateTask) QueueID() interface{} {
	return e.WorkflowKey
}

func (e *ExecutableActivityStateTask) Execute() error {
	if e.TerminalState() {
		return nil
	}

	callerInfo := getReplicaitonCallerInfo(e.GetPriority())
	namespaceName, apply, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
		context.Background(),
		callerInfo,
	), e.NamespaceID, e.WorkflowID)
	if nsError != nil {
		return nsError
	} else if !apply {
		e.Logger.Warn("Skipping the replication task",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
		)
		metrics.ReplicationTasksSkipped.With(e.MetricsHandler).Record(
			1,
			metrics.OperationTag(metrics.SyncActivityTaskScope),
			metrics.NamespaceTag(namespaceName),
		)
		return nil
	}
	ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout(), callerInfo)
	defer cancel()

	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return err
	}
	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	if e.Config.EnableReplicationTaskBatching() {
		return engine.SyncActivities(ctx, &historyservice.SyncActivitiesRequest{
			NamespaceId:    e.NamespaceID,
			WorkflowId:     e.WorkflowID,
			RunId:          e.RunID,
			ActivitiesInfo: e.activityInfos,
		})
	}

	return engine.SyncActivity(ctx, e.req)
}

func (e *ExecutableActivityStateTask) HandleErr(err error) error {
	if errors.Is(err, consts.ErrDuplicate) {
		e.MarkTaskDuplicated()
		return nil
	}
	switch retryErr := err.(type) {
	case nil, *serviceerror.NotFound:
		return nil
	case *serviceerrors.RetryReplication:
		callerInfo := getReplicaitonCallerInfo(e.GetPriority())
		namespaceName, _, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
			context.Background(),
			callerInfo,
		), e.NamespaceID, e.WorkflowID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout(), callerInfo)
		defer cancel()

		if doContinue, resendErr := e.Resend(
			ctx,
			e.ExecutableTask.SourceClusterName(),
			retryErr,
			ResendAttempt,
		); resendErr != nil || !doContinue {
			return err
		}
		return e.Execute()
	default:
		e.Logger.Error("activity state replication task encountered error",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return err
	}
}

func (e *ExecutableActivityStateTask) MarkPoisonPill() error {
	if e.ReplicationTask().GetRawTaskInfo() == nil {
		e.ReplicationTask().RawTaskInfo = &persistencespb.ReplicationTaskInfo{
			NamespaceId:      e.NamespaceID,
			WorkflowId:       e.WorkflowID,
			RunId:            e.RunID,
			TaskId:           e.ExecutableTask.TaskID(),
			TaskType:         enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
			ScheduledEventId: e.req.ScheduledEventId,
			Version:          e.req.Version,
		}
	}

	return e.ExecutableTask.MarkPoisonPill()
}

func (e *ExecutableActivityStateTask) BatchWith(incomingTask BatchableTask) (TrackableExecutableTask, bool) {
	if !e.batchable || !incomingTask.CanBatch() {
		return nil, false
	}

	incomingActivityTask, err := e.validateIncomingBatchTask(incomingTask)
	if err != nil {
		return nil, false
	}
	return &ExecutableActivityStateTask{
		ProcessToolBox: e.ProcessToolBox,
		WorkflowKey:    e.WorkflowKey,
		ExecutableTask: e.ExecutableTask,
		batchable:      true,
		activityInfos:  append(e.activityInfos, incomingActivityTask.activityInfos...),
	}, true
}

func (e *ExecutableActivityStateTask) validateIncomingBatchTask(incomingTask BatchableTask) (*ExecutableActivityStateTask, error) {
	if incomingTask == nil {
		return nil, serviceerror.NewInvalidArgument("Batch task is nil")
	}
	incomingActivityTask, isActivityTask := incomingTask.(*ExecutableActivityStateTask)
	if !isActivityTask {
		return nil, serviceerror.NewInvalidArgument("Unsupported Batch type")
	}
	if e.WorkflowKey != incomingActivityTask.WorkflowKey {
		return nil, serviceerror.NewInvalidArgument("WorkflowKey mismatch")
	}

	return incomingActivityTask, nil
}

func (e *ExecutableActivityStateTask) CanBatch() bool {
	return e.batchable
}

func (e *ExecutableActivityStateTask) MarkUnbatchable() {
	e.batchable = false
}

func (e *ExecutableActivityStateTask) Cancel() {
	e.MarkUnbatchable()
	e.ExecutableTask.Cancel()
}
