package replication

import (
	"context"
	"errors"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

type (
	ExecutableBackfillHistoryEventsTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		// baseExecutionInfo *workflowpb.BaseExecutionInfo

		taskAttr *replicationspb.BackfillHistoryTaskAttributes

		markPoisonPillAttempts int
	}
)

var _ ctasks.Task = (*ExecutableBackfillHistoryEventsTask)(nil)
var _ TrackableExecutableTask = (*ExecutableBackfillHistoryEventsTask)(nil)

func NewExecutableBackfillHistoryEventsTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	sourceClusterName string,
	sourceShardKey ClusterShardKey,
	replicationTask *replicationspb.ReplicationTask,
) *ExecutableBackfillHistoryEventsTask {
	task := replicationTask.GetBackfillHistoryTaskAttributes()
	return &ExecutableBackfillHistoryEventsTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.BackfillHistoryEventsTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
			sourceShardKey,
			replicationTask,
		),
		taskAttr:               task,
		markPoisonPillAttempts: 0,
	}
}

func (e *ExecutableBackfillHistoryEventsTask) QueueID() interface{} {
	return e.WorkflowKey
}

func (e *ExecutableBackfillHistoryEventsTask) Execute() error {
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
			metrics.OperationTag(metrics.BackfillHistoryEventsTaskScope),
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

	events, newRunEvents, err := e.getDeserializedEvents()
	if err != nil {
		return err
	}

	return engine.BackfillHistoryEvents(ctx, &historyi.BackfillHistoryEventsRequest{
		WorkflowKey:         e.WorkflowKey,
		SourceClusterName:   e.SourceClusterName(),
		VersionedHistory:    e.ReplicationTask().VersionedTransition,
		VersionHistoryItems: e.taskAttr.EventVersionHistory,
		Events:              events,
		NewEvents:           newRunEvents,
		NewRunID:            e.taskAttr.NewRunInfo.RunId,
	})

}

func (e *ExecutableBackfillHistoryEventsTask) HandleErr(err error) error {
	if errors.Is(err, consts.ErrDuplicate) {
		e.MarkTaskDuplicated()
		return nil
	}
	e.Logger.Error("BackFillHistoryEvent replication task encountered error",
		tag.WorkflowNamespaceID(e.NamespaceID),
		tag.WorkflowID(e.WorkflowID),
		tag.WorkflowRunID(e.RunID),
		tag.TaskID(e.ExecutableTask.TaskID()),
		tag.Error(err),
	)
	callerInfo := getReplicaitonCallerInfo(e.GetPriority())
	switch taskErr := err.(type) {
	case nil, *serviceerror.NotFound:
		return nil
	case *serviceerrors.SyncState:
		namespaceName, _, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
			context.Background(),
			callerInfo,
		), e.NamespaceID, e.WorkflowID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout(), callerInfo)
		defer cancel()

		if doContinue, syncStateErr := e.SyncState(
			ctx,
			taskErr,
			ResendAttempt,
		); syncStateErr != nil || !doContinue {
			if syncStateErr != nil {
				e.Logger.Error("BackFillHistoryEvent replication task encountered error during sync state",
					tag.WorkflowNamespaceID(e.NamespaceID),
					tag.WorkflowID(e.WorkflowID),
					tag.WorkflowRunID(e.RunID),
					tag.TaskID(e.ExecutableTask.TaskID()),
					tag.Error(syncStateErr),
				)
				return err
			}
			return nil
		}
		return e.Execute()
	case *serviceerrors.RetryReplication:
		namespaceName, _, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
			context.Background(),
			callerInfo,
		), e.NamespaceID, e.WorkflowID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout(), callerInfo)
		defer cancel()
		history := &historyspb.VersionHistory{
			Items: e.taskAttr.EventVersionHistory,
		}
		startEvent := taskErr.StartEventId + 1
		endEvent := taskErr.EndEventId - 1
		startEventVersion, err := versionhistory.GetVersionHistoryEventVersion(history, startEvent)
		if err != nil {
			return err
		}
		endEventVersion, err := versionhistory.GetVersionHistoryEventVersion(history, endEvent)
		if err != nil {
			return err
		}
		if resendErr := e.BackFillEvents(
			ctx,
			e.ExecutableTask.SourceClusterName(),
			definition.NewWorkflowKey(e.NamespaceID, e.WorkflowID, e.RunID),
			startEvent,
			startEventVersion,
			endEvent,
			endEventVersion,
			"",
		); resendErr != nil {
			return resendErr
		}
		return e.Execute()
	default:
		e.Logger.Error("Backfill history events replication task encountered error",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return err
	}
}

func (e *ExecutableBackfillHistoryEventsTask) getDeserializedEvents() (_ [][]*historypb.HistoryEvent, _ []*historypb.HistoryEvent, retError error) {
	eventBatches := [][]*historypb.HistoryEvent{}
	for _, eventsBlob := range e.taskAttr.EventBatches {
		events, err := e.EventSerializer.DeserializeEvents(eventsBlob)
		if err != nil {
			e.Logger.Error("unable to deserialize history events",
				tag.WorkflowNamespaceID(e.NamespaceID),
				tag.WorkflowID(e.WorkflowID),
				tag.WorkflowRunID(e.RunID),
				tag.TaskID(e.ExecutableTask.TaskID()),
				tag.Error(err),
			)
			return nil, nil, err
		}
		eventBatches = append(eventBatches, events)
	}

	newRunEvents, err := e.EventSerializer.DeserializeEvents(e.taskAttr.NewRunInfo.EventBatch)
	if err != nil {
		e.Logger.Error("unable to deserialize new run history events",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return nil, nil, err
	}
	return eventBatches, newRunEvents, err
}
