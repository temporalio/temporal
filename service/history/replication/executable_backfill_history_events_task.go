// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package replication

import (
	"context"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/shard"
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
	task *replicationspb.BackfillHistoryTaskAttributes,
	sourceClusterName string,
	priority enumsspb.TaskPriority,
	versionedTransition *persistencespb.VersionedTransition,
) *ExecutableBackfillHistoryEventsTask {
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
			priority,
			versionedTransition,
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

	namespaceName, apply, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
		context.Background(),
		headers.SystemPreemptableCallerInfo,
	), e.NamespaceID)
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

	ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout())
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

	return engine.BackfillHistoryEvents(ctx, &shard.BackfillHistoryEventsRequest{
		WorkflowKey:         e.WorkflowKey,
		SourceClusterName:   e.SourceClusterName(),
		VersionedHistory:    e.taskAttr.VersionedTransition,
		BaseExecutionInfo:   e.taskAttr.BaseExecutionInfo,
		VersionHistoryItems: e.taskAttr.VersionHistoryItems,
		Events:              events,
		NewEvents:           newRunEvents,
		NewRunID:            e.taskAttr.NewRunId,
	})

}

func (e *ExecutableBackfillHistoryEventsTask) HandleErr(err error) error {
	switch retryErr := err.(type) {
	case nil, *serviceerror.NotFound:
		return nil
	case *serviceerrors.SyncState:
		// TODO: call SyncState.

		return e.Execute()
	case *serviceerrors.RetryReplication:
		namespaceName, _, nsError := e.GetNamespaceInfo(headers.SetCallerInfo(
			context.Background(),
			headers.SystemPreemptableCallerInfo,
		), e.NamespaceID)
		if nsError != nil {
			return err
		}
		ctx, cancel := newTaskContext(namespaceName, e.Config.ReplicationTaskApplyTimeout())
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

func (e *ExecutableBackfillHistoryEventsTask) MarkPoisonPill() error {
	if e.markPoisonPillAttempts >= MarkPoisonPillMaxAttempts {
		taskInfo := &persistencespb.ReplicationTaskInfo{
			NamespaceId:         e.NamespaceID,
			WorkflowId:          e.WorkflowID,
			RunId:               e.RunID,
			TaskId:              e.ExecutableTask.TaskID(),
			TaskType:            enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION,
			VisibilityTime:      timestamppb.New(e.TaskCreationTime()),
			NewRunId:            e.taskAttr.NewRunId,
			VersionedTransition: e.taskAttr.VersionedTransition,
		}
		events, newRunEvents, err := e.getDeserializedEvents()
		if err != nil {
			taskInfo.FirstEventId = -1
			taskInfo.NextEventId = -1
			taskInfo.Version = -1
			e.Logger.Error("MarkPoisonPill reached max attempts, deserialize failed",
				tag.SourceCluster(e.SourceClusterName()),
				tag.ReplicationTask(taskInfo),
				tag.Error(err),
			)
			return nil
		}
		taskInfo.FirstEventId = events[0][0].GetEventId()
		taskInfo.NextEventId = events[len(events)-1][len(events[len(events)-1])-1].GetEventId() + 1
		taskInfo.Version = events[0][0].GetVersion()
		e.Logger.Error("MarkPoisonPill reached max attempts",
			tag.SourceCluster(e.SourceClusterName()),
			tag.ReplicationTask(taskInfo),
			tag.NewAnyTag("NewRunFirstEventId", newRunEvents[0].GetEventId()),
			tag.NewAnyTag("NewRunLastEventId", newRunEvents[len(newRunEvents)-1].GetEventId()),
		)
		return nil
	}
	e.markPoisonPillAttempts++

	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return err
	}

	eventBatches := [][]*historypb.HistoryEvent{}
	for _, eventsBlob := range e.taskAttr.EventBatches {
		events, err := e.EventSerializer.DeserializeEvents(eventsBlob)
		if err != nil {
			e.Logger.Error("unable to enqueue sync versioned transition replication task to DLQ, ser/de error",
				tag.ShardID(shardContext.GetShardID()),
				tag.WorkflowNamespaceID(e.NamespaceID),
				tag.WorkflowID(e.WorkflowID),
				tag.WorkflowRunID(e.RunID),
				tag.TaskID(e.ExecutableTask.TaskID()),
				tag.Error(err),
			)
			return nil
		} else if len(events) == 0 {
			e.Logger.Error("unable to enqueue sync versioned transition replication task to DLQ, no events",
				tag.ShardID(shardContext.GetShardID()),
				tag.WorkflowNamespaceID(e.NamespaceID),
				tag.WorkflowID(e.WorkflowID),
				tag.WorkflowRunID(e.RunID),
				tag.TaskID(e.ExecutableTask.TaskID()),
			)
			return nil
		}
		eventBatches = append(eventBatches, events)
	}

	// TODO: GetShardID will break GetDLQReplicationMessages we need to handle DLQ for cross shard replication.
	taskInfo := &persistencespb.ReplicationTaskInfo{
		NamespaceId:         e.NamespaceID,
		WorkflowId:          e.WorkflowID,
		RunId:               e.RunID,
		TaskId:              e.ExecutableTask.TaskID(),
		TaskType:            enumsspb.TASK_TYPE_REPLICATION_SYNC_VERSIONED_TRANSITION,
		VisibilityTime:      timestamppb.New(e.TaskCreationTime()),
		NewRunId:            e.taskAttr.NewRunId,
		VersionedTransition: e.taskAttr.VersionedTransition,
		FirstEventId:        eventBatches[0][0].GetEventId(),
		NextEventId:         eventBatches[len(eventBatches)-1][len(eventBatches[len(eventBatches)-1])-1].GetEventId() + 1,
		Version:             eventBatches[0][0].GetVersion(),
	}

	e.Logger.Error("enqueue sync versioned transition replication task to DLQ",
		tag.ShardID(shardContext.GetShardID()),
		tag.WorkflowNamespaceID(e.NamespaceID),
		tag.WorkflowID(e.WorkflowID),
		tag.WorkflowRunID(e.RunID),
		tag.TaskID(e.ExecutableTask.TaskID()),
	)

	ctx, cancel := newTaskContext(e.NamespaceID, e.Config.ReplicationTaskApplyTimeout())
	defer cancel()

	return writeTaskToDLQ(ctx, e.DLQWriter, shardContext, e.SourceClusterName(), taskInfo)
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

	newRunEvents, err := e.EventSerializer.DeserializeEvents(e.taskAttr.NewRunEventBatch)
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
