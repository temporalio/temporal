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

package replication

import (
	"context"
	"sync"
	"time"

	"go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowpb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	ctasks "go.temporal.io/server/common/tasks"
)

type (
	ExecutableHistoryTask struct {
		ProcessToolBox

		definition.WorkflowKey
		ExecutableTask
		baseExecutionInfo   *workflowpb.BaseExecutionInfo
		versionHistoryItems []*historyspb.VersionHistoryItem
		eventsBlob          *common.DataBlob
		newRunEventsBlob    *common.DataBlob
		newRunID            string

		deserializeLock   sync.Mutex
		eventsDesResponse *eventsDeserializeResponse

		batchable bool
	}
	eventsDeserializeResponse struct {
		events       [][]*historypb.HistoryEvent
		newRunEvents []*historypb.HistoryEvent
		err          error
	}
)

var _ ctasks.Task = (*ExecutableHistoryTask)(nil)
var _ TrackableExecutableTask = (*ExecutableHistoryTask)(nil)
var _ BatchableTask = (*ExecutableHistoryTask)(nil)

func NewExecutableHistoryTask(
	processToolBox ProcessToolBox,
	taskID int64,
	taskCreationTime time.Time,
	task *replicationspb.HistoryTaskAttributes,
	sourceClusterName string,
) *ExecutableHistoryTask {
	return &ExecutableHistoryTask{
		ProcessToolBox: processToolBox,

		WorkflowKey: definition.NewWorkflowKey(task.NamespaceId, task.WorkflowId, task.RunId),
		ExecutableTask: NewExecutableTask(
			processToolBox,
			taskID,
			metrics.HistoryReplicationTaskScope,
			taskCreationTime,
			time.Now().UTC(),
			sourceClusterName,
		),

		baseExecutionInfo:   task.BaseExecutionInfo,
		versionHistoryItems: task.VersionHistoryItems,
		eventsBlob:          task.GetEvents(),
		newRunEventsBlob:    task.GetNewRunEvents(),
		newRunID:            task.GetNewRunId(),
		batchable:           true,
	}
}

func (e *ExecutableHistoryTask) QueueID() interface{} {
	return e.WorkflowKey
}

func (e *ExecutableHistoryTask) Execute() error {
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
			metrics.OperationTag(metrics.HistoryReplicationTaskScope),
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

	if !e.Config.EnableReplicateLocalGeneratedEvent() {
		return engine.ReplicateHistoryEvents(
			ctx,
			e.WorkflowKey,
			e.baseExecutionInfo,
			e.versionHistoryItems,
			events,
			newRunEvents,
			e.newRunID,
		)
	}

	return e.HistoryEventsHandler.HandleHistoryEvents(
		ctx,
		e.SourceClusterName(),
		e.WorkflowKey,
		e.baseExecutionInfo,
		e.versionHistoryItems,
		events,
		newRunEvents,
		e.newRunID,
	)
}

func (e *ExecutableHistoryTask) HandleErr(err error) error {
	switch retryErr := err.(type) {
	case nil, *serviceerror.NotFound:
		return nil
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
		e.Logger.Error("history replication task encountered error",
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return err
	}
}

func (e *ExecutableHistoryTask) MarkPoisonPill() error {
	shardContext, err := e.ShardController.GetShardByNamespaceWorkflow(
		namespace.ID(e.NamespaceID),
		e.WorkflowID,
	)
	if err != nil {
		return err
	}

	events, err := e.EventSerializer.DeserializeEvents(e.eventsBlob)
	if err != nil {
		e.Logger.Error("unable to enqueue history replication task to DLQ, ser/de error",
			tag.ShardID(shardContext.GetShardID()),
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
			tag.Error(err),
		)
		return nil
	} else if len(events) == 0 {
		e.Logger.Error("unable to enqueue history replication task to DLQ, no events",
			tag.ShardID(shardContext.GetShardID()),
			tag.WorkflowNamespaceID(e.NamespaceID),
			tag.WorkflowID(e.WorkflowID),
			tag.WorkflowRunID(e.RunID),
			tag.TaskID(e.ExecutableTask.TaskID()),
		)
		return nil
	}

	// TODO: GetShardID will break GetDLQReplicationMessages we need to handle DLQ for cross shard replication.
	taskInfo := &persistencespb.ReplicationTaskInfo{
		NamespaceId:  e.NamespaceID,
		WorkflowId:   e.WorkflowID,
		RunId:        e.RunID,
		TaskId:       e.ExecutableTask.TaskID(),
		TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
		FirstEventId: events[0].GetEventId(),
		NextEventId:  events[len(events)-1].GetEventId() + 1,
		Version:      events[0].GetVersion(),
	}

	e.Logger.Error("enqueue history replication task to DLQ",
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

func (e *ExecutableHistoryTask) getDeserializedEvents() (_ [][]*historypb.HistoryEvent, _ []*historypb.HistoryEvent, retError error) {
	if e.eventsDesResponse != nil {
		return e.eventsDesResponse.events, e.eventsDesResponse.newRunEvents, e.eventsDesResponse.err
	}
	e.deserializeLock.Lock()
	defer e.deserializeLock.Unlock()

	if e.eventsDesResponse != nil {
		return e.eventsDesResponse.events, e.eventsDesResponse.newRunEvents, e.eventsDesResponse.err
	}

	defer func() {
		if retError != nil {
			e.eventsDesResponse = &eventsDeserializeResponse{
				events:       nil,
				newRunEvents: nil,
				err:          retError,
			}
		}
	}()

	events, err := e.EventSerializer.DeserializeEvents(e.eventsBlob)
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

	newRunEvents, err := e.EventSerializer.DeserializeEvents(e.newRunEventsBlob)
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
	eventsSlice := [][]*historypb.HistoryEvent{events}
	e.eventsDesResponse = &eventsDeserializeResponse{
		events:       eventsSlice,
		newRunEvents: newRunEvents,
		err:          nil,
	}
	return eventsSlice, newRunEvents, err
}

func (e *ExecutableHistoryTask) BatchWith(incomingTask BatchableTask) (TrackableExecutableTask, bool) {
	if !e.batchable || !incomingTask.CanBatch() {
		return nil, false
	}

	incomingHistoryTask, err := e.validateIncomingBatchTask(incomingTask)
	if err != nil {
		e.Logger.Debug("Failed to batch task", tag.Error(err))
		return nil, false
	}

	currentEvents, currentNewRunEvents, err := e.getDeserializedEvents()
	if err != nil {
		return nil, false
	}
	incomingEvents, incomingNewRunEvents, err := incomingHistoryTask.getDeserializedEvents()
	if err != nil {
		return nil, false
	}

	return &ExecutableHistoryTask{
		ProcessToolBox:      e.ProcessToolBox,
		WorkflowKey:         e.WorkflowKey,
		ExecutableTask:      e.ExecutableTask,
		baseExecutionInfo:   e.baseExecutionInfo,
		versionHistoryItems: e.getFresherVersionHistoryItems(e.versionHistoryItems, incomingHistoryTask.versionHistoryItems),
		eventsDesResponse: &eventsDeserializeResponse{
			events:       append(currentEvents, incomingEvents...),
			newRunEvents: append(currentNewRunEvents, incomingNewRunEvents...),
			err:          nil,
		},
		batchable: true,
		// we have validated that currentTask has no new run events in e.checkEvents(),
		// so e.newRunID must be empty.
		newRunID: incomingHistoryTask.newRunID,
	}, true
}

func (e *ExecutableHistoryTask) getFresherVersionHistoryItems(versionHistoryItemsA []*historyspb.VersionHistoryItem, versionHistoryItemsB []*historyspb.VersionHistoryItem) []*historyspb.VersionHistoryItem {
	fresherVersionHistoryItems := versionHistoryItemsA
	if versionHistoryItemsA[len(versionHistoryItemsA)-1].GetEventId() < versionHistoryItemsB[len(versionHistoryItemsB)-1].GetEventId() {
		fresherVersionHistoryItems = versionHistoryItemsB
	}
	var items []*historyspb.VersionHistoryItem
	for _, item := range fresherVersionHistoryItems {
		items = append(items, versionhistory.CopyVersionHistoryItem(item))
	}
	return items
}

func (e *ExecutableHistoryTask) validateIncomingBatchTask(incomingTask BatchableTask) (*ExecutableHistoryTask, error) {
	incomingHistoryTask, isHistoryTask := incomingTask.(*ExecutableHistoryTask)
	if !isHistoryTask {
		return nil, serviceerror.NewInvalidArgument("Unsupported Batch type")
	}
	if err := e.checkSourceCluster(incomingHistoryTask.SourceClusterName()); err != nil {
		return nil, err
	}

	if err := e.checkWorkflowKey(incomingHistoryTask.WorkflowKey); err != nil {
		return nil, err
	}

	if err := e.checkVersionHistoryItem(incomingHistoryTask.versionHistoryItems); err != nil {
		return nil, err
	}

	if err := e.checkBaseExecutionInfo(incomingHistoryTask.baseExecutionInfo); err != nil {
		return nil, err
	}

	events, _, err := incomingHistoryTask.getDeserializedEvents()
	if err != nil {
		return nil, err
	}

	if err = e.checkEvents(events); err != nil {
		return nil, err
	}

	return incomingHistoryTask, nil
}

func (e *ExecutableHistoryTask) checkSourceCluster(incomingTaskSourceCluster string) error {
	if e.SourceClusterName() != incomingTaskSourceCluster {
		return serviceerror.NewInvalidArgument("source cluster does not match")
	}
	return nil
}

// checkVersionHistoryItem will check if incoming tasks Version history is on the same branch as the current one
func (e *ExecutableHistoryTask) checkVersionHistoryItem(incomingHistoryItems []*historyspb.VersionHistoryItem) error {
	if versionhistory.IsVersionHistoryItemsInSameBranch(e.versionHistoryItems, incomingHistoryItems) {
		return nil
	}
	return serviceerror.NewInvalidArgument("version history does not match")
}

func (e *ExecutableHistoryTask) checkWorkflowKey(incomingWorkflowKey definition.WorkflowKey) error {
	if e.WorkflowKey != incomingWorkflowKey {
		return serviceerror.NewInvalidArgument("workflow key does not match")
	}
	return nil
}

func (e *ExecutableHistoryTask) checkBaseExecutionInfo(incomingTaskExecutionInfo *workflowpb.BaseExecutionInfo) error {
	if e.baseExecutionInfo == nil && incomingTaskExecutionInfo == nil {
		return nil
	}
	if e.baseExecutionInfo == nil || incomingTaskExecutionInfo == nil {
		return serviceerror.NewInvalidArgument("one of base execution is nil")
	}

	if !e.baseExecutionInfo.Equal(incomingTaskExecutionInfo) {
		return serviceerror.NewInvalidArgument("base execution is not equal")
	}
	return nil
}

func (e *ExecutableHistoryTask) checkEvents(
	incomingEventBatches [][]*historypb.HistoryEvent,
) error {
	if len(incomingEventBatches) == 0 {
		return serviceerror.NewInvalidArgument("incoming task is empty")
	}
	currentEvents, currentNewRunEvents, err := e.getDeserializedEvents()
	if err != nil {
		return err
	}

	if currentNewRunEvents != nil {
		return serviceerror.NewInvalidArgument("Current Task is expected to be the last event of a workflow")
	}

	currentLastBatch := currentEvents[len(currentEvents)-1]
	currentLastEvent := currentLastBatch[len(currentLastBatch)-1]
	incomingFirstBatch := incomingEventBatches[0]
	incomingFirstEvent := incomingFirstBatch[0]

	if currentLastEvent.Version != incomingFirstEvent.Version {
		return serviceerror.NewInvalidArgument("events version does not match")
	}
	if currentLastEvent.EventId+1 != incomingFirstEvent.EventId {
		return serviceerror.NewInvalidArgument("events id is not consecutive")
	}

	return nil
}

func (e *ExecutableHistoryTask) CanBatch() bool {
	return e.batchable
}

func (e *ExecutableHistoryTask) MarkUnbatchable() {
	e.batchable = false
}
