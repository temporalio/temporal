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
	"go.temporal.io/server/common/persistence"
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

		deserializeLock   sync.Mutex
		eventsDesResponse *eventsDeserializeResponse
	}
	eventsDeserializeResponse struct {
		events       []*historypb.HistoryEvent
		newRunEvents []*historypb.HistoryEvent
		err          error
	}
)

var _ ctasks.Task = (*ExecutableHistoryTask)(nil)
var _ TrackableExecutableTask = (*ExecutableHistoryTask)(nil)

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
		e.MetricsHandler.Counter(metrics.ReplicationTasksSkipped.GetMetricName()).Record(
			1,
			metrics.OperationTag(metrics.HistoryReplicationTaskScope),
			metrics.NamespaceTag(namespaceName),
		)
		return nil
	}
	ctx, cancel := newTaskContext(namespaceName)
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

	return engine.ReplicateHistoryEvents(
		ctx,
		e.WorkflowKey,
		e.baseExecutionInfo,
		e.versionHistoryItems,
		[][]*historypb.HistoryEvent{events},
		newRunEvents,
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
		ctx, cancel := newTaskContext(namespaceName)
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
	req := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           shardContext.GetShardID(),
		SourceClusterName: e.ExecutableTask.SourceClusterName(),
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId:  e.NamespaceID,
			WorkflowId:   e.WorkflowID,
			RunId:        e.RunID,
			TaskId:       e.ExecutableTask.TaskID(),
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: events[0].GetEventId(),
			NextEventId:  events[len(events)-1].GetEventId() + 1,
			Version:      events[0].GetVersion(),
		},
	}

	e.Logger.Error("enqueue history replication task to DLQ",
		tag.ShardID(shardContext.GetShardID()),
		tag.WorkflowNamespaceID(e.NamespaceID),
		tag.WorkflowID(e.WorkflowID),
		tag.WorkflowRunID(e.RunID),
		tag.TaskID(e.ExecutableTask.TaskID()),
	)

	ctx, cancel := newTaskContext(e.NamespaceID)
	defer cancel()

	return shardContext.GetExecutionManager().PutReplicationTaskToDLQ(ctx, req)
}

func (e *ExecutableHistoryTask) getDeserializedEvents() (_ []*historypb.HistoryEvent, _ []*historypb.HistoryEvent, retError error) {
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
	e.eventsDesResponse = &eventsDeserializeResponse{
		events:       events,
		newRunEvents: newRunEvents,
		err:          nil,
	}
	return events, newRunEvents, err
}
