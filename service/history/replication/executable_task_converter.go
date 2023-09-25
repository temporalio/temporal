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
	"fmt"
	"time"

	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	ExecutableTaskConverter interface {
		Convert(
			taskClusterName string,
			clientShardKey ClusterShardKey,
			serverShardKey ClusterShardKey,
			replicationTasks ...*replicationspb.ReplicationTask,
		) []TrackableExecutableTask
	}

	executableTaskConverterImpl struct {
		processToolBox ProcessToolBox
	}
)

func NewExecutableTaskConverter(
	processToolBox ProcessToolBox,
) *executableTaskConverterImpl {
	return &executableTaskConverterImpl{
		processToolBox: processToolBox,
	}
}

func (e *executableTaskConverterImpl) Convert(
	taskClusterName string,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
	replicationTasks ...*replicationspb.ReplicationTask,
) []TrackableExecutableTask {
	tasks := make([]TrackableExecutableTask, len(replicationTasks))
	for index, replicationTask := range replicationTasks {
		e.processToolBox.MetricsHandler.Counter(metrics.ReplicationTasksRecv.GetMetricName()).Record(
			int64(1),
			metrics.FromClusterIDTag(serverShardKey.ClusterID),
			metrics.ToClusterIDTag(clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTag(replicationTask)),
		)
		task, err := e.convertOne(taskClusterName, replicationTask)
		if err != nil {
			continue
		}
		tasks[index] = task
	}
	return tasks
}

func (e *executableTaskConverterImpl) convertOne(
	taskClusterName string,
	replicationTask *replicationspb.ReplicationTask,
) (TrackableExecutableTask, error) {
	var taskCreationTime time.Time
	if replicationTask.VisibilityTime != nil {
		taskCreationTime = *replicationTask.VisibilityTime
	} else {
		taskCreationTime = time.Now().UTC()
	}

	switch replicationTask.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK: // TODO to be deprecated
		return NewExecutableNoopTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			taskClusterName,
		), nil
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK: // TODO to be deprecated
		return NewExecutableNoopTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			taskClusterName,
		), nil
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		return NewExecutableActivityStateTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncActivityTaskAttributes(),
			taskClusterName,
		), nil
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		return NewExecutableWorkflowStateTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncWorkflowStateTaskAttributes(),
			taskClusterName,
		), nil
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		events, newRunEvents, err := e.deserializeHistoryEvents(replicationTask.SourceTaskId, replicationTask.GetHistoryTaskAttributes())
		if err == nil {
			return nil, err
		}
		if len(events) == 0 {
			e.processToolBox.Logger.Error("unable to create history replication task, no events",
				tag.WorkflowNamespaceID(replicationTask.GetHistoryTaskAttributes().GetNamespaceId()),
				tag.WorkflowID(replicationTask.GetHistoryTaskAttributes().GetWorkflowId()),
				tag.WorkflowRunID(replicationTask.GetHistoryTaskAttributes().GetRunId()),
				tag.TaskID(replicationTask.SourceTaskId),
			)
			return nil, serviceerror.NewInvalidArgument("unable to create replication task, no events")
		}
		return NewExecutableHistoryTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetHistoryTaskAttributes(),
			events,
			newRunEvents,
			taskClusterName,
		), nil
	default:
		e.processToolBox.Logger.Error(fmt.Sprintf("unknown replication task: %v", replicationTask))
		return NewExecutableUnknownTask(
			e.processToolBox,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask,
		), nil
	}
}

func (e *executableTaskConverterImpl) deserializeHistoryEvents(sourceTaskId int64, task *replicationspb.HistoryTaskAttributes) ([]*historypb.HistoryEvent, []*historypb.HistoryEvent, error) {
	events, err := e.processToolBox.EventSerializer.DeserializeEvents(task.GetEvents())
	if err != nil {
		e.processToolBox.Logger.Error("unable to deserialize history events",
			tag.WorkflowNamespaceID(task.NamespaceId),
			tag.WorkflowID(task.WorkflowId),
			tag.WorkflowRunID(task.RunId),
			tag.TaskID(sourceTaskId),
			tag.Error(err),
		)
		return nil, nil, err
	}

	newRunEvents, err := e.processToolBox.EventSerializer.DeserializeEvents(task.GetNewRunEvents())
	if err != nil {
		e.processToolBox.Logger.Error("unable to deserialize new run history events",
			tag.WorkflowNamespaceID(task.GetNamespaceId()),
			tag.WorkflowID(task.GetWorkflowId()),
			tag.WorkflowRunID(task.GetRunId()),
			tag.TaskID(sourceTaskId),
			tag.Error(err),
		)
		return nil, nil, err
	}
	return events, newRunEvents, nil
}
