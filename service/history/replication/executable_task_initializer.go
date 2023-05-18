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

	"go.uber.org/fx"

	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

type (
	ProcessToolBox struct {
		fx.In

		Config             *configs.Config
		ClusterMetadata    cluster.Metadata
		ClientBean         client.Bean
		ShardController    shard.Controller
		NamespaceCache     namespace.Registry
		NDCHistoryResender xdc.NDCHistoryResender
		TaskScheduler      ctasks.Scheduler[TrackableExecutableTask]
		MetricsHandler     metrics.Handler
		Logger             log.Logger
	}
)

func (i *ProcessToolBox) ConvertTasks(
	taskClusterName string,
	clientShardKey ClusterShardKey,
	serverShardKey ClusterShardKey,
	replicationTasks ...*replicationspb.ReplicationTask,
) []TrackableExecutableTask {
	tasks := make([]TrackableExecutableTask, len(replicationTasks))
	for index, replicationTask := range replicationTasks {
		i.MetricsHandler.Counter(metrics.ReplicationTasksRecv.GetMetricName()).Record(
			int64(1),
			metrics.FromClusterIDTag(serverShardKey.ClusterID),
			metrics.ToClusterIDTag(clientShardKey.ClusterID),
			metrics.OperationTag(TaskOperationTag(replicationTask)),
		)
		tasks[index] = i.convertOne(taskClusterName, replicationTask)
	}
	return tasks
}

func (i *ProcessToolBox) convertOne(
	taskClusterName string,
	replicationTask *replicationspb.ReplicationTask,
) TrackableExecutableTask {
	var taskCreationTime time.Time
	if replicationTask.VisibilityTime != nil {
		taskCreationTime = *replicationTask.VisibilityTime
	} else {
		taskCreationTime = time.Now().UTC()
	}

	switch replicationTask.GetTaskType() {
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK: // TODO to be deprecated
		return NewExecutableNoopTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
		)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK: // TODO to be deprecated
		return NewExecutableNoopTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
		)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK:
		return NewExecutableActivityStateTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncActivityTaskAttributes(),
			taskClusterName,
		)
	case enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK:
		return NewExecutableWorkflowStateTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetSyncWorkflowStateTaskAttributes(),
			taskClusterName,
		)
	case enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK:
		return NewExecutableHistoryTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask.GetHistoryTaskAttributes(),
			taskClusterName,
		)
	default:
		i.Logger.Error(fmt.Sprintf("unknown replication task: %v", replicationTask))
		return NewExecutableUnknownTask(
			*i,
			replicationTask.SourceTaskId,
			taskCreationTime,
			replicationTask,
		)
	}
}
