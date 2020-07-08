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

package history

import (
	"context"

	"github.com/gogo/protobuf/types"

	enumsspb "github.com/temporalio/temporal/api/enums/v1"
	"github.com/temporalio/temporal/api/persistenceblobs/v1"
	replicationspb "github.com/temporalio/temporal/api/replication/v1"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/task"
)

type (
	// EngineFactory is used to create an instance of sharded history engine
	EngineFactory interface {
		CreateEngine(context ShardContext) Engine
	}

	queueProcessor interface {
		common.Daemon
		notifyNewTask()
	}

	// ReplicatorQueueProcessor is the interface for replicator queue processor
	ReplicatorQueueProcessor interface {
		queueProcessor
		getTasks(
			ctx context.Context,
			pollingCluster string,
			lastReadTaskID int64,
		) (*replicationspb.ReplicationMessages, error)
		getTask(
			ctx context.Context,
			taskInfo *replicationspb.ReplicationTaskInfo,
		) (*replicationspb.ReplicationTask, error)
	}

	queueAckMgr interface {
		getFinishedChan() <-chan struct{}
		readQueueTasks() ([]queueTaskInfo, bool, error)
		completeQueueTask(taskID int64)
		getQueueAckLevel() int64
		getQueueReadLevel() int64
		updateQueueAckLevel() error
	}

	queueTaskInfo interface {
		GetVersion() int64
		GetTaskId() int64
		GetTaskType() enumsspb.TaskType
		GetVisibilityTimestamp() *types.Timestamp
		GetWorkflowId() string
		GetRunId() string
		GetNamespaceId() string
	}

	queueTask interface {
		task.PriorityTask
		queueTaskInfo
		GetQueueType() queueType
		GetShard() ShardContext
	}

	queueTaskExecutor interface {
		execute(taskInfo queueTaskInfo, shouldProcessTask bool) error
	}

	queueTaskProcessor interface {
		common.Daemon
		StopShardProcessor(ShardContext)
		Submit(queueTask) error
		TrySubmit(queueTask) (bool, error)
	}

	// TODO: deprecate this interface in favor of the task interface
	// defined in common/task package
	taskExecutor interface {
		process(taskInfo *taskInfo) (int, error)
		complete(taskInfo *taskInfo)
		getTaskFilter() taskFilter
	}

	processor interface {
		taskExecutor
		readTasks(readLevel int64) ([]queueTaskInfo, bool, error)
		updateAckLevel(taskID int64) error
		queueShutdown() error
	}

	timerProcessor interface {
		taskExecutor
		notifyNewTimers(timerTask []persistence.Task)
	}

	timerQueueAckMgr interface {
		getFinishedChan() <-chan struct{}
		readTimerTasks() ([]*persistenceblobs.TimerTaskInfo, *persistenceblobs.TimerTaskInfo, bool, error)
		completeTimerTask(timerTask *persistenceblobs.TimerTaskInfo)
		getAckLevel() timerKey
		getReadLevel() timerKey
		updateAckLevel() error
	}

	historyEventNotifier interface {
		common.Daemon
		NotifyNewHistoryEvent(event *historyEventNotification)
		WatchHistoryEvent(identifier definition.WorkflowIdentifier) (string, chan *historyEventNotification, error)
		UnwatchHistoryEvent(identifier definition.WorkflowIdentifier, subscriberID string) error
	}

	queueType int
)

const (
	transferQueueType queueType = iota + 1
	timerQueueType
	replicationQueueType
)
