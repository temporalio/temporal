// Copyright (c) 2017 Uber Technologies, Inc.
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
	"time"

	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/task"
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
		) (*replicator.ReplicationMessages, error)
		getTask(
			ctx context.Context,
			taskInfo *replicator.ReplicationTaskInfo,
		) (*replicator.ReplicationTask, error)
	}

	queueAckMgr interface {
		getFinishedChan() <-chan struct{}
		readQueueTasks() ([]queueTaskInfo, bool, error)
		completeQueueTask(taskID int64)
		getQueueAckLevel() int64
		getQueueReadLevel() int64
		updateQueueAckLevel()
	}

	queueTaskInfo interface {
		GetVersion() int64
		GetTaskID() int64
		GetTaskType() int
		GetVisibilityTimestamp() time.Time
		GetWorkflowID() string
		GetRunID() string
		GetDomainID() string
	}

	queueTask interface {
		task.PriorityTask
		queueTaskInfo
		GetQueueType() queueType
		GetShardID() int
	}

	queueTaskExecutor interface {
		execute(taskInfo queueTaskInfo, shouldProcessTask bool) error
	}

	queueTaskProcessor interface {
		common.Daemon
		StopShardProcessor(int)
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
		readTimerTasks() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, bool, error)
		completeTimerTask(timerTask *persistence.TimerTaskInfo)
		getAckLevel() timerKey
		getReadLevel() timerKey
		updateAckLevel()
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
