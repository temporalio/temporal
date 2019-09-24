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

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/persistence"
)

type (
	historyEventNotification struct {
		id                     definition.WorkflowIdentifier
		lastFirstEventID       int64
		nextEventID            int64
		previousStartedEventID int64
		timestamp              time.Time
		currentBranchToken     []byte
		workflowState          int
		workflowCloseState     int
	}

	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon

		StartWorkflowExecution(ctx context.Context, request *h.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)
		GetMutableState(ctx context.Context, request *h.GetMutableStateRequest) (*h.GetMutableStateResponse, error)
		PollMutableState(ctx context.Context, request *h.PollMutableStateRequest) (*h.PollMutableStateResponse, error)
		DescribeMutableState(ctx context.Context, request *h.DescribeMutableStateRequest) (*h.DescribeMutableStateResponse, error)
		ResetStickyTaskList(ctx context.Context, resetRequest *h.ResetStickyTaskListRequest) (*h.ResetStickyTaskListResponse, error)
		DescribeWorkflowExecution(ctx context.Context, request *h.DescribeWorkflowExecutionRequest) (*workflow.DescribeWorkflowExecutionResponse, error)
		RecordDecisionTaskStarted(ctx context.Context, request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
		RecordActivityTaskStarted(ctx context.Context, request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error)
		RespondDecisionTaskCompleted(ctx context.Context, request *h.RespondDecisionTaskCompletedRequest) (*h.RespondDecisionTaskCompletedResponse, error)
		RespondDecisionTaskFailed(ctx context.Context, request *h.RespondDecisionTaskFailedRequest) error
		RespondActivityTaskCompleted(ctx context.Context, request *h.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(ctx context.Context, request *h.RespondActivityTaskFailedRequest) error
		RespondActivityTaskCanceled(ctx context.Context, request *h.RespondActivityTaskCanceledRequest) error
		RecordActivityTaskHeartbeat(ctx context.Context, request *h.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error)
		RequestCancelWorkflowExecution(ctx context.Context, request *h.RequestCancelWorkflowExecutionRequest) error
		SignalWorkflowExecution(ctx context.Context, request *h.SignalWorkflowExecutionRequest) error
		SignalWithStartWorkflowExecution(ctx context.Context, request *h.SignalWithStartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error)
		RemoveSignalMutableState(ctx context.Context, request *h.RemoveSignalMutableStateRequest) error
		TerminateWorkflowExecution(ctx context.Context, request *h.TerminateWorkflowExecutionRequest) error
		ResetWorkflowExecution(ctx context.Context, request *h.ResetWorkflowExecutionRequest) (*workflow.ResetWorkflowExecutionResponse, error)
		ScheduleDecisionTask(ctx context.Context, request *h.ScheduleDecisionTaskRequest) error
		RecordChildExecutionCompleted(ctx context.Context, request *h.RecordChildExecutionCompletedRequest) error
		ReplicateEvents(ctx context.Context, request *h.ReplicateEventsRequest) error
		ReplicateRawEvents(ctx context.Context, request *h.ReplicateRawEventsRequest) error
		ReplicateEventsV2(ctx context.Context, request *h.ReplicateEventsV2Request) error
		SyncShardStatus(ctx context.Context, request *h.SyncShardStatusRequest) error
		SyncActivity(ctx context.Context, request *h.SyncActivityRequest) error
		GetReplicationMessages(ctx context.Context, taskID int64) (*replicator.ReplicationMessages, error)
		QueryWorkflow(ctx context.Context, request *h.QueryWorkflowRequest) (*h.QueryWorkflowResponse, error)
		ReapplyEvents(ctx context.Context, domainUUID string, workflowID string, events []*workflow.HistoryEvent) error

		NotifyNewHistoryEvent(event *historyEventNotification)
		NotifyNewTransferTasks(tasks []persistence.Task)
		NotifyNewReplicationTasks(tasks []persistence.Task)
		NotifyNewTimerTasks(tasks []persistence.Task)
	}

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
		getTasks(ctx context.Context, readLevel int64) (*replicator.ReplicationMessages, error)
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

	taskExecutor interface {
		process(task queueTaskInfo, shouldProcessTask bool) (int, error)
		complete(task queueTaskInfo)
		getTaskFilter() queueTaskFilter
	}

	processor interface {
		taskExecutor
		readTasks(readLevel int64) ([]queueTaskInfo, bool, error)
		updateAckLevel(taskID int64) error
		queueShutdown() error
	}

	transferQueueProcessor interface {
		common.Daemon
		FailoverDomain(domainIDs map[string]struct{})
		NotifyNewTask(clusterName string, transferTasks []persistence.Task)
		LockTaskPrrocessing()
		UnlockTaskPrrocessing()
	}

	// TODO the timer queue processor and the one below, timer processor
	// in combination are confusing, we should consider a better naming
	// convention, or at least come with a better name for this case.
	timerQueueProcessor interface {
		common.Daemon
		FailoverDomain(domainIDs map[string]struct{})
		NotifyNewTimers(clusterName string, timerTask []persistence.Task)
		LockTaskPrrocessing()
		UnlockTaskPrrocessing()
	}

	timerProcessor interface {
		taskExecutor
		notifyNewTimers(timerTask []persistence.Task)
	}

	timerQueueAckMgr interface {
		getFinishedChan() <-chan struct{}
		readTimerTasks() ([]*persistence.TimerTaskInfo, *persistence.TimerTaskInfo, bool, error)
		completeTimerTask(timerTask *persistence.TimerTaskInfo)
		getAckLevel() TimerSequenceID
		getReadLevel() TimerSequenceID
		updateAckLevel()
	}

	historyEventNotifier interface {
		common.Daemon
		NotifyNewHistoryEvent(event *historyEventNotification)
		WatchHistoryEvent(identifier definition.WorkflowIdentifier) (string, chan *historyEventNotification, error)
		UnwatchHistoryEvent(identifier definition.WorkflowIdentifier, subscriberID string) error
	}
)
