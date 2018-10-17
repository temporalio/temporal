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
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

type (
	workflowIdentifier struct {
		domainID   string
		workflowID string
		runID      string
	}

	historyEventNotification struct {
		workflowIdentifier
		lastFirstEventID  int64
		nextEventID       int64
		isWorkflowRunning bool
		timestamp         time.Time
	}

	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon
		// TODO: Convert workflow.WorkflowExecution to pointer all over the place
		StartWorkflowExecution(request *h.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse,
			error)
		GetMutableState(ctx context.Context, request *h.GetMutableStateRequest) (*h.GetMutableStateResponse, error)
		DescribeMutableState(ctx context.Context, request *h.DescribeMutableStateRequest) (*h.DescribeMutableStateResponse, error)
		ResetStickyTaskList(ctx context.Context, resetRequest *h.ResetStickyTaskListRequest) (*h.ResetStickyTaskListResponse, error)
		DescribeWorkflowExecution(ctx context.Context,
			request *h.DescribeWorkflowExecutionRequest) (*workflow.DescribeWorkflowExecutionResponse, error)
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
		SignalWithStartWorkflowExecution(ctx context.Context, request *h.SignalWithStartWorkflowExecutionRequest) (
			*workflow.StartWorkflowExecutionResponse, error)
		RemoveSignalMutableState(ctx context.Context, request *h.RemoveSignalMutableStateRequest) error
		TerminateWorkflowExecution(ctx context.Context, request *h.TerminateWorkflowExecutionRequest) error
		ScheduleDecisionTask(ctx context.Context, request *h.ScheduleDecisionTaskRequest) error
		RecordChildExecutionCompleted(ctx context.Context, request *h.RecordChildExecutionCompletedRequest) error
		ReplicateEvents(ctx context.Context, request *h.ReplicateEventsRequest) error
		SyncShardStatus(ctx context.Context, request *h.SyncShardStatusRequest) error
		SyncActivity(ctx context.Context, request *h.SyncActivityRequest) error
	}

	// EngineFactory is used to create an instance of sharded history engine
	EngineFactory interface {
		CreateEngine(context ShardContext) Engine
	}

	queueProcessor interface {
		common.Daemon
		notifyNewTask()
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
	}

	processor interface {
		process(task queueTaskInfo) (int, error)
		readTasks(readLevel int64) ([]queueTaskInfo, bool, error)
		updateAckLevel(taskID int64) error
		queueShutdown() error
	}

	transferQueueProcessor interface {
		common.Daemon
		FailoverDomain(domainID string)
		NotifyNewTask(clusterName string, transferTasks []persistence.Task)
	}

	// TODO the timer queue processor and the one below, timer processor
	// in combination are confusing, we should consider a better naming
	// convention, or at least come with a better name for this case.
	timerQueueProcessor interface {
		common.Daemon
		FailoverDomain(domainID string)
		NotifyNewTimers(clusterName string, currentTime time.Time, timerTask []persistence.Task)
	}

	timerProcessor interface {
		notifyNewTimers(timerTask []persistence.Task)
		process(task *persistence.TimerTaskInfo) (int, error)
		getTimerGate() TimerGate
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
		WatchHistoryEvent(identifier workflowIdentifier) (string, chan *historyEventNotification, error)
		UnwatchHistoryEvent(identifier workflowIdentifier, subscriberID string) error
	}
)

func newWorkflowIdentifier(domainID string, execution *workflow.WorkflowExecution) workflowIdentifier {
	return workflowIdentifier{
		domainID:   domainID,
		workflowID: execution.GetWorkflowId(),
		runID:      execution.GetRunId(),
	}
}
