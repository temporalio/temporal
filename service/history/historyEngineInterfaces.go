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
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/persistence"
)

type (
	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon
		// TODO: Convert workflow.WorkflowExecution to pointer all over the place
		StartWorkflowExecution(request *h.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse,
			error)
		GetWorkflowExecutionNextEventID(
			request *h.GetWorkflowExecutionNextEventIDRequest) (*h.GetWorkflowExecutionNextEventIDResponse, error)
		DescribeWorkflowExecution(
			request *h.DescribeWorkflowExecutionRequest) (*workflow.DescribeWorkflowExecutionResponse, error)
		RecordDecisionTaskStarted(request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
		RecordActivityTaskStarted(request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error)
		RespondDecisionTaskCompleted(request *h.RespondDecisionTaskCompletedRequest) error
		RespondDecisionTaskFailed(request *h.RespondDecisionTaskFailedRequest) error
		RespondActivityTaskCompleted(request *h.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(request *h.RespondActivityTaskFailedRequest) error
		RespondActivityTaskCanceled(request *h.RespondActivityTaskCanceledRequest) error
		RecordActivityTaskHeartbeat(request *h.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error)
		RequestCancelWorkflowExecution(request *h.RequestCancelWorkflowExecutionRequest) error
		SignalWorkflowExecution(request *h.SignalWorkflowExecutionRequest) error
		TerminateWorkflowExecution(request *h.TerminateWorkflowExecutionRequest) error
		ScheduleDecisionTask(request *h.ScheduleDecisionTaskRequest) error
		RecordChildExecutionCompleted(request *h.RecordChildExecutionCompletedRequest) error
	}

	// EngineFactory is used to create an instance of sharded history engine
	EngineFactory interface {
		CreateEngine(context ShardContext) Engine
	}

	historyEventSerializer interface {
		Serialize(event *workflow.HistoryEvent) ([]byte, error)
		Deserialize(data []byte) (*workflow.HistoryEvent, error)
	}

	transferQueueProcessor interface {
		common.Daemon
		NotifyNewTask()
	}

	timerQueueProcessor interface {
		common.Daemon
		NotifyNewTimer(timerTask []persistence.Task)
	}
)
