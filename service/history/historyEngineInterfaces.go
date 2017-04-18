package history

import (
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type (
	// Engine represents an interface for managing workflow execution history.
	Engine interface {
		common.Daemon
		// TODO: Convert workflow.WorkflowExecution to pointer all over the place
		StartWorkflowExecution(request *h.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse,
			error)
		GetWorkflowExecutionHistory(
			request *h.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error)
		RecordDecisionTaskStarted(request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error)
		RecordActivityTaskStarted(request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error)
		RespondDecisionTaskCompleted(request *h.RespondDecisionTaskCompletedRequest) error
		RespondActivityTaskCompleted(request *h.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(request *h.RespondActivityTaskFailedRequest) error
		RespondActivityTaskCanceled(request *h.RespondActivityTaskCanceledRequest) error
		RecordActivityTaskHeartbeat(
			request *h.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error)
		SignalWorkflowExecution(request *h.SignalWorkflowExecutionRequest) error
		TerminateWorkflowExecution(request *h.TerminateWorkflowExecutionRequest) error
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
	}

	timerQueueProcessor interface {
		common.Daemon
		NotifyNewTimer(taskID int64)
	}
)
