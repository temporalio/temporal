package workflow

import (
	workflow "code.uber.internal/devexp/minions/.gen/go/minions"
	"code.uber.internal/devexp/minions/common"
)

var nilWorkflowExecution = workflow.WorkflowExecution{}

type (
	// WorkflowEngine represents an interface for workflow engine
	WorkflowEngine interface {
		// TODO: Convert workflow.WorkflowExecution to pointer all over the place
		StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (workflow.WorkflowExecution, error)
		PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
		PollForActivityTask(request *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
		RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error
		RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error
		RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error
	}

	historySerializer interface {
		Serialize(history []*workflow.HistoryEvent) ([]byte, error)
		Deserialize(data []byte) ([]*workflow.HistoryEvent, error)
	}

	transferQueueProcessor interface {
		common.Daemon
	}

	taskTokenSerializer interface {
		Serialize(token *taskToken) ([]byte, error)
		Deserialize(data []byte) (*taskToken, error)
	}

	taskToken struct {
		WorkflowID string `json:"workflowId"`
		RunID      string `json:"runId"`
		ScheduleID int64  `json:"scheduleId"`
	}
)
