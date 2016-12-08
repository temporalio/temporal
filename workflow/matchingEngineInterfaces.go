package workflow

import workflow "code.uber.internal/devexp/minions/.gen/go/shared"

type (
	// MatchingEngine exposes interfaces for clients to poll for activity and decision tasks.
	MatchingEngine interface {
		PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
		PollForActivityTask(request *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
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
