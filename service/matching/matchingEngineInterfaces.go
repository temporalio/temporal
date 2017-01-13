package matching

import workflow "code.uber.internal/devexp/minions/.gen/go/shared"

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
		PollForActivityTask(request *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
	}
)
