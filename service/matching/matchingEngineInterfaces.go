package matching

import workflow "code.uber.internal/devexp/minions/.gen/go/shared"
import m "code.uber.internal/devexp/minions/.gen/go/matching"

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		AddDecisionTask(addRequest *m.AddDecisionTaskRequest) error
		AddActivityTask(addRequest *m.AddActivityTaskRequest) error
		PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
		PollForActivityTask(request *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
	}
)
