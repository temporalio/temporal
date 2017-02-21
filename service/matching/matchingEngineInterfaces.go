package matching

import workflow "github.com/uber/cadence/.gen/go/shared"
import m "github.com/uber/cadence/.gen/go/matching"

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		AddDecisionTask(addRequest *m.AddDecisionTaskRequest) error
		AddActivityTask(addRequest *m.AddActivityTaskRequest) error
		PollForDecisionTask(request *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
		PollForActivityTask(request *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
	}
)
