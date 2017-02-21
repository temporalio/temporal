package matching

import (
	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
)

// Client is the interface exposed by matching service client
type Client interface {
	AddActivityTask(addRequest *m.AddActivityTaskRequest) error
	AddDecisionTask(addRequest *m.AddDecisionTaskRequest) error
	PollForActivityTask(pollRequest *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
	PollForDecisionTask(pollRequest *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
}
