package matching

import (
	m "code.uber.internal/devexp/minions/.gen/go/matching"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
)

// Client is the interface exposed by matching service client
type Client interface {
	AddActivityTask(addRequest *m.AddActivityTaskRequest) error
	AddDecisionTask(addRequest *m.AddDecisionTaskRequest) error
	PollForActivityTask(pollRequest *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
	PollForDecisionTask(pollRequest *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
}
