package matching

import (
	m "github.com/uber/cadence/.gen/go/matching"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/tchannel-go/thrift"
)

type (
	// Engine exposes interfaces for clients to poll for activity and decision tasks.
	Engine interface {
		AddDecisionTask(addRequest *m.AddDecisionTaskRequest) error
		AddActivityTask(addRequest *m.AddActivityTaskRequest) error
		PollForDecisionTask(ctx thrift.Context, request *m.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error)
		PollForActivityTask(ctx thrift.Context, request *m.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error)
	}
)
