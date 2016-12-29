package matching

import (
	"time"

	"golang.org/x/net/context"

	m "code.uber.internal/devexp/minions/.gen/go/matching"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const matchingServiceName = "uber-minions-matching"

var _ Client = (*clientImpl)(nil)

type clientImpl struct {
	connection *tchannel.Channel
	client     m.TChanMatchingService
}

// NewClient creates a new history service TChannel client
func NewClient(ch *tchannel.Channel) (Client, error) {
	tClient := thrift.NewClient(ch, matchingServiceName, nil)

	client := &clientImpl{
		connection: ch,
		client:     m.NewTChanMatchingServiceClient(tClient),
	}
	return client, nil
}

func (c *clientImpl) createContext() (thrift.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	return thrift.NewContext(time.Second * 5)
}

func (c *clientImpl) AddActivityTask(addRequest *m.AddActivityTaskRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.AddActivityTask(ctx, addRequest)
}

func (c *clientImpl) AddDecisionTask(addRequest *m.AddDecisionTaskRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.AddDecisionTask(ctx, addRequest)
}

func (c *clientImpl) PollForActivityTask(pollRequest *workflow.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error) {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.PollForActivityTask(ctx, pollRequest)
}

func (c *clientImpl) PollForDecisionTask(pollRequest *workflow.PollForDecisionTaskRequest) (*workflow.PollForDecisionTaskResponse, error) {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.PollForDecisionTask(ctx, pollRequest)
}
