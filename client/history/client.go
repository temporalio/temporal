package history

import (
	"time"

	"golang.org/x/net/context"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const historyServiceName = "uber-minions-history"

var _ Client = (*clientImpl)(nil)

type clientImpl struct {
	connection *tchannel.Channel
	client     h.TChanHistoryService
}

// NewClient creates a new history service TChannel client
func NewClient(ch *tchannel.Channel) (Client, error) {
	tClient := thrift.NewClient(ch, historyServiceName, nil)

	client := &clientImpl{
		connection: ch,
		client:     h.NewTChanHistoryServiceClient(tClient),
	}
	return client, nil
}

func (c *clientImpl) createContext() (thrift.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	return thrift.NewContext(time.Second * 5)
}

func (c *clientImpl) StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.StartWorkflowExecution(ctx, request)
}

func (c *clientImpl) GetWorkflowExecutionHistory(
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.GetWorkflowExecutionHistory(ctx, request)
}

func (c *clientImpl) RecordDecisionTaskStarted(request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.RecordDecisionTaskStarted(ctx, request)
}

func (c *clientImpl) RecordActivityTaskStarted(request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.RecordActivityTaskStarted(ctx, request)
}

func (c *clientImpl) RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.RespondDecisionTaskCompleted(ctx, request)
}

func (c *clientImpl) RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.RespondActivityTaskCompleted(ctx, request)
}

func (c *clientImpl) RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error {
	ctx, cancel := c.createContext()
	defer cancel()
	return c.client.RespondActivityTaskFailed(ctx, request)
}
