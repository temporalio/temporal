package history

import (
	"time"

	"golang.org/x/net/context"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common/membership"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const historyServiceName = "cadence-history"
const shardID = "1" // TODO: actually derive shardID from request

var _ Client = (*clientImpl)(nil)

type clientImpl struct {
	connection *tchannel.Channel
	resolver   membership.ServiceResolver
}

// NewClient creates a new history service TChannel client
func NewClient(ch *tchannel.Channel, monitor membership.Monitor) (Client, error) {
	sResolver, err := monitor.GetResolver(historyServiceName)
	if err != nil {
		return nil, err
	}

	client := &clientImpl{
		connection: ch,
		resolver:   sResolver,
	}
	return client, nil
}

func (c *clientImpl) getHostForRequest(key string) (h.TChanHistoryService, error) {
	host, err := c.resolver.Lookup(key)
	if err != nil {
		return nil, err
	}
	// TODO: build client cache
	tClient := thrift.NewClient(c.connection, historyServiceName, &thrift.ClientOptions{
		HostPort: host.GetAddress(),
	})
	return h.NewTChanHistoryServiceClient(tClient), nil
}

func (c *clientImpl) createContext() (thrift.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	return thrift.NewContext(time.Second * 30)
}

func (c *clientImpl) StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	client, err := c.getHostForRequest(shardID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.StartWorkflowExecution(ctx, request)
}

func (c *clientImpl) GetWorkflowExecutionHistory(
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	client, err := c.getHostForRequest(shardID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.GetWorkflowExecutionHistory(ctx, request)
}

func (c *clientImpl) RecordDecisionTaskStarted(request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	client, err := c.getHostForRequest(shardID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RecordDecisionTaskStarted(ctx, request)
}

func (c *clientImpl) RecordActivityTaskStarted(request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	client, err := c.getHostForRequest(shardID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RecordActivityTaskStarted(ctx, request)
}

func (c *clientImpl) RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error {
	client, err := c.getHostForRequest(shardID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RespondDecisionTaskCompleted(ctx, request)
}

func (c *clientImpl) RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error {
	client, err := c.getHostForRequest(shardID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RespondActivityTaskCompleted(ctx, request)
}

func (c *clientImpl) RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error {
	client, err := c.getHostForRequest(shardID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RespondActivityTaskFailed(ctx, request)
}

func (c *clientImpl) RecordActivityTaskHeartbeat(request *workflow.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error) {
	client, err := c.getHostForRequest(shardID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RecordActivityTaskHeartbeat(ctx, request)
}
