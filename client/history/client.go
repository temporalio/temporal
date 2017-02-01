package history

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	h "code.uber.internal/devexp/minions/.gen/go/history"
	workflow "code.uber.internal/devexp/minions/.gen/go/shared"
	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/membership"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

const historyServiceName = "cadence-history"

var _ Client = (*clientImpl)(nil)

type clientImpl struct {
	connection      *tchannel.Channel
	resolver        membership.ServiceResolver
	tokenSerializer common.TaskTokenSerializer
	numberOfShards  int
	// TODO: consider refactor thriftCache into a separate struct
	thriftCacheLock sync.RWMutex
	thriftCache     map[string]h.TChanHistoryService
}

// NewClient creates a new history service TChannel client
func NewClient(ch *tchannel.Channel, monitor membership.Monitor, numberOfShards int) (Client, error) {
	sResolver, err := monitor.GetResolver(historyServiceName)
	if err != nil {
		return nil, err
	}

	client := &clientImpl{
		connection:      ch,
		resolver:        sResolver,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		numberOfShards:  numberOfShards,
		thriftCache:     make(map[string]h.TChanHistoryService),
	}
	return client, nil
}

func (c *clientImpl) StartWorkflowExecution(request *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	client, err := c.getHostForRequest(request.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.StartWorkflowExecution(ctx, request)
}

func (c *clientImpl) GetWorkflowExecutionHistory(
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	client, err := c.getHostForRequest(request.Execution.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.GetWorkflowExecutionHistory(ctx, request)
}

func (c *clientImpl) RecordDecisionTaskStarted(request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	client, err := c.getHostForRequest(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RecordDecisionTaskStarted(ctx, request)
}

func (c *clientImpl) RecordActivityTaskStarted(request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	client, err := c.getHostForRequest(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RecordActivityTaskStarted(ctx, request)
}

func (c *clientImpl) RespondDecisionTaskCompleted(request *workflow.RespondDecisionTaskCompletedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RespondDecisionTaskCompleted(ctx, request)
}

func (c *clientImpl) RespondActivityTaskCompleted(request *workflow.RespondActivityTaskCompletedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RespondActivityTaskCompleted(ctx, request)
}

func (c *clientImpl) RespondActivityTaskFailed(request *workflow.RespondActivityTaskFailedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RespondActivityTaskFailed(ctx, request)
}

func (c *clientImpl) RecordActivityTaskHeartbeat(request *workflow.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext()
	defer cancel()
	return client.RecordActivityTaskHeartbeat(ctx, request)
}

func (c *clientImpl) getHostForRequest(workflowID string) (h.TChanHistoryService, error) {
	key := common.WorkflowIDToHistoryShard(workflowID, c.numberOfShards)
	host, err := c.resolver.Lookup(string(key))
	if err != nil {
		return nil, err
	}

	return c.getThriftClient(host.GetAddress()), nil
}

func (c *clientImpl) createContext() (thrift.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	return thrift.NewContext(time.Second * 30)
}

func (c *clientImpl) getThriftClient(hostPort string) h.TChanHistoryService {
	c.thriftCacheLock.RLock()
	client, ok := c.thriftCache[hostPort]
	c.thriftCacheLock.RUnlock()
	if ok {
		return client
	}

	c.thriftCacheLock.Lock()
	defer c.thriftCacheLock.Unlock()

	// check again if in the cache cause it might have been added
	// before we acquired the lock
	client, ok = c.thriftCache[hostPort]
	if !ok {
		tClient := thrift.NewClient(c.connection, historyServiceName, &thrift.ClientOptions{
			HostPort: hostPort,
		})

		client = h.NewTChanHistoryServiceClient(tClient)
		c.thriftCache[hostPort] = client
	}
	return client
}
