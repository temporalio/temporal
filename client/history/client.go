package history

import (
	"sync"
	"time"

	"golang.org/x/net/context"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	tchannel "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
)

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
	sResolver, err := monitor.GetResolver(common.HistoryServiceName)
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

func (c *clientImpl) StartWorkflowExecution(context thrift.Context,
	request *workflow.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	client, err := c.getHostForRequest(request.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.StartWorkflowExecution(ctx, request)
}

func (c *clientImpl) GetWorkflowExecutionHistory(context thrift.Context,
	request *workflow.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	client, err := c.getHostForRequest(request.Execution.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.GetWorkflowExecutionHistory(ctx, request)
}

func (c *clientImpl) RecordDecisionTaskStarted(context thrift.Context,
	request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	client, err := c.getHostForRequest(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.RecordDecisionTaskStarted(ctx, request)
}

func (c *clientImpl) RecordActivityTaskStarted(context thrift.Context,
	request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	client, err := c.getHostForRequest(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.RecordActivityTaskStarted(ctx, request)
}

func (c *clientImpl) RespondDecisionTaskCompleted(context thrift.Context,
	request *workflow.RespondDecisionTaskCompletedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.RespondDecisionTaskCompleted(ctx, request)
}

func (c *clientImpl) RespondActivityTaskCompleted(context thrift.Context,
	request *workflow.RespondActivityTaskCompletedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.RespondActivityTaskCompleted(ctx, request)
}

func (c *clientImpl) RespondActivityTaskFailed(context thrift.Context,
	request *workflow.RespondActivityTaskFailedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.RespondActivityTaskFailed(ctx, request)
}

func (c *clientImpl) RespondActivityTaskCanceled(context thrift.Context,
	request *workflow.RespondActivityTaskCanceledRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.RespondActivityTaskCanceled(ctx, request)
}

func (c *clientImpl) RecordActivityTaskHeartbeat(context thrift.Context,
	request *workflow.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(context)
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

func (c *clientImpl) createContext(parent thrift.Context) (thrift.Context, context.CancelFunc) {
	timeout := time.Second * 30
	if parent == nil {
		// TODO: make timeout configurable
		return thrift.NewContext(timeout)
	}
	builder := tchannel.NewContextBuilder(timeout)
	builder.SetParentContext(parent)
	return builder.Build()
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
		tClient := thrift.NewClient(c.connection, common.HistoryServiceName, &thrift.ClientOptions{
			HostPort: hostPort,
		})

		client = h.NewTChanHistoryServiceClient(tClient)
		c.thriftCache[hostPort] = client
	}
	return client
}
