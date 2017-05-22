// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	request *h.StartWorkflowExecutionRequest) (*workflow.StartWorkflowExecutionResponse, error) {
	client, err := c.getHostForRequest(request.GetStartRequest().GetWorkflowId())
	if err != nil {
		return nil, err
	}
	var response *workflow.StartWorkflowExecutionResponse
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		var err error
		ctx, cancel := c.createContext(context)
		defer cancel()
		response, err = client.StartWorkflowExecution(ctx, request)
		return err
	}
	err = c.executeWithRedirect(context, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) GetWorkflowExecutionHistory(context thrift.Context,
	request *h.GetWorkflowExecutionHistoryRequest) (*workflow.GetWorkflowExecutionHistoryResponse, error) {
	client, err := c.getHostForRequest(request.GetGetRequest().GetExecution().GetWorkflowId())
	if err != nil {
		return nil, err
	}
	var response *workflow.GetWorkflowExecutionHistoryResponse
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		var err error
		ctx, cancel := c.createContext(context)
		defer cancel()
		response, err = client.GetWorkflowExecutionHistory(ctx, request)
		return err
	}
	err = c.executeWithRedirect(context, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RecordDecisionTaskStarted(context thrift.Context,
	request *h.RecordDecisionTaskStartedRequest) (*h.RecordDecisionTaskStartedResponse, error) {
	client, err := c.getHostForRequest(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	var response *h.RecordDecisionTaskStartedResponse
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		var err error
		ctx, cancel := c.createContext(context)
		defer cancel()
		response, err = client.RecordDecisionTaskStarted(ctx, request)
		return err
	}
	err = c.executeWithRedirect(context, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RecordActivityTaskStarted(context thrift.Context,
	request *h.RecordActivityTaskStartedRequest) (*h.RecordActivityTaskStartedResponse, error) {
	client, err := c.getHostForRequest(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return nil, err
	}
	var response *h.RecordActivityTaskStartedResponse
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		var err error
		ctx, cancel := c.createContext(context)
		defer cancel()
		response, err = client.RecordActivityTaskStarted(ctx, request)
		return err
	}
	err = c.executeWithRedirect(context, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RespondDecisionTaskCompleted(context thrift.Context,
	request *h.RespondDecisionTaskCompletedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.GetCompleteRequest().TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.RespondDecisionTaskCompleted(ctx, request)
	}
	err = c.executeWithRedirect(context, client, op)
	return err
}

func (c *clientImpl) RespondActivityTaskCompleted(context thrift.Context,
	request *h.RespondActivityTaskCompletedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.GetCompleteRequest().GetTaskToken())
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.RespondActivityTaskCompleted(ctx, request)
	}
	err = c.executeWithRedirect(context, client, op)
	return err
}

func (c *clientImpl) RespondActivityTaskFailed(context thrift.Context,
	request *h.RespondActivityTaskFailedRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.GetFailedRequest().GetTaskToken())
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.RespondActivityTaskFailed(ctx, request)
	}
	err = c.executeWithRedirect(context, client, op)
	return err
}

func (c *clientImpl) RespondActivityTaskCanceled(context thrift.Context,
	request *h.RespondActivityTaskCanceledRequest) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.GetCancelRequest().GetTaskToken())
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.RespondActivityTaskCanceled(ctx, request)
	}
	err = c.executeWithRedirect(context, client, op)
	return err
}

func (c *clientImpl) RecordActivityTaskHeartbeat(context thrift.Context,
	request *h.RecordActivityTaskHeartbeatRequest) (*workflow.RecordActivityTaskHeartbeatResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.GetHeartbeatRequest().GetTaskToken())
	if err != nil {
		return nil, err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return nil, err
	}
	var response *workflow.RecordActivityTaskHeartbeatResponse
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		var err error
		ctx, cancel := c.createContext(context)
		defer cancel()
		response, err = client.RecordActivityTaskHeartbeat(ctx, request)
		return err
	}
	err = c.executeWithRedirect(context, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RequestCancelWorkflowExecution(context thrift.Context,
	request *h.RequestCancelWorkflowExecutionRequest) error {
	client, err := c.getHostForRequest(request.GetCancelRequest().GetWorkflowExecution().GetWorkflowId())
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.RequestCancelWorkflowExecution(ctx, request)
	}
	return c.executeWithRedirect(context, client, op)
}

func (c *clientImpl) SignalWorkflowExecution(context thrift.Context,
	request *h.SignalWorkflowExecutionRequest) error {
	client, err := c.getHostForRequest(request.GetSignalRequest().GetWorkflowExecution().GetWorkflowId())
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.SignalWorkflowExecution(ctx, request)
	}
	err = c.executeWithRedirect(context, client, op)

	return err
}

func (c *clientImpl) TerminateWorkflowExecution(context thrift.Context,
	request *h.TerminateWorkflowExecutionRequest) error {
	client, err := c.getHostForRequest(request.GetTerminateRequest().GetWorkflowExecution().GetWorkflowId())
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.TerminateWorkflowExecution(ctx, request)
	}
	err = c.executeWithRedirect(context, client, op)
	return err
}

func (c *clientImpl) ScheduleDecisionTask(context thrift.Context, request *h.ScheduleDecisionTaskRequest) error {
	client, err := c.getHostForRequest(request.GetWorkflowExecution().GetWorkflowId())
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.ScheduleDecisionTask(ctx, request)
	}
	err = c.executeWithRedirect(context, client, op)
	return err
}

func (c *clientImpl) RecordChildExecutionCompleted(context thrift.Context, request *h.RecordChildExecutionCompletedRequest) error {
	client, err := c.getHostForRequest(request.GetWorkflowExecution().GetWorkflowId())
	if err != nil {
		return err
	}
	op := func(context thrift.Context, client h.TChanHistoryService) error {
		ctx, cancel := c.createContext(context)
		defer cancel()
		return client.RecordChildExecutionCompleted(ctx, request)
	}
	err = c.executeWithRedirect(context, client, op)
	return err
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

func (c *clientImpl) executeWithRedirect(ctx thrift.Context, client h.TChanHistoryService,
	op func(context thrift.Context, client h.TChanHistoryService) error) error {
	var err error
	if ctx == nil {
		ctx = common.BackgroundThriftContext()
	}
redirectLoop:
	for {
		err = common.IsValidContext(ctx)
		if err != nil {
			break redirectLoop
		}
		err = op(ctx, client)
		if err != nil {
			if s, ok := err.(*h.ShardOwnershipLostError); ok {
				// TODO: consider emitting a metric for number of redirects
				client = c.getThriftClient(s.GetOwner())
				continue redirectLoop
			}
		}
		break redirectLoop
	}
	return err
}
