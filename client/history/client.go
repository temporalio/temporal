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
	"context"
	"sync"
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyserviceclient"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"go.uber.org/yarpc"
)

var _ Client = (*clientImpl)(nil)

type clientImpl struct {
	resolver        membership.ServiceResolver
	tokenSerializer common.TaskTokenSerializer
	numberOfShards  int
	// TODO: consider refactor thriftCache into a separate struct
	thriftCacheLock sync.RWMutex
	thriftCache     map[string]historyserviceclient.Interface
	rpcFactory      common.RPCFactory
}

// NewClient creates a new history service TChannel client
func NewClient(d common.RPCFactory, monitor membership.Monitor, numberOfShards int) (Client, error) {
	sResolver, err := monitor.GetResolver(common.HistoryServiceName)
	if err != nil {
		return nil, err
	}

	client := &clientImpl{
		rpcFactory:      d,
		resolver:        sResolver,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		numberOfShards:  numberOfShards,
		thriftCache:     make(map[string]historyserviceclient.Interface),
	}
	return client, nil
}

func (c *clientImpl) StartWorkflowExecution(
	ctx context.Context,
	request *h.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption) (*workflow.StartWorkflowExecutionResponse, error) {
	client, err := c.getHostForRequest(*request.StartRequest.WorkflowId)
	if err != nil {
		return nil, err
	}
	var response *workflow.StartWorkflowExecutionResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.StartWorkflowExecution(ctx, request)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) GetWorkflowExecutionNextEventID(
	ctx context.Context,
	request *h.GetWorkflowExecutionNextEventIDRequest,
	opts ...yarpc.CallOption) (*h.GetWorkflowExecutionNextEventIDResponse, error) {
	client, err := c.getHostForRequest(*request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}
	var response *h.GetWorkflowExecutionNextEventIDResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.GetWorkflowExecutionNextEventID(ctx, request)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RecordDecisionTaskStarted(
	ctx context.Context,
	request *h.RecordDecisionTaskStartedRequest,
	opts ...yarpc.CallOption) (*h.RecordDecisionTaskStartedResponse, error) {
	client, err := c.getHostForRequest(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	var response *h.RecordDecisionTaskStartedResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordDecisionTaskStarted(ctx, request)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RecordActivityTaskStarted(
	ctx context.Context,
	request *h.RecordActivityTaskStartedRequest,
	opts ...yarpc.CallOption) (*h.RecordActivityTaskStartedResponse, error) {
	client, err := c.getHostForRequest(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	var response *h.RecordActivityTaskStartedResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordActivityTaskStarted(ctx, request)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RespondDecisionTaskCompleted(
	ctx context.Context,
	request *h.RespondDecisionTaskCompletedRequest,
	opts ...yarpc.CallOption) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.CompleteRequest.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RespondDecisionTaskCompleted(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) RespondActivityTaskCompleted(
	ctx context.Context,
	request *h.RespondActivityTaskCompletedRequest,
	opts ...yarpc.CallOption) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.CompleteRequest.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RespondActivityTaskCompleted(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) RespondActivityTaskFailed(
	ctx context.Context,
	request *h.RespondActivityTaskFailedRequest,
	opts ...yarpc.CallOption) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.FailedRequest.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RespondActivityTaskFailed(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) RespondActivityTaskCanceled(
	ctx context.Context,
	request *h.RespondActivityTaskCanceledRequest,
	opts ...yarpc.CallOption) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.CancelRequest.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RespondActivityTaskCanceled(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) RecordActivityTaskHeartbeat(
	ctx context.Context,
	request *h.RecordActivityTaskHeartbeatRequest,
	opts ...yarpc.CallOption) (*workflow.RecordActivityTaskHeartbeatResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.HeartbeatRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getHostForRequest(taskToken.WorkflowID)
	if err != nil {
		return nil, err
	}
	var response *workflow.RecordActivityTaskHeartbeatResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordActivityTaskHeartbeat(ctx, request)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) RequestCancelWorkflowExecution(
	ctx context.Context,
	request *h.RequestCancelWorkflowExecutionRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getHostForRequest(*request.CancelRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RequestCancelWorkflowExecution(ctx, request)
	}
	return c.executeWithRedirect(ctx, client, op)
}

func (c *clientImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *h.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getHostForRequest(*request.SignalRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.SignalWorkflowExecution(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)

	return err
}

func (c *clientImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *h.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getHostForRequest(*request.TerminateRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.TerminateWorkflowExecution(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) ScheduleDecisionTask(
	ctx context.Context,
	request *h.ScheduleDecisionTaskRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getHostForRequest(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.ScheduleDecisionTask(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) RecordChildExecutionCompleted(
	ctx context.Context,
	request *h.RecordChildExecutionCompletedRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getHostForRequest(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RecordChildExecutionCompleted(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) getHostForRequest(workflowID string) (historyserviceclient.Interface, error) {
	key := common.WorkflowIDToHistoryShard(workflowID, c.numberOfShards)
	host, err := c.resolver.Lookup(string(key))
	if err != nil {
		return nil, err
	}

	return c.getThriftClient(host.GetAddress()), nil
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	timeout := time.Second * 30
	if parent == nil {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithTimeout(parent, timeout)
}

func (c *clientImpl) getThriftClient(hostPort string) historyserviceclient.Interface {
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
		d := c.rpcFactory.CreateDispatcherForOutbound(
			"history-service-client", common.HistoryServiceName, hostPort)
		client = historyserviceclient.New(d.ClientConfig(common.HistoryServiceName))
		c.thriftCache[hostPort] = client
	}
	return client
}

func (c *clientImpl) executeWithRedirect(ctx context.Context, client historyserviceclient.Interface,
	op func(ctx context.Context, client historyserviceclient.Interface) error) error {
	var err error
	if ctx == nil {
		ctx = context.Background()
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
				client = c.getThriftClient(*s.Owner)
				continue redirectLoop
			}
		}
		break redirectLoop
	}
	return err
}
