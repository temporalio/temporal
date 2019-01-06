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
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyserviceclient"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"go.uber.org/yarpc"
)

var _ Client = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = time.Second * 30
)

type clientImpl struct {
	numberOfShards  int
	tokenSerializer common.TaskTokenSerializer
	timeout         time.Duration
	clients         common.ClientCache
}

// NewClient creates a new history service TChannel client
func NewClient(
	numberOfShards int,
	timeout time.Duration,
	clients common.ClientCache,
) Client {
	return &clientImpl{
		numberOfShards:  numberOfShards,
		tokenSerializer: common.NewJSONTaskTokenSerializer(),
		timeout:         timeout,
		clients:         clients,
	}
}

func (c *clientImpl) StartWorkflowExecution(
	ctx context.Context,
	request *h.StartWorkflowExecutionRequest,
	opts ...yarpc.CallOption) (*workflow.StartWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(*request.StartRequest.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *workflow.StartWorkflowExecutionResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.StartWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) GetMutableState(
	ctx context.Context,
	request *h.GetMutableStateRequest,
	opts ...yarpc.CallOption) (*h.GetMutableStateResponse, error) {
	client, err := c.getClientForWorkflowID(*request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *h.GetMutableStateResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.GetMutableState(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) DescribeHistoryHost(
	ctx context.Context,
	request *workflow.DescribeHistoryHostRequest,
	opts ...yarpc.CallOption) (*workflow.DescribeHistoryHostResponse, error) {

	var err error
	var client historyserviceclient.Interface

	if request.ShardIdForHost != nil {
		client, err = c.getClientForShardID(int(request.GetShardIdForHost()))
	} else if request.ExecutionForHost != nil {
		client, err = c.getClientForWorkflowID(request.ExecutionForHost.GetWorkflowId())
	} else {
		ret, err := c.clients.GetClientForClientKey(request.GetHostAddress())
		if err != nil {
			return nil, err
		}
		client = ret.(historyserviceclient.Interface)
	}
	if err != nil {
		return nil, err
	}

	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *workflow.DescribeHistoryHostResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.DescribeHistoryHost(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) DescribeMutableState(
	ctx context.Context,
	request *h.DescribeMutableStateRequest,
	opts ...yarpc.CallOption) (*h.DescribeMutableStateResponse, error) {
	client, err := c.getClientForWorkflowID(*request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *h.DescribeMutableStateResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.DescribeMutableState(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) ResetStickyTaskList(
	ctx context.Context,
	request *h.ResetStickyTaskListRequest,
	opts ...yarpc.CallOption) (*h.ResetStickyTaskListResponse, error) {
	client, err := c.getClientForWorkflowID(*request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *h.ResetStickyTaskListResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ResetStickyTaskList(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (c *clientImpl) DescribeWorkflowExecution(
	ctx context.Context,
	request *h.DescribeWorkflowExecutionRequest,
	opts ...yarpc.CallOption) (*workflow.DescribeWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(*request.Request.Execution.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *workflow.DescribeWorkflowExecutionResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.DescribeWorkflowExecution(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *h.RecordDecisionTaskStartedResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordDecisionTaskStarted(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *h.RecordActivityTaskStartedResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordActivityTaskStarted(ctx, request, opts...)
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
	opts ...yarpc.CallOption) (*h.RespondDecisionTaskCompletedResponse, error) {
	taskToken, err := c.tokenSerializer.Deserialize(request.CompleteRequest.TaskToken)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(taskToken.WorkflowID)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *h.RespondDecisionTaskCompletedResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RespondDecisionTaskCompleted(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	return response, err
}

func (c *clientImpl) RespondDecisionTaskFailed(
	ctx context.Context,
	request *h.RespondDecisionTaskFailedRequest,
	opts ...yarpc.CallOption) error {
	taskToken, err := c.tokenSerializer.Deserialize(request.FailedRequest.TaskToken)
	if err != nil {
		return err
	}
	client, err := c.getClientForWorkflowID(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RespondDecisionTaskFailed(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RespondActivityTaskCompleted(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RespondActivityTaskFailed(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(taskToken.WorkflowID)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RespondActivityTaskCanceled(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(taskToken.WorkflowID)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *workflow.RecordActivityTaskHeartbeatResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.RecordActivityTaskHeartbeat(ctx, request, opts...)
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
	client, err := c.getClientForWorkflowID(*request.CancelRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RequestCancelWorkflowExecution(ctx, request, opts...)
	}
	return c.executeWithRedirect(ctx, client, op)
}

func (c *clientImpl) SignalWorkflowExecution(
	ctx context.Context,
	request *h.SignalWorkflowExecutionRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getClientForWorkflowID(*request.SignalRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.SignalWorkflowExecution(ctx, request, opts...)
	}
	err = c.executeWithRedirect(ctx, client, op)

	return err
}

func (c *clientImpl) SignalWithStartWorkflowExecution(
	ctx context.Context,
	request *h.SignalWithStartWorkflowExecutionRequest,
	opts ...yarpc.CallOption) (*workflow.StartWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(*request.SignalWithStartRequest.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *workflow.StartWorkflowExecutionResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.SignalWithStartWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}

	return response, err
}

func (c *clientImpl) RemoveSignalMutableState(
	ctx context.Context,
	request *h.RemoveSignalMutableStateRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getClientForWorkflowID(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RemoveSignalMutableState(ctx, request)
	}
	err = c.executeWithRedirect(ctx, client, op)

	return err
}

func (c *clientImpl) TerminateWorkflowExecution(
	ctx context.Context,
	request *h.TerminateWorkflowExecutionRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getClientForWorkflowID(*request.TerminateRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.TerminateWorkflowExecution(ctx, request, opts...)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *h.ResetWorkflowExecutionRequest,
	opts ...yarpc.CallOption) (*workflow.ResetWorkflowExecutionResponse, error) {
	client, err := c.getClientForWorkflowID(*request.ResetRequest.WorkflowExecution.WorkflowId)
	if err != nil {
		return nil, err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	var response *workflow.ResetWorkflowExecutionResponse
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.ResetWorkflowExecution(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, err
}

func (c *clientImpl) ScheduleDecisionTask(
	ctx context.Context,
	request *h.ScheduleDecisionTaskRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getClientForWorkflowID(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.ScheduleDecisionTask(ctx, request, opts...)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) RecordChildExecutionCompleted(
	ctx context.Context,
	request *h.RecordChildExecutionCompletedRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getClientForWorkflowID(*request.WorkflowExecution.WorkflowId)
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.RecordChildExecutionCompleted(ctx, request, opts...)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) ReplicateEvents(
	ctx context.Context,
	request *h.ReplicateEventsRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.ReplicateEvents(ctx, request, opts...)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) ReplicateRawEvents(
	ctx context.Context,
	request *h.ReplicateRawEventsRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getClientForWorkflowID(request.WorkflowExecution.GetWorkflowId())
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.ReplicateRawEvents(ctx, request, opts...)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) SyncShardStatus(
	ctx context.Context,
	request *h.SyncShardStatusRequest,
	opts ...yarpc.CallOption) error {

	// we do not have a workflow ID here, instead, we have something even better
	client, err := c.getClientForShardID(int(request.GetShardId()))
	if err != nil {
		return err
	}

	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.SyncShardStatus(ctx, request, opts...)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) SyncActivity(
	ctx context.Context,
	request *h.SyncActivityRequest,
	opts ...yarpc.CallOption) error {

	client, err := c.getClientForWorkflowID(request.GetWorkflowId())
	if err != nil {
		return err
	}
	opts = common.AggregateYarpcOptions(ctx, opts...)
	op := func(ctx context.Context, client historyserviceclient.Interface) error {
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		return client.SyncActivity(ctx, request, opts...)
	}
	err = c.executeWithRedirect(ctx, client, op)
	return err
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.timeout)
	}
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) getClientForWorkflowID(workflowID string) (historyserviceclient.Interface, error) {
	key := common.WorkflowIDToHistoryShard(workflowID, c.numberOfShards)
	return c.getClientForShardID(key)
}

func (c *clientImpl) getClientForShardID(shardID int) (historyserviceclient.Interface, error) {
	client, err := c.clients.GetClientForKey(string(shardID))
	if err != nil {
		return nil, err
	}
	return client.(historyserviceclient.Interface), nil
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
				ret, err := c.clients.GetClientForClientKey(s.GetOwner())
				if err != nil {
					return err
				}
				client = ret.(historyserviceclient.Interface)
				continue redirectLoop
			}
		}
		break redirectLoop
	}
	return err
}
