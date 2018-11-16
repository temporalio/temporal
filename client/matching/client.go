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

package matching

import (
	"context"
	"sync"
	"time"

	m "github.com/uber/cadence/.gen/go/matching"
	"github.com/uber/cadence/.gen/go/matching/matchingserviceclient"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"go.uber.org/yarpc"
)

var _ Client = (*clientImpl)(nil)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = time.Minute
	// DefaultLongPollTimeout is the long poll default timeout used to make calls
	DefaultLongPollTimeout = time.Minute * 2
)

type clientImpl struct {
	resolver        membership.ServiceResolver
	thriftCacheLock sync.RWMutex
	thriftCache     map[string]matchingserviceclient.Interface
	rpcFactory      common.RPCFactory
	timeout         time.Duration
	longPollTimeout time.Duration
}

// NewClient creates a new history service TChannel client
func NewClient(
	d common.RPCFactory,
	monitor membership.Monitor,
	timeout time.Duration,
	longPollTimeout time.Duration,
) (Client, error) {
	sResolver, err := monitor.GetResolver(common.MatchingServiceName)
	if err != nil {
		return nil, err
	}

	client := &clientImpl{
		rpcFactory:      d,
		resolver:        sResolver,
		thriftCache:     make(map[string]matchingserviceclient.Interface),
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
	}
	return client, nil
}

func (c *clientImpl) AddActivityTask(
	ctx context.Context,
	addRequest *m.AddActivityTaskRequest,
	opts ...yarpc.CallOption) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getHostForRequest(addRequest.TaskList.GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddActivityTask(ctx, addRequest, opts...)
}

func (c *clientImpl) AddDecisionTask(
	ctx context.Context,
	addRequest *m.AddDecisionTaskRequest,
	opts ...yarpc.CallOption) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getHostForRequest(addRequest.TaskList.GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.AddDecisionTask(ctx, addRequest, opts...)
}

func (c *clientImpl) PollForActivityTask(
	ctx context.Context,
	pollRequest *m.PollForActivityTaskRequest,
	opts ...yarpc.CallOption) (*workflow.PollForActivityTaskResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getHostForRequest(pollRequest.PollRequest.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollForActivityTask(ctx, pollRequest, opts...)
}

func (c *clientImpl) PollForDecisionTask(
	ctx context.Context,
	pollRequest *m.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption) (*m.PollForDecisionTaskResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getHostForRequest(pollRequest.PollRequest.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(ctx)
	defer cancel()
	return client.PollForDecisionTask(ctx, pollRequest, opts...)
}

func (c *clientImpl) QueryWorkflow(ctx context.Context, queryRequest *m.QueryWorkflowRequest, opts ...yarpc.CallOption) (*workflow.QueryWorkflowResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getHostForRequest(queryRequest.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, queryRequest, opts...)
}

func (c *clientImpl) RespondQueryTaskCompleted(ctx context.Context, request *m.RespondQueryTaskCompletedRequest, opts ...yarpc.CallOption) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getHostForRequest(request.TaskList.GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondQueryTaskCompleted(ctx, request, opts...)
}

func (c *clientImpl) CancelOutstandingPoll(ctx context.Context, request *m.CancelOutstandingPollRequest, opts ...yarpc.CallOption) error {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getHostForRequest(request.TaskList.GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.CancelOutstandingPoll(ctx, request, opts...)
}

func (c *clientImpl) DescribeTaskList(ctx context.Context, request *m.DescribeTaskListRequest, opts ...yarpc.CallOption) (*workflow.DescribeTaskListResponse, error) {
	opts = common.AggregateYarpcOptions(ctx, opts...)
	client, err := c.getHostForRequest(request.DescRequest.TaskList.GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.DescribeTaskList(ctx, request, opts...)
}

func (c *clientImpl) getHostForRequest(key string) (matchingserviceclient.Interface, error) {
	host, err := c.resolver.Lookup(key)
	if err != nil {
		return nil, err
	}
	return c.getThriftClient(host.GetAddress()), nil
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.timeout)
	}
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.longPollTimeout)
	}
	return context.WithTimeout(parent, c.longPollTimeout)
}

func (c *clientImpl) getThriftClient(hostPort string) matchingserviceclient.Interface {
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
			"matching-service-client", common.MatchingServiceName, hostPort)
		client = matchingserviceclient.New(d.ClientConfig(common.MatchingServiceName))
		c.thriftCache[hostPort] = client
	}
	return client
}
