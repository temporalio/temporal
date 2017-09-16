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

type clientImpl struct {
	resolver        membership.ServiceResolver
	thriftCacheLock sync.RWMutex
	thriftCache     map[string]matchingserviceclient.Interface
	rpcFactory      common.RPCFactory
}

// NewClient creates a new history service TChannel client
func NewClient(d common.RPCFactory, monitor membership.Monitor) (Client, error) {
	sResolver, err := monitor.GetResolver(common.MatchingServiceName)
	if err != nil {
		return nil, err
	}

	client := &clientImpl{
		rpcFactory:  d,
		resolver:    sResolver,
		thriftCache: make(map[string]matchingserviceclient.Interface),
	}
	return client, nil
}

func (c *clientImpl) AddActivityTask(
	context context.Context,
	addRequest *m.AddActivityTaskRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getHostForRequest(*addRequest.TaskList.Name)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.AddActivityTask(ctx, addRequest)
}

func (c *clientImpl) AddDecisionTask(
	context context.Context,
	addRequest *m.AddDecisionTaskRequest,
	opts ...yarpc.CallOption) error {
	client, err := c.getHostForRequest(*addRequest.TaskList.Name)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.AddDecisionTask(ctx, addRequest)
}

func (c *clientImpl) PollForActivityTask(
	context context.Context,
	pollRequest *m.PollForActivityTaskRequest,
	opts ...yarpc.CallOption) (*workflow.PollForActivityTaskResponse, error) {
	client, err := c.getHostForRequest(*pollRequest.PollRequest.TaskList.Name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(context)
	defer cancel()
	return client.PollForActivityTask(ctx, pollRequest)
}

func (c *clientImpl) PollForDecisionTask(
	context context.Context,
	pollRequest *m.PollForDecisionTaskRequest,
	opts ...yarpc.CallOption) (*m.PollForDecisionTaskResponse, error) {
	client, err := c.getHostForRequest(*pollRequest.PollRequest.TaskList.Name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(context)
	defer cancel()
	return client.PollForDecisionTask(ctx, pollRequest)
}

func (c *clientImpl) QueryWorkflow(ctx context.Context, queryRequest *m.QueryWorkflowRequest, opts ...yarpc.CallOption) (*workflow.QueryWorkflowResponse, error) {
	client, err := c.getHostForRequest(*queryRequest.TaskList.Name)
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.QueryWorkflow(ctx, queryRequest)
}

func (c *clientImpl) RespondQueryTaskCompleted(ctx context.Context, request *m.RespondQueryTaskCompletedRequest, opts ...yarpc.CallOption) error {
	client, err := c.getHostForRequest(*request.TaskList.Name)
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.RespondQueryTaskCompleted(ctx, request)
}

func (c *clientImpl) getHostForRequest(key string) (matchingserviceclient.Interface, error) {
	host, err := c.resolver.Lookup(key)
	if err != nil {
		return nil, err
	}
	return c.getThriftClient(host.GetAddress()), nil
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	timeout := time.Minute * 1
	if parent == nil {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithTimeout(parent, timeout)
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	timeout := time.Minute * 2
	if parent == nil {
		return context.WithTimeout(context.Background(), timeout)
	}
	return context.WithTimeout(parent, timeout)
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
