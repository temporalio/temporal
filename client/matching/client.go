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
	"sync"
	"time"

	"golang.org/x/net/context"

	m "github.com/uber/cadence/.gen/go/matching"
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
	thriftCacheLock sync.RWMutex
	thriftCache     map[string]m.TChanMatchingService
}

// NewClient creates a new history service TChannel client
func NewClient(ch *tchannel.Channel, monitor membership.Monitor) (Client, error) {
	sResolver, err := monitor.GetResolver(common.MatchingServiceName)
	if err != nil {
		return nil, err
	}

	client := &clientImpl{
		connection:  ch,
		resolver:    sResolver,
		thriftCache: make(map[string]m.TChanMatchingService),
	}
	return client, nil
}

func (c *clientImpl) AddActivityTask(context thrift.Context,
	addRequest *m.AddActivityTaskRequest) error {
	client, err := c.getHostForRequest(addRequest.GetTaskList().GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.AddActivityTask(ctx, addRequest)
}

func (c *clientImpl) AddDecisionTask(context thrift.Context,
	addRequest *m.AddDecisionTaskRequest) error {
	client, err := c.getHostForRequest(addRequest.GetTaskList().GetName())
	if err != nil {
		return err
	}
	ctx, cancel := c.createContext(context)
	defer cancel()
	return client.AddDecisionTask(ctx, addRequest)
}

func (c *clientImpl) PollForActivityTask(context thrift.Context,
	pollRequest *m.PollForActivityTaskRequest) (*workflow.PollForActivityTaskResponse, error) {
	client, err := c.getHostForRequest(pollRequest.GetPollRequest().GetTaskList().GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(context)
	defer cancel()
	return client.PollForActivityTask(ctx, pollRequest)
}

func (c *clientImpl) PollForDecisionTask(context thrift.Context,
	pollRequest *m.PollForDecisionTaskRequest) (*m.PollForDecisionTaskResponse, error) {
	client, err := c.getHostForRequest(pollRequest.GetPollRequest().GetTaskList().GetName())
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createLongPollContext(context)
	defer cancel()
	return client.PollForDecisionTask(ctx, pollRequest)
}

func (c *clientImpl) getHostForRequest(key string) (m.TChanMatchingService, error) {
	host, err := c.resolver.Lookup(key)
	if err != nil {
		return nil, err
	}
	return c.getThriftClient(host.GetAddress()), nil
}

func (c *clientImpl) createContext(parent thrift.Context) (thrift.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	timeout := time.Minute * 1
	if parent == nil {
		return thrift.NewContext(timeout)
	}
	builder := tchannel.NewContextBuilder(timeout)
	builder.SetParentContext(parent)
	return builder.Build()
}

func (c *clientImpl) createLongPollContext(parent thrift.Context) (thrift.Context, context.CancelFunc) {
	// TODO: make timeout configurable
	timeout := time.Minute * 2
	if parent == nil {
		return thrift.NewContext(timeout)
	}
	builder := tchannel.NewContextBuilder(timeout)
	builder.SetParentContext(parent)
	return builder.Build()
}

func (c *clientImpl) getThriftClient(hostPort string) m.TChanMatchingService {
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
		tClient := thrift.NewClient(c.connection, common.MatchingServiceName, &thrift.ClientOptions{
			HostPort: hostPort,
		})

		client = m.NewTChanMatchingServiceClient(tClient)
		c.thriftCache[hostPort] = client
	}
	return client
}
