// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

// Generates all three generated files in this package:
//go:generate go run ../../cmd/tools/rpcwrappers -service frontend

package frontend

import (
	"context"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common"
)

const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = 10 * time.Second
	// DefaultLongPollTimeout is the long poll default timeout used to make calls
	DefaultLongPollTimeout = time.Minute * 3
)

var _ workflowservice.WorkflowServiceClient = (*clientImpl)(nil)

type clientImpl struct {
	timeout         time.Duration
	longPollTimeout time.Duration
	clients         common.ClientCache
}

// NewClient creates a new frontend service gRPC client
func NewClient(
	timeout time.Duration,
	longPollTimeout time.Duration,
	clients common.ClientCache,
) workflowservice.WorkflowServiceClient {
	return &clientImpl{
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
		clients:         clients,
	}
}

func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.longPollTimeout)
}

func (c *clientImpl) getRandomClient() (workflowservice.WorkflowServiceClient, error) {
	// generate a random shard key to do load balancing
	key := uuid.New()
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}

	return client.(workflowservice.WorkflowServiceClient), nil
}
