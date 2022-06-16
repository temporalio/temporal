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

package main

import (
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"go.temporal.io/api/workflowservice/v1"
)

type (
	service struct {
		name    string
		service reflect.Type
	}
)

var (
	services = []service{
		service{
			name:    "frontend",
			service: reflect.TypeOf((*workflowservice.WorkflowServiceClient)(nil)),
		},
		// service{
		// 	name:    "admin",
		// 	service: reflect.TypeOf((*adminservice.AdminServiceClient)(nil)),
		// },
		// service{
		// 	name:    "history",
		// 	service: reflect.TypeOf((*historyservice.HistoryServiceClient)(nil)),
		// },
		// service{
		// 	name:    "matching",
		// 	service: reflect.TypeOf((*matchingservice.MatchingServiceClient)(nil)),
		// },
	}

	longPollContext = map[string]bool{
		"ListArchivedWorkflowExecutions": true,
		"PollActivityTaskQueue":          true,
		"PollWorkflowTaskQueue":          true,
	}
)

func copyright(w io.Writer) {
	fmt.Fprintf(w, `// The MIT License
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
`)
}

func writeTemplatedMethod(w io.Writer, m reflect.Method, text string) {
	mt := m.Type // should look like: func(context.Context, request reqt, opts []grpc.CallOption) (respt, error)

	if !mt.IsVariadic() ||
		mt.NumIn() != 3 ||
		mt.NumOut() != 2 {
		panic("bad method")
	}

	reqT := mt.In(1)
	respT := mt.Out(0)

	longPoll := ""
	if longPollContext[m.Name] {
		longPoll = "LongPoll"
	}

	text = strings.Replace(text, "{METHOD}", m.Name, -1)
	text = strings.Replace(text, "{REQT}", reqT.String(), -1)
	text = strings.Replace(text, "{RESPT}", respT.String(), -1)
	text = strings.Replace(text, "{LONGPOLL}", longPoll, -1)

	w.Write([]byte(text))
}

func writeTemplatedMethods(w io.Writer, service service, text string) {
	s := service.service.Elem()
	for n := 0; n < s.NumMethod(); n++ {
		writeTemplatedMethod(w, s.Method(n), text)
	}
}

func generateClient(w io.Writer, service service) {
	copyright(w)

	fmt.Fprintf(w, `
package %s
`, service.name)

	fmt.Fprintf(w, `
import (
	"context"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

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
`)

	writeTemplatedMethods(w, service, `
func (c *clientImpl) {METHOD}(
	ctx context.Context,
	request {REQT},
	opts ...grpc.CallOption,
) ({RESPT}, error) {
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.create{LONGPOLL}Context(ctx)
	defer cancel()
	return client.{METHOD}(ctx, request, opts...)
}
`)
}

func generateMetricClient(w io.Writer, service service) {
	copyright(w)

	fmt.Fprintf(w, `
package %s
`, service.name)

	fmt.Fprintf(w, `
import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)

var _ workflowservice.WorkflowServiceClient = (*metricClient)(nil)

type metricClient struct {
	client        workflowservice.WorkflowServiceClient
	metricsClient metrics.Client
}

// NewMetricClient creates a new instance of workflowservice.WorkflowServiceClient that emits metrics
func NewMetricClient(client workflowservice.WorkflowServiceClient, metricsClient metrics.Client) workflowservice.WorkflowServiceClient {
	return &metricClient{
		client:        client,
		metricsClient: metricsClient,
	}
}
`)

	writeTemplatedMethods(w, service, `
func (c *metricClient) {METHOD}(
	ctx context.Context,
	request {REQT},
	opts ...grpc.CallOption,
) ({RESPT}, error) {

	c.metricsClient.IncCounter(metrics.FrontendClient{METHOD}Scope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClient{METHOD}Scope, metrics.ClientLatency)
	resp, err := c.client.{METHOD}(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClient{METHOD}Scope, metrics.ClientFailures)
	}
	return resp, err
}
`)
}

func generateRetryableClient(w io.Writer, service service) {
	copyright(w)

	fmt.Fprintf(w, `
package %s
`, service.name)

	fmt.Fprintf(w, `
import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/backoff"
)

var _ workflowservice.WorkflowServiceClient = (*retryableClient)(nil)

type retryableClient struct {
	client      workflowservice.WorkflowServiceClient
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of workflowservice.WorkflowServiceClient with retry policy
func NewRetryableClient(client workflowservice.WorkflowServiceClient, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) workflowservice.WorkflowServiceClient {
	return &retryableClient{
		client:      client,
		policy:      policy,
		isRetryable: isRetryable,
	}
}
`)

	writeTemplatedMethods(w, service, `
func (c *retryableClient) {METHOD}(
	ctx context.Context,
	request {REQT},
	opts ...grpc.CallOption,
) ({RESPT}, error) {
	var resp {RESPT}
	op := func() error {
		var err error
		resp, err = c.client.{METHOD}(ctx, request, opts...)
		return err
	}
	err := backoff.Retry(op, c.policy, c.isRetryable)
	return resp, err
}
`)
}

func callWithFile(f func(io.Writer, service), service service, filename string) {
	file, err := os.Create(fmt.Sprintf("client/%s/%s.go", service.name, filename))
	if err != nil {
		panic(err)
	}
	f(file, service)
	err = file.Close()
	if err != nil {
		panic(err)
	}
}

func main() {
	for _, service := range services {
		callWithFile(generateClient, service, "client")
		callWithFile(generateMetricClient, service, "metricClient")
		callWithFile(generateRetryableClient, service, "retryableClient")
	}
}
