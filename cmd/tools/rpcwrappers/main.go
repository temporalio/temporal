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
		"frontend.ListArchivedWorkflowExecutions": true,
		"frontend.PollActivityTaskQueue":          true,
		"frontend.PollWorkflowTaskQueue":          true,
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

func needsLongPollContext(service, method string) bool {
	return longPollContext[service+"."+method]
}

func generateClient(service service, w io.Writer) {
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
`)

	s := service.service.Elem()
	for n := 0; n < s.NumMethod(); n++ {
		m := s.Method(n)
		mt := m.Type // func(context.Context, request reqt, opts []grpc.CallOption) (respt, error)
		if !mt.IsVariadic() ||
			mt.NumIn() != 3 ||
			mt.NumOut() != 2 {
			panic("bad method")
		}
		reqt := mt.In(1)
		respt := mt.Out(0)
		longPoll := ""
		if needsLongPollContext(service.name, m.Name) {
			longPoll = "LongPoll"
		}
		fmt.Fprintf(w, `
func (c *clientImpl) %s(
	ctx context.Context,
	request %s,
	opts ...grpc.CallOption,
) (%s, error) {
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.create%sContext(ctx)
	defer cancel()
	return client.%s(ctx, request, opts...)
}
`,
			m.Name,
			reqt.String(),
			respt.String(),
			longPoll,
			m.Name,
		)
	}

	fmt.Fprintf(w, `
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
}

func generateMetricClient(service service, w io.Writer) {
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

	s := service.service.Elem()
	for n := 0; n < s.NumMethod(); n++ {
		m := s.Method(n)
		mt := m.Type // func(context.Context, request reqt, opts []grpc.CallOption) (respt, error)
		if !mt.IsVariadic() ||
			mt.NumIn() != 3 ||
			mt.NumOut() != 2 {
			panic("bad method")
		}
		reqt := mt.In(1)
		respt := mt.Out(0)
		fmt.Fprintf(w, `
func (c *metricClient) %s(
	ctx context.Context,
	request %s,
	opts ...grpc.CallOption,
) (%s, error) {

	c.metricsClient.IncCounter(metrics.FrontendClient%sScope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.FrontendClient%sScope, metrics.ClientLatency)
	resp, err := c.client.%s(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.FrontendClient%sScope, metrics.ClientFailures)
	}
	return resp, err
}
`,
			m.Name,
			reqt.String(),
			respt.String(),
			m.Name,
			m.Name,
			m.Name,
			m.Name,
		)
	}
}

func generateRetryableClient(service service, w io.Writer) {
}

func callWithFile(f func(service, io.Writer), service service, filename string) {
	file, err := os.Create(fmt.Sprintf("client/%s/%s.go", service.name, filename))
	if err != nil {
		panic(err)
	}
	f(service, file)
	err = file.Close()
	if err != nil {
		panic(err)
	}
}

func main() {
	for _, service := range services {
		callWithFile(generateClient, service, "client")
		callWithFile(generateMetricClient, service, "metricClient")
	}
}
