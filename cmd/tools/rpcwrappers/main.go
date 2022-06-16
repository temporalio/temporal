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
	"go.temporal.io/server/api/adminservice/v1"
)

type (
	service struct {
		name         string
		metricPrefix string
		service      reflect.Type
	}
)

var (
	services = []service{
		service{
			name:         "frontend",
			metricPrefix: "FrontendClient",
			service:      reflect.TypeOf((*workflowservice.WorkflowServiceClient)(nil)),
		},
		service{
			name:         "admin",
			metricPrefix: "AdminClient",
			service:      reflect.TypeOf((*adminservice.AdminServiceClient)(nil)),
		},
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
	largeTimeoutContext = map[string]bool{
		"GetReplicationMessages": true,
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

func writeTemplatedCode(w io.Writer, service service, text string) {
	sType := service.service.Elem()

	text = strings.Replace(text, "{SERVICENAME}", service.name, -1)
	text = strings.Replace(text, "{SERVICETYPE}", sType.String(), -1)
	text = strings.Replace(text, "{SERVICEPKGPATH}", sType.PkgPath(), -1)

	w.Write([]byte(text))
}

func writeTemplatedMethod(w io.Writer, service service, m reflect.Method, text string) {
	mt := m.Type // should look like: func(context.Context, request reqt, opts []grpc.CallOption) (respt, error)

	if !mt.IsVariadic() ||
		mt.NumIn() != 3 ||
		mt.NumOut() != 2 {
		panic("bad method")
	}

	reqType := mt.In(1)
	respType := mt.Out(0)

	longPoll := ""
	if longPollContext[m.Name] {
		longPoll = "LongPoll"
	}
	withLargeTimeout := ""
	if largeTimeoutContext[m.Name] {
		withLargeTimeout = "WithLargeTimeout"
	}

	text = strings.Replace(text, "{METHOD}", m.Name, -1)
	text = strings.Replace(text, "{REQT}", reqType.String(), -1)
	text = strings.Replace(text, "{RESPT}", respType.String(), -1)
	text = strings.Replace(text, "{LONGPOLL}", longPoll, -1)
	text = strings.Replace(text, "{WITHLARGETIMEOUT}", withLargeTimeout, -1)
	text = strings.Replace(text, "{METRICPREFIX}", service.metricPrefix, -1)

	w.Write([]byte(text))
}

func writeTemplatedMethods(w io.Writer, service service, text string) {
	sType := service.service.Elem()
	for n := 0; n < sType.NumMethod(); n++ {
		writeTemplatedMethod(w, service, sType.Method(n), text)
	}
}

func generateClient(w io.Writer, service service) {
	copyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"
	"time"

	"github.com/pborman/uuid"
	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common"
)
`)

	if service.name == "frontend" {
		writeTemplatedCode(w, service, `
const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = 10 * time.Second
	// DefaultLongPollTimeout is the long poll default timeout used to make calls
	DefaultLongPollTimeout = time.Minute * 3
)

var _ {SERVICETYPE} = (*clientImpl)(nil)

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
) {SERVICETYPE} {
	return &clientImpl{
		timeout:         timeout,
		longPollTimeout: longPollTimeout,
		clients:         clients,
	}
}

func (c *clientImpl) createLongPollContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.longPollTimeout)
}
`)
	} else if service.name == "admin" {
		writeTemplatedCode(w, service, `
const (
	// DefaultTimeout is the default timeout used to make calls
	DefaultTimeout = 10 * time.Second
	// DefaultLargeTimeout is the default timeout used to make calls
	DefaultLargeTimeout = time.Minute
)

var _ {SERVICETYPE} = (*clientImpl)(nil)

type clientImpl struct {
	timeout      time.Duration
	largeTimeout time.Duration
	clients      common.ClientCache
}

// NewClient creates a new admin service gRPC client
func NewClient(
	timeout time.Duration,
	largeTimeout time.Duration,
	clients common.ClientCache,
) {SERVICETYPE} {
	return &clientImpl{
		timeout:      timeout,
		largeTimeout: largeTimeout,
		clients:      clients,
	}
}

func (c *clientImpl) createContextWithLargeTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		return context.WithTimeout(context.Background(), c.largeTimeout)
	}
	return context.WithTimeout(parent, c.largeTimeout)
}
`)
	}

	writeTemplatedCode(w, service, `
func (c *clientImpl) createContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, c.timeout)
}

func (c *clientImpl) getRandomClient() ({SERVICETYPE}, error) {
	// generate a random shard key to do load balancing
	key := uuid.New()
	client, err := c.clients.GetClientForKey(key)
	if err != nil {
		return nil, err
	}
	return client.({SERVICETYPE}), nil
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
	ctx, cancel := c.create{LONGPOLL}Context{WITHLARGETIMEOUT}(ctx)
	defer cancel()
	return client.{METHOD}(ctx, request, opts...)
}
`)
}

func generateMetricClient(w io.Writer, service service) {
	copyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"

	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)

var _ {SERVICETYPE} = (*metricClient)(nil)

type metricClient struct {
	client        {SERVICETYPE}
	metricsClient metrics.Client
}

// NewMetricClient creates a new instance of {SERVICETYPE} that emits metrics
func NewMetricClient(client {SERVICETYPE}, metricsClient metrics.Client) {SERVICETYPE} {
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

	c.metricsClient.IncCounter(metrics.{METRICPREFIX}{METHOD}Scope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.{METRICPREFIX}{METHOD}Scope, metrics.ClientLatency)
	resp, err := c.client.{METHOD}(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.{METRICPREFIX}{METHOD}Scope, metrics.ClientFailures)
	}
	return resp, err
}
`)
}

func generateRetryableClient(w io.Writer, service service) {
	copyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"

	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/backoff"
)

var _ {SERVICETYPE} = (*retryableClient)(nil)

type retryableClient struct {
	client      {SERVICETYPE}
	policy      backoff.RetryPolicy
	isRetryable backoff.IsRetryable
}

// NewRetryableClient creates a new instance of {SERVICETYPE} with retry policy
func NewRetryableClient(client {SERVICETYPE}, policy backoff.RetryPolicy, isRetryable backoff.IsRetryable) {SERVICETYPE} {
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
