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
	"flag"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"

	"go.temporal.io/api/workflowservice/v1"
	"golang.org/x/exp/slices"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

type (
	service struct {
		name               string
		clientType         reflect.Type
		clientGenerator    func(io.Writer, service)
		metricGenerator    func(io.Writer, service)
		retryableGenerator func(io.Writer, service)
	}
)

var (
	services = []service{
		service{
			name:               "frontend",
			clientType:         reflect.TypeOf((*workflowservice.WorkflowServiceClient)(nil)),
			clientGenerator:    generateFrontendOrAdminClient,
			metricGenerator:    generateFrontendOrAdminMetricClient,
			retryableGenerator: generateRetryableClient,
		},
		service{
			name:               "admin",
			clientType:         reflect.TypeOf((*adminservice.AdminServiceClient)(nil)),
			clientGenerator:    generateFrontendOrAdminClient,
			metricGenerator:    generateFrontendOrAdminMetricClient,
			retryableGenerator: generateRetryableClient,
		},
		service{
			name:               "history",
			clientType:         reflect.TypeOf((*historyservice.HistoryServiceClient)(nil)),
			clientGenerator:    generateHistoryClient,
			metricGenerator:    generateHistoryOrMatchingMetricClient,
			retryableGenerator: generateRetryableClient,
		},
		service{
			name:               "matching",
			clientType:         reflect.TypeOf((*matchingservice.MatchingServiceClient)(nil)),
			clientGenerator:    generateMatchingClient,
			metricGenerator:    generateHistoryOrMatchingMetricClient,
			retryableGenerator: generateRetryableClient,
		},
	}

	longPollContext = map[string]bool{
		"client.frontend.ListArchivedWorkflowExecutions": true,
		"client.frontend.PollActivityTaskQueue":          true,
		"client.frontend.PollWorkflowTaskQueue":          true,
	}
	largeTimeoutContext = map[string]bool{
		"client.admin.GetReplicationMessages": true,
	}
	ignoreMethod = map[string]bool{
		// these are non-standard implementations. do not generate.
		"client.history.DescribeHistoryHost":    true,
		"client.history.GetReplicationMessages": true,
		"client.history.GetReplicationStatus":   true,
		// these need to pick a partition. too complicated.
		"client.matching.AddActivityTask":       true,
		"client.matching.AddWorkflowTask":       true,
		"client.matching.PollActivityTaskQueue": true,
		"client.matching.PollWorkflowTaskQueue": true,
		"client.matching.QueryWorkflow":         true,
		// these do forwarding stats. too complicated.
		"metricsClient.matching.AddActivityTask":       true,
		"metricsClient.matching.AddWorkflowTask":       true,
		"metricsClient.matching.PollActivityTaskQueue": true,
		"metricsClient.matching.PollWorkflowTaskQueue": true,
		"metricsClient.matching.QueryWorkflow":         true,
	}
)

func writeCopyright(w io.Writer) {
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

// Code generated by cmd/tools/rpcwrappers. DO NOT EDIT.
`)
}

func writeTemplatedCode(w io.Writer, service service, text string) {
	sType := service.clientType.Elem()

	text = strings.Replace(text, "{SERVICENAME}", service.name, -1)
	text = strings.Replace(text, "{SERVICETYPE}", sType.String(), -1)
	text = strings.Replace(text, "{SERVICEPKGPATH}", sType.PkgPath(), -1)

	w.Write([]byte(text))
}

func pathToField(t reflect.Type, name string, path string, maxDepth int) string {
	if t.Kind() != reflect.Struct || maxDepth <= 0 {
		return ""
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Name == name {
			return path + "." + name
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			if try := pathToField(ft.Elem(), name, path+"."+f.Name, maxDepth-1); try != "" {
				return try
			}
		}
	}
	return ""
}

func makeGetHistoryClientMagic(reqType reflect.Type) string {
	// this magically figures out how to get a HistoryServiceClient from a request
	t := reqType.Elem() // we know it's a pointer
	if path := pathToField(t, "ShardId", "request", 1); path != "" {
		return fmt.Sprintf("client, err := c.getClientForShardID(%s)", path)
	}
	if path := pathToField(t, "WorkflowId", "request", 3); path != "" {
		return fmt.Sprintf("client, err := c.getClientForWorkflowID(request.NamespaceId, %s)", path)
	}
	if path := pathToField(t, "TaskToken", "request", 2); path != "" {
		return fmt.Sprintf(`taskToken, err := c.tokenSerializer.Deserialize(%s)
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
`, path)
	}
	// slice needs a tiny bit of extra handling for namespace
	if path := pathToField(t, "TaskInfos", "request", 1); path != "" {
		return fmt.Sprintf(`// All workflow IDs are in the same shard per request
	client, err := c.getClientForWorkflowID(%s[0].NamespaceId, %s[0].WorkflowId)`, path, path)
	}

	panic("I don't know how to get a client from a " + t.String())
}

func makeGetMatchingClientMagic(reqType reflect.Type) string {
	// this magically figures out how to get a MatchingServiceClient from a request
	t := reqType.Elem() // we know it's a pointer
	if path := pathToField(t, "TaskQueue", "request", 2); path != "" {
		return fmt.Sprintf("client, err := c.getClientForTaskqueue(%s.GetName())", path)
	}
	panic("I don't know how to get a client from a " + t.String())
}

func writeTemplatedMethod(w io.Writer, service service, impl string, m reflect.Method, text string) {
	key := fmt.Sprintf("%s.%s.%s", impl, service.name, m.Name)
	if ignoreMethod[key] {
		return
	}

	mt := m.Type // should look like: func(context.Context, request reqType, opts []grpc.CallOption) (respType, error)

	if !mt.IsVariadic() ||
		mt.NumIn() != 3 ||
		mt.NumOut() != 2 ||
		mt.In(0).String() != "context.Context" ||
		mt.Out(1).String() != "error" {
		panic(m.Name + " doesn't look like a grpc handler method")
	}

	reqType := mt.In(1)
	respType := mt.Out(0)

	longPoll := ""
	if longPollContext[key] {
		longPoll = "LongPoll"
	}
	withLargeTimeout := ""
	if largeTimeoutContext[key] {
		withLargeTimeout = "WithLargeTimeout"
	}
	metricPrefix := fmt.Sprintf("%s%sClient", strings.ToUpper(service.name[:1]), service.name[1:])
	getClientMagic := ""
	if impl == "client" {
		if service.name == "history" {
			getClientMagic = makeGetHistoryClientMagic(reqType)
		} else if service.name == "matching" {
			getClientMagic = makeGetMatchingClientMagic(reqType)
		}
	}

	text = strings.Replace(text, "{METHOD}", m.Name, -1)
	text = strings.Replace(text, "{REQT}", reqType.String(), -1)
	text = strings.Replace(text, "{RESPT}", respType.String(), -1)
	text = strings.Replace(text, "{LONGPOLL}", longPoll, -1)
	text = strings.Replace(text, "{WITHLARGETIMEOUT}", withLargeTimeout, -1)
	text = strings.Replace(text, "{METRICPREFIX}", metricPrefix, -1)
	text = strings.Replace(text, "{GETCLIENTMAGIC}", getClientMagic, -1)

	w.Write([]byte(text))
}

func writeTemplatedMethods(w io.Writer, service service, impl string, text string) {
	sType := service.clientType.Elem()
	for n := 0; n < sType.NumMethod(); n++ {
		writeTemplatedMethod(w, service, impl, sType.Method(n), text)
	}
}

func generateFrontendOrAdminClient(w io.Writer, service service) {
	writeCopyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"

	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"
)
`)

	writeTemplatedMethods(w, service, "client", `
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

func generateHistoryClient(w io.Writer, service service) {
	writeCopyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"

	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"
)
`)

	writeTemplatedMethods(w, service, "client", `
func (c *clientImpl) {METHOD}(
	ctx context.Context,
	request {REQT},
	opts ...grpc.CallOption,
) ({RESPT}, error) {
	{GETCLIENTMAGIC}
	if err != nil {
		return nil, err
	}
	var response {RESPT}
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.{METHOD}(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}
`)
	// TODO: some methods call client.{METHOD} directly and do not use executeWithRedirect. should we preserve this?
	// GetDLQReplicationMessages
	// GetDLQMessages
	// PurgeDLQMessages
	// MergeDLQMessages
}

func generateMatchingClient(w io.Writer, service service) {
	writeCopyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"

	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"
)
`)

	writeTemplatedMethods(w, service, "client", `
func (c *clientImpl) {METHOD}(
	ctx context.Context,
	request {REQT},
	opts ...grpc.CallOption,
) ({RESPT}, error) {

	{GETCLIENTMAGIC}
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.{METHOD}(ctx, request, opts...)
}
`)
}

func generateFrontendOrAdminMetricClient(w io.Writer, service service) {
	writeCopyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"

	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)
`)

	writeTemplatedMethods(w, service, "metricsClient", `
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

func generateHistoryOrMatchingMetricClient(w io.Writer, service service) {
	writeCopyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"

	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)
`)

	writeTemplatedMethods(w, service, "metricsClient", `
func (c *metricClient) {METHOD}(
	ctx context.Context,
	request {REQT},
	opts ...grpc.CallOption,
) (_ {RESPT}, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.{METRICPREFIX}{METHOD}Scope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.{METHOD}(ctx, request, opts...)
}
`)
	// TODO: some history methods did not touch metrics. should we preserve this?
	// DescribeHistoryHost
	// RemoveTask
	// CloseShard
	// GetShard
	// RebuildMutableState
	// DescribeMutableState
	// TODO: DeleteWorkflowExecution didn't work like the others in history (the code looked
	// like the frontend/admin client version). should we preserve that?
}

func generateRetryableClient(w io.Writer, service service) {
	writeCopyright(w)

	writeTemplatedCode(w, service, `
package {SERVICENAME}

import (
	"context"

	"{SERVICEPKGPATH}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/backoff"
)
`)

	writeTemplatedMethods(w, service, "retryableClient", `
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
	file, err := os.Create(filename + "_gen.go")
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
	serviceFlag := flag.String("service", "", "which service to generate rpc client wrappers for")
	flag.Parse()

	i := slices.IndexFunc(services, func(s service) bool { return s.name == *serviceFlag })
	if i < 0 {
		panic("unknown service")
	}
	svc := services[i]

	callWithFile(svc.clientGenerator, svc, "client")
	callWithFile(svc.metricGenerator, svc, "metricClient")
	callWithFile(svc.retryableGenerator, svc, "retryableClient")
}
