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
	"text/template"

	"go.temporal.io/api/taskqueue/v1"
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

func writeTemplatedCode(w io.Writer, service service, text string) {
	t := template.Must(template.New("code").Parse(text))
	t.Execute(w, map[string]string{
		"ServiceName":        service.name,
		"ServicePackagePath": service.clientType.Elem().PkgPath(),
	})
}

func pathToField(t reflect.Type, name string, path string, maxDepth int) string {
	p, _ := findNestedField(t, name, path, maxDepth)
	return p
}

func findNestedField(t reflect.Type, name string, path string, maxDepth int) (string, *reflect.StructField) {
	if t.Kind() != reflect.Struct || maxDepth <= 0 {
		return "", nil
	}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Name == name {
			return path + ".Get" + name + "()", &f
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			if path, try := findNestedField(ft.Elem(), name, path+".Get"+f.Name+"()", maxDepth-1); try != nil {
				return path, try
			}
		}
	}
	return "", nil
}

func makeGetHistoryClient(reqType reflect.Type) string {
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
	if len(%s) == 0 {
		return nil, serviceerror.NewInvalidArgument("missing TaskInfos")
	}
	client, err := c.getClientForWorkflowID(%s[0].NamespaceId, %s[0].WorkflowId)`, path, path, path)
	}
	panic("I don't know how to get a client from a " + t.String())
}

func makeGetMatchingClient(reqType reflect.Type) string {
	// this magically figures out how to get a MatchingServiceClient from a request
	t := reqType.Elem() // we know it's a pointer
	if path, tqField := findNestedField(t, "TaskQueue", "request", 2); path != "" {
		// Some task queue fields are full messages, some are just strings
		isTaskQueueMessage := tqField.Type == reflect.TypeOf((*taskqueue.TaskQueue)(nil))
		if isTaskQueueMessage {
			path += ".GetName()"
		}
		return fmt.Sprintf("client, err := c.getClientForTaskqueue(%s)", path)
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

	fields := map[string]string{
		"Method":       m.Name,
		"RequestType":  reqType.String(),
		"ResponseType": respType.String(),
		"MetricPrefix": fmt.Sprintf("%s%sClient", strings.ToUpper(service.name[:1]), service.name[1:]),
	}
	if longPollContext[key] {
		fields["LongPoll"] = "LongPoll"
	}
	if largeTimeoutContext[key] {
		fields["WithLargeTimeout"] = "WithLargeTimeout"
	}
	if impl == "client" {
		if service.name == "history" {
			fields["GetClient"] = makeGetHistoryClient(reqType)
		} else if service.name == "matching" {
			fields["GetClient"] = makeGetMatchingClient(reqType)
		}
	}

	t := template.Must(template.New("code").Parse(text))
	t.Execute(w, fields)
}

func writeTemplatedMethods(w io.Writer, service service, impl string, text string) {
	sType := service.clientType.Elem()
	for n := 0; n < sType.NumMethod(); n++ {
		writeTemplatedMethod(w, service, impl, sType.Method(n), text)
	}
}

func generateFrontendOrAdminClient(w io.Writer, service service) {
	writeTemplatedCode(w, service, `
package {{.ServiceName}}

import (
	"context"

	"{{.ServicePackagePath}}"
	"google.golang.org/grpc"
)
`)

	writeTemplatedMethods(w, service, "client", `
func (c *clientImpl) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) ({{.ResponseType}}, error) {
	client, err := c.getRandomClient()
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.create{{or .LongPoll ""}}Context{{or .WithLargeTimeout ""}}(ctx)
	defer cancel()
	return client.{{.Method}}(ctx, request, opts...)
}
`)
}

func generateHistoryClient(w io.Writer, service service) {
	writeTemplatedCode(w, service, `
package {{.ServiceName}}

import (
	"context"

	"go.temporal.io/api/serviceerror"
	"{{.ServicePackagePath}}"
	"google.golang.org/grpc"
)
`)

	writeTemplatedMethods(w, service, "client", `
func (c *clientImpl) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) ({{.ResponseType}}, error) {
	{{.GetClient}}
	if err != nil {
		return nil, err
	}
	var response {{.ResponseType}}
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.{{.Method}}(ctx, request, opts...)
		return err
	}
	err = c.executeWithRedirect(ctx, client, op)
	if err != nil {
		return nil, err
	}
	return response, nil
}
`)
	// TODO: some methods call client.{{.Method}} directly and do not use executeWithRedirect. should we preserve this?
	// GetDLQReplicationMessages
	// GetDLQMessages
	// PurgeDLQMessages
	// MergeDLQMessages
}

func generateMatchingClient(w io.Writer, service service) {
	writeTemplatedCode(w, service, `
package {{.ServiceName}}

import (
	"context"

	"{{.ServicePackagePath}}"
	"google.golang.org/grpc"
)
`)

	writeTemplatedMethods(w, service, "client", `
func (c *clientImpl) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) ({{.ResponseType}}, error) {

	{{.GetClient}}
	if err != nil {
		return nil, err
	}
	ctx, cancel := c.createContext(ctx)
	defer cancel()
	return client.{{.Method}}(ctx, request, opts...)
}
`)
}

func generateFrontendOrAdminMetricClient(w io.Writer, service service) {
	writeTemplatedCode(w, service, `
package {{.ServiceName}}

import (
	"context"

	"{{.ServicePackagePath}}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)
`)

	writeTemplatedMethods(w, service, "metricsClient", `
func (c *metricClient) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) ({{.ResponseType}}, error) {

	c.metricsClient.IncCounter(metrics.{{.MetricPrefix}}{{.Method}}Scope, metrics.ClientRequests)

	sw := c.metricsClient.StartTimer(metrics.{{.MetricPrefix}}{{.Method}}Scope, metrics.ClientLatency)
	resp, err := c.client.{{.Method}}(ctx, request, opts...)
	sw.Stop()

	if err != nil {
		c.metricsClient.IncCounter(metrics.{{.MetricPrefix}}{{.Method}}Scope, metrics.ClientFailures)
	}
	return resp, err
}
`)
}

func generateHistoryOrMatchingMetricClient(w io.Writer, service service) {
	writeTemplatedCode(w, service, `
package {{.ServiceName}}

import (
	"context"

	"{{.ServicePackagePath}}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/metrics"
)
`)

	writeTemplatedMethods(w, service, "metricsClient", `
func (c *metricClient) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) (_ {{.ResponseType}}, retError error) {

	scope, stopwatch := c.startMetricsRecording(metrics.{{.MetricPrefix}}{{.Method}}Scope)
	defer func() {
		c.finishMetricsRecording(scope, stopwatch, retError)
	}()

	return c.client.{{.Method}}(ctx, request, opts...)
}
`)
}

func generateRetryableClient(w io.Writer, service service) {
	writeTemplatedCode(w, service, `
package {{.ServiceName}}

import (
	"context"

	"{{.ServicePackagePath}}"
	"google.golang.org/grpc"

	"go.temporal.io/server/common/backoff"
)
`)

	writeTemplatedMethods(w, service, "retryableClient", `
func (c *retryableClient) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) ({{.ResponseType}}, error) {
	var resp {{.ResponseType}}
	op := func(ctx context.Context) error {
		var err error
		resp, err = c.client.{{.Method}}(ctx, request, opts...)
		return err
	}
	err := backoff.ThrottleRetryContext(ctx, op, c.policy, c.isRetryable)
	return resp, err
}
`)
}

func callWithFile(f func(io.Writer, service), service service, filename string, licenseText string) {
	w, err := os.Create(filename + "_gen.go")
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(w, "%s\n// Code generated by cmd/tools/rpcwrappers. DO NOT EDIT.\n", licenseText)
	f(w, service)
	err = w.Close()
	if err != nil {
		panic(err)
	}
}

func readLicenseFile(path string) string {
	text, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	var lines []string
	for _, line := range strings.Split(string(text), "\n") {
		lines = append(lines, strings.TrimRight("// "+line, " "))
	}
	return strings.Join(lines, "\n") + "\n"
}

func main() {
	serviceFlag := flag.String("service", "", "which service to generate rpc client wrappers for")
	licenseFlag := flag.String("licence_file", "../../LICENSE", "path to license to copy into header")
	flag.Parse()

	i := slices.IndexFunc(services, func(s service) bool { return s.name == *serviceFlag })
	if i < 0 {
		panic("unknown service")
	}
	svc := services[i]

	licenseText := readLicenseFile(*licenseFlag)

	callWithFile(svc.clientGenerator, svc, "client", licenseText)
	callWithFile(svc.metricGenerator, svc, "metric_client", licenseText)
	callWithFile(svc.retryableGenerator, svc, "retryable_client", licenseText)
}
