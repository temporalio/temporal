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
		name            string
		clientType      reflect.Type
		clientGenerator func(io.Writer, service)
	}
)

var (
	services = []service{
		{
			name:            "frontend",
			clientType:      reflect.TypeOf((*workflowservice.WorkflowServiceClient)(nil)),
			clientGenerator: generateFrontendOrAdminClient,
		},
		{
			name:            "admin",
			clientType:      reflect.TypeOf((*adminservice.AdminServiceClient)(nil)),
			clientGenerator: generateFrontendOrAdminClient,
		},
		{
			name:            "history",
			clientType:      reflect.TypeOf((*historyservice.HistoryServiceClient)(nil)),
			clientGenerator: generateHistoryClient,
		},
		{
			name:            "matching",
			clientType:      reflect.TypeOf((*matchingservice.MatchingServiceClient)(nil)),
			clientGenerator: generateMatchingClient,
		},
	}

	longPollContext = map[string]bool{
		"client.frontend.ListArchivedWorkflowExecutions": true,
		"client.frontend.PollActivityTaskQueue":          true,
		"client.frontend.PollWorkflowTaskQueue":          true,
		"client.matching.GetTaskQueueUserData":           true,
	}
	largeTimeoutContext = map[string]bool{
		"client.admin.GetReplicationMessages": true,
	}
	ignoreMethod = map[string]bool{
		// TODO stream APIs are not supported. do not generate.
		"client.admin.StreamWorkflowReplicationMessages":            true,
		"metricsClient.admin.StreamWorkflowReplicationMessages":     true,
		"retryableClient.admin.StreamWorkflowReplicationMessages":   true,
		"client.history.StreamWorkflowReplicationMessages":          true,
		"metricsClient.history.StreamWorkflowReplicationMessages":   true,
		"retryableClient.history.StreamWorkflowReplicationMessages": true,

		// these are non-standard implementations. do not generate.
		"client.history.DescribeHistoryHost":                    true,
		"client.history.GetReplicationMessages":                 true,
		"client.history.GetReplicationStatus":                   true,
		"client.history.RecordChildExecutionCompleted":          true,
		"client.history.VerifyChildExecutionCompletionRecorded": true,
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

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func writeTemplatedCode(w io.Writer, service service, text string) {
	panicIfErr(template.Must(template.New("code").Parse(text)).Execute(w, map[string]string{
		"ServiceName":        service.name,
		"ServicePackagePath": service.clientType.Elem().PkgPath(),
	}))
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
		return fmt.Sprintf("shardID := %s", path)
	}
	if path := pathToField(t, "WorkflowId", "request", 4); path != "" {
		return fmt.Sprintf("shardID := c.shardIDFromWorkflowID(request.NamespaceId, %s)", path)
	}
	if path := pathToField(t, "TaskToken", "request", 2); path != "" {
		return fmt.Sprintf(`taskToken, err := c.tokenSerializer.Deserialize(%s)
	if err != nil {
		return nil, err
	}
	shardID := c.shardIDFromWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
`, path)
	}
	// slice needs a tiny bit of extra handling for namespace
	if path := pathToField(t, "TaskInfos", "request", 1); path != "" {
		return fmt.Sprintf(`// All workflow IDs are in the same shard per request
	if len(%s) == 0 {
		return nil, serviceerror.NewInvalidArgument("missing TaskInfos")
	}
	shardID := c.shardIDFromWorkflowID(%s[0].NamespaceId, %s[0].WorkflowId)`, path, path, path)
	}
	panic("I don't know how to get a client from a " + t.String())
}

func makeGetMatchingClient(reqType reflect.Type) string {
	// this magically figures out how to get a MatchingServiceClient from a request
	t := reqType.Elem() // we know it's a pointer

	nsIDPath := pathToField(t, "NamespaceId", "request", 1)
	tqPath, tqField := findNestedField(t, "TaskQueue", "request", 2)

	var tqtPath string
	switch t.Name() {
	case "GetBuildIdTaskQueueMappingRequest":
		// Pick a random node for this request, it's not associated with a specific task queue.
		tqPath = "&taskqueuepb.TaskQueue{Name: fmt.Sprintf(\"not-applicable-%d\", rand.Int())}"
		tqtPath = "enumspb.TASK_QUEUE_TYPE_UNSPECIFIED"
		return fmt.Sprintf("client, err := c.getClientForTaskqueue(%s, %s, %s)", nsIDPath, tqPath, tqtPath)
	case "UpdateTaskQueueUserDataRequest",
		"ReplicateTaskQueueUserDataRequest":
		// Always route these requests to the same matching node by namespace.
		tqPath = "&taskqueuepb.TaskQueue{Name: \"not-applicable\"}"
		tqtPath = "enumspb.TASK_QUEUE_TYPE_UNSPECIFIED"
		return fmt.Sprintf("client, err := c.getClientForTaskqueue(%s, %s, %s)", nsIDPath, tqPath, tqtPath)
	case "GetWorkerBuildIdCompatibilityRequest",
		"UpdateWorkerBuildIdCompatibilityRequest",
		"RespondQueryTaskCompletedRequest",
		"ListTaskQueuePartitionsRequest",
		"ApplyTaskQueueUserDataReplicationEventRequest":
		tqtPath = "enumspb.TASK_QUEUE_TYPE_WORKFLOW"
	default:
		tqtPath = pathToField(t, "TaskQueueType", "request", 2)
	}

	if nsIDPath != "" && tqPath != "" && tqField != nil && tqtPath != "" {
		// Some task queue fields are full messages, some are just strings
		isTaskQueueMessage := tqField.Type == reflect.TypeOf((*taskqueue.TaskQueue)(nil))
		if !isTaskQueueMessage {
			tqPath = fmt.Sprintf("&taskqueuepb.TaskQueue{Name: %s}", tqPath)
		}
		return fmt.Sprintf("client, err := c.getClientForTaskqueue(%s, %s, %s)", nsIDPath, tqPath, tqtPath)
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

	panicIfErr(template.Must(template.New("code").Parse(text)).Execute(w, fields))
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
	ctx, cancel := c.create{{or .LongPoll ""}}Context{{or .WithLargeTimeout ""}}(ctx)
	defer cancel()
	return c.client.{{.Method}}(ctx, request, opts...)
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
	var response {{.ResponseType}}
	op := func(ctx context.Context, client historyservice.HistoryServiceClient) error {
		var err error
		ctx, cancel := c.createContext(ctx)
		defer cancel()
		response, err = client.{{.Method}}(ctx, request, opts...)
		return err
	}
	if err := c.executeWithRedirect(ctx, shardID, op); err != nil {
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
	"fmt"
	"math/rand"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
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
	ctx, cancel := c.create{{or .LongPoll ""}}Context(ctx)
	defer cancel()
	return client.{{.Method}}(ctx, request, opts...)
}
`)
}

func generateMetricClient(w io.Writer, service service) {
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

	metricsHandler, startTime := c.startMetricsRecording(ctx, metrics.{{.MetricPrefix}}{{.Method}}Scope)
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
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
	defer func() {
		panicIfErr(w.Close())
	}()
	if _, err := fmt.Fprintf(w, "%s\n// Code generated by cmd/tools/rpcwrappers. DO NOT EDIT.\n", licenseText); err != nil {
		panic(err)
	}
	f(w, service)
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
	callWithFile(generateMetricClient, svc, "metric_client", licenseText)
	callWithFile(generateRetryableClient, svc, "retryable_client", licenseText)
}
