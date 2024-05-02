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
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"golang.org/x/exp/slices"
)

type (
	service struct {
		name            string
		clientType      reflect.Type
		clientGenerator func(io.Writer, service)
	}

	fieldWithPath struct {
		field *reflect.StructField
		path  string
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
		"client.matching.ListNexusEndpoints":             true,
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
		"client.history.DescribeHistoryHost":    true,
		"client.history.GetReplicationMessages": true,
		"client.history.GetReplicationStatus":   true,
		"client.history.GetDLQTasks":            true,
		"client.history.DeleteDLQTasks":         true,
		"client.history.ListQueues":             true,
		"client.history.ListTasks":              true,
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
	// Fields to ignore when looking for the routing fields in a request object.
	ignoreField = map[string]bool{
		// this is the workflow that sent a signal
		"SignalWorkflowExecutionRequest.ExternalWorkflowExecution": true,
		// this is the workflow that sent a cancel request
		"RequestCancelWorkflowExecutionRequest.ExternalWorkflowExecution": true,
		// this is the workflow that sent a terminate
		"TerminateWorkflowExecutionRequest.ExternalWorkflowExecution": true,
		// this is the parent for starting a child workflow
		"StartWorkflowExecutionRequest.ParentExecutionInfo": true,
		// this is the root for starting a child workflow
		"StartWorkflowExecutionRequest.RootExecutionInfo": true,
		// these get routed to the parent
		"RecordChildExecutionCompletedRequest.ChildExecution":          true,
		"VerifyChildExecutionCompletionRecordedRequest.ChildExecution": true,
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

func findNestedField(t reflect.Type, name string, path string, maxDepth int) []fieldWithPath {
	if t.Kind() != reflect.Struct || maxDepth <= 0 {
		return nil
	}
	var out []fieldWithPath
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if ignoreField[t.Name()+"."+f.Name] {
			continue
		}
		if f.Name == name {
			out = append(out, fieldWithPath{field: &f, path: path + ".Get" + name + "()"})
		}
		ft := f.Type
		if ft.Kind() == reflect.Pointer {
			out = append(out, findNestedField(ft.Elem(), name, path+".Get"+f.Name+"()", maxDepth-1)...)
		}
	}
	return out
}

func findOneNestedField(t reflect.Type, name string, path string, maxDepth int) fieldWithPath {
	fields := findNestedField(t, name, path, maxDepth)
	if len(fields) == 0 {
		panic(fmt.Sprintf("Couldn't find %s in %s", name, t))
	} else if len(fields) > 1 {
		panic(fmt.Sprintf("Found more than one %s in %s (%v)", name, t, fields))
	}
	return fields[0]
}

func makeGetHistoryClient(reqType reflect.Type) string {
	// this magically figures out how to get a HistoryServiceClient from a request
	t := reqType.Elem() // we know it's a pointer

	shardIdField := findNestedField(t, "ShardId", "request", 1)
	workflowIdField := findNestedField(t, "WorkflowId", "request", 4)
	taskTokenField := findNestedField(t, "TaskToken", "request", 2)
	namespaceIdField := findNestedField(t, "NamespaceId", "request", 2)
	taskInfosField := findNestedField(t, "TaskInfos", "request", 1)

	found := len(shardIdField) + len(workflowIdField) + len(taskTokenField) + len(taskInfosField)
	if found < 1 {
		panic(fmt.Sprintf("Found no routing fields in %s", t))
	} else if found > 1 {
		panic(fmt.Sprintf("Found more than one routing field in %s (%v, %v, %v, %v)",
			t, shardIdField, workflowIdField, taskTokenField, taskInfosField))
	}

	switch {
	case len(shardIdField) == 1:
		return fmt.Sprintf("shardID := %s", shardIdField[0].path)
	case len(workflowIdField) == 1:
		if len(namespaceIdField) == 1 {
			return fmt.Sprintf("shardID := c.shardIDFromWorkflowID(%s, %s)", namespaceIdField[0].path, workflowIdField[0].path)
		} else if len(namespaceIdField) == 0 {
			panic(fmt.Sprintf("expected at least one namespace ID field in request with nesting of 2 in %s", t))
		} else {
			// There's more than one, assume there's a top level one (e.g.
			// historyservice.GetWorkflowExecutionRawHistoryRequest)
			return fmt.Sprintf("shardID := c.shardIDFromWorkflowID(request.NamespaceId, %s)", workflowIdField[0].path)
		}
	case len(taskTokenField) == 1:
		return fmt.Sprintf(`taskToken, err := c.tokenSerializer.Deserialize(%s)
	if err != nil {
		return nil, err
	}
	shardID := c.shardIDFromWorkflowID(request.NamespaceId, taskToken.GetWorkflowId())
`, taskTokenField[0].path)
	case len(taskInfosField) == 1:
		p := taskInfosField[0].path
		// slice needs a tiny bit of extra handling for namespace
		return fmt.Sprintf(`// All workflow IDs are in the same shard per request
	if len(%s) == 0 {
		return nil, serviceerror.NewInvalidArgument("missing TaskInfos")
	}
	shardID := c.shardIDFromWorkflowID(%s[0].NamespaceId, %s[0].WorkflowId)`, p, p, p)
	default:
		panic("not reached")
	}
}

func makeGetMatchingClient(reqType reflect.Type) string {
	// this magically figures out how to get a MatchingServiceClient from a request
	t := reqType.Elem() // we know it's a pointer

	var nsID, tq, tqt fieldWithPath

	switch t.Name() {
	case "GetBuildIdTaskQueueMappingRequest":
		// Pick a random node for this request, it's not associated with a specific task queue.
		tq = fieldWithPath{path: "fmt.Sprintf(\"not-applicable-%d\", rand.Int())"}
		tqt = fieldWithPath{path: "enumspb.TASK_QUEUE_TYPE_UNSPECIFIED"}
		nsID = findOneNestedField(t, "NamespaceId", "request", 1)
	case "UpdateTaskQueueUserDataRequest",
		"ReplicateTaskQueueUserDataRequest":
		// Always route these requests to the same matching node by namespace.
		tq = fieldWithPath{path: "\"not-applicable\""}
		tqt = fieldWithPath{path: "enumspb.TASK_QUEUE_TYPE_UNSPECIFIED"}
		nsID = findOneNestedField(t, "NamespaceId", "request", 1)
	case "GetWorkerBuildIdCompatibilityRequest",
		"UpdateWorkerBuildIdCompatibilityRequest",
		"RespondQueryTaskCompletedRequest",
		"ListTaskQueuePartitionsRequest",
		"ApplyTaskQueueUserDataReplicationEventRequest",
		"GetWorkerVersioningRulesRequest",
		"UpdateWorkerVersioningRulesRequest":
		tq = findOneNestedField(t, "TaskQueue", "request", 2)
		tqt = fieldWithPath{path: "enumspb.TASK_QUEUE_TYPE_WORKFLOW"}
		nsID = findOneNestedField(t, "NamespaceId", "request", 1)
	case "DispatchNexusTaskRequest",
		"PollNexusTaskQueueRequest",
		"RespondNexusTaskCompletedRequest",
		"RespondNexusTaskFailedRequest":
		tq = findOneNestedField(t, "TaskQueue", "request", 2)
		tqt = fieldWithPath{path: "enumspb.TASK_QUEUE_TYPE_NEXUS"}
		nsID = findOneNestedField(t, "NamespaceId", "request", 1)
	case "CreateNexusEndpointRequest",
		"UpdateNexusEndpointRequest",
		"ListNexusEndpointsRequest",
		"DeleteNexusEndpointRequest":
		// Always route these requests to the same matching node for all namespaces.
		tq = fieldWithPath{path: "\"not-applicable\""}
		tqt = fieldWithPath{path: "enumspb.TASK_QUEUE_TYPE_UNSPECIFIED"}
		nsID = fieldWithPath{path: `"not-applicable"`}
	default:
		tq = findOneNestedField(t, "TaskQueue", "request", 2)
		tqt = findOneNestedField(t, "TaskQueueType", "request", 2)
		nsID = findOneNestedField(t, "NamespaceId", "request", 1)
	}

	if nsID.path != "" && tq.path != "" && tqt.path != "" {
		partitionMaker := fmt.Sprintf("tqid.PartitionFromProto(%s, %s, %s)", tq.path, nsID.path, tqt.path)
		// Some task queue fields are full messages, some are just strings
		isTaskQueueMessage := tq.field != nil && tq.field.Type == reflect.TypeOf((*taskqueue.TaskQueue)(nil))
		if !isTaskQueueMessage {
			partitionMaker = fmt.Sprintf("tqid.NormalPartitionFromRpcName(%s, %s, %s)", tq.path, nsID.path, tqt.path)
		}

		return fmt.Sprintf(
			`p, err := %s
	if err != nil {
		return nil, err
	}
	client, err := c.getClientForTaskQueuePartition(p)`,
			partitionMaker)
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
	"{{.ServicePackagePath}}"
	"go.temporal.io/server/common/tqid"
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
)
`)

	writeTemplatedMethods(w, service, "metricsClient", `
func (c *metricClient) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) (_ {{.ResponseType}}, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "{{.MetricPrefix}}{{.Method}}")
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
