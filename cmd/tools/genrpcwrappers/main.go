package main

import (
	"cmp"
	"flag"
	"fmt"
	"io"
	"log"
	"reflect"
	"slices"
	"strings"

	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/cmd/tools/codegen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

type (
	service struct {
		name            string
		clientType      reflect.Type
		clientGenerator func(io.Writer, service) error
	}

	fieldWithPath struct {
		field *reflect.StructField
		path  string
	}
)

func (f fieldWithPath) found() bool {
	return f.path != ""
}

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
	longPollRetryPolicy = map[string]string{
		"retryableClient.matching.PollWorkflowTaskQueue": "pollPolicy",
		"retryableClient.matching.PollActivityTaskQueue": "pollPolicy",
		"retryableClient.matching.PollNexusTaskQueue":    "pollPolicy",
	}
	ignoreMethod = map[string]bool{
		// TODO stream APIs are not supported. do not generate.
		"client.admin.StreamWorkflowReplicationMessages":          true,
		"metricsClient.admin.StreamWorkflowReplicationMessages":   true,
		"retryableClient.admin.StreamWorkflowReplicationMessages": true,
		// TODO(bergundy): Allow specifying custom routing for streaming messages.
		"client.history.StreamWorkflowReplicationMessages":          true,
		"metricsClient.history.StreamWorkflowReplicationMessages":   true,
		"retryableClient.history.StreamWorkflowReplicationMessages": true,

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

var historyRoutingProtoExtension = func() protoreflect.ExtensionType {
	ext, err := protoregistry.GlobalTypes.FindExtensionByName("temporal.server.api.historyservice.v1.routing")
	if err != nil {
		log.Fatalf("Error finding extension: %s", err)
	}
	return ext
}()

func writeTemplatedCode(w io.Writer, service service, tmpl string) {
	codegen.FatalIfErr(codegen.GenerateTemplateToWriter(tmpl, map[string]string{
		"ServiceName":        service.name,
		"ServicePackagePath": service.clientType.Elem().PkgPath(),
	}, w))
}

func verifyFieldExists(t reflect.Type, path string) {
	pathPrefix := t.String()
	parts := strings.Split(path, ".")
	for i, part := range parts {
		if t.Kind() != reflect.Struct {
			codegen.Fatalf("%s is not a struct", pathPrefix)
		}
		fieldName := codegen.SnakeCaseToPascalCase(part)
		f, ok := t.FieldByName(fieldName)
		if !ok {
			codegen.Fatalf("%s has no field named %s", pathPrefix, fieldName)
		}
		if i == len(parts)-1 {
			return
		}
		ft := f.Type
		if ft.Kind() != reflect.Pointer {
			codegen.Fatalf("%s.%s is not a struct pointer", pathPrefix, fieldName)
		}
		t = ft.Elem()
		pathPrefix += "." + fieldName
	}
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
		codegen.Fatalf("couldn't find %s in %s", name, t)
	} else if len(fields) > 1 {
		codegen.Fatalf("found more than one %s in %s (%v)", name, t, fields)
	}
	return fields[0]
}

func tryFindOneNestedField(t reflect.Type, name string, path string, maxDepth int) fieldWithPath {
	fields := findNestedField(t, name, path, maxDepth)
	if len(fields) == 0 {
		return fieldWithPath{}
	} else if len(fields) > 1 {
		codegen.Fatalf("found more than one %s in %s (%v)", name, t, fields)
	}
	return fields[0]
}

func historyRoutingOptions(reqType reflect.Type) *historyservice.RoutingOptions {
	t := reqType.Elem() // we know it's a pointer

	inst := reflect.New(t)
	reflectable, ok := inst.Interface().(interface{ ProtoReflect() protoreflect.Message })
	if !ok {
		log.Fatalf("Request has no ProtoReflect method %s", t)
	}
	opts := reflectable.ProtoReflect().Descriptor().Options()

	// Retrieve the value of the custom option
	optionValue := proto.GetExtension(opts, historyRoutingProtoExtension)
	if optionValue == nil {
		log.Fatalf("Got nil while retrieving extension from options")
	}

	routingOptions := optionValue.(*historyservice.RoutingOptions)
	if routingOptions == nil {
		log.Fatalf("Request has no routing options: %s", t)
	}
	return routingOptions
}

func toGetter(snake string) string {
	parts := strings.Split(snake, ".")
	for i, part := range parts {
		parts[i] = "Get" + codegen.SnakeCaseToPascalCase(part) + "()"
	}
	return "request." + strings.Join(parts, ".")
}

func makeGetHistoryClient(reqType reflect.Type, routingOptions *historyservice.RoutingOptions) string {
	t := reqType.Elem() // we know it's a pointer

	if routingOptions.AnyHost && routingOptions.ShardId != "" && routingOptions.WorkflowId != "" && routingOptions.TaskToken != "" && routingOptions.TaskInfos != "" && routingOptions.ChasmComponentRef != "" {
		log.Fatalf("Found more than one routing directive in %s", t)
	}
	if routingOptions.AnyHost {
		return "shardID := c.getRandomShard()"
	}
	if routingOptions.ShardId != "" {
		verifyFieldExists(t, routingOptions.ShardId)
		return "shardID := " + toGetter(routingOptions.ShardId)
	}
	if routingOptions.WorkflowId != "" {
		namespaceIdField := routingOptions.NamespaceId
		if namespaceIdField == "" {
			namespaceIdField = "namespace_id"
		}
		verifyFieldExists(t, namespaceIdField)
		verifyFieldExists(t, routingOptions.WorkflowId)
		return fmt.Sprintf("shardID := c.shardIDFromWorkflowID(%s, %s)", toGetter(namespaceIdField), toGetter(routingOptions.WorkflowId))
	}
	if routingOptions.TaskToken != "" {
		namespaceIdField := routingOptions.NamespaceId
		if namespaceIdField == "" {
			namespaceIdField = "namespace_id"
		}

		verifyFieldExists(t, namespaceIdField)
		verifyFieldExists(t, routingOptions.TaskToken)
		return fmt.Sprintf(`taskToken, err := c.tokenSerializer.Deserialize(%s)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument("error deserializing task token")
	}
	shardID := c.shardIDFromWorkflowID(%s, taskToken.GetWorkflowId())
`, toGetter(routingOptions.TaskToken), toGetter(namespaceIdField))
	}
	if routingOptions.ChasmComponentRef != "" {
		verifyFieldExists(t, routingOptions.ChasmComponentRef)
		return fmt.Sprintf(`ref, err := c.tokenSerializer.DeserializeChasmComponentRef(%s)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument("error deserializing component ref")
	}
	shardID := c.shardIDFromWorkflowID(ref.GetNamespaceId(), ref.GetBusinessId())
	`, toGetter(routingOptions.ChasmComponentRef))
	}
	if routingOptions.TaskInfos != "" {
		verifyFieldExists(t, routingOptions.TaskInfos)
		p := toGetter(routingOptions.TaskInfos)
		// slice needs a tiny bit of extra handling for namespace
		return fmt.Sprintf(`// All workflow IDs are in the same shard per request
	if len(%s) == 0 {
		return nil, serviceerror.NewInvalidArgument("missing TaskInfos")
	}
	shardID := c.shardIDFromWorkflowID(%s[0].NamespaceId, %s[0].WorkflowId)`, p, p, p)
	}

	log.Fatalf("No routing directive specified on %s", t)
	return ""
}

func makeGetMatchingClient(reqType reflect.Type) string {
	// this magically figures out how to get a MatchingServiceClient from a request
	t := reqType.Elem() // we know it's a pointer

	var nsID, tqp, tq, tqt fieldWithPath

	switch t.Name() {
	case "GetBuildIdTaskQueueMappingRequest":
		// Pick a random node for this request, it's not associated with a specific task queue.
		tq = fieldWithPath{path: "fmt.Sprintf(\"not-applicable-%d\", rand.Int())"}
		tqt = fieldWithPath{path: "enumspb.TASK_QUEUE_TYPE_UNSPECIFIED"}
		nsID = findOneNestedField(t, "NamespaceId", "request", 1)
	case "UpdateTaskQueueUserDataRequest",
		"ReplicateTaskQueueUserDataRequest",
		"RecordWorkerHeartbeatRequest",
		"ListWorkersRequest",
		"DescribeWorkerRequest":
		// Always route these requests to the same matching node by namespace.
		tq = fieldWithPath{path: "\"not-applicable\""}
		tqt = fieldWithPath{path: "enumspb.TASK_QUEUE_TYPE_UNSPECIFIED"}
		nsID = findOneNestedField(t, "NamespaceId", "request", 1)
	case "GetWorkerBuildIdCompatibilityRequest",
		"UpdateWorkerBuildIdCompatibilityRequest",
		"RespondQueryTaskCompletedRequest",
		"ListTaskQueuePartitionsRequest",
		"SyncDeploymentUserDataRequest",
		"CheckTaskQueueUserDataPropagationRequest",
		"ApplyTaskQueueUserDataReplicationEventRequest",
		"GetWorkerVersioningRulesRequest",
		"UpdateWorkerVersioningRulesRequest",
		"UpdateFairnessStateRequest",
		"UpdateTaskQueueConfigRequest":
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
		tq = fieldWithPath{path: `"not-applicable"`}
		tqt = fieldWithPath{path: "enumspb.TASK_QUEUE_TYPE_UNSPECIFIED"}
		nsID = fieldWithPath{path: `"not-applicable"`}
	default:
		tqp = tryFindOneNestedField(t, "TaskQueuePartition", "request", 1)
		tq = findOneNestedField(t, "TaskQueue", "request", 2)
		tqt = findOneNestedField(t, "TaskQueueType", "request", 2)
		nsID = findOneNestedField(t, "NamespaceId", "request", 1)
	}

	if !nsID.found() {
		codegen.Fatalf("I don't know how to get a client from a %s", t)
	}

	if tqp.found() {
		return fmt.Sprintf(
			`p := tqid.PartitionFromPartitionProto(%s, %s)

	client, err := c.getClientForTaskQueuePartition(p)`,
			tqp.path, nsID.path)
	}
	if tq.found() && tqt.found() {
		partitionMaker := fmt.Sprintf("tqid.PartitionFromProto(%s, %s, %s)", tq.path, nsID.path, tqt.path)
		// Some task queue fields are full messages, some are just strings
		isTaskQueueMessage := tq.field != nil && tq.field.Type == reflect.TypeOf((*taskqueuepb.TaskQueue)(nil))
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

func writeTemplatedMethod(w io.Writer, service service, impl string, m reflect.Method, tmpl string) {
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
		panic(key + " doesn't look like a grpc handler method")
	}

	reqType := mt.In(1)
	respType := mt.Out(0)

	fields := map[string]string{
		"Method":       m.Name,
		"RequestType":  reqType.String(),
		"ResponseType": respType.String(),
		"MetricPrefix": fmt.Sprintf("%s%sClient", strings.ToUpper(service.name[:1]), service.name[1:]),
		"RetryPolicy":  cmp.Or(longPollRetryPolicy[key], "policy"),
	}
	if longPollContext[key] {
		fields["LongPoll"] = "LongPoll"
	}
	if largeTimeoutContext[key] {
		fields["WithLargeTimeout"] = "WithLargeTimeout"
	}
	if impl == "client" {
		if service.name == "history" {
			routingOptions := historyRoutingOptions(reqType)
			if routingOptions.Custom {
				return
			}
			fields["GetClient"] = makeGetHistoryClient(reqType, routingOptions)
		} else if service.name == "matching" {
			fields["GetClient"] = makeGetMatchingClient(reqType)
		}
	}

	codegen.FatalIfErr(codegen.GenerateTemplateToWriter(tmpl, fields, w))
}

func writeTemplatedMethods(w io.Writer, service service, impl string, tmpl string) {
	sType := service.clientType.Elem()
	for n := 0; n < sType.NumMethod(); n++ {
		writeTemplatedMethod(w, service, impl, sType.Method(n), tmpl)
	}
}

func generateFrontendOrAdminClient(w io.Writer, service service) error {
	writeTemplatedCode(w, service, `// Code generated by cmd/tools/genrpcwrappers. DO NOT EDIT.

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
	return nil
}

func generateHistoryClient(w io.Writer, service service) error {
	writeTemplatedCode(w, service, `// Code generated by cmd/tools/genrpcwrappers. DO NOT EDIT.

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

	return nil
}

func generateMatchingClient(w io.Writer, service service) error {
	writeTemplatedCode(w, service, `// Code generated by cmd/tools/genrpcwrappers. DO NOT EDIT.

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
	return nil
}

func generateMetricClient(w io.Writer, service service) error {
	writeTemplatedCode(w, service, `// Code generated by cmd/tools/genrpcwrappers. DO NOT EDIT.

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
	return nil
}

func generateRetryableClient(w io.Writer, service service) error {
	writeTemplatedCode(w, service, `// Code generated by cmd/tools/genrpcwrappers. DO NOT EDIT.

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
	err := backoff.ThrottleRetryContext(ctx, op, c.{{.RetryPolicy}}, c.isRetryable)
	return resp, err
}
`)
	return nil
}

func main() {
	serviceFlag := flag.String("service", "", "which service to generate rpc client wrappers for")
	flag.Parse()

	i := slices.IndexFunc(services, func(s service) bool { return s.name == *serviceFlag })
	if i < 0 {
		codegen.Fatalf("unknown service: %s", *serviceFlag)
	}
	svc := services[i]

	codegen.GenerateToFile(svc.clientGenerator, svc, "", "client")
	codegen.GenerateToFile(generateMetricClient, svc, "", "metric_client")
	codegen.GenerateToFile(generateRetryableClient, svc, "", "retryable_client")
}
