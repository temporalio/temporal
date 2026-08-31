package main

import (
	"cmp"
	"flag"
	"fmt"
	"go/format"
	"io"
	"log"
	"os"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"go.temporal.io/api/operatorservice/v1"
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

	// loadBalancedMethod describes a matching client method that picks a partition with
	// the load balancer (when the request is for a non-forwarded root partition) in
	// addition to routing it to the owning host.
	loadBalancedMethod struct {
		// taskQueueType is the task queue type of the partition to route to. It can't be
		// inferred from the request since these requests don't carry a task queue type.
		taskQueueType string
		// poll marks a long-poll read: the read partition is picked (which may hold a
		// lease that has to be released afterwards) and the long poll timeout is used.
		// Otherwise the write partition is picked with the regular timeout.
		poll bool
	}
)

func (f fieldWithPath) found() bool {
	return f.path != ""
}

var (
	services = []service{
		{
			name:            "frontend",
			clientType:      reflect.TypeFor[*workflowservice.WorkflowServiceClient](),
			clientGenerator: generateFrontendOrAdminClient,
		},
		{
			name:            "admin",
			clientType:      reflect.TypeFor[*adminservice.AdminServiceClient](),
			clientGenerator: generateFrontendOrAdminClient,
		},
		{
			name:            "operator",
			clientType:      reflect.TypeFor[*operatorservice.OperatorServiceClient](),
			clientGenerator: generateFrontendOrAdminClient,
		},
		{
			name:            "history",
			clientType:      reflect.TypeFor[*historyservice.HistoryServiceClient](),
			clientGenerator: generateHistoryClient,
		},
		{
			name:            "matching",
			clientType:      reflect.TypeFor[*matchingservice.MatchingServiceClient](),
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
	// stateSyncTimeoutContext are the cross-cluster workflow state sync hops, whose callers set a
	// deadline that can exceed even the large timeout. DefaultStateSyncTimeout is only a backstop.
	stateSyncTimeoutContext = map[string]bool{
		"client.admin.SyncWorkflowState":   true,
		"client.history.SyncWorkflowState": true,
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

		// Nexus metrics are an exception since they use the information from the request.
		"metricsClient.history.StartNexusOperation":  true,
		"metricsClient.history.CancelNexusOperation": true,
	}
	// loadBalancedMethods are the matching methods that load balance across the
	// partitions of a task queue instead of routing to one given partition. They get a
	// different client wrapper (and a metric wrapper that emits forwarding stats).
	loadBalancedMethods = map[string]loadBalancedMethod{
		"matching.AddActivityTask":       {taskQueueType: "ACTIVITY"},
		"matching.AddWorkflowTask":       {taskQueueType: "WORKFLOW"},
		"matching.QueryWorkflow":         {taskQueueType: "WORKFLOW"},
		"matching.DispatchNexusTask":     {taskQueueType: "NEXUS"},
		"matching.PollActivityTaskQueue": {taskQueueType: "ACTIVITY", poll: true},
		"matching.PollWorkflowTaskQueue": {taskQueueType: "WORKFLOW", poll: true},
		"matching.PollNexusTaskQueue":    {taskQueueType: "NEXUS", poll: true},
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

var getterRegexp = regexp.MustCompile(`Get(\w+)\(\)`)

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
	for f := range t.Fields() {
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
	var namespaceID string
	var businessID string
	if len(taskToken.GetComponentRef()) > 0 {
		ref, err := c.tokenSerializer.DeserializeChasmComponentRef(taskToken.GetComponentRef())
		if err != nil {
			return nil, err
		}
		namespaceID = ref.GetNamespaceId()
		businessID = ref.GetBusinessId()
	} else {
		namespaceID = %s
		businessID = taskToken.GetWorkflowId()
	}
	shardID := c.shardIDFromWorkflowID(namespaceID, businessID)
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
		"CountWorkersRequest",
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
	case "RespondNexusTaskCompletedRequest",
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
		isTaskQueueMessage := tq.field != nil && tq.field.Type == reflect.TypeFor[*taskqueuepb.TaskQueue]()
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

// makeLoadBalancedFields computes the template fields for a matching method that load
// balances across the partitions of a task queue. Everything but the task queue type is
// derived from the request type.
func makeLoadBalancedFields(reqType reflect.Type, lb loadBalancedMethod, fields map[string]string) {
	t := reqType.Elem() // we know it's a pointer

	tq := findOneNestedField(t, "TaskQueue", "request", 2)
	nsID := findOneNestedField(t, "NamespaceId", "request", 1)

	// The source partition of a forwarded request lives either in a plain field on the
	// request or inside its forward info.
	forwardedSource := tryFindOneNestedField(t, "ForwardedSource", "request", 1)
	if !forwardedSource.found() {
		fi := findOneNestedField(t, "ForwardInfo", "request", 1)
		forwardedSource = fieldWithPath{path: fi.path + ".GetSourcePartition()"}
	}

	fields["TaskQueue"] = tq.path
	fields["NamespaceId"] = nsID.path
	fields["TaskQueueType"] = "enumspb.TASK_QUEUE_TYPE_" + lb.taskQueueType
	fields["ForwardedSource"] = forwardedSource.path
	fields["CopyRequest"] = makeCopyRequest(reqType, tq.path)

	if lb.poll {
		fields["LongPoll"] = "LongPoll"
		fields["PickClient"] = fmt.Sprintf(`client, release, err := c.pickClientForRead(%s, p, loadBalance, pc)
	if err != nil {
		return nil, err
	}
	if release != nil {
		defer release()
	}`, tq.path)
	} else {
		fields["PickClient"] = fmt.Sprintf(`client, err := c.pickClientForWrite(%s, p, loadBalance, pc)
	if err != nil {
		return nil, err
	}`, tq.path)
	}
}

// makeCopyRequest returns code that replaces request with a copy, since picking a
// partition rewrites the name of the task queue in it. Only the messages on the path to
// the task queue are copied, field by field; everything else is shared with the original
// request, which may be large.
func makeCopyRequest(reqType reflect.Type, tqPath string) string {
	// tqPath looks like "request.GetPollRequest().GetTaskQueue()", i.e. it names the
	// chain of fields from the request down to the task queue.
	var chain []string
	for _, m := range getterRegexp.FindAllStringSubmatch(tqPath, -1) {
		chain = append(chain, m[1])
	}

	var b strings.Builder
	b.WriteString("// Copy the messages on the path to the task queue, since picking a partition\n")
	b.WriteString("\t// rewrites its name. The rest is shared with the original request.\n")
	b.WriteString("\trequest = ")
	writeCopy(&b, reqType.Elem(), "request", chain)
	return b.String()
}

// writeCopy writes a composite literal copying each field of t, recursing into the field
// named by the head of chain. The output is not indented or aligned since the generated
// file is gofmt-ed afterwards.
func writeCopy(b *strings.Builder, t reflect.Type, path string, chain []string) {
	fmt.Fprintf(b, "&%s{\n", goTypeName(t))
	for f := range t.Fields() {
		if !f.IsExported() {
			continue // state, sizeCache, unknownFields
		}
		fmt.Fprintf(b, "%s: ", f.Name)
		if len(chain) > 0 && f.Name == chain[0] {
			if f.Type.Kind() != reflect.Pointer {
				codegen.Fatalf("%s.%s is not a message", t, f.Name)
			}
			writeCopy(b, f.Type.Elem(), path+"."+f.Name, chain[1:])
		} else {
			fmt.Fprintf(b, "%s.%s", path, f.Name)
		}
		b.WriteString(",\n")
	}
	b.WriteString("}")
}

// pbPackage matches the api proto packages whose imports the linter requires to be
// aliased: public ones as "<name>pb" and internal server ones as "<name>spb". Services
// are the exception, they're imported unaliased. See .github/.golangci.yml.
var pbPackage = regexp.MustCompile(`^go\.temporal\.io(/server)?/api/(\w+)/v1$`)

// goTypeName returns how t is referred to in generated code.
func goTypeName(t reflect.Type) string {
	m := pbPackage.FindStringSubmatch(t.PkgPath())
	if m == nil || strings.HasSuffix(m[2], "service") {
		return t.String()
	}
	if m[1] == "" {
		return m[2] + "pb." + t.Name()
	}
	return m[2] + "spb." + t.Name()
}

// verifyLoadBalancedMethods catches typos in the loadBalancedMethods table, which would
// otherwise just generate a plain routing wrapper.
func verifyLoadBalancedMethods(svc service) {
	for key := range loadBalancedMethods {
		name, ok := strings.CutPrefix(key, svc.name+".")
		if !ok {
			continue
		}
		if _, found := svc.clientType.Elem().MethodByName(name); !found {
			codegen.Fatalf("%s service has no method %s", svc.name, name)
		}
	}
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
	if stateSyncTimeoutContext[key] {
		fields["WithLargeTimeout"] = "WithStateSyncTimeout"
	}
	lb, isLoadBalanced := loadBalancedMethods[service.name+"."+m.Name]
	if isLoadBalanced && (impl == "client" || impl == "metricsClient") {
		makeLoadBalancedFields(reqType, lb, fields)
		if impl == "client" {
			tmpl = loadBalancedClientTemplate
		} else {
			tmpl = loadBalancedMetricClientTemplate
		}
	} else if impl == "client" {
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
	for method := range sType.Methods() {
		writeTemplatedMethod(w, service, impl, method, tmpl)
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
		ctx, cancel := c.createContext{{or .WithLargeTimeout ""}}(ctx)
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

// loadBalancedClientTemplate wraps a matching method that picks a partition with the
// load balancer: the exported method resolves the partition and hands the call to the
// unexported one, which may be retried by invokeWithPartitionCounts with fresh partition
// counts.
const loadBalancedClientTemplate = `
func (c *clientImpl) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) ({{.ResponseType}}, error) {
	p, loadBalance := c.resolvePartition(
		{{.TaskQueue}},
		{{.NamespaceId}},
		{{.TaskQueueType}},
		{{.ForwardedSource}},
	)
	return invokeWithPartitionCounts(ctx, c.logger, c.partitionCache, p, loadBalance, request, opts, c.do{{.Method}})
}

func (c *clientImpl) do{{.Method}}(
	ctx context.Context,
	p tqid.Partition,
	loadBalance bool,
	pc PartitionCounts,
	request {{.RequestType}},
	opts []grpc.CallOption,
) ({{.ResponseType}}, error) {
	{{.CopyRequest}}
	{{.PickClient}}
	ctx, cancel := c.create{{or .LongPoll ""}}Context(ctx)
	defer cancel()
	return client.{{.Method}}(ctx, request, opts...)
}
`

// loadBalancedMetricClientTemplate is the metric wrapper for a load balanced method: the
// same as the regular one plus forwarding stats.
const loadBalancedMetricClientTemplate = `
func (c *metricClient) {{.Method}}(
	ctx context.Context,
	request {{.RequestType}},
	opts ...grpc.CallOption,
) (_ {{.ResponseType}}, retError error) {

	metricsHandler, startTime := c.startMetricsRecording(ctx, "{{.MetricPrefix}}{{.Method}}")
	defer func() {
		c.finishMetricsRecording(metricsHandler, startTime, retError)
	}()

	c.emitForwardedSourceStats(metricsHandler, {{.ForwardedSource}}, {{.TaskQueue}})

	return c.client.{{.Method}}(ctx, request, opts...)
}
`

func generateMatchingClient(w io.Writer, service service) error {
	writeTemplatedCode(w, service, `// Code generated by cmd/tools/genrpcwrappers. DO NOT EDIT.

package {{.ServiceName}}

import (
	"context"
	"fmt"
	"math/rand"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
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

// generateToFormattedFile generates a file and then gofmts it, so that the templates
// don't have to worry about indentation and alignment.
func generateToFormattedFile(generator func(io.Writer, service) error, svc service, name string) {
	filename := name + "_gen.go"
	codegen.GenerateToFile(generator, svc, "", name)
	src, err := os.ReadFile(filename)
	codegen.FatalIfErr(err)
	formatted, err := format.Source(src)
	codegen.FatalIfErr(err)
	codegen.FatalIfErr(os.WriteFile(filename, formatted, 0644))
}

func main() {
	serviceFlag := flag.String("service", "", "which service to generate rpc client wrappers for")
	flag.Parse()

	i := slices.IndexFunc(services, func(s service) bool { return s.name == *serviceFlag })
	if i < 0 {
		codegen.Fatalf("unknown service: %s", *serviceFlag)
	}
	svc := services[i]
	verifyLoadBalancedMethods(svc)

	generateToFormattedFile(svc.clientGenerator, svc, "client")
	generateToFormattedFile(generateMetricClient, svc, "metric_client")
	generateToFormattedFile(generateRetryableClient, svc, "retryable_client")
}
