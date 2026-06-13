package main

import (
	"cmp"
	_ "embed"
	"flag"
	"fmt"
	"reflect"
	"regexp"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/cmd/tools/codegen"
)

const maxMessageDepth = 5

type (
	messageData struct {
		Type string

		WorkflowIDGetter  string
		RunIDGetter       string
		TaskTokenGetter   string
		ActivityIDGetter  string
		OperationIDGetter string
		ChasmRunIDGetter  string
	}

	grpcServerData struct {
		Server   string
		Imports  []string
		Messages []messageData
	}
)

var (
	//go:embed server_interceptors.tmpl
	serverInterceptorsTemplate string

	// List of types for which Workflow tag getters are generated.
	grpcServers = []reflect.Type{
		reflect.TypeOf((*workflowservice.WorkflowServiceServer)(nil)).Elem(),
		reflect.TypeOf((*adminservice.AdminServiceServer)(nil)).Elem(),
		reflect.TypeOf((*historyservice.HistoryServiceServer)(nil)).Elem(),
		reflect.TypeOf((*matchingservice.MatchingServiceServer)(nil)).Elem(),
	}

	// Only request fields that match the pattern are eligible for deeper inspection.
	fieldNameRegex = regexp.MustCompile("^(?:.*Request|Completion|UpdateRef|ParentExecution|WorkflowState|ExecutionInfo|ExecutionState)$")

	// These types have task_token field, but it is not of type *tokenspb.Task and doesn't have Workflow tags.
	excludeTaskTokenTypes = []reflect.Type{
		reflect.TypeOf((*workflowservice.RespondQueryTaskCompletedRequest)(nil)),
		reflect.TypeOf((*workflowservice.RespondNexusTaskCompletedRequest)(nil)),
		reflect.TypeOf((*workflowservice.RespondNexusTaskFailedRequest)(nil)),
	}

	executionGetterT = reflect.TypeOf((*interface {
		GetExecution() *commonpb.WorkflowExecution
	})(nil)).Elem()

	workflowExecutionGetterT = reflect.TypeOf((*interface {
		GetWorkflowExecution() *commonpb.WorkflowExecution
	})(nil)).Elem()

	taskTokenGetterT = reflect.TypeOf((*interface {
		GetTaskToken() []byte
	})(nil)).Elem()

	workflowIDGetterT = reflect.TypeOf((*interface {
		GetWorkflowId() string
	})(nil)).Elem()

	runIDGetterT = reflect.TypeOf((*interface {
		GetRunId() string
	})(nil)).Elem()

	activityIDGetterT = reflect.TypeOf((*interface {
		GetActivityId() string
	})(nil)).Elem()

	operationIDGetterT = reflect.TypeOf((*interface {
		GetOperationId() string
	})(nil)).Elem()
)

func main() {
	outPathFlag := flag.String("out", ".", "path to write generated files")
	flag.Parse()

	for _, grpcServerT := range grpcServers {
		codegen.GenerateTemplateToFile(serverInterceptorsTemplate, getGrpcServerData(grpcServerT), *outPathFlag, codegen.CamelCaseToSnakeCase(grpcServerT.Name()))
	}
}

func getGrpcServerData(grpcServerT reflect.Type) grpcServerData {
	sd := grpcServerData{
		Server:  grpcServerT.Name(),
		Imports: []string{grpcServerT.PkgPath()},
	}

	for i := 0; i < grpcServerT.NumMethod(); i++ {
		rpcT := grpcServerT.Method(i).Type
		if rpcT.NumIn() < 2 {
			continue
		}

		requestT := rpcT.In(1) // Assume request is always the second parameter.
		requestMd := workflowTagGetters(requestT, 0)
		requestMd.Type = requestT.String()
		sd.Messages = append(sd.Messages, requestMd)

		respT := rpcT.Out(0) // Assume response is always the first parameter.
		responseMd := workflowTagGetters(respT, 0)
		responseMd.Type = respT.String()
		sd.Messages = append(sd.Messages, responseMd)
	}

	return sd
}

//nolint:revive // cognitive complexity 37 (> max enabled 25)
func workflowTagGetters(messageType reflect.Type, depth int) messageData {
	pd := messageData{}
	if depth > maxMessageDepth {
		return pd
	}

	switch {
	case messageType.AssignableTo(executionGetterT):
		pd.WorkflowIDGetter = "GetExecution().GetWorkflowId()"
		pd.RunIDGetter = "GetExecution().GetRunId()"
	case messageType.AssignableTo(workflowExecutionGetterT):
		pd.WorkflowIDGetter = "GetWorkflowExecution().GetWorkflowId()"
		pd.RunIDGetter = "GetWorkflowExecution().GetRunId()"
	default:
		// Might have any combination of these, or none.
		if messageType.AssignableTo(workflowIDGetterT) {
			pd.WorkflowIDGetter = "GetWorkflowId()"
		}
		if messageType.AssignableTo(runIDGetterT) {
			pd.RunIDGetter = "GetRunId()"
		}
		if messageType.AssignableTo(activityIDGetterT) {
			pd.ActivityIDGetter = "GetActivityId()"
		}
		if messageType.AssignableTo(operationIDGetterT) {
			pd.OperationIDGetter = "GetOperationId()"
		}
	}

	if pd.ActivityIDGetter == "" && messageType.AssignableTo(activityIDGetterT) {
		pd.ActivityIDGetter = "GetActivityId()"
	}
	if pd.OperationIDGetter == "" && messageType.AssignableTo(operationIDGetterT) {
		pd.OperationIDGetter = "GetOperationId()"
	}
	if messageType.AssignableTo(taskTokenGetterT) {
		excluded := false
		for _, ert := range excludeTaskTokenTypes {
			if messageType.AssignableTo(ert) {
				excluded = true
				break
			}
		}
		if !excluded {
			pd.TaskTokenGetter = "GetTaskToken()"
		}
	}

	// Iterates over fields in order they defined in proto file, not proto index.
	// Order is important because the first match wins.
	for fieldNum := 0; fieldNum < messageType.Elem().NumField(); fieldNum++ {
		nestedRequest := messageType.Elem().Field(fieldNum)
		if nestedRequest.Type.Kind() != reflect.Ptr {
			continue
		}
		if nestedRequest.Type.Elem().Kind() != reflect.Struct {
			continue
		}
		if !fieldNameRegex.MatchString(nestedRequest.Name) {
			continue
		}

		nestedRd := workflowTagGetters(nestedRequest.Type, depth+1)
		// First match wins: if getter is already set, it won't be overwritten.
		if pd.WorkflowIDGetter == "" && nestedRd.WorkflowIDGetter != "" {
			pd.WorkflowIDGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.WorkflowIDGetter)
		}
		if pd.RunIDGetter == "" && nestedRd.RunIDGetter != "" {
			pd.RunIDGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.RunIDGetter)
		}
		if pd.TaskTokenGetter == "" && nestedRd.TaskTokenGetter != "" {
			pd.TaskTokenGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.TaskTokenGetter)
		}
		if pd.ActivityIDGetter == "" && nestedRd.ActivityIDGetter != "" {
			pd.ActivityIDGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.ActivityIDGetter)
		}
		if pd.OperationIDGetter == "" && nestedRd.OperationIDGetter != "" {
			pd.OperationIDGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.OperationIDGetter)
		}
	}

	// When a business ID (activity or operation) is present without a workflow ID,
	// the run_id is not a workflow run ID. Only apply at the top level.
	if depth == 0 {
		hasChasmBusinessID := pd.WorkflowIDGetter == "" && cmp.Or(pd.ActivityIDGetter, pd.OperationIDGetter) != ""
		if hasChasmBusinessID && pd.RunIDGetter != "" {
			pd.ChasmRunIDGetter = pd.RunIDGetter
			pd.RunIDGetter = ""
		}
	}

	return pd
}
