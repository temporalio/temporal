package main

import (
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

		WorkflowIdGetter string
		RunIdGetter      string
		TaskTokenGetter  string
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

	workflowIdGetterT = reflect.TypeOf((*interface {
		GetWorkflowId() string
	})(nil)).Elem()

	runIdGetterT = reflect.TypeOf((*interface {
		GetRunId() string
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
		pd.WorkflowIdGetter = "GetExecution().GetWorkflowId()"
		pd.RunIdGetter = "GetExecution().GetRunId()"
	case messageType.AssignableTo(workflowExecutionGetterT):
		pd.WorkflowIdGetter = "GetWorkflowExecution().GetWorkflowId()"
		pd.RunIdGetter = "GetWorkflowExecution().GetRunId()"
	case messageType.AssignableTo(taskTokenGetterT):
		for _, ert := range excludeTaskTokenTypes {
			if messageType.AssignableTo(ert) {
				return pd
			}
		}
		pd.TaskTokenGetter = "GetTaskToken()"
	default:
		// Might be one of these, both, or neither.
		if messageType.AssignableTo(workflowIdGetterT) {
			pd.WorkflowIdGetter = "GetWorkflowId()"
		}
		if messageType.AssignableTo(runIdGetterT) {
			pd.RunIdGetter = "GetRunId()"
		}
	}

	// Iterates over fields in order they defined in proto file, not proto index.
	// Order is important because the first match wins.
	for fieldNum := 0; fieldNum < messageType.Elem().NumField(); fieldNum++ {
		if (pd.WorkflowIdGetter != "" && pd.RunIdGetter != "") || pd.TaskTokenGetter != "" {
			break
		}

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
		if pd.WorkflowIdGetter == "" && nestedRd.WorkflowIdGetter != "" {
			pd.WorkflowIdGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.WorkflowIdGetter)
		}
		if pd.RunIdGetter == "" && nestedRd.RunIdGetter != "" {
			pd.RunIdGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.RunIdGetter)
		}
		if pd.TaskTokenGetter == "" && nestedRd.TaskTokenGetter != "" {
			pd.TaskTokenGetter = fmt.Sprintf("Get%s().%s", nestedRequest.Name, nestedRd.TaskTokenGetter)
		}
	}
	return pd
}
