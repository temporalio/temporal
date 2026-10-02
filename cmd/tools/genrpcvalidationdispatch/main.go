package main

import (
	_ "embed"
	"flag"
	"fmt"
	"reflect"
	"slices"

	"buf.build/go/protovalidate"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1" // trigger proto file registration
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/cmd/tools/codegen"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

//go:embed template.tmpl
var templateStr string

func main() {
	outFlag := flag.String("out", ".", "output directory")
	flag.Parse()
	services := []protoreflect.ServiceDescriptor{
		workflowservice.File_temporal_api_workflowservice_v1_service_proto.Services().ByName("WorkflowService"),
		adminservice.File_temporal_server_api_adminservice_v1_service_proto.Services().ByName("AdminService"),
		operatorservice.File_temporal_api_operatorservice_v1_service_proto.Services().ByName("OperatorService"),
	}
	types, err := validationTypes(services...)
	if err != nil {
		codegen.Fatalf("finding annotated messages: %v", err)
	}
	data := templateData{}
	imports := make(map[string]struct{})
	for _, name := range types {
		message, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(name))
		if err != nil {
			codegen.Fatalf("finding Go message type for %s: %v", name, err)
		}
		messageType := reflect.TypeOf(message.New().Interface()).Elem()
		data.Types = append(data.Types, messageType.String())
		imports[messageType.PkgPath()] = struct{}{}
	}
	for path := range imports {
		data.Imports = append(data.Imports, path)
	}
	slices.Sort(data.Imports)
	slices.Sort(data.Types)
	codegen.GenerateTemplateToFile(templateStr, data, *outFlag, "proto_validation")
}

type templateData struct {
	Imports []string
	Types   []string
}

func validationTypes(services ...protoreflect.ServiceDescriptor) ([]string, error) {
	selected := make(map[string]struct{})
	for _, service := range services {
		for index := range service.Methods().Len() {
			method := service.Methods().Get(index)
			for _, descriptor := range []protoreflect.MessageDescriptor{method.Input(), method.Output()} {
				applicable, err := hasValidationRules(descriptor, make(map[protoreflect.MessageDescriptor]struct{}))
				if err != nil {
					return nil, fmt.Errorf("%s: %w", descriptor.FullName(), err)
				}
				if applicable {
					selected[string(descriptor.FullName())] = struct{}{}
				}
			}
		}
	}
	types := make([]string, 0, len(selected))
	for name := range selected {
		types = append(types, name)
	}
	slices.Sort(types)
	return types, nil
}

func hasValidationRules(descriptor protoreflect.MessageDescriptor, visited map[protoreflect.MessageDescriptor]struct{}) (bool, error) {
	if _, ok := visited[descriptor]; ok {
		return false, nil
	}
	visited[descriptor] = struct{}{}
	if rules, err := protovalidate.ResolveMessageRules(descriptor); err != nil || rules != nil {
		return rules != nil, err
	}
	for index := range descriptor.Oneofs().Len() {
		if rules, err := protovalidate.ResolveOneofRules(descriptor.Oneofs().Get(index)); err != nil || rules != nil {
			return rules != nil, err
		}
	}
	for index := range descriptor.Fields().Len() {
		field := descriptor.Fields().Get(index)
		if rules, err := protovalidate.ResolveFieldRules(field); err != nil || rules != nil {
			return rules != nil, err
		}
		if nested := field.Message(); nested != nil {
			if applicable, err := hasValidationRules(nested, visited); err != nil || applicable {
				return applicable, err
			}
		}
	}
	return false, nil
}
