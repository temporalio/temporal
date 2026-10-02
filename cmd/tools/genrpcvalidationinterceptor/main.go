package main

import (
	_ "embed"
	"flag"
	"fmt"
	"slices"

	"buf.build/go/protovalidate"
	_ "go.temporal.io/api/workflowservice/v1" // trigger proto file registration
	"go.temporal.io/server/cmd/tools/codegen"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

//go:embed template.tmpl
var templateStr string

func main() {
	outFlag := flag.String("out", ".", "output directory")
	flag.Parse()
	file, err := protoregistry.GlobalFiles.FindFileByPath("temporal/api/workflowservice/v1/service.proto")
	if err != nil {
		codegen.Fatalf("finding WorkflowService: %v", err)
	}
	types, err := validationTypes(file.Services().ByName("WorkflowService"))
	if err != nil {
		codegen.Fatalf("finding annotated messages: %v", err)
	}
	codegen.GenerateTemplateToFile(templateStr, types, *outFlag, "proto_validation")
}

func validationTypes(service protoreflect.ServiceDescriptor) ([]string, error) {
	selected := make(map[string]struct{})
	for index := range service.Methods().Len() {
		method := service.Methods().Get(index)
		for _, descriptor := range []protoreflect.MessageDescriptor{method.Input(), method.Output()} {
			applicable, err := hasValidationRules(descriptor, make(map[protoreflect.MessageDescriptor]struct{}))
			if err != nil {
				return nil, fmt.Errorf("%s: %w", descriptor.FullName(), err)
			}
			if applicable {
				selected[string(descriptor.Name())] = struct{}{}
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
