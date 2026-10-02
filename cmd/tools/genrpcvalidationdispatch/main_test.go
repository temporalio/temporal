package main

import (
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestValidationTypesSelectsAnnotatedRequestsAndResponses(t *testing.T) {
	t.Parallel()

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("service_test.proto"), Package: new("interceptor.test"), Syntax: new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: new("Request"), Field: []*descriptorpb.FieldDescriptorProto{{Name: new("name"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: requiredTestFieldOptions()}}},
			{Name: new("Response"), Field: []*descriptorpb.FieldDescriptorProto{{Name: new("run_id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: requiredTestFieldOptions()}}},
			{Name: new("Unannotated")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: new("WorkflowService"), Method: []*descriptorpb.MethodDescriptorProto{
			{Name: new("Start"), InputType: new(".interceptor.test.Request"), OutputType: new(".interceptor.test.Response")},
			{Name: new("Again"), InputType: new(".interceptor.test.Request"), OutputType: new(".interceptor.test.Unannotated")},
		}}},
	}, nil)
	require.NoError(t, err)
	types, err := validationTypes(file.Services().Get(0), file.Services().Get(0))
	require.NoError(t, err)
	require.Equal(t, []string{"interceptor.test.Request", "interceptor.test.Response"}, types)
}

func TestHasValidationRulesFindsNestedAnnotations(t *testing.T) {
	t.Parallel()

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("nested_test.proto"), Package: new("interceptor.test"), Syntax: new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: new("Parent"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: new("child"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".interceptor.test.Child")},
					{Name: new("children"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".interceptor.test.Child")},
					{Name: new("values"), Number: proto.Int32(3), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".interceptor.test.Parent.ValuesEntry")},
				},
				NestedType: []*descriptorpb.DescriptorProto{{
					Name: new("ValuesEntry"), Options: &descriptorpb.MessageOptions{MapEntry: new(true)},
					Field: []*descriptorpb.FieldDescriptorProto{
						{Name: new("key"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
						{Name: new("value"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".interceptor.test.Child")},
					},
				}},
			},
			{
				Name: new("Child"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: new("name"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: requiredTestFieldOptions()},
					{Name: new("parent"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".interceptor.test.Parent")},
				},
			},
		},
	}, nil)
	require.NoError(t, err)
	parent := protodesc.ToDescriptorProto(file.Messages().ByName("Parent"))
	for _, name := range []string{"child", "children", "values"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			isolated := proto.Clone(parent).(*descriptorpb.DescriptorProto)
			isolated.Field = nil
			for _, field := range parent.Field {
				if field.GetName() == name {
					isolated.Field = append(isolated.Field, field)
				}
			}
			schema := protodesc.ToFileDescriptorProto(file)
			schema.MessageType[0] = isolated
			isolatedFile, err := protodesc.NewFile(schema, nil)
			require.NoError(t, err)
			found, err := hasValidationRules(isolatedFile.Messages().ByName("Parent"), make(map[protoreflect.MessageDescriptor]struct{}))
			require.NoError(t, err)
			require.True(t, found)
			schema.MessageType[1].Field[0].Options = nil
			unannotated, err := protodesc.NewFile(schema, nil)
			require.NoError(t, err)
			found, err = hasValidationRules(unannotated.Messages().ByName("Parent"), make(map[protoreflect.MessageDescriptor]struct{}))
			require.NoError(t, err)
			require.False(t, found)
		})
	}
}

func requiredTestFieldOptions() *descriptorpb.FieldOptions {
	options := &descriptorpb.FieldOptions{}
	proto.SetExtension(options, validate.E_Field, (&validate.FieldRules_builder{Required: new(true)}).Build())
	return options
}

func TestValidationTypesPreservesMessagePackages(t *testing.T) {
	t.Parallel()
	external, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("request_test.proto"), Package: new("interceptor.external"), Syntax: new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: new("Request"), Field: []*descriptorpb.FieldDescriptorProto{{Name: new("name"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: requiredTestFieldOptions()}}}},
	}, nil)
	require.NoError(t, err)
	registry := new(protoregistry.Files)
	require.NoError(t, registry.RegisterFile(external))
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("package_test.proto"), Package: new("interceptor.test"), Syntax: new("proto3"), Dependency: []string{"request_test.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{Name: new("Request"), Field: []*descriptorpb.FieldDescriptorProto{{Name: new("name"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: requiredTestFieldOptions()}}}},
		Service:     []*descriptorpb.ServiceDescriptorProto{{Name: new("Service"), Method: []*descriptorpb.MethodDescriptorProto{{Name: new("Call"), InputType: new(".interceptor.external.Request"), OutputType: new(".interceptor.test.Request")}}}},
	}, registry)
	require.NoError(t, err)
	types, err := validationTypes(file.Services().Get(0))
	require.NoError(t, err)
	require.Equal(t, []string{"interceptor.external.Request", "interceptor.test.Request"}, types)
}
