package main

import (
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestValidationTypesSelectsAnnotatedRequestsAndResponses(t *testing.T) {
	t.Parallel()

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("service_test.proto"), Package: proto.String("interceptor.test"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Request"), Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("name"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: requiredTestFieldOptions()}}},
			{Name: proto.String("Response"), Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("run_id"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: requiredTestFieldOptions()}}},
			{Name: proto.String("Unannotated")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("WorkflowService"), Method: []*descriptorpb.MethodDescriptorProto{
			{Name: proto.String("Start"), InputType: proto.String(".interceptor.test.Request"), OutputType: proto.String(".interceptor.test.Response")},
			{Name: proto.String("Again"), InputType: proto.String(".interceptor.test.Request"), OutputType: proto.String(".interceptor.test.Unannotated")},
		}}},
	}, nil)
	require.NoError(t, err)
	types, err := validationTypes(file.Services().Get(0))
	require.NoError(t, err)
	require.Equal(t, []string{"Request", "Response"}, types)
}

func TestHasValidationRulesFindsNestedAnnotations(t *testing.T) {
	t.Parallel()

	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: proto.String("nested_test.proto"), Package: proto.String("interceptor.test"), Syntax: proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("Parent"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("child"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".interceptor.test.Child")},
					{Name: proto.String("children"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".interceptor.test.Child")},
					{Name: proto.String("values"), Number: proto.Int32(3), Label: descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".interceptor.test.Parent.ValuesEntry")},
				},
				NestedType: []*descriptorpb.DescriptorProto{{
					Name: proto.String("ValuesEntry"), Options: &descriptorpb.MessageOptions{MapEntry: proto.Bool(true)},
					Field: []*descriptorpb.FieldDescriptorProto{
						{Name: proto.String("key"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
						{Name: proto.String("value"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".interceptor.test.Child")},
					},
				}},
			},
			{
				Name: proto.String("Child"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{Name: proto.String("name"), Number: proto.Int32(1), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: requiredTestFieldOptions()},
					{Name: proto.String("parent"), Number: proto.Int32(2), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: proto.String(".interceptor.test.Parent")},
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
	proto.SetExtension(options, validate.E_Field, (&validate.FieldRules_builder{Required: proto.Bool(true)}).Build())
	return options
}
