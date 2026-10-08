package interceptor

import (
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

func TestProtoValidationRejectsInvalidRulesInAbsentChildrenAtStartup(t *testing.T) {
	t.Parallel()
	options := new(descriptorpb.MessageOptions)
	proto.SetExtension(options, validate.E_Message, (&validate.MessageRules_builder{Cel: []*validate.Rule{
		(&validate.Rule_builder{Id: new("invalid"), Expression: new("this.missing > 0")}).Build(),
	}}).Build())
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("startup_test.proto"), Package: new("interceptor.startup"), Syntax: new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: new("Root"), Field: []*descriptorpb.FieldDescriptorProto{{Name: new("child"), Number: new(int32(1)), Type: descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(), TypeName: new(".interceptor.startup.Child")}}},
			{Name: new("Child"), Options: options},
		},
	}, nil)
	require.NoError(t, err)
	_, err = newProtoValidator(dynamicpb.NewMessage(file.Messages().ByName("Root")))
	require.ErrorContains(t, err, "compile protobuf validation")
}

func TestProtoValidationStartupChecksEveryScopeWithoutEvaluatingValues(t *testing.T) {
	t.Parallel()
	for _, invalidField := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid schema", true: "invalid field behind runtime error"}[invalidField], func(t *testing.T) {
			t.Parallel()
			options := new(descriptorpb.MessageOptions)
			proto.SetExtension(options, validate.E_Message, (&validate.MessageRules_builder{Cel: []*validate.Rule{
				(&validate.Rule_builder{Id: new("division"), Expression: new("1 / this.value == 1")}).Build(),
			}}).Build())
			fieldOptions := new(descriptorpb.FieldOptions)
			if invalidField {
				proto.SetExtension(fieldOptions, validate.E_Field, (&validate.FieldRules_builder{Cel: []*validate.Rule{
					(&validate.Rule_builder{Id: new("invalid"), Expression: new("this.missing > 0")}).Build(),
				}}).Build())
			}
			file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
				Name: new("scope_test.proto"), Package: new("interceptor.scope"), Syntax: new("proto3"),
				MessageType: []*descriptorpb.DescriptorProto{{Name: new("Message"), Options: options, Field: []*descriptorpb.FieldDescriptorProto{
					{Name: new("value"), Number: new(int32(1)), Type: descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(), Options: fieldOptions},
				}}},
			}, nil)
			require.NoError(t, err)
			message := dynamicpb.NewMessage(file.Messages().Get(0))
			validator, err := newProtoValidator(message)
			if invalidField {
				require.ErrorContains(t, err, "compile protobuf validation")
				return
			}
			require.NoError(t, err)
			message.Set(message.Descriptor().Fields().ByName("value"), protoreflect.ValueOfInt64(1))
			require.NoError(t, validator.Validate(message))
		})
	}
}
