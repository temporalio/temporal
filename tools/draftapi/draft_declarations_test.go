package draftapi

import (
	"testing"

	"github.com/stretchr/testify/require"
	protometapb "go.temporal.io/api/protometa/v1"
	"golang.org/x/tools/go/analysis"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

// One analyzer reports both declaration and Go-mapping errors for the same package.
func TestAnalyzerReportsDeclarationErrors(t *testing.T) {
	require.NoError(t, analysis.Validate([]*analysis.Analyzer{Analyzer}))
	schema, file := draftFixture()
	proto.SetExtension(file.MessageType[0].Field[0].Options, protometapb.E_Draft, "")
	file.MessageType = append(file.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("Unmapped"), Options: &descriptorpb.MessageOptions{}})
	proto.SetExtension(file.MessageType[1].Options, protometapb.E_DraftMessage, "feature")
	diagnostics, _ := runDraftFixture(t, schema, file)
	require.ElementsMatch(t, []string{
		"draft API validation failed: go.temporal.io/api/test/v1: message.proto.message_type[0].field[0].options: draft feature must be a nonempty whitespace-free token",
		"draft API validation failed: go.temporal.io/api/test/v1: message.proto: cannot map draft message Unmapped to Go; unsupported generated descriptor accessors",
	}, diagnostics)
}

func TestDraftFieldAnnotationLocations(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options proto.Message
		attach  func(*descriptorpb.FileDescriptorProto, proto.Message)
		allowed bool
	}{
		{"field", &descriptorpb.FieldOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.MessageType[0].Field[0].Options = o.(*descriptorpb.FieldOptions)
		}, true},
		{"message", &descriptorpb.MessageOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.MessageType[0].Options = o.(*descriptorpb.MessageOptions)
		}, true},
		{"enum", &descriptorpb.EnumOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.EnumType[0].Options = o.(*descriptorpb.EnumOptions)
		}, false},
		{"enum-value", &descriptorpb.EnumValueOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.EnumType[0].Value[0].Options = o.(*descriptorpb.EnumValueOptions)
		}, false},
		{"oneof", &descriptorpb.OneofOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.MessageType[0].OneofDecl[0].Options = o.(*descriptorpb.OneofOptions)
		}, false},
		{"service", &descriptorpb.ServiceOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.Service[0].Options = o.(*descriptorpb.ServiceOptions)
		}, false},
		{"method", &descriptorpb.MethodOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.Service[0].Method[0].Options = o.(*descriptorpb.MethodOptions)
		}, false},
		{"file", &descriptorpb.FileOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) { f.Options = o.(*descriptorpb.FileOptions) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := &descriptorpb.FileDescriptorProto{Name: proto.String("test.proto"),
				MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Message"), Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("field")}}, OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: proto.String("choice")}}}},
				EnumType:    []*descriptorpb.EnumDescriptorProto{{Name: proto.String("Enum"), Value: []*descriptorpb.EnumValueDescriptorProto{{Name: proto.String("VALUE")}}}},
				Service:     []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Service"), Method: []*descriptorpb.MethodDescriptorProto{{Name: proto.String("Method")}}}},
			}
			number := int32(50001)
			c := &catalog{options: []option{{Extendee: string(tc.options.ProtoReflect().Descriptor().FullName()), Number: number}}}
			tc.attach(file, tc.options)
			require.NoError(t, c.checkFieldAnnotations(file))
			data := protowire.AppendTag(nil, protowire.Number(number), protowire.BytesType)
			tc.options.ProtoReflect().SetUnknown(protowire.AppendString(data, "feature"))
			err := c.checkFieldAnnotations(file)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "draft annotations are supported only on fields and messages")
			}
		})
	}
}

func TestDraftFeatureTokens(t *testing.T) {
	for _, feature := range []string{"", "feature a", " feature", "feature\tname", "feature\u00a0name"} {
		opts := &descriptorpb.FieldOptions{}
		data := protowire.AppendTag(nil, 50001, protowire.BytesType)
		opts.ProtoReflect().SetUnknown(protowire.AppendString(data, feature))
		c := &catalog{options: []option{{Extendee: "google.protobuf.FieldOptions", Number: 50001}}}
		fd := &descriptorpb.FileDescriptorProto{Name: proto.String("test.proto"), MessageType: []*descriptorpb.DescriptorProto{{Field: []*descriptorpb.FieldDescriptorProto{{Options: opts}}}}}
		require.ErrorContains(t, c.checkFieldAnnotations(fd), "whitespace-free token")
	}
}

func TestDraftExtensionAnnotation(t *testing.T) {
	opts := &descriptorpb.FieldOptions{}
	data := protowire.AppendTag(nil, 50001, protowire.BytesType)
	opts.ProtoReflect().SetUnknown(protowire.AppendString(data, "feature"))
	field := &descriptorpb.FieldDescriptorProto{Extendee: proto.String("Draft"), Options: opts}
	c := &catalog{options: []option{{Extendee: "google.protobuf.FieldOptions", Number: 50001}}}
	require.ErrorContains(t, c.checkDraftOptions(field, "extension"), "unsupported on extensions")
}
