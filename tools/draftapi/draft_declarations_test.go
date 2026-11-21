package draftapi

import (
	"testing"

	"github.com/stretchr/testify/require"
	protometapb "go.temporal.io/api/protometa/v1"
	"golang.org/x/tools/go/analysis"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// One analyzer reports both declaration and Go-mapping errors for the same package.
func TestAnalyzerReportsDeclarationErrors(t *testing.T) {
	require.NoError(t, analysis.Validate([]*analysis.Analyzer{Analyzer}))
	schema, file := draftFixture()
	proto.SetExtension(file.MessageType[0].Field[0].Options, protometapb.E_Draft, "")
	file.MessageType = append(file.MessageType, &descriptorpb.DescriptorProto{Name: new("Unmapped"), Options: &descriptorpb.MessageOptions{}})
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
	}{
		{"field", &descriptorpb.FieldOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.MessageType[0].Field[0].Options = o.(*descriptorpb.FieldOptions)
		}},
		{"message", &descriptorpb.MessageOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.MessageType[0].Options = o.(*descriptorpb.MessageOptions)
		}},
		{"enum", &descriptorpb.EnumOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.EnumType[0].Options = o.(*descriptorpb.EnumOptions)
		}},
		{"enum-value", &descriptorpb.EnumValueOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.EnumType[0].Value[0].Options = o.(*descriptorpb.EnumValueOptions)
		}},
		{"oneof", &descriptorpb.OneofOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.MessageType[0].OneofDecl[0].Options = o.(*descriptorpb.OneofOptions)
		}},
		{"service", &descriptorpb.ServiceOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.Service[0].Options = o.(*descriptorpb.ServiceOptions)
		}},
		{"method", &descriptorpb.MethodOptions{}, func(f *descriptorpb.FileDescriptorProto, o proto.Message) {
			f.Service[0].Method[0].Options = o.(*descriptorpb.MethodOptions)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := &descriptorpb.FileDescriptorProto{Name: new("test.proto"),
				MessageType: []*descriptorpb.DescriptorProto{{Name: new("Message"), Field: []*descriptorpb.FieldDescriptorProto{{Name: new("field")}}, OneofDecl: []*descriptorpb.OneofDescriptorProto{{Name: new("choice")}}}},
				EnumType:    []*descriptorpb.EnumDescriptorProto{{Name: new("Enum"), Value: []*descriptorpb.EnumValueDescriptorProto{{Name: new("VALUE")}}}},
				Service:     []*descriptorpb.ServiceDescriptorProto{{Name: new("Service"), Method: []*descriptorpb.MethodDescriptorProto{{Name: new("Method")}}}},
			}
			number := int32(50001)
			c := &catalog{options: []option{{Extendee: string(tc.options.ProtoReflect().Descriptor().FullName()), Number: number}}}
			tc.attach(file, tc.options)
			require.NoError(t, c.checkDeclarations(map[string]*descriptorpb.FileDescriptorProto{"test": file}))
			data := protowire.AppendTag(nil, protowire.Number(number), protowire.BytesType)
			tc.options.ProtoReflect().SetUnknown(protowire.AppendString(data, "feature"))
			require.NoError(t, c.checkDeclarations(map[string]*descriptorpb.FileDescriptorProto{"test": file}))
		})
	}
}

func TestDraftFeatureTokens(t *testing.T) {
	for _, feature := range []string{"", "feature a", " feature", "feature\tname", "feature\u00a0name"} {
		opts := &descriptorpb.FieldOptions{}
		data := protowire.AppendTag(nil, 50001, protowire.BytesType)
		opts.ProtoReflect().SetUnknown(protowire.AppendString(data, feature))
		c := &catalog{options: []option{{Extendee: "google.protobuf.FieldOptions", Number: 50001}}}
		fd := &descriptorpb.FileDescriptorProto{Name: new("test.proto"), MessageType: []*descriptorpb.DescriptorProto{{Field: []*descriptorpb.FieldDescriptorProto{{Options: opts}}}}}
		require.ErrorContains(t, c.checkDeclarations(map[string]*descriptorpb.FileDescriptorProto{"test": fd}), "whitespace-free token")
	}
}

func TestDraftExtensionAnnotation(t *testing.T) {
	opts := &descriptorpb.FieldOptions{}
	data := protowire.AppendTag(nil, 50001, protowire.BytesType)
	opts.ProtoReflect().SetUnknown(protowire.AppendString(data, "feature"))
	field := &descriptorpb.FieldDescriptorProto{Extendee: new("Draft"), Options: opts}
	c := &catalog{options: []option{{Extendee: "google.protobuf.FieldOptions", Number: 50001}}}
	require.ErrorContains(t, c.checkDraftOptions(field, "extension"), "unsupported on extensions")
}

func TestDraftAnnotationsInDraftContainers(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		parent protoreflect.ExtensionType
		child  protoreflect.ExtensionType
	}{
		{name: "draft message field", parent: protometapb.E_DraftMessage, child: protometapb.E_Draft},
		{name: "draft oneof member", parent: protometapb.E_DraftOneof, child: protometapb.E_Draft},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			schema, file := draftFixture()
			message := file.MessageType[0]
			if tc.parent == protometapb.E_DraftMessage {
				proto.SetExtension(message.Options, tc.parent, "feature")
			} else {
				message.OneofDecl = []*descriptorpb.OneofDescriptorProto{{Name: new("choice"), Options: &descriptorpb.OneofOptions{}}}
				message.Field[0].OneofIndex = new(int32(0))
				proto.SetExtension(message.OneofDecl[0].Options, tc.parent, "feature")
			}
			proto.SetExtension(message.Field[0].Options, tc.child, "feature")
			diagnostics, _ := runDraftFixture(t, schema, file)
			require.Len(t, diagnostics, 1)
			require.Contains(t, diagnostics[0], "draft members cannot declare their own feature")
		})
	}
}
