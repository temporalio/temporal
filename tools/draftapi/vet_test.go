package draftapi

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestDraftAPIVetDependencies(t *testing.T) {
	serverRoot, err := filepath.Abs("../..")
	require.NoError(t, err)
	tool := filepath.Join(t.TempDir(), "draftapi")
	build := exec.CommandContext(t.Context(), "go", "build", "-tags", "test_dep", "-o", tool, "./cmd/tools/draftapi")
	build.Dir = serverRoot
	output, err := build.CombinedOutput()
	require.NoError(t, err, "%s", output)

	for _, tc := range []struct {
		name   string
		mutate func(*descriptorpb.FileDescriptorProto, *descriptorpb.FileDescriptorProto) string
		want   string
	}{
		{name: "valid"},
		{name: "mapping", mutate: func(_, _ *descriptorpb.FileDescriptorProto) string {
			return "LegacyDescriptor"
		}, want: "message.proto: cannot map draft message Message to Go"},
		{name: "descriptor", mutate: func(_, _ *descriptorpb.FileDescriptorProto) string {
			return "malformed"
		}, want: "decode file_message_proto_rawDesc"},
		{name: "schema", mutate: func(_, schema *descriptorpb.FileDescriptorProto) string {
			schema.Extension[0].Type = descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
			return ""
		}, want: "draft option draft must have string type"},
		{name: "placement", mutate: func(file, _ *descriptorpb.FileDescriptorProto) string {
			file.EnumType = []*descriptorpb.EnumDescriptorProto{{Name: proto.String("Enum"), Options: &descriptorpb.EnumOptions{}}}
			setVetDraftOption(file.EnumType[0].Options, 50003, "feature")
			return ""
		}, want: "message.proto.enum_type[0].options: draft annotations are supported only on fields and messages"},
		{name: "extension", mutate: func(file, _ *descriptorpb.FileDescriptorProto) string {
			file.Extension = []*descriptorpb.FieldDescriptorProto{{Name: proto.String("extension"), Extendee: proto.String(".Message"), Options: &descriptorpb.FieldOptions{}}}
			setVetDraftOption(file.Extension[0].Options, 50001, "feature")
			return ""
		}, want: "message.proto.extension[0]: draft annotations are unsupported on extensions"},
		{name: "empty-token", mutate: func(file, _ *descriptorpb.FileDescriptorProto) string {
			setVetDraftOption(file.MessageType[0].Field[0].Options, 50001, "")
			return ""
		}, want: "message.proto.message_type[0].field[0].options: draft feature must be a nonempty whitespace-free token"},
		{name: "whitespace-token", mutate: func(file, _ *descriptorpb.FileDescriptorProto) string {
			setVetDraftOption(file.MessageType[0].Field[0].Options, 50001, "feature name")
			return ""
		}, want: "message.proto.message_type[0].field[0].options: draft feature must be a nonempty whitespace-free token"},
		{name: "schema-error-preserves-options", mutate: func(file, schema *descriptorpb.FileDescriptorProto) string {
			schema.EnumType = []*descriptorpb.EnumDescriptorProto{{Name: proto.String("Enum"), Options: &descriptorpb.EnumOptions{}}}
			setVetDraftOption(schema.EnumType[0].Options, 50003, "feature")
			setVetDraftOption(file.MessageType[0].Field[0].Options, 50001, "")
			return ""
		}, want: "message.proto.message_type[0].field[0].options: draft feature must be a nonempty whitespace-free token"},
		{name: "public-import", mutate: func(file, schema *descriptorpb.FileDescriptorProto) string {
			// Option facts would not reach files that see the schema only through this import.
			file.Dependency = []string{schema.GetName()}
			file.PublicDependency = []int32{0}
			return ""
		}, want: "message.proto: public import of temporal/api_next/protometa/v1/draft.proto is not supported; import it directly"},
		{name: "mapping-error-preserves-options", mutate: func(_, schema *descriptorpb.FileDescriptorProto) string {
			schema.MessageType = []*descriptorpb.DescriptorProto{{Name: proto.String("SchemaDraft"), Options: &descriptorpb.MessageOptions{}}}
			setVetDraftOption(schema.MessageType[0].Options, 50002, "feature")
			return "LegacyDescriptor"
		}, want: "message.proto: cannot map draft message Message to Go"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			writeVetFixture(t, dir, tc.mutate)
			for _, target := range []string{"./direct", "./transitive"} {
				for range 2 {
					cmd := exec.CommandContext(t.Context(), "go", "vet", "-tags", "test_dep", "-vettool="+tool, target)
					cmd.Dir = dir
					output, err := cmd.CombinedOutput()
					if tc.want == "" {
						require.NoError(t, err, "%s", output)
					} else {
						require.Error(t, err, "%s", output)
						require.Equal(t, 1, strings.Count(string(output), tc.want), "%s", output)
					}
					require.NotContains(t, string(output), "use of draft API")
				}
			}
			// Exercise the real lint target with dependency-only API analysis.
			cmd := exec.CommandContext(t.Context(), "make", "-f", filepath.Join(serverRoot, "Makefile"), "-o", tool,
				"ROOT="+serverRoot, "DRAFTAPI="+tool, "LINT_CODE_TARGETS=./transitive", "lint-draft-api")
			cmd.Dir = dir
			output, err := cmd.CombinedOutput()
			if tc.want == "" {
				require.NoError(t, err, "%s", output)
			} else {
				require.Error(t, err, "%s", output)
				require.Contains(t, string(output), tc.want)
			}
			require.NotContains(t, string(output), "use of draft API")
		})
	}
}

func setVetDraftOption(options proto.Message, number protowire.Number, feature string) {
	options.ProtoReflect().SetUnknown(protowire.AppendString(protowire.AppendTag(nil, number, protowire.BytesType), feature))
}

func writeVetFixture(t *testing.T, dir string, mutate func(*descriptorpb.FileDescriptorProto, *descriptorpb.FileDescriptorProto) string) {
	t.Helper()
	schema := &descriptorpb.FileDescriptorProto{Name: proto.String("temporal/api_next/protometa/v1/draft.proto"), Package: proto.String("temporal.api.protometa.v1")}
	for index, extendee := range []string{"FieldOptions", "MessageOptions", "EnumOptions"} {
		schema.Extension = append(schema.Extension, &descriptorpb.FieldDescriptorProto{
			Name:   proto.String([]string{"draft", "draft_message", "draft_enum"}[index]),
			Number: proto.Int32(int32(50001 + index)), Extendee: proto.String(".google.protobuf." + extendee),
			Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		})
	}
	file := &descriptorpb.FileDescriptorProto{Name: proto.String("message.proto"), Package: proto.String("temporal.api.test.v1"), MessageType: []*descriptorpb.DescriptorProto{{
		Name: proto.String("Message"), Field: []*descriptorpb.FieldDescriptorProto{{Name: proto.String("draft"), Number: proto.Int32(1), Options: &descriptorpb.FieldOptions{}}},
	}}}
	setVetDraftOption(file.MessageType[0].Field[0].Options, 50001, "feature")
	accessor := "Descriptor"
	if mutate != nil {
		if replacement := mutate(file, schema); replacement != "" {
			accessor = replacement
		}
	}
	schemaBytes, err := proto.Marshal(schema)
	require.NoError(t, err)
	fileBytes, err := proto.Marshal(file)
	require.NoError(t, err)
	if accessor == "malformed" {
		fileBytes = []byte{0xff}
		accessor = "Descriptor"
	}
	files := map[string]string{
		"go.mod":                       "module go.temporal.io/server\ngo 1.26.4\nrequire (\ngo.temporal.io/api v0.0.0\nexample.com/bridge v0.0.0\n)\nreplace go.temporal.io/api => ./api\nreplace example.com/bridge => ./bridge\n",
		"api/go.mod":                   "module go.temporal.io/api\ngo 1.26.4\n",
		"api/protometa/v1/draft.pb.go": fmt.Sprintf("// Code generated by fixture. DO NOT EDIT.\npackage protometa\nconst file_draft_proto_rawDesc = %q\n", schemaBytes),
		"api/test/v1/message.pb.go":    fmt.Sprintf("// Code generated by fixture. DO NOT EDIT.\npackage test\nimport _ %q\ntype Message struct { Draft string `protobuf:\"bytes,1,opt,name=draft,proto3\"` }\nconst file_message_proto_rawDesc = %q\nfunc file_message_proto_rawDescGZIP() []byte { return []byte(file_message_proto_rawDesc) }\nfunc (*Message) %s() ([]byte, []int) { return file_message_proto_rawDescGZIP(), []int{0} }\n", "go.temporal.io/api/protometa/v1", fileBytes, accessor),
		"bridge/go.mod":                "module example.com/bridge\ngo 1.26.4\nrequire go.temporal.io/api v0.0.0\n",
		"bridge/first/first.go":        "package first\nimport _ \"go.temporal.io/api/test/v1\"\n",
		"bridge/second/second.go":      "package second\nimport _ \"go.temporal.io/api/test/v1\"\n",
		"bridge/diamond/diamond.go":    "package diamond\nimport (_ \"example.com/bridge/first\"; _ \"example.com/bridge/second\")\n",
		"direct/direct.go":             "package direct\nimport _ \"go.temporal.io/api/test/v1\"\n",
		"transitive/transitive.go":     "package transitive\nimport _ \"example.com/bridge/diamond\"\n",
	}
	for name, contents := range files {
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		require.NoError(t, os.WriteFile(path, []byte(contents), 0600))
	}
}
