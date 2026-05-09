package rule

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestAnalyzer(t *testing.T) {
	descriptorPath := writeTestDescriptorSet(t)
	oldDescriptorSetPaths := descriptorSetPaths
	descriptorSetPaths = func() ([]string, error) {
		return []string{descriptorPath}, nil
	}
	t.Cleanup(func() {
		descriptorSetPaths = oldDescriptorSetPaths
	})

	analysistest.Run(t, analysistest.TestData(), Analyzer, "useexp")
}

func TestAllowsDraft(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		draft   string
		want    bool
	}{
		{
			name:    "matching draft",
			comment: "// temporal:allow-draft-api example",
			draft:   "example",
			want:    true,
		},
		{
			name:    "matching draft with trailing text",
			comment: "// temporal:allow-draft-api example -- intentional functional test",
			draft:   "example",
			want:    true,
		},
		{
			name:    "wrong draft",
			comment: "// temporal:allow-draft-api other -- intentional functional test",
			draft:   "example",
			want:    false,
		},
		{
			name:    "missing draft",
			comment: "// temporal:allow-draft-api",
			draft:   "example",
			want:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := allowsDraft(test.comment, test.draft); got != test.want {
				t.Fatalf("allowsDraft() = %v, want %v", got, test.want)
			}
		})
	}
}

func writeTestDescriptorSet(t *testing.T) string {
	t.Helper()

	descriptorSet := &descriptorpb.FileDescriptorSet{}
	if err := prototext.Unmarshal([]byte(testDescriptorSet), descriptorSet); err != nil {
		t.Fatal(err)
	}
	fieldOptions := descriptorSet.File[0].MessageType[0].Field[0].Options
	fieldOptions.ProtoReflect().SetUnknown(draftStringOption("draft-a"))
	enumValueOptions := descriptorSet.File[0].EnumType[0].Value[1].Options
	enumValueOptions.ProtoReflect().SetUnknown(draftStringOption("draft-b"))

	data, err := proto.Marshal(descriptorSet)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "api.binpb")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func draftStringOption(value string) []byte {
	return protowire.AppendString(protowire.AppendTag(nil, draftExtensionNumber, protowire.BytesType), value)
}

const testDescriptorSet = `
file: {
  name: "temporal/api/example/v1/message.proto"
  package: "temporal.api.example.v1"
  syntax: "proto3"
  options: {
    go_package: "go.temporal.io/api/example/v1;example"
  }
  message_type: {
    name: "Request"
    field: {
      name: "new_field"
      number: 1
      label: LABEL_OPTIONAL
      type: TYPE_STRING
      options: {}
    }
  }
  enum_type: {
    name: "Status"
    value: {
      name: "STATUS_UNSPECIFIED"
      number: 0
    }
    value: {
      name: "STATUS_DRAFT"
      number: 1
      options: {}
    }
  }
}
`
