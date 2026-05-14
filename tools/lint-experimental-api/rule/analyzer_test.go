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

func TestAllowsExperiment(t *testing.T) {
	tests := []struct {
		name       string
		comment    string
		experiment string
		want       bool
	}{
		{
			name:       "matching experiment",
			comment:    "// temporal:allow-experimental-api example",
			experiment: "example",
			want:       true,
		},
		{
			name:       "matching experiment with trailing text",
			comment:    "// temporal:allow-experimental-api example -- intentional functional test",
			experiment: "example",
			want:       true,
		},
		{
			name:       "wrong experiment",
			comment:    "// temporal:allow-experimental-api other -- intentional functional test",
			experiment: "example",
			want:       false,
		},
		{
			name:       "missing experiment",
			comment:    "// temporal:allow-experimental-api",
			experiment: "example",
			want:       false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := allowsExperiment(test.comment, test.experiment); got != test.want {
				t.Fatalf("allowsExperiment() = %v, want %v", got, test.want)
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
	fieldOptions.ProtoReflect().SetUnknown(experimentalStringOption("experiment-a"))
	enumValueOptions := descriptorSet.File[0].EnumType[0].Value[1].Options
	enumValueOptions.ProtoReflect().SetUnknown(experimentalStringOption("experiment-b"))

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

func experimentalStringOption(value string) []byte {
	return protowire.AppendString(protowire.AppendTag(nil, experimentalExtensionNumber, protowire.BytesType), value)
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
      name: "STATUS_EXPERIMENTAL"
      number: 1
      options: {}
    }
  }
}
`
