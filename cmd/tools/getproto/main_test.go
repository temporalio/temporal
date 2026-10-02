package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestExportedDescriptorsResolveValidationImports(t *testing.T) {
	t.Parallel()

	output := filepath.Join(t.TempDir(), "api.binpb")
	command := exec.CommandContext(t.Context(), "go", "run", "./cmd/tools/getproto", "-out", output)
	command.Dir = "../../.."
	result, err := command.CombinedOutput()
	require.NoError(t, err, "%s", result)
	encoded, err := os.ReadFile(output)
	require.NoError(t, err)
	set := &descriptorpb.FileDescriptorSet{}
	require.NoError(t, proto.Unmarshal(encoded, set))
	files, err := protodesc.NewFiles(set)
	require.NoError(t, err)
	_, err = files.FindFileByPath("buf/validate/validate.proto")
	require.NoError(t, err)
}
