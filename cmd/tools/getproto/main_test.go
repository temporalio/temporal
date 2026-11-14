package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	expmaps "golang.org/x/exp/maps"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	checkImportsDirEnv  = "GETPROTO_TEST_CHECK_IMPORTS_DIR"
	checkImportsDropEnv = "GETPROTO_TEST_CHECK_IMPORTS_DROP"
	checkImportsDone    = "<checked>"
)

// checkImports exits the process when imports are missing, so
// TestCheckImportsFollowsLinkedImports runs it in a child process. The child
// runs here, before the testing package is set up, because the testing
// package turns os.Exit(0) inside a test into a panic.
func TestMain(m *testing.M) {
	if dir := os.Getenv(checkImportsDirEnv); dir != "" {
		fatalIfErr(os.Chdir(dir))
		files := linkedClosure(workflowservice.File_temporal_api_next_workflowservice_v1_request_response_proto)
		delete(files, os.Getenv(checkImportsDropEnv))
		checkImports(files) // exits after printing <rerun> if an import is missing
		fmt.Println(checkImportsDone)
		return
	}
	m.Run()
}

// linkedClosure returns root and its transitive imports keyed by stable path,
// the way files.go keys importMap.
func linkedClosure(root protoreflect.FileDescriptor) map[string]protoreflect.FileDescriptor {
	files := make(map[string]protoreflect.FileDescriptor)
	var visit func(protoreflect.FileDescriptor)
	visit = func(file protoreflect.FileDescriptor) {
		if _, ok := files[stablePath(file.Path())]; ok {
			return
		}
		files[stablePath(file.Path())] = file
		for i := 0; i < file.Imports().Len(); i++ {
			visit(file.Imports().Get(i).FileDescriptor)
		}
	}
	visit(root)
	return files
}

func TestFindProtoImports(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	for _, dir := range []string{"proto/internal", "common/draft/testfixture"} {
		require.NoError(t, os.MkdirAll(dir, 0755))
	}
	require.NoError(t, os.WriteFile(filepath.Join("proto/internal", "root.proto"), []byte(`import "temporal/api/common/v1/message.proto";
import "google/protobuf/any.proto";
import "nexusannotations/v1/options.proto";
import "temporal/server/api/internal.proto";
`), 0600))
	require.NoError(t, os.WriteFile(filepath.Join("common/draft/testfixture", "fixture.proto"), []byte(`import "temporal/api/protometa/v1/draft.proto";`), 0600))
	require.ElementsMatch(t, []string{"temporal/api/common/v1/message.proto", "google/protobuf/any.proto", "nexusannotations/v1/options.proto", "temporal/api/protometa/v1/draft.proto"}, findProtoImports())
}

func TestDescriptorPaths(t *testing.T) {
	require.Equal(t, "File_temporal_api_next_common_v1_message_proto", mangle("temporal/api/common/v1/message.proto"))
	require.Equal(t, "File_google_protobuf_any_proto", mangle("google/protobuf/any.proto"))
	require.Equal(t, "temporal/api/common/v1/message.proto", stablePath("temporal/api_next/common/v1/message.proto"))
	require.Equal(t, "google/protobuf/any.proto", stablePath("google/protobuf/any.proto"))
}

func TestDescriptorSetMapping(t *testing.T) {
	files := make(map[string]protoreflect.FileDescriptor)
	var visit func(protoreflect.FileDescriptor)
	visit = func(file protoreflect.FileDescriptor) {
		if _, ok := files[file.Path()]; ok {
			return
		}
		files[file.Path()] = file
		for i := 0; i < file.Imports().Len(); i++ {
			visit(file.Imports().Get(i).FileDescriptor)
		}
	}
	visit(commonpb.File_temporal_api_next_common_v1_message_proto)
	set := descriptorSet(files)
	_, err := protodesc.NewFiles(set)
	require.NoError(t, err)
	for _, file := range set.File {
		require.NotContains(t, file.GetName(), "temporal/api_next/")
		for _, dependency := range file.Dependency {
			require.NotContains(t, dependency, "temporal/api_next/")
		}
	}
}

// The linked files import each other under temporal/api_next/ paths, but
// importMap is keyed by stable paths. checkImports must translate each linked
// import before looking it up, or a missing transitive dependency is never
// added to files.go.
func TestCheckImportsFollowsLinkedImports(t *testing.T) {
	root := workflowservice.File_temporal_api_next_workflowservice_v1_request_response_proto
	var drop string
	for i := 0; i < root.Imports().Len(); i++ {
		if path := root.Imports().Get(i).Path(); strings.HasPrefix(path, "temporal/api_next/") {
			drop = stablePath(path)
			break
		}
	}
	require.NotEmpty(t, drop, "fixture root must import an api_next file")

	run := func(t *testing.T, drop string) (string, string) {
		dir := t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(dir, "cmd/tools/getproto"), 0755))
		cmd := exec.Command(os.Args[0], "-test.run=^$")
		cmd.Env = append(os.Environ(), checkImportsDirEnv+"="+dir, checkImportsDropEnv+"="+drop)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, string(out))
		generated, err := os.ReadFile(filepath.Join(dir, "cmd/tools/getproto/files.go"))
		if os.IsNotExist(err) {
			return string(out), ""
		}
		require.NoError(t, err)
		return string(out), string(generated)
	}

	t.Run("complete closure", func(t *testing.T) {
		out, generated := run(t, "")
		require.Contains(t, out, checkImportsDone)
		require.NotContains(t, out, "<rerun>")
		require.Empty(t, generated)
	})
	t.Run("missing transitive import", func(t *testing.T) {
		out, generated := run(t, drop)
		require.Contains(t, out, "<rerun>")
		require.NotContains(t, out, checkImportsDone)
		require.Contains(t, generated, fmt.Sprintf("importMap[%q] = ", drop))
		require.NotContains(t, generated, `importMap["temporal/api_next/`)
	})
}

// The descriptor set is written to api.binpb, which make tracks for changes,
// so its file order must not depend on map iteration.
func TestDescriptorSetOrder(t *testing.T) {
	files := linkedClosure(workflowservice.File_temporal_api_next_workflowservice_v1_request_response_proto)
	want := expmaps.Keys(files)
	slices.Sort(want)
	require.Greater(t, len(want), 10)
	for range 5 {
		var names []string
		for _, file := range descriptorSet(files).File {
			names = append(names, file.GetName())
		}
		require.Equal(t, want, names)
	}
}
