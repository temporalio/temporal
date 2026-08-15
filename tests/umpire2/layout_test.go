package umpire2

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPackageLayout(t *testing.T) {
	repoRoot := repositoryRoot(t)

	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire"))
	require.DirExists(t, filepath.Join(repoRoot, "tests", "umpire1"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire1", "protocolv2"))
	require.DirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "internal", "protocol"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "protocol"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "assurance"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "fact"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "model"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "rule"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "action"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "planner"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "ksdriver"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "regress", "rpc"))
	require.NoFileExists(t, filepath.Join(repoRoot, "tests", "umpire2", "entity_key.go"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "genmodels"))
	require.NoDirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "genmodels-results"))
	require.DirExists(t, filepath.Join(repoRoot, "tests", "umpire2", "testdata", "genmodels"))
	require.DirExists(t, filepath.Join(repoRoot, "common", "testing", "umpire", "verify", "toolchain"))
	require.NoDirExists(t, filepath.Join(repoRoot, "common", "testing", "umpire", "verify", "runner"))
	require.NoDirExists(t, filepath.Join(repoRoot, "common", "testing", "umpire", "verify", "tla"))
	require.NoDirExists(t, filepath.Join(repoRoot, "common", "testing", "umpire", "verify", "p"))
	require.NoDirExists(t, filepath.Join(repoRoot, "common", "testing", "umpire", "verify", "ivy"))
	require.NoDirExists(t, filepath.Join(repoRoot, "common", "testing", "umpire", "verify", "fizz"))

	requirePackageNames(t, filepath.Join(repoRoot, "tests", "umpire1"), "umpire1")
	requirePackageNames(t, filepath.Join(repoRoot, "tests", "umpire2", "internal", "protocol"), "protocol", "protocol_test")
	requireNoProductionImports(t, filepath.Join(repoRoot, "tests", "umpire2"), "go.temporal.io/server/tests/umpire1")
	requireNoRootProductionImports(t, filepath.Join(repoRoot, "tests", "umpire2"), "go.temporal.io/server/tests/testcore")
	requireNoExactProductionImport(t, filepath.Join(repoRoot, "tests", "umpire2", "internal"), "go.temporal.io/server/tests/testcore")
	requireNoProductionImports(t, filepath.Join(repoRoot, "common", "testing", "umpire"), "go.temporal.io/server/tests/umpire2")
	requireNoExactProductionImport(t, filepath.Join(repoRoot, "common", "testing", "umpire"), "go.temporal.io/server/common/log")
	requireNoExactProductionImport(t, filepath.Join(repoRoot, "common", "testing", "umpire"), "go.temporal.io/api/common/v1")
	requireNoRootProductionImports(t, filepath.Join(repoRoot, "common", "testing", "umpire"), "google.golang.org/grpc")
	requireNoRootProductionImports(t, filepath.Join(repoRoot, "common", "testing", "umpire"), "go.opentelemetry.io/otel")
	requireNoGenericChangedCalls(t, filepath.Join(repoRoot, "tests", "umpire2", "internal", "rule"))
}

func repositoryRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func requirePackageNames(t *testing.T, directory string, allowed ...string) {
	t.Helper()

	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	allowedNames := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedNames[name] = struct{}{}
	}

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		filename := filepath.Join(directory, entry.Name())
		parsed, err := parser.ParseFile(token.NewFileSet(), filename, nil, parser.PackageClauseOnly)
		require.NoError(t, err)
		_, ok := allowedNames[parsed.Name.Name]
		require.Truef(t, ok, "%s declares unexpected package %q", filename, parsed.Name.Name)
	}
}

func requireNoGenericChangedCalls(t *testing.T, directory string) {
	t.Helper()

	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		require.NoError(t, err)
		ast.Inspect(parsed, func(node ast.Node) bool {
			indexed, ok := node.(*ast.IndexExpr)
			if !ok {
				return true
			}
			selector, ok := indexed.X.(*ast.SelectorExpr)
			if ok && selector.Sel.Name == "Changed" {
				require.Failf(t, "generic Changed call", "%s calls analyzer-incompatible Changed[T]", path)
			}
			return true
		})
		return nil
	})
	require.NoError(t, err)
}

func requireNoExactProductionImport(t *testing.T, directory string, forbidden string) {
	t.Helper()

	visitProductionImports(t, directory, func(path, importPath string) {
		require.NotEqualf(t, forbidden, importPath, "%s imports forbidden package %q", path, importPath)
	})
}

func requireNoProductionImports(t *testing.T, directory string, forbidden string) {
	t.Helper()

	visitProductionImports(t, directory, func(path, importPath string) {
		require.Falsef(
			t,
			importPath == forbidden || strings.HasPrefix(importPath, forbidden+"/"),
			"%s imports forbidden v1 package %q",
			path,
			importPath,
		)
	})
}

func requireNoRootProductionImports(t *testing.T, directory string, forbidden string) {
	t.Helper()

	entries, err := os.ReadDir(directory)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		require.NoError(t, err)
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			require.NoError(t, err)
			require.Falsef(
				t,
				importPath == forbidden || strings.HasPrefix(importPath, forbidden+"/"),
				"%s imports forbidden adapter package %q",
				path,
				importPath,
			)
		}
	}
}

func visitProductionImports(t *testing.T, directory string, visit func(path, importPath string)) {
	t.Helper()

	err := filepath.WalkDir(directory, func(path string, entry os.DirEntry, err error) error {
		require.NoError(t, err)
		if entry.IsDir() || filepath.Ext(path) != ".go" || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		require.NoError(t, err)
		for _, imported := range parsed.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			require.NoError(t, err)
			visit(path, importPath)
		}
		return nil
	})
	require.NoError(t, err)
}
