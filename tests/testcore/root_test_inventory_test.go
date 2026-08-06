package testcore

import (
	_ "embed"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

//go:embed root_test_names.txt
var rootTestNamesBaseline string

func TestRootTestWrappersMatchBaseline(t *testing.T) {
	expected := strings.Fields(rootTestNamesBaseline)
	assertSortedUniqueRootTestNames(t, "baseline", expected)

	actual := rootTestWrapperNames(t)
	assertSortedUniqueRootTestNames(t, "wrappers", actual)
	if !slices.Equal(expected, actual) {
		t.Fatalf("root test wrappers differ from frozen baseline\nwant: %v\ngot:  %v", expected, actual)
	}
}

func assertSortedUniqueRootTestNames(t *testing.T, source string, names []string) {
	t.Helper()
	if !slices.IsSorted(names) {
		t.Fatalf("%s root test names are not sorted: %v", source, names)
	}
	for i := 1; i < len(names); i++ {
		if names[i-1] == names[i] {
			t.Fatalf("%s root test names contain duplicate %q", source, names[i])
		}
	}
}

func rootTestWrapperNames(t *testing.T) []string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locating root test wrapper source")
	}
	root := filepath.Dir(filepath.Dir(sourceFile))
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read root test wrapper source directory: %v", err)
	}

	fileSet := token.NewFileSet()
	var names []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(root, entry.Name())
		file, err := parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			t.Fatalf("parse root test wrapper source %s: %v", path, err)
		}
		for _, declaration := range file.Decls {
			function, ok := declaration.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if function.Recv != nil || !strings.HasPrefix(function.Name.Name, "Test") {
				t.Fatalf("root test source %s contains non-wrapper function %s", path, function.Name.Name)
			}
			if !isThinRootTestWrapper(function) {
				t.Fatalf("root test source %s contains non-thin wrapper %s", path, function.Name.Name)
			}
			names = append(names, function.Name.Name)
		}
	}
	return names
}

func isThinRootTestWrapper(function *ast.FuncDecl) bool {
	if function.Type.TypeParams != nil || function.Type.Results != nil || len(function.Type.Params.List) != 1 || function.Body == nil || len(function.Body.List) != 1 {
		return false
	}
	parameter := function.Type.Params.List[0]
	if len(parameter.Names) != 1 || parameter.Names[0].Name != "t" {
		return false
	}
	pointer, ok := parameter.Type.(*ast.StarExpr)
	if !ok {
		return false
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "testing" || selector.Sel.Name != "T" {
		return false
	}
	expression, ok := function.Body.List[0].(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := expression.X.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 || call.Ellipsis.IsValid() {
		return false
	}
	callee, ok := call.Fun.(*ast.Ident)
	if !ok || callee.Name != "run"+function.Name.Name {
		return false
	}
	argument, ok := call.Args[0].(*ast.Ident)
	return ok && argument.Name == "t"
}
