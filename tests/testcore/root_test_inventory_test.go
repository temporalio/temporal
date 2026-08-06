package testcore

import (
	_ "embed"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
)

//go:embed root_test_names.txt
var rootTestNamesBaseline string

func TestRootTestWrappersMatchBaseline(t *testing.T) {
	expected := strings.Fields(rootTestNamesBaseline)
	assertSortedUniqueRootTestNames(t, "baseline", expected)
	for _, name := range expected {
		if strings.ContainsAny(name, "/ ") {
			t.Fatalf("baseline root test name %q is not safe for testing.T.Run", name)
		}
	}

	wrappers := rootTestWrappers(t)
	wrapperNames := make([]string, len(wrappers))
	for i, wrapper := range wrappers {
		wrapperNames[i] = wrapper.name
	}
	assertSortedUniqueRootTestNames(t, "wrappers", wrapperNames)
	if !slices.Equal(expected, wrapperNames) {
		t.Fatalf("root test wrappers differ from frozen baseline\nwant: %v\ngot:  %v", expected, wrapperNames)
	}

	entries := functionalTestRegistryEntries(t)
	registryNames := make([]string, len(entries))
	registryByName := make(map[string]string, len(entries))
	for i, entry := range entries {
		registryNames[i] = entry.name
		registryByName[entry.name] = entry.run
	}
	assertSortedUniqueRootTestNames(t, "registry", registryNames)
	if !slices.Equal(expected, registryNames) {
		t.Fatalf("functional test registry differs from frozen baseline\nwant: %v\ngot:  %v", expected, registryNames)
	}
	for _, wrapper := range wrappers {
		if registryByName[wrapper.name] != wrapper.run {
			t.Errorf("registry entry %q invokes %q, want wrapper callback %q", wrapper.name, registryByName[wrapper.name], wrapper.run)
		}
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

type rootTestWrapper struct {
	name string
	run  string
}

func rootTestWrappers(t *testing.T) []rootTestWrapper {
	t.Helper()
	fileSet := token.NewFileSet()
	path := rootTestSourcePath(t, "root_test_wrappers_test.go")
	file, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatalf("parse root test wrapper source %s: %v", path, err)
	}
	var wrappers []rootTestWrapper
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if function.Recv != nil || !strings.HasPrefix(function.Name.Name, "Test") {
			t.Fatalf("root test source %s contains non-wrapper function %s", path, function.Name.Name)
		}
		run, ok := thinRootTestWrapperCallback(function)
		if !ok {
			t.Fatalf("root test source %s contains non-thin wrapper %s", path, function.Name.Name)
		}
		wrappers = append(wrappers, rootTestWrapper{name: function.Name.Name, run: run})
	}
	return wrappers
}

type functionalTestRegistryEntry struct {
	name string
	run  string
}

func functionalTestRegistryEntries(t *testing.T) []functionalTestRegistryEntry {
	t.Helper()
	path := rootTestSourcePath(t, "functional_test_registry.go")
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse functional test registry source %s: %v", path, err)
	}

	for _, declaration := range file.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.VAR {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok || len(value.Names) != 1 || value.Names[0].Name != "functionalTestEntries" || len(value.Values) != 1 {
				continue
			}
			return parseFunctionalTestRegistryEntries(t, value.Values[0])
		}
	}
	t.Fatal("functional test registry declaration not found")
	return nil
}

func parseFunctionalTestRegistryEntries(t *testing.T, expression ast.Expr) []functionalTestRegistryEntry {
	t.Helper()
	entries, ok := expression.(*ast.CompositeLit)
	if !ok {
		t.Fatal("functional test registry is not a composite literal")
	}
	result := make([]functionalTestRegistryEntry, 0, len(entries.Elts))
	for _, element := range entries.Elts {
		entry, ok := element.(*ast.CompositeLit)
		if !ok {
			t.Fatal("functional test registry contains a non-composite entry")
		}
		var resultEntry functionalTestRegistryEntry
		for _, field := range entry.Elts {
			pair, ok := field.(*ast.KeyValueExpr)
			if !ok {
				t.Fatal("functional test registry entry contains an unkeyed field")
			}
			key, ok := pair.Key.(*ast.Ident)
			if !ok {
				t.Fatal("functional test registry entry key is not an identifier")
			}
			switch key.Name {
			case "Name":
				literal, ok := pair.Value.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					t.Fatal("functional test registry entry name is not a string literal")
				}
				name, err := strconv.Unquote(literal.Value)
				if err != nil {
					t.Fatalf("unquote functional test registry name: %v", err)
				}
				resultEntry.name = name
			case "run":
				function, ok := pair.Value.(*ast.Ident)
				if !ok {
					t.Fatal("functional test registry callback is not an identifier")
				}
				resultEntry.run = function.Name
			default:
				t.Fatalf("functional test registry entry has unexpected field %q", key.Name)
			}
		}
		if resultEntry.name == "" || resultEntry.run == "" {
			t.Fatal("functional test registry entry is missing Name or run")
		}
		result = append(result, resultEntry)
	}
	return result
}

func rootTestSourcePath(t *testing.T, name string) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locating root test source")
	}
	return filepath.Join(filepath.Dir(filepath.Dir(sourceFile)), name)
}

func thinRootTestWrapperCallback(function *ast.FuncDecl) (string, bool) {
	if function.Type.TypeParams != nil || function.Type.Results != nil || len(function.Type.Params.List) != 1 || function.Body == nil || len(function.Body.List) != 1 {
		return "", false
	}
	parameter := function.Type.Params.List[0]
	if len(parameter.Names) != 1 || parameter.Names[0].Name != "t" {
		return "", false
	}
	pointer, ok := parameter.Type.(*ast.StarExpr)
	if !ok {
		return "", false
	}
	selector, ok := pointer.X.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok || pkg.Name != "testing" || selector.Sel.Name != "T" {
		return "", false
	}
	expression, ok := function.Body.List[0].(*ast.ExprStmt)
	if !ok {
		return "", false
	}
	call, ok := expression.X.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 || call.Ellipsis.IsValid() {
		return "", false
	}
	callee, ok := call.Fun.(*ast.Ident)
	if !ok || !strings.HasPrefix(callee.Name, "runTest") {
		return "", false
	}
	argument, ok := call.Args[0].(*ast.Ident)
	if !ok || argument.Name != "t" {
		return "", false
	}
	return callee.Name, true
}
