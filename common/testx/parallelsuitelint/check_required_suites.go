package parallelsuitelint

import (
	"go/ast"
	"go/types"
	"path"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/tools/go/analysis"
)

const testifySuitePath = "github.com/stretchr/testify/suite"

// checkRequiredSuites requires suite entrypoints unless the package allows ordinary tests.
// Benchmarks, fuzz tests, examples and TestMain retain their standard entrypoints.
func checkRequiredSuites(pass *analysis.Pass, allowedPackages string) {
	recordSuiteRunners(pass)
	if standardTestsAllowed(packageImportPath(pass), allowedPackages) {
		return
	}
	for _, file := range pass.Files {
		isTestFile := strings.HasSuffix(pass.Fset.PositionFor(file.Pos(), false).Filename, "_test.go")
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.FuncDecl:
				if isTestFile {
					checkTestEntrypoint(pass, node)
				}
			case *ast.Ident:
				checkTestifySuiteType(pass, node)
			case *ast.CallExpr:
				checkTestifyRunner(pass, node)
			default:
			}
			return true
		})
	}
}

// checkTestEntrypoint requires ordinary tests to delegate directly to a suite runner.
func checkTestEntrypoint(pass *analysis.Pass, fn *ast.FuncDecl) {
	if ordinaryTest(pass, fn) && !suiteEntrypoint(pass, fn) {
		pass.Reportf(fn.Pos(), "use parallelsuite.Run for tests; allow ordinary tests with -allow-standard-tests")
	}
}

// checkTestifySuiteType rejects legacy suite inheritance and type aliases.
func checkTestifySuiteType(pass *analysis.Pass, id *ast.Ident) {
	typ, ok := pass.TypesInfo.Uses[id].(*types.TypeName)
	if !ok || typ.Pkg() == nil {
		return
	}
	if typ.Pkg().Path() == testifySuitePath && typ.Name() == "Suite" {
		pass.Reportf(id.Pos(), "use parallelsuite.Suite instead of testify/suite.Suite")
	}
}

// checkTestifyRunner rejects the legacy testify test runner.
func checkTestifyRunner(pass *analysis.Pass, call *ast.CallExpr) {
	fn := calledFunction(pass, call.Fun)
	if fn == nil || fn.Pkg() == nil {
		return
	}
	if fn.Pkg().Path() == testifySuitePath && fn.Name() == "Run" {
		pass.Reportf(call.Pos(), "use parallelsuite.Run instead of testify/suite.Run")
	}
}

func standardTestsAllowed(pkg, patterns string) bool {
	for pattern := range strings.SplitSeq(patterns, ",") {
		pattern = strings.TrimSpace(pattern)
		if pattern == "..." || pattern == pkg {
			return true
		}
		if prefix, recursive := strings.CutSuffix(pattern, "/..."); recursive {
			if pkg == prefix || strings.HasPrefix(pkg, prefix+"/") {
				return true
			}
		}
	}
	return false
}

func packageImportPath(pass *analysis.Pass) string {
	pkg := pass.Pkg.Path()
	if !strings.HasSuffix(pkg, "_test") || len(pass.Files) == 0 {
		return pkg
	}
	dir := filepath.Dir(pass.Fset.PositionFor(pass.Files[0].Pos(), false).Filename)
	if pass.Module != nil && pass.Module.Dir != "" {
		if relative, err := filepath.Rel(pass.Module.Dir, dir); err == nil && filepath.IsLocal(relative) {
			actual := path.Join(pass.Module.Path, filepath.ToSlash(relative))
			if pkg == actual+"_test" {
				return actual
			}
			return pkg
		}
	}
	if strings.HasSuffix(pass.Pkg.Name(), "_test") && filepath.Base(dir) != path.Base(pkg) {
		return strings.TrimSuffix(pkg, "_test")
	}
	return pkg
}

func ordinaryTest(pass *analysis.Pass, fn *ast.FuncDecl) bool {
	if fn.Recv != nil || fn.Body == nil {
		return false
	}
	name, hasPrefix := strings.CutPrefix(fn.Name.Name, "Test")
	if !hasPrefix {
		return false
	}
	if first, _ := utf8.DecodeRuneInString(name); unicode.IsLower(first) {
		return false
	}
	if fn.Type.Params.NumFields() != 1 {
		return false
	}
	typ := types.Unalias(pass.TypesInfo.TypeOf(fn.Type.Params.List[0].Type))
	ptr, ok := typ.(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := types.Unalias(ptr.Elem()).(*types.Named)
	return ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "testing" && named.Obj().Name() == "T"
}

func suiteEntrypoint(pass *analysis.Pass, fn *ast.FuncDecl) bool {
	if len(fn.Body.List) == 0 {
		return false
	}
	for _, setup := range fn.Body.List[:len(fn.Body.List)-1] {
		statement, ok := setup.(*ast.ExprStmt)
		if !ok {
			return false
		}
		call, ok := statement.X.(*ast.CallExpr)
		if !ok {
			return false
		}
		callee := calledFunction(pass, call.Fun)
		if callee == nil || !packageFunction(callee) || suiteRunner(pass, callee) {
			return false
		}
	}
	statement, ok := fn.Body.List[len(fn.Body.List)-1].(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := statement.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	runner := calledFunction(pass, call.Fun)
	if runner == nil || runner.Pkg() == nil {
		return false
	}
	if runner.Pkg().Path() == testifySuitePath && runner.Name() == "Run" {
		return true // The call receives the more specific policy diagnostic.
	}
	if !suiteRunner(pass, runner) {
		return false
	}
	if len(call.Args) < 2 || len(fn.Type.Params.List[0].Names) != 1 {
		return false
	}
	argument, ok := ast.Unparen(call.Args[0]).(*ast.Ident)
	if !ok {
		return false
	}
	parameter := fn.Type.Params.List[0].Names[0]
	return pass.TypesInfo.ObjectOf(argument) == pass.TypesInfo.ObjectOf(parameter) && suiteExpression(pass, call.Args[1])
}
