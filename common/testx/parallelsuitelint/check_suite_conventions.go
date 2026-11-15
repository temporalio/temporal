package parallelsuitelint

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// checkSuiteConventions enforces suite names and direct assertions while preserving Scope helper APIs.
func checkSuiteConventions(pass *analysis.Pass) {
	for _, file := range pass.Files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.FuncDecl:
				if node.Recv != nil {
					checkSuiteNames(pass, node.Recv.List, "suite receiver must be named s")
				}
			case *ast.CallExpr:
				checkSuiteCall(pass, node)
			default:
			}
			return true
		})
	}
}

func checkSuiteNames(pass *analysis.Pass, fields []*ast.Field, message string) {
	for _, field := range fields {
		if !embedsSuite(pass.TypesInfo.TypeOf(field.Type), make(map[types.Type]bool)) {
			continue
		}
		if len(field.Names) == 0 {
			pass.Reportf(field.Pos(), "%s", message)
		}
		for _, name := range field.Names {
			if name.Name != "s" {
				pass.Reportf(name.Pos(), "%s", message)
			}
		}
	}
}

func checkSuiteCall(pass *analysis.Pass, call *ast.CallExpr) {
	fn := calledFunction(pass, call.Fun)
	if fn == nil || fn.Pkg() == nil {
		return
	}
	path := fn.Pkg().Path()
	isAssertion := path == "github.com/stretchr/testify/require" || path == "github.com/stretchr/testify/assert"
	if isAssertion && packageFunction(fn) && firstArgumentIsSuiteHandle(pass, call) {
		pass.Reportf(call.Pos(), "use suite assertion methods directly")
	}
	sel, ok := callTarget(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return
	}
	if path == suitePath && suiteExpression(pass, sel.X) {
		switch fn.Name() {
		case "Run", "RunSequential", "Await", "Awaitf":
			for _, arg := range call.Args {
				if callback, ok := ast.Unparen(arg).(*ast.FuncLit); ok {
					checkSuiteNames(pass, callback.Type.Params.List, "suite callback parameter must be named s")
				}
			}
		case "Assert", "SetT", "SetS":
			pass.Reportf(call.Pos(), "%s is unsupported by parallelsuite", fn.Name())
		default:
		}
	}
	if isAssertion && assertionAccessor(pass, sel.X) {
		pass.Reportf(call.Pos(), "use suite assertion methods directly")
	}
	if suiteHandle(pass, sel.X) {
		switch fn.Name() {
		case "Run":
			pass.Reportf(call.Pos(), "use suite Run or RunSequential instead of the test handle's Run")
		case "Parallel":
			pass.Reportf(call.Pos(), "do not call Parallel on a suite test handle; the suite runner controls parallelism")
		case "Error", "Errorf", "Fatal", "Fatalf", "Fail", "FailNow":
			pass.Reportf(call.Pos(), "use suite assertion methods directly")
		default:
		}
	}
}

func packageFunction(fn *types.Func) bool {
	signature, ok := fn.Type().(*types.Signature)
	return ok && signature.Recv() == nil
}

func suiteHandle(pass *analysis.Pass, expr ast.Expr) bool {
	call, ok := ast.Unparen(expr).(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, fn := selectedFunction(pass, call)
	if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != suitePath {
		return false
	}
	switch fn.Name() {
	case "T", "TB", "AssertionT":
		return suiteExpression(pass, sel.X)
	default:
		return false
	}
}

func firstArgumentIsSuiteHandle(pass *analysis.Pass, call *ast.CallExpr) bool {
	return len(call.Args) > 0 && suiteHandle(pass, call.Args[0])
}

func assertionAccessor(pass *analysis.Pass, expr ast.Expr) bool {
	switch expr := ast.Unparen(expr).(type) {
	case *ast.SelectorExpr:
		return expr.Sel.Name == "Assertions" && suiteExpression(pass, expr.X)
	case *ast.CallExpr:
		sel, fn := selectedFunction(pass, expr)
		if fn == nil || fn.Pkg() == nil {
			return false
		}
		return fn.Pkg().Path() == suitePath && fn.Name() == "Require" && suiteExpression(pass, sel.X)
	default:
		return false
	}
}
