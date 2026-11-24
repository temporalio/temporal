package parallelsuitelint

import (
	"go/ast"

	"golang.org/x/tools/go/analysis"
)

// checkSuiteConventions enforces suite names and direct use of suite methods while preserving Scope helper APIs.
func checkSuiteConventions(pass *analysis.Pass) {
	for _, file := range pass.Files {
		for node := range ast.Preorder(file) {
			switch node := node.(type) {
			case *ast.FuncDecl:
				if node.Recv != nil {
					checkSuiteNames(pass, node.Recv.List, "suite receiver must be named s")
				}
			case *ast.CallExpr:
				checkSuiteCall(pass, node)
			default:
			}
		}
	}
}

func checkSuiteNames(pass *analysis.Pass, fields []*ast.Field, message string) {
	for _, field := range fields {
		if !suiteExpression(pass, field.Type) {
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
	sel, isSelector := callTarget(call.Fun).(*ast.SelectorExpr)
	switch fn.Pkg().Path() {
	case "github.com/stretchr/testify/require", "github.com/stretchr/testify/assert":
		passesSuiteHandle := fn.Signature().Recv() == nil && len(call.Args) > 0 && suiteHandle(pass, call.Args[0])
		usesAssertionAccessor := isSelector && assertionAccessor(pass, sel.X)
		if passesSuiteHandle || usesAssertionAccessor {
			pass.Reportf(call.Pos(), "use suite assertion methods directly")
		}
	case suitePath:
		if !isSelector || !suiteExpression(pass, sel.X) {
			break
		}
		switch fn.Name() {
		case "Run", "RunSequential":
			for _, arg := range call.Args {
				if callback, ok := ast.Unparen(arg).(*ast.FuncLit); ok {
					checkSuiteNames(pass, callback.Type.Params.List, "suite callback parameter must be named s")
				}
			}
		case "Assert", "SetT", "SetS":
			pass.Reportf(call.Pos(), "%s is unsupported by parallelsuite", fn.Name())
		default:
		}
	default:
	}

	if !isSelector || !suiteHandle(pass, sel.X) {
		return
	}
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

func suiteHandle(pass *analysis.Pass, expr ast.Expr) bool {
	accessor := suiteAccessor(pass, expr)
	if accessor == nil {
		return false
	}
	switch accessor.Sel.Name {
	case "T", "TB", "AssertionT":
		return true
	default:
		return false
	}
}

func suiteAccessor(pass *analysis.Pass, expr ast.Expr) *ast.SelectorExpr {
	call, ok := ast.Unparen(expr).(*ast.CallExpr)
	if !ok {
		return nil
	}
	sel, fn := selectedFunction(pass, call)
	if fn == nil || fn.Pkg() == nil || fn.Pkg().Path() != suitePath || !suiteExpression(pass, sel.X) {
		return nil
	}
	return sel
}

func assertionAccessor(pass *analysis.Pass, expr ast.Expr) bool {
	switch expr := ast.Unparen(expr).(type) {
	case *ast.SelectorExpr:
		return expr.Sel.Name == "Assertions" && suiteExpression(pass, expr.X)
	case *ast.CallExpr:
		accessor := suiteAccessor(pass, expr)
		return accessor != nil && accessor.Sel.Name == "Require"
	default:
		return false
	}
}
