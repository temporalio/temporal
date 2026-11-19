package parallelsuitelint

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

// checkSuiteConventions enforces suite names and direct use of suite methods while preserving Scope helper APIs.
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
	if path == "go.temporal.io/testx/await" || path == "go.temporal.io/server/common/testing/await" {
		checkSuiteAwaitCall(pass, call, fn.Name())
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

// checkSuiteAwaitCall flags standalone await calls that already use the suite's scope.
func checkSuiteAwaitCall(pass *analysis.Pass, call *ast.CallExpr, name string) {
	var method string
	handleIndex := 0
	switch name {
	case "Require":
		method, handleIndex = "Await", 1
	case "Requiref":
		method, handleIndex = "Awaitf", 1
	case "RequireTrue":
		method = "AwaitTrue"
	case "RequireTruef":
		method = "AwaitTruef"
	case "Rcv", "Snd":
		method = name
	default:
		return
	}
	if len(call.Args) <= handleIndex {
		return
	}
	handle := suiteAccessor(pass, call.Args[handleIndex])
	if handle == nil || (handle.Sel.Name != "T" && handle.Sel.Name != "TB") {
		return
	}
	if handleIndex == 1 {
		ctx := suiteAccessor(pass, call.Args[0])
		if ctx == nil || ctx.Sel.Name != "Context" || !sameSuiteReceiver(pass, ctx.X, handle.X) {
			return
		}
	}
	pass.Reportf(call.Pos(), "use suite %s instead of await.%s", method, name)
}

func sameSuiteReceiver(pass *analysis.Pass, a, b ast.Expr) bool {
	left, leftOK := ast.Unparen(a).(*ast.Ident)
	right, rightOK := ast.Unparen(b).(*ast.Ident)
	return leftOK && rightOK && pass.TypesInfo.ObjectOf(left) == pass.TypesInfo.ObjectOf(right)
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
