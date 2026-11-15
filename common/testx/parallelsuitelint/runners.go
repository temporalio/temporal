package parallelsuitelint

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

type suiteRunnerFact struct{}

func (*suiteRunnerFact) AFact() {}

func suiteRunner(pass *analysis.Pass, fn *types.Func) bool {
	if fn == nil || fn.Pkg() == nil || !packageFunction(fn) {
		return false
	}
	if fn.Pkg().Path() == suitePath && (fn.Name() == "Run" || fn.Name() == "RunLegacySequential") {
		return true
	}
	return pass.ImportObjectFact(fn.Origin(), new(suiteRunnerFact))
}

// recordSuiteRunners propagates verified forwarding wrappers across package boundaries.
func recordSuiteRunners(pass *analysis.Pass) {
	var candidates []*ast.FuncDecl
	for _, file := range pass.Files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Body != nil {
				candidates = append(candidates, fn)
			}
		}
	}
	for changed := true; changed; {
		changed = false
		for _, decl := range candidates {
			fn, ok := pass.TypesInfo.Defs[decl.Name].(*types.Func)
			if !ok || suiteRunner(pass, fn) || !forwardsSuiteRunner(pass, fn, decl.Body) {
				continue
			}
			pass.ExportObjectFact(fn.Origin(), new(suiteRunnerFact))
			changed = true
		}
	}
}

func forwardsSuiteRunner(pass *analysis.Pass, fn *types.Func, body *ast.BlockStmt) bool {
	params := fn.Type().(*types.Signature).Params()
	if params.Len() < 2 || len(body.List) == 0 || len(body.List) > 2 {
		return false
	}
	if len(body.List) == 2 {
		statement, ok := body.List[0].(*ast.ExprStmt)
		if !ok {
			return false
		}
		call, ok := statement.X.(*ast.CallExpr)
		if !ok || len(call.Args) != 0 {
			return false
		}
		sel, helper := selectedFunction(pass, call)
		if helper == nil || helper.Pkg() == nil || helper.Pkg().Path() != "testing" || helper.Name() != "Helper" || !parameterArgument(pass, sel.X, params.At(0)) {
			return false
		}
	}
	statement, ok := body.List[len(body.List)-1].(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := statement.X.(*ast.CallExpr)
	return ok && len(call.Args) >= 2 && suiteRunner(pass, calledFunction(pass, call.Fun)) &&
		parameterArgument(pass, call.Args[0], params.At(0)) && parameterArgument(pass, call.Args[1], params.At(1))
}

func parameterArgument(pass *analysis.Pass, argument ast.Expr, parameter types.Object) bool {
	id, ok := ast.Unparen(argument).(*ast.Ident)
	return ok && pass.TypesInfo.ObjectOf(id) == parameter
}
