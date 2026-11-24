package parallelsuitelint

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

func selectedFunction(pass *analysis.Pass, call *ast.CallExpr) (*ast.SelectorExpr, *types.Func) {
	sel, ok := callTarget(call.Fun).(*ast.SelectorExpr)
	if !ok {
		return nil, nil
	}
	return sel, calledFunction(pass, sel)
}

func calledFunction(pass *analysis.Pass, expr ast.Expr) *types.Func {
	switch expr := callTarget(expr).(type) {
	case *ast.SelectorExpr:
		fn, ok := pass.TypesInfo.ObjectOf(expr.Sel).(*types.Func)
		if !ok {
			return nil
		}
		return fn
	case *ast.Ident:
		fn, ok := pass.TypesInfo.ObjectOf(expr).(*types.Func)
		if !ok {
			return nil
		}
		return fn
	default:
		return nil
	}
}

func callTarget(expr ast.Expr) ast.Expr {
	switch expr := ast.Unparen(expr).(type) {
	case *ast.IndexExpr:
		return callTarget(expr.X)
	case *ast.IndexListExpr:
		return callTarget(expr.X)
	default:
		return expr
	}
}
