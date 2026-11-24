package parallelsuitelint

import (
	"go/ast"
	"go/types"

	"golang.org/x/tools/go/analysis"
)

const suitePath = "go.temporal.io/testx/parallelsuite"

func embedsSuite(t types.Type, seen map[types.Type]bool) bool {
	return embeddedSuite(t, seen) != nil
}

func embeddedSuite(t types.Type, seen map[types.Type]bool) *types.Named {
	t = types.Unalias(t)
	if seen[t] {
		return nil
	}
	seen[t] = true
	if ptr, ok := t.(*types.Pointer); ok {
		return embeddedSuite(ptr.Elem(), seen)
	}
	if named, ok := t.(*types.Named); ok {
		obj := named.Obj()
		if obj.Pkg() != nil && obj.Pkg().Path() == suitePath && obj.Name() == "Suite" {
			return named
		}
		t = named.Underlying()
	}
	if structure, ok := t.(*types.Struct); ok {
		for field := range structure.Fields() {
			if field.Embedded() {
				if suite := embeddedSuite(field.Type(), seen); suite != nil {
					return suite
				}
			}
		}
	}
	return nil
}

func suiteExpression(pass *analysis.Pass, expr ast.Expr) bool {
	return embedsSuite(pass.TypesInfo.TypeOf(expr), make(map[types.Type]bool))
}
