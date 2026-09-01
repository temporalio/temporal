package custom

import (
	"go/ast"
	"go/types"

	"github.com/avito-tech/go-mutesting/mutator"
)

// BooleanLiteral replaces universe boolean constants with their opposite value.
func BooleanLiteral(_ *types.Package, info *types.Info, node ast.Node) []mutator.Mutation {
	ident, ok := node.(*ast.Ident)
	if !ok || info == nil {
		return nil
	}
	if ident.Name != "true" && ident.Name != "false" {
		return nil
	}
	if info.ObjectOf(ident) != types.Universe.Lookup(ident.Name) {
		return nil
	}

	original := ident.Name
	mutated := "true"
	if original == "true" {
		mutated = "false"
	}
	return []mutator.Mutation{{
		Change: func() {
			ident.Name = mutated
		},
		Reset: func() {
			ident.Name = original
		},
	}}
}
