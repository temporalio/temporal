package lintconditions

import (
	"go/ast"
	"go/token"

	"golang.org/x/tools/go/analysis"
)

const maxTerms = 3

// Analyzer checks maximal unparenthesized chains of && and || in every expression
// context, including function literals. Mixed operators count alike: each operand
// that is not itself a logical binary expression is a term. Parenthesized groups,
// negations, comparisons and calls each contribute one outer term; logical chains
// inside them are checked independently. A chain exceeding [maxTerms] is reported
// once. Generated files are skipped; test files follow the same rule as other code.
var Analyzer = &analysis.Analyzer{
	Name: "lintconditions",
	Doc:  "report boolean chains exceeding the term limit; extract a named predicate",
	Run:  run,
}

func run(pass *analysis.Pass) (any, error) {
	for _, file := range pass.Files {
		if ast.IsGenerated(file) {
			continue
		}
		checked := make(map[*ast.BinaryExpr]bool)
		ast.Inspect(file, func(node ast.Node) bool {
			expr, ok := node.(*ast.BinaryExpr)
			if !ok || checked[expr] {
				return true
			}
			if terms := countTerms(expr, checked); terms > maxTerms {
				pass.Reportf(expr.Pos(),
					"boolean chain has %d terms (limit %d); extract a named predicate, preferably a method on the owning type",
					terms, maxTerms)
			}
			return true
		})
	}
	return nil, nil //nolint:nilnil // A diagnostics-only analyzer has no result.
}

func countTerms(expr ast.Expr, checked map[*ast.BinaryExpr]bool) int {
	binary, ok := expr.(*ast.BinaryExpr)
	if !ok || (binary.Op != token.LAND && binary.Op != token.LOR) {
		return 1
	}
	checked[binary] = true
	return countTerms(binary.X, checked) + countTerms(binary.Y, checked)
}
