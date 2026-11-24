package parallelsuitelint

import "golang.org/x/tools/go/analysis"

// Analyzer checks parallelsuite conventions.
var Analyzer = New()

// New returns an independent analyzer.
func New() *analysis.Analyzer {
	return &analysis.Analyzer{
		Name: "parallelsuitelint",
		Doc:  "check parallelsuite conventions",
		Run: func(pass *analysis.Pass) (any, error) {
			checkSuiteConventions(pass)
			return nil, nil //nolint:nilnil // Analysis reports diagnostics and has no result value.
		},
	}
}
