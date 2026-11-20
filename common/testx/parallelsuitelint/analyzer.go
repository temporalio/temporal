package parallelsuitelint

import "golang.org/x/tools/go/analysis"

// Analyzer checks parallelsuite conventions and scope.
var Analyzer = New()

// New returns an independent analyzer.
func New() *analysis.Analyzer {
	return &analysis.Analyzer{
		Name: "parallelsuitelint",
		Doc:  "check parallelsuite conventions and scope",
		Run: func(pass *analysis.Pass) (any, error) {
			checkSuiteScope(pass)
			checkSuiteConventions(pass)
			return nil, nil //nolint:nilnil // Analysis reports diagnostics and has no result value.
		},
	}
}
