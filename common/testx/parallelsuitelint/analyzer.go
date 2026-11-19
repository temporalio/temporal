package parallelsuitelint

import "golang.org/x/tools/go/analysis"

// Analyzer is the parallelsuitelint analyzer.
var Analyzer = New()

// New returns an independent analyzer.
func New() *analysis.Analyzer {
	return &analysis.Analyzer{
		Name: "parallelsuitelint",
		Doc:  "check parallelsuite usage",
		Run: func(*analysis.Pass) (any, error) {
			return nil, nil //nolint:nilnil // Analysis reports diagnostics and has no result value.
		},
	}
}
