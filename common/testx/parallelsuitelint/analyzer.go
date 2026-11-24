package parallelsuitelint

import "golang.org/x/tools/go/analysis"

// Analyzer checks parallelsuite conventions.
var Analyzer = New()

// New returns an independent analyzer.
func New() *analysis.Analyzer {
	return NewWithAwaitPackages()
}

// NewWithAwaitPackages returns an analyzer that also recognizes await functions from the given import paths.
func NewWithAwaitPackages(additionalAwaitPackages ...string) *analysis.Analyzer {
	awaitPackages := append([]string{"go.temporal.io/testx/await"}, additionalAwaitPackages...)
	return &analysis.Analyzer{
		Name: "parallelsuitelint",
		Doc:  "check parallelsuite conventions",
		Run: func(pass *analysis.Pass) (any, error) {
			checkSuiteConventions(pass, awaitPackages)
			return nil, nil //nolint:nilnil // Analysis reports diagnostics and has no result value.
		},
	}
}
