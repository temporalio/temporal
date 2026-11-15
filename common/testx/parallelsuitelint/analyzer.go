package parallelsuitelint

import "golang.org/x/tools/go/analysis"

// Analyzer reports assertions that use a sealed parallelsuite receiver.
// It also checks suite naming, direct assertions and suite-based test entrypoints.
var Analyzer = New()

// New returns an analyzer with independent flag settings.
func New() *analysis.Analyzer {
	var allowedPackages string
	analyzer := &analysis.Analyzer{
		Name:      "parallelsuitelint",
		Doc:       "check parallelsuite ownership, naming, assertions and test entrypoints",
		FactTypes: []analysis.Fact{new(suiteRunnerFact)},
		Run: func(pass *analysis.Pass) (any, error) {
			checkSuiteScope(pass)
			checkSuiteConventions(pass)
			checkRequiredSuites(pass, allowedPackages)
			return nil, nil //nolint:nilnil // Analysis reports diagnostics and has no result value.
		},
	}
	analyzer.Flags.StringVar(&allowedPackages, "allow-standard-tests", "",
		"comma-separated package import paths allowing ordinary tests; /... includes descendants, ... allows all packages")
	return analyzer
}
