package parallelsuitelint

import "golang.org/x/tools/go/analysis"

// Analyzer reports assertions that use a sealed parallelsuite receiver.
// It also checks suite naming, direct assertions and suite-based test entrypoints.
var Analyzer = newAnalyzer()

func newAnalyzer() *analysis.Analyzer {
	var allowedPackages string
	var runnerPackages string
	analyzer := &analysis.Analyzer{
		Name: "parallelsuitelint",
		Doc:  "check parallelsuite ownership, naming, assertions and test entrypoints",
		Run: func(pass *analysis.Pass) (any, error) {
			checkSuiteScope(pass)
			checkSuiteConventions(pass)
			checkRequiredSuites(pass, allowedPackages, runnerPackages)
			return nil, nil //nolint:nilnil // Analysis reports diagnostics and has no result value.
		},
	}
	analyzer.Flags.StringVar(&allowedPackages, "allow-standard-tests", "",
		"comma-separated package import paths allowing ordinary tests; /... includes descendants, ... allows all packages")
	analyzer.Flags.StringVar(&runnerPackages, "runner-packages", "",
		"comma-separated additional package import paths whose Run functions forward to parallelsuite")
	return analyzer
}
