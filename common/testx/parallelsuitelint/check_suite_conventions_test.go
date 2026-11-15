package parallelsuitelint_test

import (
	"testing"

	"go.temporal.io/testx/parallelsuitelint"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestSuiteConventions(t *testing.T) {
	t.Parallel()

	analysistest.Run(t, analysistest.TestData(), parallelsuitelint.Analyzer, "conventions", "dotassertions")
}
