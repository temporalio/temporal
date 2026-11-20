package parallelsuitelint_test

import (
	"testing"

	"go.temporal.io/testx/parallelsuitelint"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestSuiteScope(t *testing.T) {
	t.Parallel()

	analysistest.Run(t, analysistest.TestData(), parallelsuitelint.Analyzer, "ownership")
}
