package lintconditions_test

import (
	"testing"

	"go.temporal.io/testx/lintconditions"
	"golang.org/x/tools/go/analysis/analysistest"
)

func TestConditions(t *testing.T) {
	t.Parallel()

	analysistest.Run(t, analysistest.TestData(), lintconditions.Analyzer, "conditions")
}
