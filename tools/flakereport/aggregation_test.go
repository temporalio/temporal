package flakereport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTestRunSummaryAddAndMerge(t *testing.T) {
	left := newTestRunSummary()
	left.add([]TestRun{
		{Name: "TestSuite/TestCase (retry 1)", SuiteName: "TestSuite", RunID: 10, MatrixName: "sqlite"},
		{Name: "TestSuite/TestCase (final)", SuiteName: "TestSuite", RunID: 10, MatrixName: "sqlite", Failed: true},
		{Name: "TestSuite/TestSkipped", SuiteName: "TestSuite", RunID: 10, MatrixName: "sqlite", Skipped: true},
	})

	right := newTestRunSummary()
	right.add([]TestRun{
		{Name: "TestSuite/TestCase", SuiteName: "TestSuite", RunID: 11, MatrixName: "mysql8", Failed: true},
		{Name: "TestFunction", SuiteName: "TestFunction", RunID: 11, MatrixName: "mysql8"},
	})
	left.merge(right)

	require.Equal(t, 5, left.totalRuns)
	require.Equal(t, map[string]int{
		"TestSuite/TestCase": 3,
		"TestFunction":       1,
	}, left.countsByTest())

	testRuns := left.tests["TestSuite/TestCase"]
	require.Equal(t, 3, testRuns.totalRuns)
	require.Equal(t, 2, testRuns.failures)
	require.Equal(t, commitRunCounts{passes: 1, fails: 1}, testRuns.byWorkflowRun[10])
	require.Equal(t, commitRunCounts{fails: 1}, testRuns.byWorkflowRun[11])
	require.Len(t, left.suiteRuns["TestSuite"], 2)
	require.NotContains(t, left.suiteRuns, "TestFunction")
}
