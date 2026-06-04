package flakereport

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFormatSparkline(t *testing.T) {
	require.Equal(t, "▁▄█▁", formatSparkline([]int{0, 1, 3, 0}))
	require.Equal(t, "███", formatSparkline([]int{1, 1, 1}))
	require.Equal(t, "-", formatSparkline(nil))
	require.Equal(t, "▁▁▁", formatSparkline([]int{0, 0, 0}))
}

func TestGenerateTestReportTable(t *testing.T) {
	report := TestReport{
		TestName:     "TestFlake",
		FailureCount: 3,
		TotalRuns:    10,
		GitHubURLs:   []string{"https://github.com/temporalio/temporal/actions/runs/1"},
		LastFailure:  time.Now().Add(-2 * time.Hour),
		TrendPoints:  []int{0, 1, 2, 0},
	}

	table := generateTestReportTable([]TestReport{report}, "Flake Rate", 1)

	require.Equal(t, "| Test | Flake Rate | Last Failure | Trend | Links |\n"+
		"|------|------------|-------------|-------|-------|\n"+
		"| `TestFlake` | **30.0% (3/10)** | 2h ago | `▁▅█▁` | [1](https://github.com/temporalio/temporal/actions/runs/1) |\n", table)
}

func TestGenerateSuiteBreakdownTable(t *testing.T) {
	report := SuiteReport{
		SuiteName:   "TestFunctionalSuite",
		FlakeRate:   25.0,
		FailedRuns:  2,
		TotalRuns:   8,
		LastFailure: time.Now().Add(-3 * time.Hour),
	}

	table := generateSuiteBreakdownTable([]SuiteReport{report})

	require.Equal(t, "| Suite | Flake Rate | Last Failure |\n"+
		"|-------|------------|-------------|\n"+
		"| `TestFunctionalSuite` | **25.0% (2/8)** | 3h ago |\n", table)
}
