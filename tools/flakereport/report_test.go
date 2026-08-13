package flakereport

import (
	"strconv"
	"strings"
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

func TestGenerateGitHubSummaryLimitsFlakyTests(t *testing.T) {
	flakyTests := make([]TestReport, maxFlakyTestsPerReport+1)
	for i := range flakyTests {
		flakyTests[i].TestName = "TestFlake" + strconv.Itoa(i+1)
	}
	summary := &ReportSummary{
		FlakyTests:      flakyTests,
		TotalFlakyCount: len(flakyTests),
	}

	summaryContent := generateGitHubSummary(summary, "", 0)

	require.Contains(t, summaryContent, "| Flaky Tests | 101 |")
	require.Contains(t, summaryContent, "`TestFlake1`")
	require.Contains(t, summaryContent, "`TestFlake100`")
	require.NotContains(t, summaryContent, "`TestFlake101`")
	require.Contains(t, summaryContent, "Showing the top 100 of 101 flaky tests")
	require.NotContains(t, summaryContent, "complete list")
	require.Equal(t, maxFlakyTestsPerReport, strings.Count(summaryContent, "| `TestFlake"))
}
