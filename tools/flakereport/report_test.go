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

func TestGenerateReportLimitsAllTableRows(t *testing.T) {
	const (
		maxRows = 100
		total   = maxRows + 1
	)
	makeTestReports := func(prefix string) []TestReport {
		reports := make([]TestReport, total)
		for i := range reports {
			reports[i].TestName = prefix + strconv.Itoa(i+1)
		}
		return reports
	}

	suites := make([]SuiteReport, total)
	bisectReports := make([]TestBisectReport, total)
	for i := range total {
		suites[i] = SuiteReport{
			SuiteName: "Suite" + strconv.Itoa(i+1),
			FlakeRate: float64(i + 1),
		}
		bisectReports[i] = TestBisectReport{
			TestName: "Bisect" + strconv.Itoa(i+1),
			TopSuspects: []BisectResult{{
				CommitSHA:   "abcdef0",
				Probability: float64(i+1) / total,
			}},
		}
	}
	summary := &ReportSummary{
		CIBreakers:      makeTestReports("CIBreaker"),
		Crashes:         makeTestReports("Crash"),
		Timeouts:        makeTestReports("Timeout"),
		FlakyTests:      makeTestReports("Flake"),
		Suites:          suites,
		TotalFlakyCount: total,
	}

	summaryContent := generateGitHubSummary(summary, "", 0)
	summaryContent += generateBisectSummary(bisectReports, "temporalio/temporal", 0.5)

	require.Contains(t, summaryContent, "| Flaky Tests | 101 |")
	for _, prefix := range []string{"CIBreaker", "Crash", "Timeout", "Flake"} {
		require.Contains(t, summaryContent, "`"+prefix+"1`")
		require.Contains(t, summaryContent, "`"+prefix+"100`")
		require.NotContains(t, summaryContent, "`"+prefix+"101`")
		require.Equal(t, maxRows, strings.Count(summaryContent, "| `"+prefix))
	}
	require.NotContains(t, summaryContent, "`Suite1`")
	require.Contains(t, summaryContent, "`Suite101`")
	require.Equal(t, maxRows, strings.Count(summaryContent, "| `Suite"))
	require.NotContains(t, summaryContent, "`Bisect1`")
	require.Contains(t, summaryContent, "`Bisect101`")
	require.Equal(t, maxRows, strings.Count(summaryContent, "| `Bisect"))
	require.Equal(t, 6, strings.Count(summaryContent, "Showing the top 100 of 101 entries."))
}
