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

func TestGenerateOccurrenceReportTable(t *testing.T) {
	report := TestReport{
		TestName:     testRunnerTotalTimeout,
		FailureCount: 3,
		GitHubURLs:   []string{"https://github.com/temporalio/temporal/actions/runs/1"},
		TrendPoints:  []int{0, 1, 2, 0},
	}

	table := generateOccurrenceReportTable([]TestReport{report}, "Event", "Affected Artifacts", 1)

	require.Equal(t, "| Event | Affected Artifacts | Last Occurrence | Trend | Links |\n"+
		"|------|--------------------|-----------------|-------|-------|\n"+
		"| `testrunner.TotalTimeout` | 3 | N/A | `▁▅█▁` | [1](https://github.com/temporalio/temporal/actions/runs/1) |\n", table)
}

func TestGenerateGitHubSummaryPutsBayesianSuspectsBeforeFailureCategories(t *testing.T) {
	summary := &ReportSummary{
		TestRunnerTimeouts: []TestReport{{TestName: testRunnerTotalTimeout, FailureCount: 1}},
		CIBreakers:         []TestReport{{TestName: "TestFinalRetry", FailureCount: 1}},
	}
	bisectReports := []TestBisectReport{{
		TestName: "TestFoo",
		TopSuspects: []BisectResult{{
			CommitSHA:   "0123456789abcdef",
			Probability: 0.6,
		}},
	}}

	content := generateGitHubSummary(summary, bisectReports, "temporalio/temporal", "123", 1, 0.5)

	bayesianIndex := strings.Index(content, "### Bayesian Commit Suspects")
	failureCategoriesIndex := strings.Index(content, "### Failure Categories Summary")
	require.NotEqual(t, -1, bayesianIndex)
	require.NotEqual(t, -1, failureCategoriesIndex)
	require.Less(t, bayesianIndex, failureCategoriesIndex)
	require.Contains(t, content, "| CI Execution Interruptions |")
	require.Contains(t, content, "### CI Execution Interruptions")
	require.Contains(t, content, "### Final-Retry Test Failures")
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

func TestBuildReportSummaryExcludesHundredPercentFlakyTests(t *testing.T) {
	fullyFailing := TestReport{TestName: "TestAlwaysFails", FailureCount: 2, TotalRuns: 2}
	flaky := TestReport{TestName: "TestSometimesFails", FailureCount: 1, TotalRuns: 2}
	timeout := TestReport{TestName: "TestTimeout", FailureCount: 2, TotalRuns: 2}
	crash := TestReport{TestName: "TestCrash", FailureCount: 2, TotalRuns: 2}
	ciBreaker := TestReport{TestName: "TestCIBreaker", FailureCount: 2, TotalRuns: 2}

	summary := buildReportSummary(
		[]TestReport{fullyFailing, flaky},
		[]TestReport{timeout},
		nil,
		[]TestReport{crash},
		[]TestReport{ciBreaker},
		nil, nil, nil, nil, 0,
	)

	require.Equal(t, []TestReport{flaky}, summary.FlakyTests)
	require.Equal(t, 1, summary.TotalFlakyCount)
	require.Equal(t, []TestReport{timeout}, summary.Timeouts)
	require.Equal(t, []TestReport{crash}, summary.Crashes)
	require.Equal(t, []TestReport{ciBreaker}, summary.CIBreakers)
}

func TestGenerateReportLimitsNonSuiteTableRows(t *testing.T) {
	const (
		maxRows = maxReportRowsPerTable
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
	bisectReports := make([]TestBisectReport, maxRows)
	for i := range total {
		suiteRank := total - i
		suites[i] = SuiteReport{
			SuiteName: "Suite" + strconv.Itoa(suiteRank),
			FlakeRate: float64(suiteRank),
		}
		if i == maxRows {
			continue
		}
		bisectReports[i] = TestBisectReport{
			TestName: "Bisect" + strconv.Itoa(i+1),
			TopSuspects: []BisectResult{{
				CommitSHA:   "abcdef0",
				Probability: float64(i+1) / maxRows,
			}},
		}
	}
	bisectReports[maxRows-1].TopSuspects = append(bisectReports[maxRows-1].TopSuspects, BisectResult{
		CommitSHA:   "1234567",
		Probability: 0.5,
	})
	summary := &ReportSummary{
		CIBreakers:      makeTestReports("CIBreaker"),
		Crashes:         makeTestReports("Crash"),
		Timeouts:        makeTestReports("Timeout"),
		FlakyTests:      makeTestReports("Flake"),
		Suites:          suites,
		TotalFlakyCount: total,
	}

	summaryContent := generateGitHubSummary(summary, nil, "", "", 0, 0)
	summaryContent += generateBisectSummary(bisectReports, "temporalio/temporal", 0.5)

	require.Contains(t, summaryContent, "| Flaky Tests | 101 |")
	for _, prefix := range []string{"CIBreaker", "Crash", "Timeout", "Flake"} {
		require.Contains(t, summaryContent, "`"+prefix+"1`")
		require.Contains(t, summaryContent, "`"+prefix+"100`")
		require.NotContains(t, summaryContent, "`"+prefix+"101`")
		require.Equal(t, maxRows, strings.Count(summaryContent, "| `"+prefix))
	}
	require.Contains(t, summaryContent, "`Suite1`")
	require.Contains(t, summaryContent, "`Suite101`")
	require.Equal(t, total, strings.Count(summaryContent, "| `Suite"))
	var bisectRows []string
	for line := range strings.Lines(summaryContent) {
		if strings.HasPrefix(line, "| `Bisect") {
			bisectRows = append(bisectRows, line)
		}
	}
	require.Len(t, bisectRows, maxRows)
	require.Contains(t, bisectRows[0], "`Bisect100`")
	require.Contains(t, bisectRows[1], "`Bisect100`")
	require.NotContains(t, summaryContent, "`Bisect1`")
	require.NotContains(t, summaryContent, "Showing the top")
}

func TestBuildBisectTableRowsSortsReportsByTopSuspect(t *testing.T) {
	reports := []TestBisectReport{
		{
			TestName:    "TestB",
			TopSuspects: []BisectResult{{CommitSHA: "b", Probability: 0.5}},
		},
		{
			TestName: "TestA",
			TopSuspects: []BisectResult{
				{CommitSHA: "a1", Probability: 0.5},
				{CommitSHA: "a2", Probability: 0.25},
			},
		},
		{
			TestName:    "TestC",
			TopSuspects: []BisectResult{{CommitSHA: "c", Probability: 0.75}},
		},
	}

	rows := buildBisectTableRows(reports)

	require.Len(t, rows, 4)
	require.Equal(t, []string{"TestC", "TestA", "TestA", "TestB"}, []string{
		rows[0].testName,
		rows[1].testName,
		rows[2].testName,
		rows[3].testName,
	})
}
