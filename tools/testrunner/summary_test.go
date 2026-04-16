package testrunner

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRenderSummaryFromReports_RendersAssertionFailure(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-assertion-failure.xml")

	summary := mustNewMergedSummary(t, report)
	require.Equal(t, []summaryRow{{
		kind:    failureTypeFailed,
		name:    "TestBar",
		details: "    bar_test.go:9:\n        Error Trace:\t__SOURCE__:2\n        Error:      \tNot equal:\n                    \texpected: 1\n                    \tactual  : 2",
	}}, summary.rows)

	rendered := summary.String()
	require.Contains(t, rendered, "<table>")
	require.Contains(t, rendered, "<details><summary>TestBar</summary>")
	require.NotContains(t, rendered, "<th>Details</th>")
	require.Contains(t, rendered, "expected: 1")
}

func TestRenderSummaryFromReports_EmptyWhenNoFailures(t *testing.T) {
	s := newSummaryFromReports([]*junitReport{mustReadReportFixture(t, "testdata/junit-empty.xml")})
	require.Empty(t, s.rows)
	require.Empty(t, s.String())
}

func TestRenderSummaryFromReports_KeepsFailureAndAlertRows(t *testing.T) {
	failureReport := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	failureReport.Suites[0].Testcases[0].Name = "TestFoo"
	failureReport.Suites[0].Testcases[0].Failure.Data = "FAIL\n"

	alertReport := mustReadReportFixture(t, "testdata/junit-alert-panic.xml")

	summary := mustNewMergedSummary(t, failureReport, alertReport)
	require.Equal(t, []summaryRow{
		{kind: failureTypeFailed, name: "TestFoo", details: "FAIL\n"},
		{kind: failureTypePanic, name: "panic test (retry 1) (final)", details: "panic: boom"},
	}, summary.rows)

	s := summary.String()
	require.Contains(t, s, "panic: boom")
	require.Contains(t, s, "❌ PANIC")
	require.Contains(t, s, "<pre>FAIL\n</pre>")
	require.Contains(t, s, "<details><summary>TestFoo</summary>")
	require.Contains(t, s, "<details><summary>panic test (retry 1) (final)</summary>")
	require.Equal(t, 2, strings.Count(s, "<tr><td>"))
}

func TestRenderSummaryFromReports_RendersTrimmedFailureBody(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-unexpected-call-failure.xml")

	summary := mustNewMergedSummary(t, report)
	require.Equal(t, []summaryRow{{
		kind:    failureTypeFailed,
		name:    "TestBaz",
		details: "    mock_file.go:42: Unexpected call to SomeMethod(...)\n        expected call at foo_test.go:10 doesn't match argument at index 0.\n        Got:  \"actual\"\n        Want: is equal to \"expected\"",
	}}, summary.rows)

	s := summary.String()
	require.Contains(t, s, "<details><summary>TestBaz</summary>")
	require.Contains(t, s, "Unexpected call to SomeMethod")
	require.NotContains(t, s, "some setup log")
}

func TestRenderSummaryFromReports_RendersAlertRow(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-alert-data-race.xml")

	s := newSummaryFromReports([]*junitReport{report})
	require.Equal(t, []summaryRow{{
		kind:    failureTypeDataRace,
		name:    "DATA RACE: Data race detected in TestFoo",
		details: "WARNING: DATA RACE\nWrite at 0x00c000123456 by goroutine 7:\n  main.TestFoo()\n      /path/to/foo_test.go:42 +0x123\n\nPrevious read at 0x00c000123456 by goroutine 8:\n  main.TestFoo()\n      /path/to/foo_test.go:43 +0x456",
	}}, s.rows)
	rendered := s.String()
	require.Contains(t, rendered, failureTypeDataRace)
	require.Contains(t, rendered, "Write at 0x00c000123456 by goroutine 7")
}

func TestRenderSummaryFromReports_TruncatesOversizedDetail(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	report.Suites[0].Testcases[0].Failure.Data = strings.Repeat("x", summaryMaxDetailBytes+1)

	summary := mustNewMergedSummary(t, report)
	require.Equal(t, []summaryRow{{
		kind:    failureTypeFailed,
		name:    "TestStandalone",
		details: strings.Repeat("x", summaryMaxDetailBytes+1),
	}}, summary.rows)

	s := summary.String()
	require.Contains(t, s, "truncated")
	require.Less(t, len(s), summaryMaxBytes)
}

func TestRenderSummaryFromReports_OmitsRowsWhenBudgetExceeded(t *testing.T) {
	n := summaryMaxBytes/summaryMaxDetailBytes + 1
	reports := make([]*junitReport, n)
	for i := range n {
		report := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
		report.Suites[0].Testcases[0].Name = "Test" + strconv.Itoa(i)
		report.Suites[0].Testcases[0].Failure.Data = strings.Repeat("x", summaryMaxDetailBytes)
		reports[i] = report
	}
	summary := newSummaryFromReports(reports)
	require.Len(t, summary.rows, n)

	s := summary.String()
	require.Less(t, len(s), summaryMaxBytes+len("</table>\n")+200)
	require.Contains(t, s, "not shown")
	require.Contains(t, s, "</table>")
}

func TestRenderSummaryFromReports_MergesMultipleReportsIntoSingleTable(t *testing.T) {
	reportOld := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	reportOld.Suites[0].Name = "SuiteA"
	reportOld.Suites[0].Testcases[0].Name = "TestOld"
	reportOld.Suites[0].Testcases[0].Failure.Data = "old failure"

	reportNew := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	reportNew.Suites[0].Name = "SuiteB"
	reportNew.Suites[0].Testcases[0].Name = "TestNew"
	reportNew.Suites[0].Testcases[0].Failure.Data = "new failure"

	summary := newSummaryFromReports([]*junitReport{reportOld, reportNew})
	require.Equal(t, []summaryRow{
		{kind: failureTypeFailed, name: "TestNew", details: "new failure"},
		{kind: failureTypeFailed, name: "TestOld", details: "old failure"},
	}, summary.rows)

	s := summary.String()
	require.Equal(t, 1, strings.Count(s, "<table>"))
	require.Contains(t, s, "TestNew")
	require.Contains(t, s, "TestOld")
}

func mustNewMergedSummary(t *testing.T, reports ...*junitReport) summary {
	t.Helper()
	merged, err := mergeReports(reports)
	require.NoError(t, err)
	return newSummaryFromReports([]*junitReport{merged})
}
