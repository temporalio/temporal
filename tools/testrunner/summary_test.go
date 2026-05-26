package testrunner

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewSummaryFromReports_RendersAssertionFailureRow(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-assertion-failure.xml")

	summary := mustNewMergedSummary(t, report)
	require.Equal(t, []summaryRow{{
		Kind:    failureTypeFailed,
		Name:    "TestBar",
		Details: "    bar_test.go:9:\n        Error Trace:\t__SOURCE__:2\n        Error:      \tNot equal:\n                    \texpected: 1\n                    \tactual  : 2",
	}}, summary.Rows)
}

func TestNewSummaryFromReports_EmptyWhenNoFailures(t *testing.T) {
	s := newSummaryFromReports([]*junitReport{mustReadReportFixture(t, "testdata/junit-empty.xml")})
	require.Empty(t, s.Rows)
}

func TestNewSummaryFromReports_KeepsFailureAndAlertRows(t *testing.T) {
	failureReport := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	failureReport.Suites[0].Testcases[0].Name = "TestFoo"
	failureReport.Suites[0].Testcases[0].Failure.Data = "FAIL\n"

	alertReport := mustReadReportFixture(t, "testdata/junit-alert-panic.xml")

	summary := mustNewMergedSummary(t, failureReport, alertReport)
	require.Equal(t, []summaryRow{
		{Kind: failureTypeFailed, Name: "TestFoo", Details: "FAIL\n"},
		{Kind: failureTypePanic, Name: "panic test (retry 1) (final)", Details: "panic: boom", Final: true},
	}, summary.Rows)
}

func TestNewSummaryFromReports_RendersTrimmedFailureBodyRow(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-unexpected-call-failure.xml")

	summary := mustNewMergedSummary(t, report)
	require.Equal(t, []summaryRow{{
		Kind:    failureTypeFailed,
		Name:    "TestBaz",
		Details: "    mock_file.go:42: Unexpected call to SomeMethod(...)\n        expected call at foo_test.go:10 doesn't match argument at index 0.\n        Got:  \"actual\"\n        Want: is equal to \"expected\"",
	}}, summary.Rows)
}

func TestNewSummaryFromReports_RendersAlertRow(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-alert-data-race.xml")

	s := newSummaryFromReports([]*junitReport{report})
	require.Equal(t, []summaryRow{{
		Kind:    failureTypeDataRace,
		Name:    "DATA RACE: Data race detected in TestFoo",
		Details: "WARNING: DATA RACE\nWrite at 0x00c000123456 by goroutine 7:\n  main.TestFoo()\n      /path/to/foo_test.go:42 +0x123\n\nPrevious read at 0x00c000123456 by goroutine 8:\n  main.TestFoo()\n      /path/to/foo_test.go:43 +0x456",
	}}, s.Rows)
}

func TestNewSummaryFromReports_MergesMultipleReports(t *testing.T) {
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
		{Kind: failureTypeFailed, Name: "TestNew", Details: "new failure"},
		{Kind: failureTypeFailed, Name: "TestOld", Details: "old failure"},
	}, summary.Rows)
}

func TestNewSummaryRow_TruncatesOversizedDetail(t *testing.T) {
	detail := "head" + strings.Repeat("x", summaryMaxDetailBytes) + "tail"

	row := newSummaryRow(failureTypeFailed, "TestStandalone", detail)
	require.Equal(t, failureTypeFailed, row.Kind)
	require.Equal(t, "TestStandalone", row.Name)
	require.LessOrEqual(t, len(row.Details), summaryMaxDetailBytes)
	require.Contains(t, row.Details, "head")
	require.Contains(t, row.Details, "tail")
	require.Contains(t, row.Details, summaryTruncatedMarker)
}

func TestRenderSummaryFromReports_Markdown_RendersFailureRows(t *testing.T) {
	failureReport := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	failureReport.Suites[0].Testcases[0].Name = "TestFoo"
	failureReport.Suites[0].Testcases[0].Failure.Data = "FAIL\n"

	alertReport := mustReadReportFixture(t, "testdata/junit-alert-panic.xml")

	s := mustNewMergedSummary(t, failureReport, alertReport).Markdown()
	require.Contains(t, s, "<table>")
	require.NotContains(t, s, "<th>Details</th>")
	require.Contains(t, s, "<details><summary>TestFoo</summary>")
	require.Contains(t, s, "<pre>FAIL\n</pre>")
	require.Contains(t, s, "<details><summary>panic test (retry 1) (final)</summary>")
	require.Contains(t, s, "❌ PANIC")
	require.Equal(t, 2, strings.Count(s, "<tr><td>"))
}

func TestRenderSummaryFromReports_Markdown_RendersTrimmedFailureBody(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-unexpected-call-failure.xml")

	s := mustNewMergedSummary(t, report).Markdown()
	require.Contains(t, s, "<details><summary>TestBaz</summary>")
	require.Contains(t, s, "Unexpected call to SomeMethod")
	require.NotContains(t, s, "some setup log")
}

func TestRenderSummaryFromReports_Markdown_RendersAlertRow(t *testing.T) {
	report := mustReadReportFixture(t, "testdata/junit-alert-data-race.xml")

	rendered := newSummaryFromReports([]*junitReport{report}).Markdown()
	require.Contains(t, rendered, failureTypeDataRace)
	require.Contains(t, rendered, "Write at 0x00c000123456 by goroutine 7")
}

func TestRenderSummaryFromReports_Markdown_EmptyWhenNoFailures(t *testing.T) {
	s := newSummaryFromReports([]*junitReport{mustReadReportFixture(t, "testdata/junit-empty.xml")})
	require.Empty(t, s.Markdown())
}

func TestRenderSummaryFromReports_Markdown_ShowsTruncatedDetail(t *testing.T) {
	detail := "head" + strings.Repeat("x", summaryMaxDetailBytes) + "tail"
	summary := summary{
		Rows: []summaryRow{newSummaryRow(failureTypeFailed, "TestStandalone", detail)},
	}

	s := summary.Markdown()
	require.Contains(t, s, "truncated")
	require.Contains(t, s, "head")
	require.Contains(t, s, "tail")
	require.Less(t, len(s), summaryMarkdownMaxBytes)
}

func TestRenderSummaryFromReports_Markdown_OmitsRowsWhenBudgetExceeded(t *testing.T) {
	n := summaryMarkdownMaxBytes/summaryMaxDetailBytes + 1
	rows := make([]summaryRow, n)
	for i := range n {
		rows[i] = summaryRow{
			Kind:    failureTypeFailed,
			Name:    "Test" + strconv.Itoa(i),
			Details: strings.Repeat("x", summaryMaxDetailBytes),
		}
	}

	s := (summary{Rows: rows}).Markdown()
	require.Less(t, len(s), summaryMarkdownMaxBytes+len("</table>\n")+200)
	require.Contains(t, s, "not shown")
	require.Contains(t, s, "</table>")
}

func TestRenderSummaryFromReports_JSON(t *testing.T) {
	failureReport := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	failureReport.Suites[0].Testcases[0].Name = "TestFoo"
	failureReport.Suites[0].Testcases[0].Failure.Data = "FAIL\n"

	alertReport := mustReadReportFixture(t, "testdata/junit-alert-panic.xml")

	summary := mustNewMergedSummary(t, failureReport, alertReport)
	content, err := summary.JSON()
	require.NoError(t, err)

	require.JSONEq(t, `{
		"rows": [
			{
				"kind": "Failed",
				"name": "TestFoo",
				"details": "FAIL\n"
			},
			{
				"kind": "PANIC",
				"name": "panic test (retry 1) (final)",
				"details": "panic: boom",
				"final": true
			}
		]
	}`, string(content))
}

func mustNewMergedSummary(t *testing.T, reports ...*junitReport) summary {
	t.Helper()
	merged, err := mergeReports(reports)
	require.NoError(t, err)
	return newSummaryFromReports([]*junitReport{merged})
}
