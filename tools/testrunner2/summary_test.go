package testrunner2

import (
	"bytes"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/stretchr/testify/require"
)

func TestSummaryRendersToggleDetails(t *testing.T) {
	t.Parallel()

	report := summaryReport("TestFoo", string(failureTypeFailed), "expected <actual>")

	summary := newSummaryFromReports([]*junitReport{report})
	require.Equal(t, []summaryRow{{
		kind:    failureTypeFailed,
		name:    "TestFoo",
		details: "expected <actual>",
	}}, summary.rows)

	rendered := summary.String()
	require.Contains(t, rendered, "<table>")
	require.Contains(t, rendered, "<details><summary>TestFoo</summary>")
	require.Contains(t, rendered, "expected &lt;actual&gt;")
	require.NotContains(t, rendered, "<th>Details</th>")
}

func TestSummaryEmptyWhenNoFailures(t *testing.T) {
	t.Parallel()

	report := &junitReport{Testsuites: junit.Testsuites{
		Suites: []junit.Testsuite{{Testcases: []junit.Testcase{{Name: "TestFoo"}}}},
	}}

	summary := newSummaryFromReports([]*junitReport{report})
	require.Empty(t, summary.rows)
	require.Empty(t, summary.String())
}

func TestSummaryTruncatesOversizedDetails(t *testing.T) {
	t.Parallel()

	report := summaryReport("TestFoo", string(failureTypeFailed), strings.Repeat("x", summaryMaxDetailBytes+1))

	rendered := newSummaryFromReports([]*junitReport{report}).String()
	require.Contains(t, rendered, "truncated")
	require.Less(t, len(rendered), summaryMaxBytes)
}

func TestPrintSummary(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	report1 := summaryReport("TestAlpha", string(failureTypeFailed), "alpha failure")
	report1.path = filepath.Join(dir, "junit.alpha.xml")
	require.NoError(t, report1.write())
	report2 := summaryReport("TestBeta", string(failureTypePanic), "beta failure")
	report2.path = filepath.Join(dir, "junit.beta.xml")
	require.NoError(t, report2.write())

	var out bytes.Buffer
	require.NoError(t, printSummary([]string{
		junitGlobFlag + "=" + filepath.Join(dir, "junit.*.xml"),
	}, &out))

	body := out.String()
	require.Equal(t, 1, strings.Count(body, "<table>"))
	require.Contains(t, body, "<details><summary>TestAlpha</summary>")
	require.Contains(t, body, "<details><summary>TestBeta</summary>")
}

func summaryReport(name, kind, details string) *junitReport {
	return &junitReport{Testsuites: junit.Testsuites{
		Tests:    1,
		Failures: 1,
		Suites: []junit.Testsuite{{
			Name:     "suite",
			Tests:    1,
			Failures: 1,
			Testcases: []junit.Testcase{{
				Name: name,
				Failure: &junit.Result{
					Message: kind,
					Type:    kind,
					Data:    details,
				},
			}},
		}},
	}}
}
