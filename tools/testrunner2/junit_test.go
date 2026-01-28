package testrunner2

import (
	"flag"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update golden files")

func TestGenerateJUnitReportForTimedoutTests(t *testing.T) {
	t.Parallel()

	out, err := os.CreateTemp("", "junit-report-*.xml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(out.Name()) }()

	testNames := []string{
		"TestFooSuite/TestFoo_1",
		"TestFooSuite/TestFoo_2",
	}
	j := generateStatic(testNames, "timeout", "Timeout")
	j.path = out.Name()
	require.NoError(t, j.write())
	requireReportEquals(t, "testdata/timeout_output.junit.xml", out.Name())
}

func TestMergeReports_SingleReport(t *testing.T) {
	t.Parallel()

	j1 := &junitReport{path: "testdata/attempt1.junit.xml"}
	require.NoError(t, j1.read())

	report, err := mergeReports([]*junitReport{j1})
	require.NoError(t, err)

	out, err := os.CreateTemp("", "junit-merge-*.xml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(out.Name()) }()

	report.path = out.Name()
	require.NoError(t, report.write())
	requireReportEquals(t, "testdata/merge_single.junit.xml", out.Name())
}

func TestMergeReports_MultipleReports(t *testing.T) {
	t.Parallel()

	j1 := &junitReport{path: "testdata/attempt1.junit.xml", attempt: 1}
	require.NoError(t, j1.read())
	j2 := &junitReport{path: "testdata/attempt2.junit.xml", attempt: 2}
	require.NoError(t, j2.read())

	report, err := mergeReports([]*junitReport{j1, j2})
	require.NoError(t, err)
	require.Empty(t, report.reportingErrs)

	out, err := os.CreateTemp("", "junit-merge-*.xml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(out.Name()) }()

	report.path = out.Name()
	require.NoError(t, report.write())
	requireReportEquals(t, "testdata/merge_multiple.junit.xml", out.Name())
}

func TestMergeReports_IndependentReports(t *testing.T) {
	t.Parallel()

	j1 := &junitReport{path: "testdata/attempt1.junit.xml", attempt: 1}
	require.NoError(t, j1.read())
	j2 := &junitReport{path: "testdata/empty.junit.xml", attempt: 1}
	require.NoError(t, j2.read())
	j3 := &junitReport{path: "testdata/attempt2.junit.xml", attempt: 1}
	require.NoError(t, j3.read())

	report, err := mergeReports([]*junitReport{j1, j2, j3})
	require.NoError(t, err)
	require.Empty(t, report.reportingErrs)

	out, err := os.CreateTemp("", "junit-merge-*.xml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(out.Name()) }()

	report.path = out.Name()
	require.NoError(t, report.write())
	requireReportEquals(t, "testdata/merge_independent.junit.xml", out.Name())
}

func TestMergeReports_MissingRetry(t *testing.T) {
	t.Parallel()

	j1 := &junitReport{path: "testdata/attempt1.junit.xml", attempt: 1}
	require.NoError(t, j1.read())
	j2 := &junitReport{path: "testdata/empty.junit.xml", attempt: 2}
	require.NoError(t, j2.read())

	report, err := mergeReports([]*junitReport{j1, j2})
	require.NoError(t, err)
	require.Len(t, report.reportingErrs, 1)
	require.EqualError(t, report.reportingErrs[0],
		"expected rerun of all failures from attempt 1, missing in attempt 2: [TestFooSuite/TestFoo_InvalidArgument]")
}

func TestAppendAlertsSuite(t *testing.T) {
	t.Parallel()

	j := &junitReport{}
	a := alerts{
		{Kind: failureKindDataRace, Summary: "Data race detected", Details: "WARNING: DATA RACE\n...", Tests: []string{"fakepkg1.TestFoo1"}},
		{Kind: failureKindPanic, Summary: "This is a panic", Details: "panic: This is a panic\n...", Tests: []string{"fakepkg1.TestFoo2"}},
	}
	j.appendAlerts(a)

	out, err := os.CreateTemp("", "junit-alerts-*.xml")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(out.Name()))
	}()

	j.path = out.Name()
	require.NoError(t, j.write())
	requireReportEquals(t, "testdata/alerts_output.junit.xml", out.Name())
}

func requireReportEquals(t *testing.T, goldenFile, actualFile string) {
	t.Helper()

	actual, err := os.ReadFile(actualFile)
	require.NoError(t, err)

	if *update {
		require.NoError(t, os.WriteFile(goldenFile, actual, 0644))
		return
	}

	expected, err := os.ReadFile(goldenFile)
	require.NoError(t, err)
	require.Equal(t, string(expected), string(actual))
}

func TestExtractResults_Timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		fixture          string
		expectedFailures []string
		expectedPasses   []string
	}{
		{
			// timeout1.log: TestFoo1 passes, then TestFoo2 times out.
			// The panic interrupts TestFoo2 before it can finish, so it's not marked as FAIL.
			// TestFoo1 should be in passes since it completed with "--- PASS".
			name:             "partial_timeout_with_passed_test",
			fixture:          "timeout1.log",
			expectedFailures: nil,
			expectedPasses:   []string{"TestFoo1"},
		},
		{
			// timeout2.log: TestFoo1/SubTest1 and TestFoo2 are all running when timeout occurs.
			// None complete, so none are marked as PASS or FAIL.
			name:             "all_running_at_timeout",
			fixture:          "timeout2.log",
			expectedFailures: nil,
			expectedPasses:   nil,
		},
		{
			// When a test explicitly fails then times out, the failure is recorded.
			name:    "explicit_fail_before_timeout",
			fixture: "fail_then_timeout.log",
			expectedFailures: []string{
				"TestFoo1",
			},
			expectedPasses: nil,
		},
		{
			// pass_fail_timeout.log: TestFoo1 passes, TestFoo2 fails, TestFoo3 times out.
			// This tests the key scenario where we have both passed and failed tests.
			name:    "pass_fail_then_timeout",
			fixture: "pass_fail_timeout.log",
			expectedFailures: []string{
				"TestFoo2",
			},
			expectedPasses: []string{"TestFoo1"},
		},
		{
			// suite_timeout.log: Testify-style suite with subtests.
			// With -test.v=test2json, Go emits --- PASS lines as subtests complete,
			// allowing us to know which tests passed before a timeout.
			name:             "testify_suite_with_timeout",
			fixture:          "suite_timeout.log",
			expectedFailures: nil,
			expectedPasses: []string{
				"TestVersioningSuite/TestFirstMethod",
				"TestVersioningSuite/TestSecondMethod",
				"TestVersioningSuite/TestThirdMethod",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := os.ReadFile("testdata/" + tt.fixture)
			if os.IsNotExist(err) {
				t.Skip("fixture not found:", tt.fixture)
			}
			require.NoError(t, err)

			tmpFile, err := os.CreateTemp("", "junit-*.xml")
			require.NoError(t, err)
			defer func() { _ = os.Remove(tmpFile.Name()) }()
			_ = tmpFile.Close()

			results := newJUnitReport(string(data), tmpFile.Name())

			var failureNames []string
			for _, f := range results.failures {
				failureNames = append(failureNames, f.Name)
			}
			require.Equal(t, tt.expectedFailures, failureNames, "failures mismatch")
			require.Equal(t, tt.expectedPasses, results.passes, "passes mismatch")
		})
	}
}
