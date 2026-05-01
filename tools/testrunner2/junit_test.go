package testrunner2

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "update golden files")

func generateStatic(names []string, suffix string, message string) *junitReport {
	var testcases []junit.Testcase
	for _, name := range names {
		testcases = append(testcases, junit.Testcase{
			Name:    fmt.Sprintf("%s (%s)", name, suffix),
			Failure: &junit.Result{Message: message},
		})
	}
	return &junitReport{
		Testsuites: junit.Testsuites{
			Tests:    len(names),
			Failures: len(names),
			Suites: []junit.Testsuite{
				{
					Name:      "suite",
					Tests:     len(names),
					Failures:  len(names),
					Testcases: testcases,
				},
			},
		},
	}
}

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

func TestMergeReports_PreservesFailureDetailsForSummary(t *testing.T) {
	t.Parallel()

	report := summaryReport("TestStandalone", "", "plain failure output with no recognizable block")

	merged, err := mergeReports([]*junitReport{report})
	require.NoError(t, err)

	failure := merged.Suites[0].Testcases[0].Failure
	require.Equal(t, string(failureTypeFailed), failure.Type)
	require.Equal(t, "plain failure output with no recognizable block", failure.Data)
}

func TestMergeReports_TrimsActionableFailureDetails(t *testing.T) {
	t.Parallel()

	report := summaryReport("TestFoo", "", "setup log\n    foo_test.go:99:\n        Error Trace:\tfoo_test.go:99\n        Error:      \tNot equal:\n                    \texpected: 1\n                    \tactual  : 2\n        Test:       \tTestFoo\n--- FAIL: TestFoo (0.00s)\n")

	merged, err := mergeReports([]*junitReport{report})
	require.NoError(t, err)

	failure := merged.Suites[0].Testcases[0].Failure
	require.Equal(t, string(failureTypeFailed), failure.Type)
	require.Contains(t, failure.Data, "Error Trace:")
	require.Contains(t, failure.Data, "expected: 1")
	require.NotContains(t, failure.Data, "setup log")
}

func TestAppendAlertsSuite(t *testing.T) {
	t.Parallel()

	j := &junitReport{}
	a := alerts{
		{Kind: failureKindDataRace, Summary: "Data race detected", Tests: []string{"fakepkg1.TestFoo1"}},
		{Kind: failureKindPanic, Summary: "This is a panic", Tests: []string{"fakepkg1.TestFoo2"}},
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

func TestParseGoTestJSONOutputIgnoresNestedGoTestOutput(t *testing.T) {
	t.Parallel()

	output := strings.Join([]string{
		`{"Time":"2026-05-01T18:22:00Z","Action":"start","Package":"go.temporal.io/server/tools/testrunner2"}`,
		`{"Time":"2026-05-01T18:22:01Z","Action":"run","Package":"go.temporal.io/server/tools/testrunner2","Test":"TestIntegration/passing_tests"}`,
		`{"Time":"2026-05-01T18:22:02Z","Action":"output","Package":"go.temporal.io/server/tools/testrunner2","Test":"TestIntegration/passing_tests","Output":"=== RUN   TestAlwaysFails\n"}`,
		`{"Time":"2026-05-01T18:22:02Z","Action":"output","Package":"go.temporal.io/server/tools/testrunner2","Test":"TestIntegration/passing_tests","Output":"--- FAIL: TestAlwaysFails (0.00s)\n"}`,
		`{"Time":"2026-05-01T18:22:02Z","Action":"output","Package":"go.temporal.io/server/tools/testrunner2","Test":"TestIntegration/passing_tests","Output":"FAIL\tgo.temporal.io/server/tools/testrunner2/testpkg/failing\t0.004s\n"}`,
		`{"Time":"2026-05-01T18:22:03Z","Action":"pass","Package":"go.temporal.io/server/tools/testrunner2","Test":"TestIntegration/passing_tests","Elapsed":1}`,
		`{"Time":"2026-05-01T18:22:04Z","Action":"pass","Package":"go.temporal.io/server/tools/testrunner2","Elapsed":2}`,
	}, "\n")

	report, isJSON, err := parseGoTestOutput(output)
	require.NoError(t, err)
	require.True(t, isJSON)

	results := extractResults(report)
	require.Empty(t, results.failures)
	require.Equal(t, []string{"TestIntegration/passing_tests"}, results.passes)
	require.Len(t, report.Packages, 1)
	require.Equal(t, "go.temporal.io/server/tools/testrunner2", report.Packages[0].Name)
}

func TestParseGoTestJSONOutputUsesLeafFailures(t *testing.T) {
	t.Parallel()

	output := strings.Join([]string{
		`{"Time":"2026-05-01T18:22:00Z","Action":"start","Package":"go.temporal.io/server/fake"}`,
		`{"Time":"2026-05-01T18:22:01Z","Action":"run","Package":"go.temporal.io/server/fake","Test":"TestSuite"}`,
		`{"Time":"2026-05-01T18:22:01Z","Action":"run","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase"}`,
		`{"Time":"2026-05-01T18:22:02Z","Action":"output","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase","Output":"    fake_test.go:12: failed\n"}`,
		`{"Time":"2026-05-01T18:22:03Z","Action":"fail","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase","Elapsed":1}`,
		`{"Time":"2026-05-01T18:22:03Z","Action":"fail","Package":"go.temporal.io/server/fake","Test":"TestSuite","Elapsed":2}`,
		`{"Time":"2026-05-01T18:22:04Z","Action":"fail","Package":"go.temporal.io/server/fake","Elapsed":3}`,
	}, "\n")

	report, isJSON, err := parseGoTestOutput(output)
	require.NoError(t, err)
	require.True(t, isJSON)

	results := extractResults(report)
	require.Len(t, results.failures, 1)
	require.Equal(t, "TestSuite/TestCase", results.failures[0].Name)
	require.Contains(t, results.failures[0].ErrorTrace, "fake_test.go:12")
	require.Equal(t, []testFailureRef{{pkg: "go.temporal.io/server/fake", name: "TestSuite/TestCase"}},
		(&junitReport{Testsuites: junit.CreateFromReport(report, "localhost")}).collectTestCaseFailureRefs())
}

func TestParseGoTestJSONOutputUsesAncestorOutputForFailedLeaf(t *testing.T) {
	t.Parallel()

	output := strings.Join([]string{
		`{"Time":"2026-05-01T18:22:00Z","Action":"start","Package":"go.temporal.io/server/fake"}`,
		`{"Time":"2026-05-01T18:22:01Z","Action":"run","Package":"go.temporal.io/server/fake","Test":"TestSuite"}`,
		`{"Time":"2026-05-01T18:22:01Z","Action":"run","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase"}`,
		`{"Time":"2026-05-01T18:22:01Z","Action":"run","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase/Leaf"}`,
		`{"Time":"2026-05-01T18:22:02Z","Action":"output","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase","Output":"    fake_test.go:12: failed\\n"}`,
		`{"Time":"2026-05-01T18:22:02Z","Action":"output","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase","Output":"        Error Trace:\\tfake_test.go:12\\n"}`,
		`{"Time":"2026-05-01T18:22:03Z","Action":"fail","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase/Leaf","Elapsed":1}`,
		`{"Time":"2026-05-01T18:22:03Z","Action":"fail","Package":"go.temporal.io/server/fake","Test":"TestSuite/TestCase","Elapsed":2}`,
		`{"Time":"2026-05-01T18:22:03Z","Action":"fail","Package":"go.temporal.io/server/fake","Test":"TestSuite","Elapsed":3}`,
		`{"Time":"2026-05-01T18:22:04Z","Action":"fail","Package":"go.temporal.io/server/fake","Elapsed":4}`,
	}, "\n")

	report, isJSON, err := parseGoTestOutput(output)
	require.NoError(t, err)
	require.True(t, isJSON)

	results := extractResults(report)
	require.Len(t, results.failures, 1)
	require.Equal(t, "TestSuite/TestCase/Leaf", results.failures[0].Name)
	require.Contains(t, results.failures[0].ErrorTrace, "Error Trace:")

	testsuites := junit.CreateFromReport(report, "localhost")
	require.Len(t, testsuites.Suites, 1)
	require.Len(t, testsuites.Suites[0].Testcases, 1)
	failure := testsuites.Suites[0].Testcases[0].Failure
	require.NotNil(t, failure)
	require.Contains(t, failure.Data, "fake_test.go:12")
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

			// Write test data to a temp log file (newJUnitReport reads from disk)
			logFile, err := os.CreateTemp("", "testlog-*.log")
			require.NoError(t, err)
			defer func() { _ = os.Remove(logFile.Name()) }()
			_, err = logFile.Write(data)
			require.NoError(t, err)
			_ = logFile.Close()

			tmpFile, err := os.CreateTemp("", "junit-*.xml")
			require.NoError(t, err)
			defer func() { _ = os.Remove(tmpFile.Name()) }()
			_ = tmpFile.Close()

			results := newJUnitReport(logFile.Name(), tmpFile.Name())

			var failureNames []string
			for _, f := range results.failures {
				failureNames = append(failureNames, f.Name)
			}
			require.Equal(t, tt.expectedFailures, failureNames, "failures mismatch")
			require.Equal(t, tt.expectedPasses, results.passes, "passes mismatch")
		})
	}
}
