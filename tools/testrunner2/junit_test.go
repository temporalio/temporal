package testrunner2

import (
	"encoding/xml"
	"os"
	"slices"
	"testing"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/stretchr/testify/require"
)

func TestReadJUnitReport(t *testing.T) {
	j := &junitReport{path: "testdata/foo_attempt1.junit.xml"}
	require.NoError(t, j.read())
	require.Len(t, j.Suites, 1)
	require.Equal(t, 2, j.Failures)
	require.Equal(t, []string{"TestFooSuite/TestFoo_InvalidArgument"}, j.collectTestCaseFailures())
}

func TestCollectTestCasePasses(t *testing.T) {
	tests := []struct {
		name     string
		xmlData  string
		expected []string
	}{
		{
			name: "normal_passed_tests",
			xmlData: `<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="2">
	<testsuite name="test" tests="2">
		<testcase name="TestOne" time="1.500"></testcase>
		<testcase name="TestTwo" time="2.000"></testcase>
	</testsuite>
</testsuites>`,
			expected: []string{"TestOne", "TestTwo"},
		},
		{
			name: "fast_test_with_zero_time_is_collected",
			xmlData: `<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="2">
	<testsuite name="test" tests="2">
		<testcase name="TestFast" time="0.000"></testcase>
		<testcase name="TestNormal" time="5.000"></testcase>
	</testsuite>
</testsuites>`,
			expected: []string{"TestFast", "TestNormal"},
		},
		{
			name: "test_with_error_is_not_collected",
			xmlData: `<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="3" errors="1">
	<testsuite name="test" tests="3" errors="1">
		<testcase name="TestPassed" time="5.000"></testcase>
		<testcase name="TestFastPassed" time="0.000"></testcase>
		<testcase name="TestTimedOut" time="0.000">
			<error message="No test result found">panic: test timed out</error>
		</testcase>
	</testsuite>
</testsuites>`,
			expected: []string{"TestPassed", "TestFastPassed"},
		},
		{
			name: "test_with_failure_is_not_collected",
			xmlData: `<?xml version="1.0" encoding="UTF-8"?>
<testsuites tests="2" failures="1">
	<testsuite name="test" tests="2" failures="1">
		<testcase name="TestPassed" time="5.000"></testcase>
		<testcase name="TestFailed" time="3.000">
			<failure message="assertion failed">expected true, got false</failure>
		</testcase>
	</testsuite>
</testsuites>`,
			expected: []string{"TestPassed"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &junitReport{}
			err := xml.Unmarshal([]byte(tt.xmlData), &j.Testsuites)
			require.NoError(t, err)

			passes := j.collectTestCasePasses()
			require.Equal(t, tt.expected, passes)
		})
	}
}

func TestGenerateJUnitReportForTimedoutTests(t *testing.T) {
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
	requireReportEquals(t, "testdata/foo_timeout_output.junit.xml", out.Name())
}

func TestNode(t *testing.T) {
	n := testCaseNode{
		children: map[string]testCaseNode{
			"a": {
				children: map[string]testCaseNode{
					"b": {
						children: make(map[string]testCaseNode),
					},
				},
			},
			"b": {
				children: make(map[string]testCaseNode),
			},
		},
	}

	var paths []string
	for p := range n.walk() {
		paths = append(paths, p)
	}
	slices.Sort(paths)
	require.Equal(t, []string{"a", "a/b", "b"}, paths)
}

func TestMergeReports_SingleReport(t *testing.T) {
	j1 := &junitReport{path: "testdata/foo_attempt1.junit.xml"}
	require.NoError(t, j1.read())

	report, err := mergeReports([]*junitReport{j1})
	require.NoError(t, err)

	suites := report.Suites
	require.Len(t, suites, 1)
	require.Equal(t, 2, report.Failures)

	testNames := collectTestNames(suites)
	require.Len(t, testNames, 5)
	require.NotContains(t, testNames, "TestFooSuite")
	require.NotContains(t, testNames, "TestFooSuite/TestFoo_CarriedOver")
}

func TestMergeReports_MultipleReports(t *testing.T) {
	j1 := &junitReport{path: "testdata/foo_attempt1.junit.xml", attempt: 1}
	require.NoError(t, j1.read())
	j2 := &junitReport{path: "testdata/foo_attempt2.junit.xml", attempt: 2}
	require.NoError(t, j2.read())

	report, err := mergeReports([]*junitReport{j1, j2})
	require.NoError(t, err)
	require.Empty(t, report.reportingErrs)

	suites := report.Suites
	require.Len(t, suites, 2)
	require.Equal(t, 4, report.Failures)
	require.Equal(t, "go.temporal.io/server/tools/testrunner2/testdata/fakepkg1", suites[0].Name)
	require.Equal(t, "go.temporal.io/server/tools/testrunner2/testdata/fakepkg1 (retry 1)", suites[1].Name)

	testNames := collectTestNames(suites)
	require.Len(t, testNames, 6)
	require.NotContains(t, testNames, "TestFooSuite")
	require.NotContains(t, testNames, "TestFooSuite/TestFoo_CarriedOver")
	require.Contains(t, testNames, "TestFooSuite/TestFoo_InvalidArgument")
	require.Contains(t, testNames, "TestFooSuite/TestFoo_InvalidArgument (retry 1)")
}

func TestMergeReports_IndependentReports(t *testing.T) {
	// Test merging independent reports from parallel execution (no retries)
	j1 := &junitReport{path: "testdata/foo_attempt1.junit.xml", attempt: 1}
	require.NoError(t, j1.read())
	j2 := &junitReport{path: "testdata/empty.junit.xml", attempt: 1}
	require.NoError(t, j2.read())
	j3 := &junitReport{path: "testdata/foo_attempt2.junit.xml", attempt: 1}
	require.NoError(t, j3.read())

	report, err := mergeReports([]*junitReport{j1, j2, j3})
	require.NoError(t, err)
	require.Empty(t, report.reportingErrs)

	// All suites should have no suffix since they're all first attempts
	for _, suite := range report.Suites {
		require.NotContains(t, suite.Name, "retry")
	}
}

func TestMergeReports_MissingRetry(t *testing.T) {
	// Test that validation catches when a failed test is not retried
	j1 := &junitReport{path: "testdata/foo_attempt1.junit.xml", attempt: 1}
	require.NoError(t, j1.read())
	// j2 uses empty report (no tests), so the failure from j1 won't be retried
	j2 := &junitReport{path: "testdata/empty.junit.xml", attempt: 2}
	require.NoError(t, j2.read())

	report, err := mergeReports([]*junitReport{j1, j2})
	require.NoError(t, err)
	require.NotEmpty(t, report.reportingErrs, "should report missing retry")
	require.Contains(t, report.reportingErrs[0].Error(), "missing in attempt 2")
}

func TestMergeReports_AlertsUseTestName(t *testing.T) {
	// Alerts should use the test name so they're properly tracked for retries
	j1 := &junitReport{path: "testdata/empty.junit.xml", attempt: 1}
	require.NoError(t, j1.read())
	j1.appendAlerts(alerts{
		{Kind: failureKindTimeout, Summary: "test timed out after 10m0s", Tests: []string{"TestFoo"}},
	})

	// Verify the alert uses the test name, not a synthetic name
	require.Len(t, j1.Suites, 1)
	require.Equal(t, "ALERTS", j1.Suites[0].Name)
	require.Len(t, j1.Suites[0].Testcases, 1)
	require.Equal(t, "TestFoo", j1.Suites[0].Testcases[0].Name)
	require.Contains(t, j1.Suites[0].Testcases[0].Failure.Message, "timeout")
}

func TestAppendAlertsSuite(t *testing.T) {
	j := &junitReport{}
	a := alerts{
		{Kind: failureKindDataRace, Summary: "Data race detected", Details: "WARNING: DATA RACE\n...", Tests: []string{"fakepkg1.TestFoo1"}},
		{Kind: failureKindPanic, Summary: "This is a panic", Details: "panic: This is a panic\n...", Tests: []string{"fakepkg1.TestFoo2"}},
	}
	j.appendAlerts(a)

	// Write the report to a temporary file for comparison
	out, err := os.CreateTemp("", "junit-alerts-*.xml")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(out.Name()))
	}()

	j.path = out.Name()
	require.NoError(t, j.write())

	// Compare against the expected output file
	requireReportEquals(t, "testdata/foo_alerts_output.junit.xml", out.Name())
}

func collectTestNames(suites []junit.Testsuite) []string {
	var testNames []string
	for _, suite := range suites {
		for _, test := range suite.Testcases {
			testNames = append(testNames, test.Name)
		}
	}
	return testNames
}

func requireReportEquals(t *testing.T, expectedFile, actualFile string) {
	expectedReport, err := os.ReadFile(expectedFile)
	require.NoError(t, err)

	actualReport, err := os.ReadFile(actualFile)
	require.NoError(t, err)
	require.Equal(t, string(expectedReport), string(actualReport))
}

func TestJUnitXMLWellFormed(t *testing.T) {
	// Test that written JUnit XML is well-formed and can be parsed
	tests := []struct {
		name  string
		setup func() *junitReport
	}{
		{
			name: "basic_report",
			setup: func() *junitReport {
				return generateStatic([]string{"TestBasic"}, "test", "Test failed")
			},
		},
		{
			name: "report_with_alerts",
			setup: func() *junitReport {
				j := &junitReport{}
				a := alerts{
					{
						Kind:    failureKindPanic,
						Summary: "runtime error: invalid memory address or nil pointer dereference",
						Details: "panic: runtime error: invalid memory address or nil pointer dereference\n[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x123456]\n\ngoroutine 1 [running]:\nmain.TestPanic()\n\t/path/to/test.go:123 +0x456",
						Tests: []string{
							"TestTaskQueueStats_Pri_Suite/TestAddMultipleTasks_MultiplePartitions_ValidateStats_Cached",
							"TestTaskQueueStats_Pri_Suite/TestDescribeTaskQueue_NonRoot",
							"TestTaskQueueStats_Pri_Suite/TestMultipleTasks_MultiplePartitions_WithMatchingBehavior_ValidateStats",
						},
					},
					{
						Kind:    failureKindDataRace,
						Summary: "Data race detected",
						Details: "WARNING: DATA RACE\nWrite at 0x00c000123456 by goroutine 7:\n  runtime.racewrite()\n      /usr/local/go/src/runtime/race_amd64.s:269 +0x21\n  main.TestDataRace()\n      /path/to/test.go:456 +0x789\n\nPrevious read at 0x00c000123456 by goroutine 8:\n  runtime.raceread()\n      /usr/local/go/src/runtime/race_amd64.s:260 +0x21\n  main.TestDataRace()\n      /path/to/test.go:789 +0xabc",
						Tests:   []string{"TestDataRace"},
					},
				}
				j.appendAlerts(a)
				return j
			},
		},
		{
			name: "report_with_special_characters",
			setup: func() *junitReport {
				j := &junitReport{}
				a := alerts{
					{
						Kind:    failureKindFatal,
						Summary: "Fatal error with special chars: <>&\"'",
						Details: "fatal error: unexpected signal during runtime execution\n[signal SIGABRT: abort]\n\nStack trace:\n<function> & \"quoted\" 'string'\n\t/path/to/file.go:123",
						Tests:   []string{"TestSpecialChars"},
					},
				}
				j.appendAlerts(a)
				return j
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary file
			out, err := os.CreateTemp("", "junit-validation-*.xml")
			require.NoError(t, err)
			defer func() {
				require.NoError(t, os.Remove(out.Name()))
			}()

			// Setup the report
			j := tt.setup()
			j.path = out.Name()

			// Write the report
			require.NoError(t, j.write())

			// Read the written file
			content, err := os.ReadFile(out.Name())
			require.NoError(t, err)

			// Validate that the content is well-formed XML
			var parsed junit.Testsuites
			err = xml.Unmarshal(content, &parsed)
			require.NoError(t, err, "Written XML should be well-formed and parseable")

			// Additional validation: ensure we can re-parse it using our own read method
			j2 := &junitReport{path: out.Name()}
			require.NoError(t, j2.read(), "Should be able to re-read the written XML")

			// Validate that the structure is reasonable
			require.NotEmpty(t, parsed.Suites, "Should have at least one test suite")
		})
	}
}

func TestExtractResults_Timeout(t *testing.T) {
	// This test verifies that extractResults correctly identifies passed and failed tests
	// from test output, even when a timeout occurs. This is critical because the JUnit XML
	// may mark all tests as errors, but the parsed gtr.Report has accurate pass/fail info.
	tests := []struct {
		name             string
		fixture          string
		expectedFailures []string
		expectedPasses   []string
	}{
		{
			// foo_timeout1.log: TestFoo1 passes, then TestFoo2 times out.
			// The panic interrupts TestFoo2 before it can finish, so it's not marked as FAIL.
			// TestFoo1 should be in passes since it completed with "--- PASS".
			name:             "partial_timeout_with_passed_test",
			fixture:          "foo_timeout1.log",
			expectedFailures: nil,
			expectedPasses:   []string{"TestFoo1"},
		},
		{
			// foo_timeout2.log: TestFoo1/SubTest1 and TestFoo2 are all running when timeout occurs.
			// None complete, so none are marked as PASS or FAIL.
			name:             "all_running_at_timeout",
			fixture:          "foo_timeout2.log",
			expectedFailures: nil,
			expectedPasses:   nil,
		},
		{
			// When a test explicitly fails then times out, the failure is recorded.
			// No tests passed in this scenario.
			name:    "explicit_fail_before_timeout",
			fixture: "foo_fail_then_timeout.log",
			expectedFailures: []string{
				"TestFoo1",
			},
			expectedPasses: nil,
		},
		{
			// foo_pass_fail_timeout.log: TestFoo1 passes, TestFoo2 fails, TestFoo3 times out.
			// This tests the key scenario where we have both passed and failed tests.
			name:    "pass_fail_then_timeout",
			fixture: "foo_pass_fail_timeout.log",
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
