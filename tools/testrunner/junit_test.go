package testrunner

import (
	"encoding/xml"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/jstemmer/go-junit-report/v2/junit"
	"github.com/stretchr/testify/require"
)

func TestReadJUnitReport(t *testing.T) {
	j := &junitReport{path: "testdata/junit-attempt-1.xml"}
	require.NoError(t, j.read())
	require.Len(t, j.Suites, 1)
	require.Equal(t, 2, j.Failures)
	require.Equal(t, []string{"TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument"}, j.collectTestCaseFailures())
}

func TestGenerateJUnitReportForTimedoutTests(t *testing.T) {
	out, err := os.CreateTemp("", "junit-report-*.xml")
	require.NoError(t, err)
	defer os.Remove(out.Name())

	testNames := []string{
		"TestCallbacksSuite/TestWorkflowCallbacks_1",
		"TestCallbacksSuite/TestWorkflowCallbacks_2",
	}
	j := generateStatic(testNames, "timeout", "Timeout")
	j.path = out.Name()
	require.NoError(t, j.write())
	requireReportEquals(t, "testdata/junit-timeout-output.xml", out.Name())
}

func TestNode(t *testing.T) {
	n := node{
		children: map[string]node{
			"a": {
				children: map[string]node{
					"b": {
						children: make(map[string]node),
					},
				},
			},
			"b": {
				children: make(map[string]node),
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
	j1 := &junitReport{path: "testdata/junit-attempt-1.xml"}
	require.NoError(t, j1.read())

	report, err := mergeReports([]*junitReport{j1})
	require.NoError(t, err)

	suites := report.Testsuites.Suites
	require.Len(t, suites, 1)
	require.Equal(t, 2, report.Testsuites.Failures)

	testNames := collectTestNames(suites)
	require.Len(t, testNames, 5)
	require.NotContains(t, testNames, "TestCallbacksSuite")
	require.NotContains(t, testNames, "TestCallbacksSuite/TestWorkflowNexusCallbacks_CarriedOver")
}

func TestMergeReports_MultipleReports(t *testing.T) {
	j1 := &junitReport{path: "testdata/junit-attempt-1.xml"}
	require.NoError(t, j1.read())
	j2 := &junitReport{path: "testdata/junit-attempt-2.xml"}
	require.NoError(t, j2.read())

	report, err := mergeReports([]*junitReport{j1, j2})
	require.NoError(t, err)
	require.Empty(t, report.reportingErrs)

	suites := report.Testsuites.Suites
	require.Len(t, suites, 2)
	require.Equal(t, 4, report.Testsuites.Failures)
	require.Equal(t, "go.temporal.io/server/tests", suites[0].Name)
	require.Equal(t, "go.temporal.io/server/tests (retry 1)", suites[1].Name)

	testNames := collectTestNames(suites)
	require.Len(t, testNames, 6)
	require.NotContains(t, testNames, "TestCallbacksSuite")
	require.NotContains(t, testNames, "TestCallbacksSuite/TestWorkflowNexusCallbacks_CarriedOver")
	require.Contains(t, testNames, "TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument")
	require.Contains(t, testNames, "TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument (retry 1)")
}

func TestMergeReports_MissingRerun(t *testing.T) {
	j1 := &junitReport{path: "testdata/junit-attempt-1.xml"}
	require.NoError(t, j1.read())
	j2 := &junitReport{path: "testdata/junit-empty.xml"}
	require.NoError(t, j2.read())
	j3 := &junitReport{path: "testdata/junit-attempt-2.xml"}
	require.NoError(t, j3.read())
	j4 := &junitReport{path: "testdata/junit-empty.xml"}
	require.NoError(t, j4.read())

	report, err := mergeReports([]*junitReport{j1, j2, j3, j4})
	require.NoError(t, err)
	require.Len(t, report.reportingErrs, 2)
	require.Equal(t, errors.New("expected rerun of all failures from previous attempt, missing: [TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument]"), report.reportingErrs[0])
	require.Equal(t, errors.New("expected rerun of all failures from previous attempt, missing: [TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument]"), report.reportingErrs[1])
}

func TestAppendAlertsSuite(t *testing.T) {
	j := &junitReport{}
	alerts := []alert{
		{Kind: alertKindDataRace, Summary: "Data race detected", Details: "WARNING: DATA RACE\n...", Tests: []string{"go.temporal.io/server/tools/testrunner.TestShowPanic"}},
		{Kind: alertKindPanic, Summary: "This is a panic", Details: "panic: This is a panic\n...", Tests: []string{"TestPanicExample"}},
	}
	j.appendAlertsSuite(alerts)

	// Write the report to a temporary file for comparison
	out, err := os.CreateTemp("", "junit-alerts-*.xml")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(out.Name()))
	}()

	j.path = out.Name()
	require.NoError(t, j.write())

	// Compare against the expected output file
	requireReportEquals(t, "testdata/junit-alerts-output.xml", out.Name())
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
				alerts := []alert{
					{
						Kind:    alertKindPanic,
						Summary: "runtime error: invalid memory address or nil pointer dereference",
						Details: "panic: runtime error: invalid memory address or nil pointer dereference\n[signal SIGSEGV: segmentation violation code=0x1 addr=0x0 pc=0x123456]\n\ngoroutine 1 [running]:\nmain.TestPanic()\n\t/path/to/test.go:123 +0x456",
						Tests: []string{
							"TestTaskQueueStats_Pri_Suite/TestAddMultipleTasks_MultiplePartitions_ValidateStats_Cached",
							"TestTaskQueueStats_Pri_Suite/TestDescribeTaskQueue_NonRoot",
							"TestTaskQueueStats_Pri_Suite/TestMultipleTasks_MultiplePartitions_WithMatchingBehavior_ValidateStats",
						},
					},
					{
						Kind:    alertKindDataRace,
						Summary: "Data race detected",
						Details: "WARNING: DATA RACE\nWrite at 0x00c000123456 by goroutine 7:\n  runtime.racewrite()\n      /usr/local/go/src/runtime/race_amd64.s:269 +0x21\n  main.TestDataRace()\n      /path/to/test.go:456 +0x789\n\nPrevious read at 0x00c000123456 by goroutine 8:\n  runtime.raceread()\n      /usr/local/go/src/runtime/race_amd64.s:260 +0x21\n  main.TestDataRace()\n      /path/to/test.go:789 +0xabc",
						Tests:   []string{"TestDataRace"},
					},
				}
				j.appendAlertsSuite(alerts)
				return j
			},
		},
		{
			name: "report_with_special_characters",
			setup: func() *junitReport {
				j := &junitReport{}
				alerts := []alert{
					{
						Kind:    alertKindFatal,
						Summary: "Fatal error with special chars: <>&\"'",
						Details: "fatal error: unexpected signal during runtime execution\n[signal SIGABRT: abort]\n\nStack trace:\n<function> & \"quoted\" 'string'\n\t/path/to/file.go:123",
						Tests:   []string{"TestSpecialChars"},
					},
				}
				j.appendAlertsSuite(alerts)
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
			require.Greater(t, len(parsed.Suites), 0, "Should have at least one test suite")
		})
	}
}
