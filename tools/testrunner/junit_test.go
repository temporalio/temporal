package testrunner

import (
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
	require.Len(t, j.Testsuites.Suites, 1)
	require.Equal(t, 2, j.Testsuites.Failures)
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
