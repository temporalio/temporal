package testrunner

import (
	"os"
	"slices"
	"testing"

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

	j := generateForTimedoutTests([]string{
		"TestCallbacksSuite/TestWorkflowCallbacks_1",
		"TestCallbacksSuite/TestWorkflowCallbacks_2",
	})
	j.path = out.Name()
	require.NoError(t, j.write())

	expectedReport, err := os.ReadFile("testdata/junit-timeout-output.xml")
	require.NoError(t, err)
	actualReport, err := os.ReadFile(out.Name())
	require.NoError(t, err)
	require.Equal(t, string(expectedReport), string(actualReport))
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

func TestMergeReports(t *testing.T) {
	j1 := &junitReport{path: "testdata/junit-attempt-1.xml"}
	require.NoError(t, j1.read())
	j2 := &junitReport{path: "testdata/junit-attempt-2.xml"}
	require.NoError(t, j2.read())

	report, err := mergeReports([]*junitReport{j1, j2})
	require.NoError(t, err)

	suites := report.Testsuites.Suites
	require.Len(t, suites, 2)
	require.Equal(t, 4, report.Testsuites.Failures)
	require.Equal(t, "go.temporal.io/server/tests (retry 1)", suites[1].Name)
	require.Len(t, suites[1].Testcases, 2)
	require.Equal(t, "TestCallbacksSuite/TestWorkflowCallbacks_InvalidArgument (retry 1)", suites[1].Testcases[0].Name)
}
