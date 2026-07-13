package testrunner

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGoTestJSONOutput_ExcludesIncompleteTests(t *testing.T) {
	output := newGoTestJSONOutput()
	output.stdout = io.Discard
	_, err := output.Write([]byte(strings.Join([]string{
		`{"Action":"run","Package":"go.temporal.io/server/tests","Test":"TestWorkflowUpdateSuite/TestValidateWorkerMessages/success-case"}`,
		`{"Action":"output","Package":"go.temporal.io/server/tests","Test":"TestWorkflowUpdateSuite/TestValidateWorkerMessages/success-case","Output":"=== RUN   TestWorkflowUpdateSuite/TestValidateWorkerMessages/success-case\n"}`,
		`{"Action":"pause","Package":"go.temporal.io/server/tests","Test":"TestWorkflowUpdateSuite/TestValidateWorkerMessages/success-case"}`,
		`{"Action":"run","Package":"go.temporal.io/server/tests","Test":"TestRealFailure"}`,
		`{"Action":"output","Package":"go.temporal.io/server/tests","Test":"TestRealFailure","Output":"=== RUN   TestRealFailure\n    foo_test.go:99: boom\n"}`,
		`{"Action":"fail","Package":"go.temporal.io/server/tests","Test":"TestRealFailure","Elapsed":0.01}`,
		"",
	}, "\n")))
	require.NoError(t, err)

	report := output.junitReport()

	require.Equal(t, 1, report.Tests)
	require.Equal(t, 1, report.Failures)
	require.Len(t, report.Suites, 1)
	require.Len(t, report.Suites[0].Testcases, 1)
	require.Equal(t, "TestRealFailure", report.Suites[0].Testcases[0].Name)
	require.NotNil(t, report.Suites[0].Testcases[0].Failure)
	require.Contains(t, report.Suites[0].Testcases[0].Failure.Data, "boom")
}

func TestGoTestJSONOutput_ReportsPackageFailureWithoutFailedTest(t *testing.T) {
	output := newGoTestJSONOutput()
	output.stdout = io.Discard
	_, err := output.Write([]byte(strings.Join([]string{
		`{"Action":"output","Package":"go.temporal.io/server/broken","Output":"# go.temporal.io/server/broken\n"}`,
		`{"Action":"output","Package":"go.temporal.io/server/broken","Output":"broken_test.go:10: compile error\n"}`,
		`{"Action":"fail","Package":"go.temporal.io/server/broken","Elapsed":0.01}`,
		"",
	}, "\n")))
	require.NoError(t, err)

	report := output.junitReport()

	require.Equal(t, 1, report.Tests)
	require.Equal(t, 1, report.Failures)
	require.Len(t, report.Suites, 1)
	require.Equal(t, "go.temporal.io/server/broken", report.Suites[0].Testcases[0].Name)
	require.Contains(t, report.Suites[0].Testcases[0].Failure.Data, "compile error")
}
