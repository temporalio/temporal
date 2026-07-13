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
		`{"Action":"output","Package":"go.temporal.io/server/tests","Test":"TestRealFailure","Output":"=== RUN   TestRealFailure\n"}`,
		`{"Action":"output","Package":"go.temporal.io/server/tests","Test":"TestRealFailure","Output":"    foo_test.go:99: boom\n"}`,
		`{"Action":"output","Package":"go.temporal.io/server/tests","Test":"TestRealFailure","Output":"--- FAIL: TestRealFailure (0.01s)\n"}`,
		`{"Action":"fail","Package":"go.temporal.io/server/tests","Test":"TestRealFailure","Elapsed":0.01}`,
		`{"Action":"output","Package":"go.temporal.io/server/tests","Output":"FAIL\n"}`,
		`{"Action":"fail","Package":"go.temporal.io/server/tests","Elapsed":0.01}`,
		"",
	}, "\n")))
	require.NoError(t, err)

	report, err := output.junitReport()
	require.NoError(t, err)

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
		`{"ImportPath":"go.temporal.io/server/broken [go.temporal.io/server/broken.test]","Action":"build-output","Output":"# go.temporal.io/server/broken [go.temporal.io/server/broken.test]\n"}`,
		`{"ImportPath":"go.temporal.io/server/broken [go.temporal.io/server/broken.test]","Action":"build-output","Output":"broken_test.go:10: compile error\n"}`,
		`{"ImportPath":"go.temporal.io/server/broken [go.temporal.io/server/broken.test]","Action":"build-fail"}`,
		`{"Action":"start","Package":"go.temporal.io/server/broken"}`,
		`{"Action":"output","Package":"go.temporal.io/server/broken","Output":"FAIL\tgo.temporal.io/server/broken [build failed]\n"}`,
		`{"Action":"fail","Package":"go.temporal.io/server/broken","Elapsed":0.01,"FailedBuild":"go.temporal.io/server/broken [go.temporal.io/server/broken.test]"}`,
		"",
	}, "\n")))
	require.NoError(t, err)

	report, err := output.junitReport()
	require.NoError(t, err)

	require.Equal(t, 1, report.Tests)
	require.Equal(t, 1, report.Errors)
	require.Contains(t, report.Suites[0].Testcases[0].Error.Data, "compile error")
}
