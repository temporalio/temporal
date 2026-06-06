package testrunner

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewJUnitReportFromGoTestJSON(t *testing.T) {
	report, err := newJUnitReportFromGoTestJSON(strings.Join([]string{
		`{"Action":"run","Package":"go.temporal.io/server/tools/testrunner","Test":"TestSuite"}`,
		`{"Action":"run","Package":"go.temporal.io/server/tools/testrunner","Test":"TestSuite/TestPass"}`,
		`{"Action":"output","Package":"go.temporal.io/server/tools/testrunner","Test":"TestSuite/TestPass","Output":"=== RUN   TestSuite/TestPass\n"}`,
		`{"Action":"pass","Package":"go.temporal.io/server/tools/testrunner","Test":"TestSuite/TestPass","Elapsed":0.01}`,
		`{"Action":"run","Package":"go.temporal.io/server/tools/testrunner","Test":"TestSuite/TestFail"}`,
		`{"Action":"output","Package":"go.temporal.io/server/tools/testrunner","Test":"TestSuite/TestFail","Output":"    suite_test.go:10: boom\n"}`,
		`{"Action":"fail","Package":"go.temporal.io/server/tools/testrunner","Test":"TestSuite/TestFail","Elapsed":0.02}`,
		`{"Action":"fail","Package":"go.temporal.io/server/tools/testrunner","Test":"TestSuite","Elapsed":0.03}`,
		`{"Action":"fail","Package":"go.temporal.io/server/tools/testrunner","Elapsed":0.03}`,
	}, "\n") + "\n")

	require.NoError(t, err)
	require.Equal(t, 2, report.Tests)
	require.Equal(t, 1, report.Failures)
	require.Len(t, report.Suites, 1)
	require.Len(t, report.Suites[0].Testcases, 2)
	require.Equal(t, "TestSuite/TestPass", report.Suites[0].Testcases[0].Name)
	require.Equal(t, "TestSuite/TestFail", report.Suites[0].Testcases[1].Name)
	require.NotNil(t, report.Suites[0].Testcases[1].Failure)
	require.Contains(t, report.Suites[0].Testcases[1].Failure.Data, "boom")
}
