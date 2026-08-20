package optimizetestsharding

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/junit"
)

func TestProcessJUnitReportReadsRenderedTestcaseShape(t *testing.T) {
	suite := junit.Testsuite{Name: "example.com/tests", Time: "1.250000"}
	suite.AddTestcase(junit.Testcase{
		Name: "TestSuite/TestCase (retry 1) (final)",
		Time: "1.250000",
	})
	report := &junit.Testsuites{Time: "1.250000"}
	report.AddSuite(suite)
	path := filepath.Join(t.TempDir(), "junit.xml")
	require.NoError(t, junit.Write(path, report))

	times := make(map[string][]float64)
	require.NoError(t, processJUnitReport(path, times))
	require.Equal(t, map[string][]float64{
		"TestSuite/TestCase": {1.25},
	}, times)
}
