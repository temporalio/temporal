package testrunner

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestGoTestJSONReporterFlushesReportAndConsoleOutput(t *testing.T) {
	out, err := os.CreateTemp("", "junit-report-*.xml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(out.Name()) }()

	report := &junitReport{path: out.Name()}
	var console bytes.Buffer
	var flushes int
	reporter := newGoTestJSONReporter(report, &console, func() {
		flushes++
	})
	reporter.lastFlush = time.Now().Add(-reportFlushInterval)

	_, err = reporter.Write([]byte(`{"Action":"run","Package":"go.temporal.io/server/tools/testrunner","Test":"TestPass"}` + "\n"))
	require.NoError(t, err)
	_, err = reporter.Write([]byte(`{"Action":"output","Package":"go.temporal.io/server/tools/testrunner","Test":"TestPass","Output":"=== RUN   TestPass\n"}` + "\n"))
	require.NoError(t, err)
	_, err = reporter.Write([]byte(`{"Action":"pass","Package":"go.temporal.io/server/tools/testrunner","Test":"TestPass","Elapsed":0.01}` + "\n"))
	require.NoError(t, err)
	reporter.Close()

	require.Contains(t, console.String(), "=== RUN   TestPass")
	require.Positive(t, flushes)
	require.Equal(t, 1, report.Tests)
	require.Len(t, report.Suites, 1)

	readBack := &junitReport{path: out.Name()}
	require.NoError(t, readBack.read())
	require.Equal(t, 1, readBack.Tests)
	require.Len(t, readBack.Suites, 1)
}
