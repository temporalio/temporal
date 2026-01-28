package testrunner

import (
	"bytes"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseTestTimeouts(t *testing.T) {
	logInput, err := os.ReadFile("testdata/timeout-input.log")
	require.NoError(t, err)
	logOutput, err := os.ReadFile("testdata/timeout-output.log")
	require.NoError(t, err)

	stacktrace, tests := parseTestTimeouts(string(logInput))
	require.Equal(t, []string{
		"TestActivityApiResetClientTestSuite",
		"TestNDCFuncTestSuite",
		"TestActivityApiStateReplicationSuite",
	}, tests)
	require.Equal(t, string(logOutput), stacktrace)
}

func TestParseAlerts_DataRaceAndPanic(t *testing.T) {
	input, err := os.ReadFile("testdata/alerts-input.log")
	require.NoError(t, err)

	alerts := parseAlerts(string(input))
	require.NotEmpty(t, alerts)
	require.Equal(t, 2, len(alerts))

	require.Contains(t, alerts[0].Details, "WARNING: DATA RACE")
	require.Contains(t, alerts[0].Tests[0], "test.TestDataRaceExample")
	require.Contains(t, primaryTestName(alerts[0].Tests), "test.TestDataRaceExample")
	require.Contains(t, alerts[1].Details, "panic: ")
	require.Contains(t, alerts[1].Tests[0], "test.TestPanicExample")
	require.Contains(t, primaryTestName(alerts[1].Tests), "TestPanicExample")

	// Ensure dedupe works
	deduped := dedupeAlerts(append(alerts, alerts...))
	require.Equal(t, len(deduped), len(alerts))
}

func TestLogCapture_FilterPrefix(t *testing.T) {
	var output bytes.Buffer
	c := newLogCapture("warning: no packages")
	stdoutW := c.Writer(&output)
	stderrW := c.Writer(&output)

	// Write mixed content with warning lines to stdout
	_, err := stdoutW.Write([]byte("line 1\n"))
	require.NoError(t, err)
	_, err = stdoutW.Write([]byte("warning: no packages being tested depend on matches for pattern foo\n"))
	require.NoError(t, err)
	_, err = stdoutW.Write([]byte("line 2\n"))
	require.NoError(t, err)

	// Write to stderr too (goes to same buffer)
	_, err = stderrW.Write([]byte("error line\n"))
	require.NoError(t, err)
	_, err = stderrW.Write([]byte("warning: no packages xyz\n"))
	require.NoError(t, err)

	require.NoError(t, c.Flush())

	// Output should contain all non-filtered lines
	require.Equal(t, "line 1\nline 2\nerror line\n", output.String())

	// But internal buffer should contain everything (for alert parsing)
	captured := c.output.String()
	require.Contains(t, captured, "line 1")
	require.Contains(t, captured, "warning: no packages being tested")
	require.Contains(t, captured, "line 2")
	require.Contains(t, captured, "error line")
	require.Contains(t, captured, "warning: no packages xyz")
}

func TestLogCapture_PartialLines(t *testing.T) {
	var output bytes.Buffer
	c := newLogCapture("filter:")
	w := c.Writer(&output)

	// Write partial lines
	_, err := w.Write([]byte("hel"))
	require.NoError(t, err)
	_, err = w.Write([]byte("lo\nwor"))
	require.NoError(t, err)
	_, err = w.Write([]byte("ld\n"))
	require.NoError(t, err)
	require.NoError(t, c.Flush())

	require.Equal(t, "hello\nworld\n", output.String())
}

func TestLogCapture_Alerts(t *testing.T) {
	var output bytes.Buffer
	c := newLogCapture("")
	w := c.Writer(&output)

	// Write content with a panic
	_, err := w.Write([]byte("some output\n"))
	require.NoError(t, err)
	_, err = w.Write([]byte("panic: something went wrong\n"))
	require.NoError(t, err)
	_, err = w.Write([]byte("goroutine 1 [running]:\n"))
	require.NoError(t, err)
	_, err = w.Write([]byte("FAIL\n"))
	require.NoError(t, err)
	require.NoError(t, c.Flush())

	alerts := parseAlerts(c.output.String())
	require.Len(t, alerts, 1)
	require.Equal(t, alertKindPanic, alerts[0].Kind)
	require.Equal(t, "something went wrong", alerts[0].Summary)
}
