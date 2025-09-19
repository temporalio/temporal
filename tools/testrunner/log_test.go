package testrunner

import (
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
