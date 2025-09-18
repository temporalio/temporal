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

	// Expect at least one data race and one panic alert.
	var hasRace, hasPanic bool
	for _, a := range alerts {
		switch a.Kind {
		case alertKindDataRace:
			hasRace = true
			require.Contains(t, a.Details, "WARNING: DATA RACE")
			// Ensure we extracted at least one test name.
			require.NotEmpty(t, a.Tests)
		case alertKindPanic:
			hasPanic = true
			require.Contains(t, a.Details, "panic: ")
			// Ensure we extracted at least one test name.
			require.NotEmpty(t, a.Tests)
		}
	}
	require.True(t, hasRace, "expected at least one data race alert")
	require.True(t, hasPanic, "expected at least one panic alert")

	// Ensure dedupe works: applying twice should not increase count.
	deduped := dedupeAlerts(append(alerts, alerts...))
	require.LessOrEqual(t, len(deduped), len(alerts))
}
