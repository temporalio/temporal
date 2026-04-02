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

func TestParseFailedTestsFromOutput(t *testing.T) {
	t.Run("ExtractsFailedTestNames", func(t *testing.T) {
		stdout := `
=== RUN   TestFoo
=== RUN   TestFoo/SubTest1
    foo_test.go:42: assertion failed
--- FAIL: TestFoo/SubTest1 (1.23s)
--- FAIL: TestFoo (1.23s)
=== RUN   TestBar
--- PASS: TestBar (0.00s)
=== RUN   TestBaz
    baz_test.go:10: something wrong
--- FAIL: TestBaz (0.50s)
FAIL
`
		got := parseFailedTestsFromOutput(stdout)
		require.Equal(t, []string{"TestFoo/SubTest1", "TestFoo", "TestBaz"}, got)
	})

	t.Run("DeduplicatesDuplicateLines", func(t *testing.T) {
		stdout := `
--- FAIL: TestDupe (0.10s)
--- FAIL: TestDupe (0.10s)
--- FAIL: TestOther (0.20s)
`
		got := parseFailedTestsFromOutput(stdout)
		require.Equal(t, []string{"TestDupe", "TestOther"}, got)
	})

	t.Run("ReturnsEmptyWhenNoFailures", func(t *testing.T) {
		stdout := `
=== RUN   TestPass
--- PASS: TestPass (0.01s)
ok  	go.temporal.io/server/tests
`
		got := parseFailedTestsFromOutput(stdout)
		require.Empty(t, got)
	})

	t.Run("ReturnsEmptyOnEmptyInput", func(t *testing.T) {
		require.Empty(t, parseFailedTestsFromOutput(""))
	})
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
