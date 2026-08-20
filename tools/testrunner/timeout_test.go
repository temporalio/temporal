package testrunner

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/junit"
)

func TestExtractTimeoutDiagnostic(t *testing.T) {
	input, err := os.ReadFile("testdata/timeout-input.log")
	require.NoError(t, err)
	expected, err := os.ReadFile("testdata/timeout-output.log")
	require.NoError(t, err)

	timeout := extractTimeoutDiagnostic(outputScope{packageName: "example.com/tests"}, string(input))
	require.NotNil(t, timeout)
	require.Equal(t, diagnosticTimeout, timeout.kind)
	require.Equal(t, string(expected), timeout.details)
	require.Equal(t, []testID{
		{"example.com/tests", "TestActivityApiResetClientTestSuite"},
		{"example.com/tests", "TestNDCFuncTestSuite"},
		{"example.com/tests", "TestActivityApiStateReplicationSuite"},
	}, timeout.tests)

	report := renderJUnit([]attemptResult{{
		diagnostics: []diagnostic{*timeout},
		process:     processResult{state: processExited, exitCode: 1},
	}})
	require.NoError(t, junit.ValidateCounters(&report))
	testcase := findJUnitTestcase(t, report, "TestActivityApiResetClientTestSuite (timed out)")
	require.NotNil(t, testcase.Failure)
	require.Contains(t, testcase.Failure.Data, "panic: test timed out after 5s")
	require.Contains(t, testcase.Failure.Data, "abridged stacktrace:")
}

func TestParseTestTimeoutsTruncatedAfterPanic(t *testing.T) {
	stacktrace, tests := parseTestTimeouts("panic: test timed out after 1m")
	require.Empty(t, tests)
	require.Contains(t, stacktrace, "0 timed out test(s)")
}
