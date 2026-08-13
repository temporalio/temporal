package testrunner

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractDiagnosticsUsesEventScope(t *testing.T) {
	input, err := os.ReadFile("testdata/alerts-input.log")
	require.NoError(t, err)
	id := testID{"example.com/tests", "TestOwned"}

	diagnostics := extractDiagnostics(outputScope{packageName: id.packageName, test: &id}, string(input))
	require.Len(t, diagnostics, 2)
	require.Equal(t, diagnosticDataRace, diagnostics[0].kind)
	require.Equal(t, []testID{id}, diagnostics[0].tests)
	require.Equal(t, diagnosticPanic, diagnostics[1].kind)
	require.Equal(t, []testID{id}, diagnostics[1].tests)
	require.Len(t, dedupeDiagnostics(append(diagnostics, diagnostics...)), 2)
}

func TestExtractFailureEvidence(t *testing.T) {
	tests := []struct {
		name        string
		output      string
		contains    []string
		notContains []string
		actionable  bool
	}{
		{
			name: "structured assertion",
			output: "    suite_test.go:85:\n" +
				"        Error Trace:\tsuite_test.go:85\n" +
				"        Error:\tNot equal\nFAIL\n",
			contains:    []string{"suite_test.go:85:", "Error Trace:"},
			notContains: []string{"FAIL"},
			actionable:  true,
		},
		{
			name: "uses last failure block",
			output: "    helper.go:1: setup log\n" +
				"    first_mock.go:10: first failure\n        first continuation\n" +
				"    second_mock.go:20: Unexpected call to SomeMethod(...)\n        second continuation\nFAIL\n",
			contains:    []string{"second_mock.go:20", "second continuation"},
			notContains: []string{"first_mock.go:10", "first continuation", "setup log"},
			actionable:  true,
		},
		{
			name: "stops before logs after assertion block",
			output: "    suite_test.go:481:\n" +
				"        Error Trace: suite_test.go:481\n" +
				"        Error: Not equal\n" +
				"        Test: TestSuite/TestCase\n" +
				"    logger.go:146: info after assertion\n" +
				"    controller.go:97: missing call(s)\nFAIL\n",
			contains:    []string{"Error Trace:", "Not equal"},
			notContains: []string{"logger.go", "controller.go"},
			actionable:  true,
		},
		{
			name: "keeps last await attempt without trailing logs",
			output: "    report.go:54: attempt errors:\n" +
				"  --- attempt 1 ---\n    Error Trace:\tsuite_test.go:10\n    Error:\tfirst failure\n\n" +
				"  --- attempt 2 ---\n    Error Trace:\tsuite_test.go:10\n    Error:\tlast failure\n\n" +
				"logger.go:146: connection refused\n--- FAIL: TestSuite/TestCase (1.00s)\nFAIL\n",
			contains:    []string{"--- attempt 2 ---", "last failure", "--- FAIL: TestSuite/TestCase (1.00s)"},
			notContains: []string{"--- attempt 1 ---", "first failure", "logger.go", "connection refused"},
			actionable:  true,
		},
		{
			name:       "generic event-owned log",
			output:     "    parent_test.go:20: cleanup log\nFAIL\n",
			contains:   []string{"cleanup log"},
			actionable: false,
		},
		{
			name: "content-free timeout does not replace failure evidence",
			output: "    controller.go:1: Unexpected call to SomeMethod(...)\n" +
				"panic: test timed out after 10m0s",
			contains:   []string{"Unexpected call to SomeMethod"},
			actionable: true,
		},
		{name: "raw panic", output: "panic: parent panic\n", contains: []string{"panic: parent panic"}, actionable: true},
		{name: "raw data race", output: "WARNING: DATA RACE\nwrite at address\n", contains: []string{"WARNING: DATA RACE"}, actionable: true},
		{name: "raw fatal", output: "fatal error: concurrent map writes\n", contains: []string{"fatal error:"}, actionable: true},
		{name: "no detail", output: "FAIL\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			evidence := extractFailureEvidence(test.output)
			require.Equal(t, test.actionable, evidence.actionable)
			if len(test.contains) == 0 {
				require.Empty(t, evidence.details)
			}
			for _, expected := range test.contains {
				require.Contains(t, evidence.details, expected)
			}
			for _, unexpected := range test.notContains {
				require.NotContains(t, evidence.details, unexpected)
			}
		})
	}
}

func TestCollectAttemptDiagnosticsScopesFiltersAndDeduplicates(t *testing.T) {
	observed := testID{"example.com/tests", "TestObserved"}
	testRace := "WARNING: DATA RACE\nexample.TestObserved()\nFAIL\n"
	packageRace := "WARNING: DATA RACE\nexample.TestUnobserved()\nFAIL\n"
	duplicatePanic := "panic: duplicate\nFAIL\n"
	result := attemptResult{
		packages: []packageResult{{
			name: "example.com/tests",
			executions: []testExecution{{
				id:      observed,
				outcome: testFailed,
				output:  testRace,
			}},
			output: packageRace,
		}},
		process:            processResult{stderr: duplicatePanic},
		unstructuredOutput: duplicatePanic,
	}

	diagnostics := collectAttemptDiagnostics(result)
	require.Len(t, diagnostics, 3)
	require.Equal(t, []testID{observed}, diagnostics[0].tests)
	require.Empty(t, diagnostics[1].tests)
	require.Equal(t, diagnosticPanic, diagnostics[2].kind)
}
