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

func TestParseFailureDetails(t *testing.T) {
	tests := []struct {
		name        string
		data        string
		contains    []string
		notContains []string
		exact       string
	}{
		{
			name: "extracts testify block",
			data: `    suite_test.go:42: some log line
    suite_test.go:43: another log line
    suite_test.go:85:
        	Error Trace:	suite_test.go:85
        	Error:      	Not equal:
        	            	expected: 1
        	            	actual  : 2
FAIL`,
			contains:    []string{"Error Trace:", "expected: 1"},
			notContains: []string{"some log line", "FAIL"},
		},
		{
			name: "keeps testify file header line",
			data: `    suite_test.go:85:
        	Error Trace:	suite_test.go:85
        	Error:      	Not equal:
        	            	expected: 1
        	            	actual  : 2
FAIL`,
			contains: []string{
				"suite_test.go:85:",
				"Error Trace:",
			},
		},
		{
			name: "uses last gomock style block",
			data: `=== RUN   TestFoo
=== CONT  TestFoo
    test_env.go:1: some log
    mock_file.go:42: Unexpected call to SomeMethod(...)
        expected call at foo_test.go:10 doesn't match argument at index 0.
        Got:  "actual"
        Want: is equal to "expected"
FAIL`,
			contains:    []string{"Unexpected call", "Got:", "Want:"},
			notContains: []string{"some log"},
		},
		{
			name: "uses last failure block when multiple exist",
			data: `=== RUN   TestFoo
    helper.go:1: some setup log
    first_mock.go:10: first failure
        first continuation
    second_mock.go:20: second failure
        second continuation
FAIL`,
			contains:    []string{"second_mock.go:20", "second continuation"},
			notContains: []string{"first_mock.go:10", "first continuation", "some setup log"},
		},
		{
			name:  "returns sentinel when no output exists",
			data:  "FAIL\n",
			exact: noFailureDetails,
		},
		{
			name: "stops before logs printed after assertion block",
			data: `    suite_test.go:481:
            Error Trace:    suite_test.go:481
            Error:          Not equal:
                            expected: false
                            actual  : true
            Test:           TestSuite/TestCase
    logger.go:146: some info log after the assertion
    controller.go:97: missing call(s) to ...
FAIL`,
			contains:    []string{"Error Trace:", "expected: false"},
			notContains: []string{"logger.go", "controller.go"},
		},
		{
			name:        "strips trailing FAIL marker",
			data:        "    foo_test.go:1:\n\tError Trace:\tfoo_test.go:1\n\tError:\toops\n\n\nFAIL\n",
			contains:    []string{"Error Trace:"},
			notContains: []string{"FAIL"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseFailureDetails(tt.data)
			if tt.exact != "" {
				require.Equal(t, tt.exact, got)
				return
			}
			for _, want := range tt.contains {
				require.Contains(t, got, want)
			}
			for _, unwanted := range tt.notContains {
				require.NotContains(t, got, unwanted)
			}
		})
	}
}
