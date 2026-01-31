package testrunner2

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseAlerts_Timeout(t *testing.T) {
	logInput, err := os.ReadFile("testdata/foo_timeout2.log")
	require.NoError(t, err)

	alerts := parseAlerts(string(logInput))

	// Filter to just timeout alerts
	var timeoutAlerts []alert
	for _, a := range alerts {
		if a.Kind == failureKindTimeout {
			timeoutAlerts = append(timeoutAlerts, a)
		}
	}

	// Should have 2 timeout alerts: one for TestFoo1/SubTest1 and one for TestFoo2.
	// TestFoo1 (the parent) should NOT be reported separately since only the
	// full subtest name should appear in logs and junit output.
	require.Len(t, timeoutAlerts, 2, "should have 2 timeout alerts")

	require.Equal(t, failureKindTimeout, timeoutAlerts[0].Kind)
	require.Contains(t, timeoutAlerts[0].Summary, "test timed out after")

	// Collect all test names from the alerts
	var allTests []string
	for _, a := range timeoutAlerts {
		allTests = append(allTests, a.Tests...)
	}
	require.ElementsMatch(t, []string{
		"TestFoo1/SubTest1",
		"TestFoo2",
	}, allTests)
}

func TestAlertsDedupe_ByKindSummaryTests(t *testing.T) {
	// Alerts with same Kind+Summary+Tests should dedupe (e.g., from window trims)
	input := alerts{
		{Kind: failureKindTimeout, Summary: "test timed out after 2m0s", Tests: []string{"TestFoo"}, Details: "stack 1"},
		{Kind: failureKindTimeout, Summary: "test timed out after 2m0s", Tests: []string{"TestFoo"}, Details: "stack 2"},
	}
	deduped := input.dedupe()
	require.Len(t, deduped, 1, "same Kind+Summary+Tests should dedupe to 1")

	// Alerts with same Kind+Summary but different Tests should NOT dedupe
	input = alerts{
		{Kind: failureKindTimeout, Summary: "test timed out after 2m0s", Tests: []string{"TestFoo1"}},
		{Kind: failureKindTimeout, Summary: "test timed out after 2m0s", Tests: []string{"TestFoo2"}},
		{Kind: failureKindTimeout, Summary: "test timed out after 2m0s", Tests: []string{"TestFoo3"}},
	}
	deduped = input.dedupe()
	require.Len(t, deduped, 3, "different Tests should produce separate alerts")
}

func TestParseAlerts_DataRaceAndPanic(t *testing.T) {
	input, err := os.ReadFile("testdata/foo_race.log")
	require.NoError(t, err)

	parsed := parseAlerts(string(input))
	require.NotEmpty(t, parsed)
	require.Len(t, parsed, 2)

	require.Contains(t, parsed[0].Details, "WARNING: DATA RACE")
	require.Contains(t, parsed[0].Tests[0], "fakepkg1.TestFoo1")
	require.Contains(t, primaryTestName(parsed[0].Tests), "fakepkg1.TestFoo1")
	require.Contains(t, parsed[1].Details, "panic: ")
	require.Contains(t, parsed[1].Tests[0], "fakepkg1.TestFoo2")
	require.Contains(t, primaryTestName(parsed[1].Tests), "TestFoo2")

	// Ensure dedupe works
	doubled := alerts(append(parsed, parsed...))
	deduped := doubled.dedupe()
	require.Len(t, parsed, len(deduped))
}

func TestParseAlerts_Fatal(t *testing.T) {
	input := `=== RUN   TestFatalExample
fatal error: concurrent map writes

goroutine 18 [running]:
mypackage.TestFatalExample(0xc000102e00)
        /path/to/test_file.go:42 +0x100
testing.tRunner(0xc000102e00, 0x1006992b0)
        /opt/homebrew/go/src/testing/testing.go:1792 +0x184
FAIL    mypackage    0.311s
`

	alerts := parseAlerts(input)
	require.Len(t, alerts, 1)
	require.Equal(t, failureKindFatal, alerts[0].Kind)
	require.Equal(t, "concurrent map writes", alerts[0].Summary)
	require.Contains(t, alerts[0].Details, "fatal error: concurrent map writes")
	require.Contains(t, alerts[0].Tests, "mypackage.TestFatalExample")
}

func TestExtractErrorTrace_Testify(t *testing.T) {
	// Testify-style assertion failure
	output := []string{
		"some log line before",
		"another log line",
		"    Error Trace:    /path/to/test_file.go:123",
		"                    /path/to/test_file.go:456",
		"    Error:          Not equal:",
		"                    expected: \"foo\"",
		"                    actual  : \"bar\"",
		"    Test:           TestSomething",
		"    Messages:       Expected values to match",
		"more log lines after",
	}

	result := extractErrorTrace(output)

	require.Contains(t, result, "Error Trace:")
	require.Contains(t, result, "/path/to/test_file.go:123")
	require.Contains(t, result, "Error:")
	require.Contains(t, result, "expected: \"foo\"")
	require.Contains(t, result, "Test:")
	require.NotContains(t, result, "some log line before")
	require.NotContains(t, result, "more log lines after")
}

func TestExtractErrorTrace_StandardGo(t *testing.T) {
	// Standard Go test log lines (t.Log output) should be filtered out
	output := []string{
		"some log line",
		"    test_file.go:123: expected foo, got bar",
		"    test_file.go:456: another error message",
		"more log output",
	}

	result := extractErrorTrace(output)

	// Only testify sections are extracted, not standard Go test log lines
	require.Empty(t, result)
}

func TestExtractErrorTrace_Empty(t *testing.T) {
	output := []string{
		"just regular log output",
		"no errors here",
	}

	result := extractErrorTrace(output)
	require.Empty(t, result)
}
