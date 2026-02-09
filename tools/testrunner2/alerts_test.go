package testrunner2

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAlertsDedupe_ByKindSummaryTests(t *testing.T) {
	t.Parallel()

	// Alerts with same Kind+Summary+Tests should dedupe (e.g., from window trims)
	input := alerts{
		{Kind: failureKindTimeout, Summary: "test timed out after 2m0s", Tests: []string{"TestFoo"}},
		{Kind: failureKindTimeout, Summary: "test timed out after 2m0s", Tests: []string{"TestFoo"}},
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

func TestParseAlerts(t *testing.T) {
	t.Parallel()

	t.Run("fatal", func(t *testing.T) {
		t.Parallel()

		input, err := os.ReadFile("testdata/fatal.log")
		require.NoError(t, err)

		alerts := parseAlerts(string(input))
		require.Len(t, alerts, 1)
		require.Equal(t, failureKindFatal, alerts[0].Kind)
		require.Equal(t, "concurrent map writes", alerts[0].Summary)
		require.Contains(t, alerts[0].Tests, "mypackage.TestFatalExample")
	})

	t.Run("data race dedupe", func(t *testing.T) {
		t.Parallel()

		input, err := os.ReadFile("testdata/race.log")
		require.NoError(t, err)

		parsed := parseAlerts(string(input))
		require.NotEmpty(t, parsed)

		doubled := alerts(append(parsed, parsed...))
		deduped := doubled.dedupe()
		require.Len(t, parsed, len(deduped))
	})
}

func TestExtractErrorTrace(t *testing.T) {
	t.Parallel()

	t.Run("testify", func(t *testing.T) {
		t.Parallel()

		data, err := os.ReadFile("testdata/testify_error_trace.log")
		require.NoError(t, err)
		output := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")

		result := extractErrorTrace(output)

		require.Contains(t, result, "Error Trace:")
		require.Contains(t, result, "/path/to/test_file.go:123")
		require.Contains(t, result, "Error:")
		require.Contains(t, result, "expected: \"foo\"")
		require.NotContains(t, result, "Test:")
		require.NotContains(t, result, "some log line before")
		require.NotContains(t, result, "more log lines after")
	})

	t.Run("standard go", func(t *testing.T) {
		t.Parallel()

		output := []string{
			"some log line",
			"    test_file.go:123: expected foo, got bar",
			"    test_file.go:456: another error message",
			"more log output",
		}

		require.Empty(t, extractErrorTrace(output))
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		output := []string{
			"just regular log output",
			"no errors here",
		}

		require.Empty(t, extractErrorTrace(output))
	})
}

func TestSplitTestName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     string
		classname string
		name      string
	}{
		{"go.temporal.io/server/pkg.TestFoo", "go.temporal.io/server/pkg", "TestFoo"},
		{"go.temporal.io/server/pkg.TestFoo.func1", "go.temporal.io/server/pkg", "TestFoo"},
		{"fakepkg1.TestFoo1", "fakepkg1", "TestFoo1"},
		{"TestFoo/SubTest", "", "TestFoo/SubTest"},
		{"TestFoo", "", "TestFoo"},
		{"pkg.TestA.func1.func2", "pkg", "TestA"},
		{"go.temporal.io/server/service/history/workflow.TestActivityInfo", "go.temporal.io/server/service/history/workflow", "TestActivityInfo"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			classname, name := splitTestName(tt.input)
			require.Equal(t, tt.classname, classname, "classname")
			require.Equal(t, tt.name, name, "name")
		})
	}
}
