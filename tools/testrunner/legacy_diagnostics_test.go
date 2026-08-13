package testrunner

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFailedTestsFromOutput(t *testing.T) {
	tests := []struct {
		name   string
		output string
		want   []string
	}{
		{
			name: "extracts failed test names",
			output: "--- FAIL: TestFoo/SubTest1 (1.23s)\n" +
				"--- FAIL: TestFoo (1.23s)\n" +
				"--- PASS: TestBar (0.00s)\n" +
				"--- FAIL: TestBaz (0.50s)\n",
			want: []string{"TestFoo/SubTest1", "TestFoo", "TestBaz"},
		},
		{
			name:   "deduplicates duplicate lines",
			output: "--- FAIL: TestDupe (0.10s)\n--- FAIL: TestDupe (0.10s)\n--- FAIL: TestOther (0.20s)\n",
			want:   []string{"TestDupe", "TestOther"},
		},
		{name: "returns empty when no failures", output: "--- PASS: TestPass (0.01s)\n"},
		{name: "returns empty on empty input"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, parseFailedTestsFromOutput(test.output))
		})
	}
}

func TestParseAlertsPreservesDetectedTestNames(t *testing.T) {
	input, err := os.ReadFile("testdata/alerts-input.log")
	require.NoError(t, err)

	alerts := parseAlerts(string(input))
	require.Len(t, alerts, 2)
	require.Contains(t, alerts[0].Tests, "test.TestDataRaceExample")
	require.Contains(t, alerts[1].Tests, "test.TestPanicExample")
	require.Len(t, dedupeAlerts(append(alerts, alerts...)), len(alerts))
}
