package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResolveRelativeTimes(t *testing.T) {
	t.Parallel()

	anchor := time.Date(2026, 3, 31, 17, 0, 0, 0, time.UTC)

	testCases := []struct {
		name    string
		input   string
		output  string
		wantErr string
	}{
		{
			name:   "plain now",
			input:  "StartTime > NOW()",
			output: "StartTime > '2026-03-31T17:00:00Z'",
		},
		{
			name:   "lowercase now()",
			input:  "StartTime > now()",
			output: "StartTime > '2026-03-31T17:00:00Z'",
		},
		{
			name:   "mixed case Now()",
			input:  "StartTime > Now() - 5h",
			output: "StartTime > '2026-03-31T12:00:00Z'",
		},
		{
			name:   "now minus duration",
			input:  "StartTime > NOW() - 5h",
			output: "StartTime > '2026-03-31T12:00:00Z'",
		},
		{
			name:   "now plus compound duration",
			input:  "CloseTime < NOW()+1h30m",
			output: "CloseTime < '2026-03-31T18:30:00Z'",
		},
		{
			name:   "quoted literal untouched",
			input:  "WorkflowType = 'NOW() - 5h' AND StartTime > NOW()",
			output: "WorkflowType = 'NOW() - 5h' AND StartTime > '2026-03-31T17:00:00Z'",
		},
		{
			name:   "days and weeks",
			input:  "StartTime > NOW() - 1w2d",
			output: "StartTime > '2026-03-22T17:00:00Z'",
		},
		{
			name:    "missing duration value",
			input:   "StartTime > NOW() - h",
			wantErr: "invalid NOW() duration",
		},
		{
			name:    "incomplete operator",
			input:   "StartTime > NOW() - ",
			wantErr: "incomplete NOW() expression",
		},
		{
			name:   "microseconds with ASCII us",
			input:  "StartTime > NOW() - 500us",
			output: "StartTime > '2026-03-31T16:59:59.9995Z'",
		},
		{
			name:   "microseconds with µ (U+00B5 micro sign)",
			input:  "StartTime > NOW() - 500µs",
			output: "StartTime > '2026-03-31T16:59:59.9995Z'",
		},
		{
			name:   "microseconds with μ (U+03BC greek mu)",
			input:  "StartTime > NOW() - 500μs",
			output: "StartTime > '2026-03-31T16:59:59.9995Z'",
		},
		{
			name:   "uppercase unit H",
			input:  "StartTime > NOW() - 5H",
			output: "StartTime > '2026-03-31T12:00:00Z'",
		},
		{
			name:   "mixed-case unit Ms",
			input:  "StartTime > NOW() - 500Ms",
			output: "StartTime > '2026-03-31T16:59:59.5Z'",
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := ResolveRelativeTimes(tc.input, anchor)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.output, out)
		})
	}
}
