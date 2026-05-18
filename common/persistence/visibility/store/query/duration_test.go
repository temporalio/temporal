package query

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseRelativeDuration(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr string
	}{
		{name: "hours", input: "5h", want: 5 * time.Hour},
		{name: "minutes", input: "30m", want: 30 * time.Minute},
		{name: "seconds", input: "90s", want: 90 * time.Second},
		{name: "milliseconds", input: "500ms", want: 500 * time.Millisecond},
		{name: "microseconds ascii us", input: "500us", want: 500 * time.Microsecond},
		{name: "microseconds µs", input: "500µs", want: 500 * time.Microsecond},
		{name: "microseconds μs", input: "500μs", want: 500 * time.Microsecond},
		{name: "nanoseconds", input: "1ns", want: time.Nanosecond},
		{name: "days", input: "2d", want: 48 * time.Hour},
		{name: "weeks", input: "1w", want: 7 * 24 * time.Hour},
		{name: "compound 1h30m", input: "1h30m", want: 90 * time.Minute},
		{name: "compound 1w2d", input: "1w2d", want: 9 * 24 * time.Hour},
		{name: "compound 1h30m45s", input: "1h30m45s", want: time.Hour + 30*time.Minute + 45*time.Second},
		{name: "uppercase H", input: "5H", want: 5 * time.Hour},
		{name: "mixed case Ms", input: "500Ms", want: 500 * time.Millisecond},
		{name: "empty", input: "", wantErr: "empty duration"},
		{name: "missing value", input: "h", wantErr: "missing duration value"},
		{name: "missing unit", input: "5", wantErr: "missing duration unit"},
		{name: "unsupported unit", input: "5y", wantErr: "unsupported duration unit"},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseRelativeDuration(tc.input)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
