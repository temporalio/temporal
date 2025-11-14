package nexusrpc

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMarshalTimestampFormats(t *testing.T) {
	// 0 fractional digits
	t0 := time.Date(2006, 1, 2, 15, 4, 5, 0, time.FixedZone("-0700", -7*3600))
	got := marshalTimestamp(t0)
	require.Equal(t, "2006-01-02T22:04:05Z", got) // normalized to UTC

	// 3 fractional digits (ns = 123000000 -> .123)
	t3 := time.Date(2006, 1, 2, 15, 4, 5, 123000000, time.UTC)
	require.Equal(t, "2006-01-02T15:04:05.123Z", marshalTimestamp(t3))

	// 6 fractional digits (ns = 123000 -> .000123)
	t6 := time.Date(2006, 1, 2, 15, 4, 5, 123000, time.UTC)
	require.Equal(t, "2006-01-02T15:04:05.000123Z", marshalTimestamp(t6))

	// 9 fractional digits (ns = 123)
	t9 := time.Date(2006, 1, 2, 15, 4, 5, 123, time.UTC)
	require.Equal(t, "2006-01-02T15:04:05.000000123Z", marshalTimestamp(t9))
}

func TestUnmarshalTimestampRoundtripAndErrors(t *testing.T) {
	// Roundtrip with fractional seconds
	orig := time.Date(2020, 7, 1, 12, 34, 56, 123000000, time.FixedZone("+0200", 2*3600))
	s := marshalTimestamp(orig)
	parsed, err := unmarshalTimestamp(s)
	require.NoError(t, err)
	require.Equal(t, orig.UTC(), parsed)

	// Parse error for invalid string
	_, err = unmarshalTimestamp("not-a-time")
	require.Error(t, err)

	// Seconds out of range (one second above max). Depending on Go's time
	// parsing this may fail during Parse (year > 9999) or return our
	// range-check error. Accept either.
	tooBig := time.Unix(maxTimestampSeconds+1, 0).UTC().Format(time.RFC3339Nano)
	_, err = unmarshalTimestamp(tooBig)
	require.Error(t, err)
	if !strings.Contains(err.Error(), "second value out of range") && !strings.Contains(err.Error(), "cannot parse") {
		t.Fatalf("unexpected error for out-of-range seconds: %v", err)
	}

	// Too many subsecond digits (10 digits)
	badSub := "2006-01-02T15:04:05.1234567890Z"
	_, err = unmarshalTimestamp(badSub)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid subsecond value")
}
