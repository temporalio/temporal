package query

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// relativeTimeUnits maps lowercase unit suffixes to durations.
var relativeTimeUnits = map[string]time.Duration{
	"ns": time.Nanosecond,
	"us": time.Microsecond,
	"µs": time.Microsecond,
	"μs": time.Microsecond,
	"ms": time.Millisecond,
	"s":  time.Second,
	"m":  time.Minute,
	"h":  time.Hour,
	"d":  24 * time.Hour,
	"w":  7 * 24 * time.Hour,
}

// isDurationByte reports whether b can be part of a duration token.
// Non ASCII bytes (>= 0x80) are always included so multi byte unit suffixes like µs/μs are consumed whole.
func isDurationByte(b byte) bool {
	return b >= 0x80 ||
		(b >= '0' && b <= '9') ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z')
}

// parseRelativeDuration parses a compound duration string such as "1h30m" or "2w3d".
// Mirrors Go's time.ParseDuration: consume an integer, then consume the unit as bytes until
// the next digit. Digit bytes (0x30 to 0x39) never appear in multi byte UTF-8 sequences, so
// multi byte suffixes like µs/μs are consumed whole. Unit matching is case insensitive.
func parseRelativeDuration(input string) (time.Duration, error) {
	if input == "" {
		return 0, errors.New("empty duration")
	}

	var total time.Duration
	s := input
	for len(s) > 0 {
		// Consume decimal integer.
		i := 0
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i == 0 {
			return 0, errors.New("missing duration value")
		}
		value, err := strconv.ParseInt(s[:i], 10, 64)
		if err != nil {
			return 0, err
		}
		s = s[i:]

		// Consume unit: everything up to the next digit (non-ASCII bytes are always > '9').
		i = 0
		for i < len(s) && (s[i] < '0' || s[i] > '9') {
			i++
		}
		if i == 0 {
			return 0, errors.New("missing duration unit")
		}
		unit := strings.ToLower(s[:i])
		scale, ok := relativeTimeUnits[unit]
		if !ok {
			return 0, fmt.Errorf("unsupported duration unit %q", s[:i])
		}
		total += time.Duration(value) * scale
		s = s[i:]
	}

	return total, nil
}
