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

// ResolveRelativeTimes converts relative time expressions specifying identifiers like NOW() to absolute RFC3339Nano timestamps.
// NOW() is case insensitive. Single quoted string literals are passed through unchanged.
func ResolveRelativeTimes(queryString string, anchor time.Time) (string, error) {
	if !strings.Contains(strings.ToUpper(queryString), "NOW") {
		return queryString, nil
	}

	anchor = anchor.UTC()
	var out strings.Builder
	out.Grow(len(queryString))

	for i := 0; i < len(queryString); {
		if queryString[i] == '\'' {
			j := i + 1
			for j < len(queryString) {
				if queryString[j] == '\'' {
					if j+1 < len(queryString) && queryString[j+1] == '\'' {
						j += 2
						continue
					}
					j++
					break
				}
				j++
			}
			out.WriteString(queryString[i:j])
			i = j
			continue
		}

		if !hasNowPrefix(queryString[i:]) {
			out.WriteByte(queryString[i])
			i++
			continue
		}

		resolved, consumed, err := resolveNowExpression(queryString[i:], anchor)
		if err != nil {
			return "", err
		}
		out.WriteString(resolved)
		i += consumed
	}

	return out.String(), nil
}

func hasNowPrefix(input string) bool {
	if len(input) < len("NOW()") {
		return false
	}
	if !strings.EqualFold(input[:3], "NOW") {
		return false
	}
	return input[3] == '(' && input[4] == ')'
}

func resolveNowExpression(input string, anchor time.Time) (string, int, error) {
	i := len("NOW()")
	j := i
	for j < len(input) && isASCIISpace(input[j]) {
		j++
	}
	if j >= len(input) || (input[j] != '+' && input[j] != '-') {
		return fmt.Sprintf("'%s'", anchor.Format(time.RFC3339Nano)), i, nil
	}

	op := input[j]
	j++
	for j < len(input) && isASCIISpace(input[j]) {
		j++
	}
	if j >= len(input) {
		return "", 0, NewConverterError("%s: incomplete NOW() expression", InvalidExpressionErrMessage)
	}

	durationStart := j
	for j < len(input) && isDurationByte(input[j]) {
		j++
	}
	if durationStart == j {
		return "", 0, NewConverterError("%s: invalid NOW() duration", InvalidExpressionErrMessage)
	}

	duration, err := parseRelativeDuration(input[durationStart:j])
	if err != nil {
		return "", 0, NewConverterError("%s: invalid NOW() duration %q", InvalidExpressionErrMessage, input[durationStart:j])
	}

	resolved := anchor
	if op == '+' {
		resolved = resolved.Add(duration)
	} else {
		resolved = resolved.Add(-duration)
	}

	return fmt.Sprintf("'%s'", resolved.Format(time.RFC3339Nano)), j, nil
}

// isDurationByte reports whether b can be part of a duration token.
// Non ASCII bytes (≥ 0x80) are always included so multi byte unit suffixes like µs/μs are consumed whole.
func isDurationByte(b byte) bool {
	return b >= 0x80 ||
		(b >= '0' && b <= '9') ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z')
}

func isASCIISpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
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
