package query

import (
	"strconv"
	"time"

	"go.temporal.io/server/common/primitives/timestamp"
)

func ParseExecutionDurationStr(durationStr string) (time.Duration, error) {
	if durationNanos, err := strconv.ParseInt(durationStr, 10, 64); err == nil {
		return time.Duration(durationNanos), nil
	}

	// To support durations passed as golang durations such as "300ms", "-1.5h" or "2h45m".
	// Valid time units are "ns", "us" (or "µs"), "ms", "s", "m", "h".
	// Custom timestamp.ParseDuration also supports "d" as additional unit for days.
	if duration, err := timestamp.ParseDuration(durationStr); err == nil {
		return duration, nil
	}

	// To support "hh:mm:ss" durations.
	return timestamp.ParseHHMMSSDuration(durationStr)
}

// ParseRelativeOrAbsoluteTime parses a time string that can be either:
// 1. A relative duration (e.g., "5m", "1h", "2d") - converted to absolute time by adding to now
// 2. An absolute timestamp in RFC3339Nano format
//
// For relative durations, the absolute time is calculated as time.Now().Add(duration).
// Valid duration units are "ns", "us" (or "µs"), "ms", "s", "m", "h", "d" (days).
func ParseRelativeOrAbsoluteTime(timeStr string) (time.Time, error) {
	if duration, err := timestamp.ParseDuration(timeStr); err == nil {
		return time.Now().Add(duration).UTC(), nil
	}

	parsedTime, err := time.Parse(time.RFC3339Nano, timeStr)
	if err != nil {
		return time.Time{}, err
	}
	return parsedTime.UTC(), nil
}
