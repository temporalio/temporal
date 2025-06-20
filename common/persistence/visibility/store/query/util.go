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
