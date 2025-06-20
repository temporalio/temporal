package nexus

import (
	"strconv"
	"time"
)

// FormatDuration converts a duration into a string representation in millisecond resolution.
// TODO: replace this with the version exported from the Nexus SDK
func FormatDuration(d time.Duration) string {
	return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
}
