package timeutil

import "time"

// MinTime returns the earlier of two given time.Time
func MinTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
