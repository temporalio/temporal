//go:build test_dep

package wci

import "time"

// SetMaxPollIntervalForTest shortens the first-poll delay and the overdue margin for in-process multi-cluster
// tests. Call it before the clusters start; it returns the restore func.
func SetMaxPollIntervalForTest(d time.Duration) (restore func()) {
	prev := maxPollInterval
	maxPollInterval = d
	return func() { maxPollInterval = prev }
}
