package nexus

import "time"

// RetryBackoffTimeSkippingTargets returns the deadlines that bound a skip while a Nexus invocation
// waits to retry. A deadline that is already due makes the operation non-skippable so the
// corresponding timer can run instead of being skipped over.
func RetryBackoffTimeSkippingTargets(
	now time.Time,
	scheduledTime time.Time,
	nextAttemptTime time.Time,
	scheduleToCloseTimeout time.Duration,
	scheduleToStartTimeout time.Duration,
) ([]time.Time, bool) {
	targets := []time.Time{nextAttemptTime}
	if scheduleToCloseTimeout > 0 {
		targets = append(targets, scheduledTime.Add(scheduleToCloseTimeout))
	}
	if scheduleToStartTimeout > 0 {
		targets = append(targets, scheduledTime.Add(scheduleToStartTimeout))
	}
	for _, target := range targets {
		if !target.After(now) {
			return nil, false
		}
	}
	return targets, true
}
