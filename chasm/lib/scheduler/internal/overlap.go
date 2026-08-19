package internal

import enumspb "go.temporal.io/api/enums/v1"

// TracksCompletionResult reports whether an action participates in scheduler-wide
// completion state and overlap resolution after it starts.
func TracksCompletionResult(overlapPolicy enumspb.ScheduleOverlapPolicy) bool {
	return overlapPolicy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
}
