package internal

import enumspb "go.temporal.io/api/enums/v1"

// ResolveOverlapPolicy applies a per-action override, then the schedule policy,
// then the API default.
func ResolveOverlapPolicy(
	overlapPolicy enumspb.ScheduleOverlapPolicy,
	schedulePolicy enumspb.ScheduleOverlapPolicy,
) enumspb.ScheduleOverlapPolicy {
	if overlapPolicy != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		return overlapPolicy
	}
	if schedulePolicy != enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		return schedulePolicy
	}
	return enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
}

// TracksCompletionResult reports whether an action participates in scheduler-wide
// completion state and overlap resolution after it starts.
func TracksCompletionResult(overlapPolicy enumspb.ScheduleOverlapPolicy) bool {
	return overlapPolicy != enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
}
