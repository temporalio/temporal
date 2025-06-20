package scheduler

import (
	enumspb "go.temporal.io/api/enums/v1"
)

type (
	Overlappable interface {
		comparable
		GetOverlapPolicy() enumspb.ScheduleOverlapPolicy
	}

	ProcessBufferResult[T Overlappable] struct {
		// We can start allow-all all at once
		OverlappingStarts []T
		// Ignoring allow-all, we can start either zero or one now.
		// This is the one that we want to start, or nil.
		NonOverlappingStart T
		// The remaining buffer
		NewBuffer []T
		// Whether to cancel/terminate the currently-running one
		NeedCancel    bool
		NeedTerminate bool
		// Stats
		OverlapSkipped int64
	}
)

func ProcessBuffer[T Overlappable](
	buffer []T,
	isRunning bool,
	resolve func(enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy,
) ProcessBufferResult[T] {
	// We should try to do something reasonable with any combination of overlap
	// policies in the buffer, although some combinations don't make much sense
	// and would require a convoluted series of calls to set up.
	//
	// Buffer entries that have unspecified overlap policy are resolved to the
	// current policy here, not earlier, so that updates to the policy can
	// affect them.

	var action ProcessBufferResult[T]
	var zeroVal T

	for _, start := range buffer {
		overlapPolicy := resolve(start.GetOverlapPolicy())

		// For allow-all, just collect and start all at once
		if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
			action.OverlappingStarts = append(action.OverlappingStarts, start)
			continue
		}

		// Now handle non-overlapping.

		// If there's nothing running, we can start this one no matter what the policy is
		if !isRunning && action.NonOverlappingStart == zeroVal {
			action.NonOverlappingStart = start
			continue
		}

		// Otherwise this one overlaps and we should apply the policy
		switch overlapPolicy {
		case enumspb.SCHEDULE_OVERLAP_POLICY_SKIP:
			// just skip
			action.OverlapSkipped++
		case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
			// allow one (the first one) in the buffer
			if len(action.NewBuffer) == 0 {
				action.NewBuffer = append(action.NewBuffer, start)
			} else {
				action.OverlapSkipped++
			}
		case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
			// always add to buffer
			action.NewBuffer = append(action.NewBuffer, start)
		case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
			if isRunning {
				// an actual workflow is running, cancel it (asynchronously)
				action.NeedCancel = true
				// keep in buffer so it will get started once cancel completes
				action.NewBuffer = append(action.NewBuffer, start)
			} else {
				// it's not running yet, it's just the one we were going to start. replace it
				action.NonOverlappingStart = start
			}
		case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
			if isRunning {
				// an actual workflow is running, terminate it
				action.NeedTerminate = true
				// keep in buffer so it will get started once terminate completes
				action.NewBuffer = append(action.NewBuffer, start)
			} else {
				// it's not running yet, it's just the one we were going to start. replace it
				action.NonOverlappingStart = start
			}
		}
	}

	if action.NeedCancel || action.NeedTerminate {
		// In a very contrived situation, we could end up with overlapping starts at the same
		// time as a non-overlapping start tries to cancel/terminate a running workflow. But
		// then it will immediately cancel/terminate the overlapping ones too (either on this
		// iteration or the next one). So we shouldn't even bother starting them.
		action.OverlappingStarts = nil
	}

	return action
}
