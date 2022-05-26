// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package scheduler

import (
	enumspb "go.temporal.io/api/enums/v1"
)

type (
	overlappable interface {
		comparable
		GetOverlapPolicy() enumspb.ScheduleOverlapPolicy
	}

	processBufferResult[T overlappable] struct {
		// We can start allow-all all at once
		overlappingStarts []T
		// Ignoring allow-all, we can start either zero or one now.
		// This is the one that we want to start, or nil.
		nonOverlappingStart T
		// The remaining buffer
		newBuffer []T
		// Whether to cancel/terminate the currently-running one
		needCancel    bool
		needTerminate bool
		// Stats
		overlapSkipped int64
	}
)

func processBuffer[T overlappable](
	buffer []T,
	isRunning bool,
	resolve func(enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy,
) processBufferResult[T] {
	// We should try to do something reasonable with any combination of overlap
	// policies in the buffer, although some combinations don't make much sense
	// and would require a convoluted series of calls to set up.
	//
	// Buffer entries that have unspecified overlap policy are resolved to the
	// current policy here, not earlier, so that updates to the policy can
	// affect them.

	var action processBufferResult[T]
	var zeroVal T

	for _, start := range buffer {
		overlapPolicy := resolve(start.GetOverlapPolicy())

		// For allow-all, just collect and start all at once
		if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
			action.overlappingStarts = append(action.overlappingStarts, start)
			continue
		}

		// Now handle non-overlapping.

		// If there's nothing running, we can start this one no matter what the policy is
		if !isRunning && action.nonOverlappingStart == zeroVal {
			action.nonOverlappingStart = start
			continue
		}

		// Otherwise this one overlaps and we should apply the policy
		switch overlapPolicy {
		case enumspb.SCHEDULE_OVERLAP_POLICY_SKIP:
			// just skip
			action.overlapSkipped++
		case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:
			// allow one (the first one) in the buffer
			if len(action.newBuffer) == 0 {
				action.newBuffer = append(action.newBuffer, start)
			} else {
				action.overlapSkipped++
			}
		case enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:
			// always add to buffer
			action.newBuffer = append(action.newBuffer, start)
		case enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:
			if isRunning {
				// an actual workflow is running, cancel it (asynchronously)
				action.needCancel = true
				// keep in buffer so it will get started once cancel completes
				action.newBuffer = append(action.newBuffer, start)
			} else {
				// it's not running yet, it's just the one we were going to start. replace it
				action.nonOverlappingStart = start
			}
		case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
			if isRunning {
				// an actual workflow is running, terminate it
				action.needTerminate = true
				// keep in buffer so it will get started once terminate completes
				action.newBuffer = append(action.newBuffer, start)
			} else {
				// it's not running yet, it's just the one we were going to start. replace it
				action.nonOverlappingStart = start
			}
		}
	}

	if action.needCancel || action.needTerminate {
		// In a very contrived situation, we could end up with overlapping starts at the same
		// time as a non-overlapping start tries to cancel/terminate a running workflow. But
		// then it will immediately cancel/terminate the overlapping ones too (either on this
		// iteration or the next one). So we shouldn't even bother starting them.
		action.overlappingStarts = nil
	}

	return action
}
