package taskqueue

import (
	enumsspb "go.temporal.io/server/api/enums/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
)

func NeedsRelease(ref *taskqueuespb.LimiterRef) bool {
	switch ref.GetLimiterType() {
	case enumsspb.LIMITER_TYPE_CONCURRENCY:
		return true
	default:
		return false
	}
}
