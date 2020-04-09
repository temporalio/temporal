package history

import (
	"math"
	"time"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/backoff"
)

func getBackoffInterval(
	now time.Time,
	expirationTime time.Time,
	currAttempt int32,
	maxAttempts int32,
	initInterval int32,
	maxInterval int32,
	backoffCoefficient float64,
	failureReason string,
	nonRetriableErrors []string,
) time.Duration {

	if maxAttempts == 0 && expirationTime.IsZero() {
		return backoff.NoBackoff
	}

	if maxAttempts > 0 && currAttempt >= maxAttempts-1 {
		// currAttempt starts from 0.
		// MaximumAttempts is the total attempts, including initial (non-retry) attempt.
		return backoff.NoBackoff
	}

	nextInterval := int64(float64(initInterval) * math.Pow(backoffCoefficient, float64(currAttempt)))
	if nextInterval <= 0 {
		// math.Pow() could overflow
		if maxInterval > 0 {
			nextInterval = int64(maxInterval)
		} else {
			return backoff.NoBackoff
		}
	}

	if maxInterval > 0 && nextInterval > int64(maxInterval) {
		// cap next interval to MaxInterval
		nextInterval = int64(maxInterval)
	}

	backoffInterval := time.Duration(nextInterval) * time.Second
	nextScheduleTime := now.Add(backoffInterval)
	if !expirationTime.IsZero() && nextScheduleTime.After(expirationTime) {
		return backoff.NoBackoff
	}

	// make sure we don't retry size exceeded error reasons. Note that FailureReasonFailureDetailsExceedsLimit is retryable.
	if failureReason == common.FailureReasonCancelDetailsExceedsLimit ||
		failureReason == common.FailureReasonCompleteResultExceedsLimit ||
		failureReason == common.FailureReasonHeartbeatExceedsLimit ||
		failureReason == common.FailureReasonDecisionBlobSizeExceedsLimit {
		return backoff.NoBackoff
	}

	// check if error is non-retriable
	for _, er := range nonRetriableErrors {
		if er == failureReason {
			return backoff.NoBackoff
		}
	}

	return backoffInterval
}
