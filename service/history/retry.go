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

package history

import (
	"math"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"

	"go.temporal.io/server/common/backoff"
)

func getBackoffInterval(
	now time.Time,
	expirationTime time.Time,
	currAttempt int32,
	maxAttempts int32,
	initInterval int32,
	maxInterval int32,
	backoffCoefficient float64,
	failure *failurepb.Failure,
	nonRetryableTypes []string,
) (time.Duration, enumspb.RetryState) {
	if !isRetryable(failure, nonRetryableTypes) {
		return backoff.NoBackoff, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE
	}

	if maxAttempts == 0 && expirationTime.IsZero() {
		return backoff.NoBackoff, enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET
	}

	if maxAttempts > 0 && currAttempt >= maxAttempts-1 {
		// currAttempt starts from 0.
		// MaximumAttempts is the total attempts, including initial (non-retry) attempt.
		return backoff.NoBackoff, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
	}

	nextInterval := int64(float64(initInterval) * math.Pow(backoffCoefficient, float64(currAttempt)))
	if nextInterval <= 0 {
		// math.Pow() could overflow
		if maxInterval > 0 {
			nextInterval = int64(maxInterval)
		} else {
			return backoff.NoBackoff, enumspb.RETRY_STATE_TIMEOUT
		}
	}

	if maxInterval > 0 && nextInterval > int64(maxInterval) {
		// cap next interval to MaxInterval
		nextInterval = int64(maxInterval)
	}

	backoffInterval := time.Duration(nextInterval) * time.Second
	nextScheduleTime := now.Add(backoffInterval)
	if !expirationTime.IsZero() && nextScheduleTime.After(expirationTime) {
		return backoff.NoBackoff, enumspb.RETRY_STATE_TIMEOUT
	}

	return backoffInterval, enumspb.RETRY_STATE_IN_PROGRESS
}

func isRetryable(failure *failurepb.Failure, nonRetryableTypes []string) bool {
	if failure == nil {
		return true
	}

	if failure.GetTerminatedFailureInfo() != nil || failure.GetCanceledFailureInfo() != nil {
		return false
	}

	if failure.GetTimeoutFailureInfo() != nil {
		return failure.GetTimeoutFailureInfo().GetTimeoutType() == enumspb.TIMEOUT_TYPE_START_TO_CLOSE ||
			failure.GetTimeoutFailureInfo().GetTimeoutType() == enumspb.TIMEOUT_TYPE_HEARTBEAT
	}

	if failure.GetServerFailureInfo() != nil {
		return !failure.GetServerFailureInfo().GetNonRetryable()
	}

	if failure.GetApplicationFailureInfo() != nil {
		if failure.GetApplicationFailureInfo().GetNonRetryable() {
			return false
		}

		failureType := failure.GetApplicationFailureInfo().GetType()
		for _, nrt := range nonRetryableTypes {
			if nrt == failureType {
				return false
			}
		}
	}
	return true
}
