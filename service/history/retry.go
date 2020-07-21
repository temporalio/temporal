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
	"fmt"
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
)

func getBackoffInterval(
	now time.Time,
	expirationTime time.Time,
	currentAttemptCounterValue int32,
	maxAttempts int32,
	initInterval int32,
	maxInterval int32,
	backoffCoefficient float64,
	failure *failurepb.Failure,
	nonRetryableTypes []string,
) (time.Duration, enumspb.RetryState) {

	// Sanitiy check to make sure currentAttemptCounterValue started with 1.
	if currentAttemptCounterValue < 1 {
		currentAttemptCounterValue = 1
	}

	if !isRetryable(failure, nonRetryableTypes) {
		return backoff.NoBackoff, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE
	}

	if maxAttempts == 0 && expirationTime.IsZero() {
		return backoff.NoBackoff, enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET
	}

	// currentAttemptCounterValue starts from 1.
	// maxAttempts is the total attempts, including initial (non-retry) attempt.
	// At this point we are about to make next attempt and all calculations in this func are for this next attempt.
	// For example, if maxAttepmtps is set to 2 and we are making 1st retry, currentAttemptCounterValue will be 1
	// (we made 1 non-retry attempt already) and condition (currentAttemptCounterValue+1 > maxAttempts) will be false.
	// With 2nd retry, currentAttemptCounterValue will be 2 (1 non-retry + 1 retry attempt already made) and
	// condition (currentAttemptCounterValue+1 > maxAttempts) will be true (means stop retrying, we tried 2 time already).
	if maxAttempts > 0 && currentAttemptCounterValue+1 > maxAttempts {
		return backoff.NoBackoff, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
	}

	nextInterval := int64(float64(initInterval) * math.Pow(backoffCoefficient, float64(currentAttemptCounterValue-1)))
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

func getDefaultActivityRetryPolicyConfigOptions() map[string]interface{} {
	return map[string]interface{}{
		"InitialRetryIntervalInSeconds": 1,
		"MaximumRetryIntervalInSeconds": 100,
		"ExponentialBackoffCoefficient": 2.0,
		"MaximumAttempts":               0,
	}
}

func fromConfigToActivityRetryPolicy(options map[string]interface{}) *commonpb.RetryPolicy {
	retryPolicy := &commonpb.RetryPolicy{}
	initialRetryInterval, ok := options["InitialRetryIntervalInSeconds"]
	if ok {
		retryPolicy.InitialIntervalInSeconds = int32(initialRetryInterval.(int))
	}

	maxRetryInterval, ok := options["MaximumRetryIntervalInSeconds"]
	if ok {
		retryPolicy.MaximumIntervalInSeconds = int32(maxRetryInterval.(int))
	}

	exponentialBackoffCoefficient, ok := options["ExponentialBackoffCoefficient"]
	if ok {
		retryPolicy.BackoffCoefficient = exponentialBackoffCoefficient.(float64)
	}

	maximumAttempts, ok := options["MaximumAttempts"]
	if ok {
		retryPolicy.MaximumAttempts = int32(maximumAttempts.(int))
	}

	err := common.ValidateRetryPolicy(retryPolicy)
	if err != nil {
		panic(
			fmt.Sprintf(
				"Bad Default Activity Retry Policy defined: %+v failed validation %v",
				retryPolicy,
				err))
	}

	return retryPolicy
}
