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

const defaultInitialIntervalInSeconds = 1
const defaultMaximumIntervalCoefficient = 100.0
const defaultBackoffCoefficient = 2.0
const defaultMaximumAttempts = 0

func getBackoffInterval(
	now time.Time,
	expirationTime time.Time,
	currentAttemptCounterValue int32,
	maxAttempts int32,
	initInterval time.Duration,
	maxInterval time.Duration,
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

	maxIntervalSeconds := int64(maxInterval.Round(time.Second).Seconds())
	nextIntervalSeconds := int64(initInterval.Seconds() * math.Pow(backoffCoefficient, float64(currentAttemptCounterValue-1)))
	if nextIntervalSeconds <= 0 {
		// math.Pow() could overflow
		if maxInterval > 0 {
			nextIntervalSeconds = maxIntervalSeconds
		} else {
			return backoff.NoBackoff, enumspb.RETRY_STATE_TIMEOUT
		}
	}

	if maxInterval > 0 && nextIntervalSeconds > maxIntervalSeconds {
		// cap next interval to MaxInterval
		nextIntervalSeconds = maxIntervalSeconds
	}

	backoffInterval := time.Duration(nextIntervalSeconds) * time.Second
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

func getDefaultRetryPolicyConfigOptions() map[string]interface{} {
	return map[string]interface{}{
		"InitialIntervalInSeconds": defaultInitialIntervalInSeconds,
		"MaximumIntervalInSeconds": defaultMaximumIntervalCoefficient,
		"BackoffCoefficient":       defaultBackoffCoefficient,
		"MaximumAttempts":          defaultMaximumAttempts,
	}
}

func fromConfigToDefaultRetrySettings(options map[string]interface{}) common.DefaultRetrySettings {
	defaultSettings := common.DefaultRetrySettings{
		InitialIntervalInSeconds:   defaultInitialIntervalInSeconds,
		MaximumIntervalCoefficient: defaultMaximumIntervalCoefficient,
		BackoffCoefficient:         defaultBackoffCoefficient,
		MaximumAttempts:            defaultMaximumAttempts,
	}

	initialIntervalInSeconds, ok := options["InitialIntervalInSeconds"]
	if ok {
		defaultSettings.InitialIntervalInSeconds = int32(initialIntervalInSeconds.(int))
	}

	maximumIntervalCoefficient, ok := options["MaximumIntervalCoefficient"]
	if ok {
		defaultSettings.MaximumIntervalCoefficient = maximumIntervalCoefficient.(float64)
	}

	backoffCoefficient, ok := options["BackoffCoefficient"]
	if ok {
		defaultSettings.BackoffCoefficient = backoffCoefficient.(float64)
	}

	maximumAttempts, ok := options["MaximumAttempts"]
	if ok {
		defaultSettings.MaximumAttempts = int32(maximumAttempts.(int))
	}

	var empty commonpb.RetryPolicy
	common.EnsureRetryPolicyDefaults(&empty, defaultSettings)
	err := common.ValidateRetryPolicy(&empty)
	if err != nil {
		panic(
			fmt.Sprintf(
				"Bad Default Retry Settings defined: %+v failed validation %v",
				defaultSettings,
				err))
	}

	return defaultSettings
}
