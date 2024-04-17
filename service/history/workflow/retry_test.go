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

package workflow

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/number"
	"go.temporal.io/server/common/retrypolicy"

	"go.temporal.io/server/common/backoff"
)

func Test_IsRetryable(t *testing.T) {
	a := assert.New(t)

	f := &failurepb.Failure{
		FailureInfo: &failurepb.Failure_TerminatedFailureInfo{TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{}},
	}
	a.False(isRetryable(f, nil))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}},
	}
	a.False(isRetryable(f, nil))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType: enumspb.TIMEOUT_TYPE_UNSPECIFIED,
		}},
	}
	a.False(isRetryable(f, nil))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		}},
	}
	a.True(isRetryable(f, nil))
	a.False(isRetryable(f, []string{retrypolicy.TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()}))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		}},
	}
	a.False(isRetryable(f, nil))
	a.False(isRetryable(f, []string{retrypolicy.TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START.String()}))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		}},
	}
	a.False(isRetryable(f, nil))
	a.False(isRetryable(f, []string{retrypolicy.TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE.String()}))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
			TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
		}},
	}
	a.True(isRetryable(f, nil))
	a.False(isRetryable(f, []string{retrypolicy.TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_HEARTBEAT.String()}))
	a.True(isRetryable(f, []string{retrypolicy.TimeoutFailureTypePrefix + enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()}))
	a.True(isRetryable(f, []string{retrypolicy.TimeoutFailureTypePrefix + "unknown timeout type string"}))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: false,
		}},
	}
	a.True(isRetryable(f, nil))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_ServerFailureInfo{ServerFailureInfo: &failurepb.ServerFailureInfo{
			NonRetryable: true,
		}},
	}
	a.False(isRetryable(f, nil))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			NonRetryable: true,
		}},
	}
	a.False(isRetryable(f, nil))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			NonRetryable: false,
			Type:         "type",
		}},
	}
	a.True(isRetryable(f, nil))
	a.True(isRetryable(f, []string{"otherType"}))
	a.False(isRetryable(f, []string{"otherType", "type"}))
	a.False(isRetryable(f, []string{"type"}))

	// When any failure is inside ChildWorkflowExecutionFailure, it is always retryable because ChildWorkflow is always retryable.
	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_ChildWorkflowExecutionFailureInfo{ChildWorkflowExecutionFailureInfo: &failurepb.ChildWorkflowExecutionFailureInfo{}},
		Cause: &failurepb.Failure{
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				NonRetryable: true,
			}},
		},
	}
	a.True(isRetryable(f, nil))

	f = &failurepb.Failure{
		FailureInfo: &failurepb.Failure_ChildWorkflowExecutionFailureInfo{ChildWorkflowExecutionFailureInfo: &failurepb.ChildWorkflowExecutionFailureInfo{}},
		Cause: &failurepb.Failure{
			FailureInfo: &failurepb.Failure_ActivityFailureInfo{ActivityFailureInfo: &failurepb.ActivityFailureInfo{}},
			Cause: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				}},
			},
		},
	}
	a.True(isRetryable(f, nil))
}

func Test_NonRetriableErrors(t *testing.T) {
	attempt := int32(1)
	now, _ := time.Parse(time.RFC3339, "2018-04-13T16:08:08+00:00")
	maxRetryAttempts := int32(2)
	retryInterval := durationpb.New(time.Duration(time.Second))
	maxRetryInterval := durationpb.New(time.Duration(0))
	expirationTime := timestamppb.New(now.Add(100 * time.Second))
	backoffCoefficient := float64(2)
	nonRetryableErrorTypes := []string{}

	t.Run("when non-retriable error provided should fail with non retriable failure", func(t *testing.T) {
		nonRetriableFailure := failure.NewServerFailure("some non-retryable server failure", true)
		interval, retryState := getBackoffInterval(
			doNotCare(now),
			doNotCare(attempt),
			doNotCare(maxRetryAttempts),
			doNotCare(retryInterval),
			doNotCare(maxRetryInterval),
			doNotCare(expirationTime),
			doNotCare(backoffCoefficient),
			nonRetriableFailure,
			doNotCare(nonRetryableErrorTypes),
		)
		assert.Equal(t, backoff.NoBackoff, interval)
		assert.Equal(t, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, retryState)
	})

	t.Run("when retriable error provided should proceed to calculate backoff interval", func(t *testing.T) {
		retriableFailure := failure.NewServerFailure("good-reason", false)

		_, retryState := getBackoffInterval(
			doNotCare(now),
			doNotCare(attempt),
			doNotCare(maxRetryAttempts),
			doNotCare(retryInterval),
			doNotCare(maxRetryInterval),
			doNotCare(expirationTime),
			doNotCare(backoffCoefficient),
			retriableFailure,
			doNotCare(nonRetryableErrorTypes),
		)
		assert.NotEqual(t, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, retryState)
	})
}

func Test_nextBackoffInterval(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2018-04-13T16:08:08+00:00")
	expirationIn := func(t time.Duration) *timestamppb.Timestamp { return timestamppb.New(now.Add(t)) }
	initInterval := func(t time.Duration) *durationpb.Duration { return durationpb.New(t) }
	maxInterval := initInterval

	t.Run("first attempt should use initial backoff", func(t *testing.T) {
		currentAttempt := int32(1)
		initialDelay := 5 * time.Second
		interval, retryState := nextBackoffInterval(
			doNotCare(now),
			currentAttempt,
			5,
			initInterval(initialDelay),
			doNotCare(maxInterval(10*time.Second)),
			doNotCare(expirationIn(30*time.Second)),
			doNotCare[float64](2),
			ExponentialBackoffAlgorithm,
		)
		assert.Equal(t, initialDelay, interval)
		assert.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retryState)
	})

	t.Run("negative or 0 attempt should be treated as first", func(t *testing.T) {
		currentAttempt := int32(-1)
		maxAttempts := int32(5)
		initialDelay := 5 * time.Second
		interval, retryState := nextBackoffInterval(
			doNotCare(now),
			currentAttempt,
			maxAttempts,
			doNotCare(initInterval(initialDelay)),
			doNotCare(maxInterval(10*time.Second)),
			doNotCare(expirationIn(30*time.Second)),
			doNotCare[float64](2),
			ExponentialBackoffAlgorithm,
		)
		assert.Equal(t, initialDelay, interval)
		assert.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retryState)
	})

	t.Run("n-th retry should be initial * backoff^(n - 1)", func(t *testing.T) {
		initialDelay := 2 * time.Second
		attempt := int32(4)
		maxAttempts := int32(5)
		interval, retryState := nextBackoffInterval(
			doNotCare(now),
			attempt,
			maxAttempts,
			initInterval(initialDelay),
			doNotCare(maxInterval(200*time.Second)),
			doNotCare(expirationIn(600*time.Second)),
			3,
			ExponentialBackoffAlgorithm,
		)
		assert.Equal(t, initialDelay*pow(3, int32(attempt)-1), interval)
		assert.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retryState)
	})

	t.Run("if retry exceeds max backoff interval should set it to max", func(t *testing.T) {
		maxBackoff := 1 * time.Second
		interval, retryState := nextBackoffInterval(
			doNotCare(now),
			5,
			doNotCare[int32](20),
			initInterval(3*time.Second),
			maxInterval(maxBackoff),
			doNotCare(expirationIn(600*time.Second)),
			doNotCare[float64](2),
			ExponentialBackoffAlgorithm,
		)
		assert.Equal(t, maxBackoff, interval)
		assert.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retryState)
	})

	t.Run("when max attempts is specified and current exceeds max should return no more retries", func(t *testing.T) {
		interval, retryState := nextBackoffInterval(
			doNotCare(now),
			10,
			10,
			doNotCare(initInterval(3*time.Second)),
			doNotCare(maxInterval(10*time.Second)),
			doNotCare(expirationIn(600*time.Second)),
			doNotCare[float64](2),
			ExponentialBackoffAlgorithm,
		)
		assert.Equal(t, backoff.NoBackoff, interval)
		assert.Equal(t, enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, retryState)
	})

	t.Run("when max attempts is set to 0 should keep trying", func(t *testing.T) {
		initialDelay := 2 * time.Second
		interval, retryState := nextBackoffInterval(
			doNotCare(now),
			10,
			0,
			doNotCare(initInterval(initialDelay)),
			doNotCare(maxInterval(30*time.Minute)),
			doNotCare(expirationIn(60*time.Minute)),
			2,
			ExponentialBackoffAlgorithm,
		)
		assert.Equal(t, initialDelay*pow(2, 10-1), interval)
		assert.Equal(t, enumspb.RETRY_STATE_IN_PROGRESS, retryState)
	})

	t.Run("if expiration is not 0 and expected delay beyond expiration should return no more retries", func(t *testing.T) {
		initialDelay := 2 * time.Second
		interval, retryState := nextBackoffInterval(
			doNotCare(now),
			10,
			0,
			initInterval(initialDelay),
			maxInterval(30*time.Minute),
			expirationIn(1*time.Minute),
			2,
			ExponentialBackoffAlgorithm,
		)
		assert.Equal(t, backoff.NoBackoff, interval)
		assert.Equal(t, enumspb.RETRY_STATE_TIMEOUT, retryState)
	})

	t.Run("if expiration is 0 should retry", func(t *testing.T) {
		initialDelay := 2 * time.Second
		interval, retryState := nextBackoffInterval(
			doNotCare(now),
			10,
			0,
			initInterval(initialDelay),
			maxInterval(30*time.Minute),
			expirationIn(0),
			2,
			ExponentialBackoffAlgorithm,
		)
		assert.Equal(t, backoff.NoBackoff, interval)
		assert.Equal(t, enumspb.RETRY_STATE_TIMEOUT, retryState)
	})
}

func doNotCare[T any](x T) T { return x }

func pow[T any](base, exponent T) time.Duration {
	b := number.NewNumber(base).GetFloatOrDefault(math.NaN())
	if math.IsNaN(b) || b < 0 {
		panic("base is not a non-negative number")
	}
	e := number.NewNumber(exponent).GetFloatOrDefault(math.NaN())
	if math.IsNaN(e) || e < 0 {
		panic("exponent is not a non-negative number")
	}
	return time.Duration(math.Pow(b, e))
}
