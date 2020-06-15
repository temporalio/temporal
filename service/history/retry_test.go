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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/temporal-proto/enums/v1"

	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/failure"
	"github.com/temporalio/temporal/common/persistence"
)

func Test_NextRetry(t *testing.T) {
	a := assert.New(t)
	now, _ := time.Parse(time.RFC3339, "2018-04-13T16:08:08+00:00")
	reason := failure.NewServerFailure("some retryable server failure", false)
	identity := "some-worker-identity"

	// no retry without retry policy
	ai := &persistence.ActivityInfo{
		ScheduleToStartTimeout: 5,
		ScheduleToCloseTimeout: 30,
		StartToCloseTimeout:    25,
		HasRetryPolicy:         false,
		NonRetryableErrorTypes: []string{},
		StartedIdentity:        identity,
	}
	interval, retryStatus := getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(backoff.NoBackoff, interval)
	a.Equal(enumspb.RETRY_STATUS_RETRY_POLICY_NOT_SET, retryStatus)

	// no retry if cancel requested
	ai.HasRetryPolicy = true
	ai.CancelRequested = true
	interval, retryStatus = getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(backoff.NoBackoff, interval)
	a.Equal(enumspb.RETRY_STATUS_RETRY_POLICY_NOT_SET, retryStatus)

	// no retry if both MaximumAttempts and WorkflowExpirationTime are not set
	ai.CancelRequested = false
	interval, retryStatus = getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(backoff.NoBackoff, interval)
	a.Equal(enumspb.RETRY_STATUS_RETRY_POLICY_NOT_SET, retryStatus)

	// no retry if MaximumAttempts is 1 (for initial attempt)
	ai.InitialInterval = 1
	ai.MaximumAttempts = 1
	interval, retryStatus = getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(backoff.NoBackoff, interval)
	a.Equal(enumspb.RETRY_STATUS_MAXIMUM_ATTEMPTS_REACHED, retryStatus)

	// backoff retry, intervals: 1s, 2s, 4s, 8s.
	ai.MaximumAttempts = 5
	ai.BackoffCoefficient = 2
	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(time.Second, interval)
	a.Equal(enumspb.RETRY_STATUS_IN_PROGRESS, retryStatus)
	ai.Attempt++

	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(time.Second*2, interval)
	a.Equal(enumspb.RETRY_STATUS_IN_PROGRESS, retryStatus)
	ai.Attempt++

	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(time.Second*4, interval)
	a.Equal(enumspb.RETRY_STATUS_IN_PROGRESS, retryStatus)
	ai.Attempt++

	// test non-retryable error
	reason = failure.NewServerFailure("some non-retryable server failure", true)
	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(backoff.NoBackoff, interval)
	a.Equal(enumspb.RETRY_STATUS_NON_RETRYABLE_FAILURE, retryStatus)

	reason = failure.NewServerFailure("good-reason", false)

	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(time.Second*8, interval)
	a.Equal(enumspb.RETRY_STATUS_IN_PROGRESS, retryStatus)
	ai.Attempt++

	// no retry as max attempt reached
	a.Equal(ai.MaximumAttempts-1, ai.Attempt)
	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(backoff.NoBackoff, interval)
	a.Equal(enumspb.RETRY_STATUS_MAXIMUM_ATTEMPTS_REACHED, retryStatus)

	// increase max attempts, with max interval cap at 10s
	ai.MaximumAttempts = 6
	ai.MaximumInterval = 10
	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(time.Second*10, interval)
	a.Equal(enumspb.RETRY_STATUS_IN_PROGRESS, retryStatus)
	ai.Attempt++

	// no retry because expiration time before next interval
	ai.MaximumAttempts = 8
	ai.ExpirationTime = now.Add(time.Second * 5)
	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(backoff.NoBackoff, interval)
	a.Equal(enumspb.RETRY_STATUS_TIMEOUT, retryStatus)

	// extend expiration, next interval should be 10s
	ai.ExpirationTime = now.Add(time.Minute)
	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(time.Second*10, interval)
	a.Equal(enumspb.RETRY_STATUS_IN_PROGRESS, retryStatus)
	ai.Attempt++

	// with big max retry, math.Pow() could overflow, verify that it uses the MaxInterval
	ai.Attempt = 64
	ai.MaximumAttempts = 100
	interval, retryStatus = getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	)
	a.Equal(time.Second*10, interval)
	a.Equal(enumspb.RETRY_STATUS_IN_PROGRESS, retryStatus)
	ai.Attempt++
}
