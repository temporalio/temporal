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

	"github.com/temporalio/temporal/common/backoff"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/failure"
	"github.com/temporalio/temporal/common/persistence"
)

func Test_NextRetry(t *testing.T) {
	a := assert.New(t)
	now, _ := time.Parse(time.RFC3339, "2018-04-13T16:08:08+00:00")
	reason := failure.NewServerFailure("good-reason", false)
	identity := "some-worker-identity"

	// no retry without retry policy
	ai := &persistence.ActivityInfo{
		ScheduleToStartTimeout: 5,
		ScheduleToCloseTimeout: 30,
		StartToCloseTimeout:    25,
		HasRetryPolicy:         false,
		NonRetryableErrorTypes: []string{"bad-reason", "ugly-reason"},
		StartedIdentity:        identity,
	}
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))

	// no retry if cancel requested
	ai.HasRetryPolicy = true
	ai.CancelRequested = true
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))

	// no retry if both MaximumAttempts and WorkflowExpirationTime are not set
	ai.CancelRequested = false
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))

	// no retry if MaximumAttempts is 1 (for initial attempt)
	ai.InitialInterval = 1
	ai.MaximumAttempts = 1
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		clock.NewRealTimeSource().Now(),
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))

	// backoff retry, intervals: 1s, 2s, 4s, 8s.
	ai.MaximumAttempts = 5
	ai.BackoffCoefficient = 2
	a.Equal(time.Second, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))
	ai.Attempt++

	a.Equal(time.Second*2, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))
	ai.Attempt++

	a.Equal(time.Second*4, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))
	ai.Attempt++

	// test non-retriable error
	reason = failure.NewServerFailure("bad-reason", true)
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))

	reason = failure.NewServerFailure("good-reason", false)

	a.Equal(time.Second*8, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))
	ai.Attempt++

	// no retry as max attempt reached
	a.Equal(ai.MaximumAttempts-1, ai.Attempt)
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))

	// increase max attempts, with max interval cap at 10s
	ai.MaximumAttempts = 6
	ai.MaximumInterval = 10
	a.Equal(time.Second*10, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))
	ai.Attempt++

	// no retry because expiration time before next interval
	ai.MaximumAttempts = 8
	ai.ExpirationTime = now.Add(time.Second * 5)
	a.Equal(backoff.NoBackoff, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))

	// extend expiration, next interval should be 10s
	ai.ExpirationTime = now.Add(time.Minute)
	a.Equal(time.Second*10, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))
	ai.Attempt++

	// with big max retry, math.Pow() could overflow, verify that it uses the MaxInterval
	ai.Attempt = 64
	ai.MaximumAttempts = 100
	a.Equal(time.Second*10, getBackoffInterval(
		now,
		ai.ExpirationTime,
		ai.Attempt,
		ai.MaximumAttempts,
		ai.InitialInterval,
		ai.MaximumInterval,
		ai.BackoffCoefficient,
		reason,
		ai.NonRetryableErrorTypes,
	))
	ai.Attempt++
}
