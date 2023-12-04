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
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
)

type RetryableActivity struct {
	ai          *persistence.ActivityInfo
	scheduledAt *timestamppb.Timestamp
	timesource  clock.TimeSource
	calculator  BackoffIntervalCalculator
}

func NewRetryableActivity(
	ai *persistence.ActivityInfo,
	failure *failurepb.Failure,
	timesource clock.TimeSource,
	backoffCalculator BackoffIntervalCalculator,
) (*RetryableActivity, enumspb.RetryState) {
	if !ai.HasRetryPolicy {
		return nil, enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET
	}

	if ai.CancelRequested {
		return nil, enumspb.RETRY_STATE_CANCEL_REQUESTED
	}

	if !isRetryable(failure, ai.RetryNonRetryableErrorTypes) {
		return nil, enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE
	}

	builder := &RetryableActivity{ai: ai, calculator: backoffCalculator, timesource: timesource}
	state := builder.calculateSchedule()
	return builder, state
}

func (ra *RetryableActivity) UpdateActivityInfo(ai *persistence.ActivityInfo, version int64, failure *failurepb.Failure) *persistence.ActivityInfo {
	ai.Attempt++
	ai.Version = version
	ai.ScheduledTime = ra.scheduledAt
	ai.StartedEventId = common.EmptyEventID
	ai.RequestId = ""
	ai.StartedTime = nil
	ai.TimerTaskStatus = TimerTaskStatusNone
	ai.RetryLastWorkerIdentity = ai.StartedIdentity
	ai.RetryLastFailure = failure
	return ai
}

func (ra *RetryableActivity) calculateSchedule() enumspb.RetryState {
	now := ra.timesource.Now()
	backoff, retryState := ra.calculator.Calculate(
		now,
		ra.ai.Attempt,
		ra.ai.RetryMaximumAttempts,
		ra.ai.RetryInitialInterval,
		ra.ai.RetryMaximumInterval,
		ra.ai.RetryExpirationTime,
		ra.ai.RetryBackoffCoefficient,
	)
	if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return retryState
	}

	ra.scheduledAt = timestamppb.New(now.Add(backoff)) // update to next schedule time
	return retryState
}
