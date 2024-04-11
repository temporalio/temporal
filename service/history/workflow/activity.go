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
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
)

type RequestedDelay struct {
	Interval *time.Duration
}

type ActivityVisitor interface {
	UpdateActivityInfo(ai *persistence.ActivityInfo, version int64, attempt int32, failure *failurepb.Failure) *persistence.ActivityInfo
	State() enumspb.RetryState
	NextScheduledTime() time.Time
}

type nonRetryableActivityVisitor struct {
	state enumspb.RetryState
}

type retryableActivityVisitor struct {
	ai                *persistence.ActivityInfo
	nextScheduledTime time.Time
	timesource        clock.TimeSource
	state             enumspb.RetryState
}

func newActivityVisitor(
	ai *persistence.ActivityInfo,
	failure *failurepb.Failure,
	timesource clock.TimeSource,
) ActivityVisitor {
	if !ai.HasRetryPolicy {
		return &nonRetryableActivityVisitor{state: enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET}
	}
	if ai.CancelRequested {
		return &nonRetryableActivityVisitor{state: enumspb.RETRY_STATE_CANCEL_REQUESTED}
	}

	if !isRetryable(failure, ai.RetryNonRetryableErrorTypes) {
		return &nonRetryableActivityVisitor{state: enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE}
	}

	now := timesource.Now().In(time.UTC)
	delay := nextRetryDelayFrom(failure)

	backoff, retryState := nextBackoffInterval(
		now,
		ai.Attempt,
		ai.RetryMaximumAttempts,
		ai.RetryInitialInterval,
		ai.RetryMaximumInterval,
		ai.RetryExpirationTime,
		ai.RetryBackoffCoefficient,
		makeBackoffAlgorithm(delay),
	)
	if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return &nonRetryableActivityVisitor{state: retryState}
	}

	nextScheduledTime := now.Add(backoff)
	visitor := &retryableActivityVisitor{
		ai:                ai,
		timesource:        timesource,
		nextScheduledTime: nextScheduledTime,
		state:             retryState,
	}
	return visitor
}

func (nra *nonRetryableActivityVisitor) UpdateActivityInfo(ai *persistence.ActivityInfo, _ int64, _ int32, _ *failurepb.Failure) *persistence.ActivityInfo {
	return ai
}

func (nra *nonRetryableActivityVisitor) State() enumspb.RetryState {
	return nra.state
}

func (nra *nonRetryableActivityVisitor) NextScheduledTime() time.Time {
	return time.Unix(0, 0).In(time.UTC)
}

func (ra *retryableActivityVisitor) State() enumspb.RetryState {
	return ra.state
}

func (ra *retryableActivityVisitor) UpdateActivityInfo(ai *persistence.ActivityInfo, version int64, attempt int32, failure *failurepb.Failure) *persistence.ActivityInfo {
	ai.Attempt = attempt
	ai.Version = version
	ai.ScheduledTime = timestamppb.New(ra.nextScheduledTime)
	ai.StartedEventId = common.EmptyEventID
	ai.RequestId = ""
	ai.StartedTime = nil
	ai.TimerTaskStatus = TimerTaskStatusNone
	ai.RetryLastWorkerIdentity = ai.StartedIdentity
	ai.RetryLastFailure = failure
	return ai
}

func (ra *retryableActivityVisitor) NextScheduledTime() time.Time {
	return ra.nextScheduledTime
}

func makeBackoffAlgorithm(requestedDelay *time.Duration) BackoffCalculatorAlgorithmFunc {
	return func(duration *durationpb.Duration, coefficient float64, currentAttempt int32) time.Duration {
		if requestedDelay != nil {
			return *requestedDelay
		}
		return ExponentialBackoffAlgorithm(duration, coefficient, currentAttempt)
	}
}

func nextRetryDelayFrom(failure *failurepb.Failure) *time.Duration {
	var delay *time.Duration
	afi, ok := failure.GetFailureInfo().(*failurepb.Failure_ApplicationFailureInfo)
	if !ok {
		return delay
	}
	p := afi.ApplicationFailureInfo.GetNextRetryDelay()
	if p != nil {
		d := p.AsDuration()
		delay = &d
	}
	return delay
}
