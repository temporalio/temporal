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

	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

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

func updateActivityInfoForRetries(
	ai *persistence.ActivityInfo,
	version int64,
	attempt int32,
	failure *failurepb.Failure,
	nextScheduledTime *timestamppb.Timestamp,
) *persistence.ActivityInfo {
	ai.Attempt = attempt
	ai.Version = version
	ai.ScheduledTime = nextScheduledTime
	ai.StartedEventId = common.EmptyEventID
	ai.RequestId = ""
	ai.StartedTime = nil
	ai.TimerTaskStatus = TimerTaskStatusNone
	ai.RetryLastWorkerIdentity = ai.StartedIdentity
	ai.RetryLastFailure = failure

	return ai
}
