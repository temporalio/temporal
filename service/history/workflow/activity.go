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
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func GetActivityState(ai *persistence.ActivityInfo) enumspb.PendingActivityState {
	if ai.CancelRequested {
		return enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	}
	if ai.StartedEventId != common.EmptyEventID {
		return enumspb.PENDING_ACTIVITY_STATE_STARTED
	}
	return enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
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

func UpdateActivityInfoForRetries(
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

func ActivityState(ai *persistence.ActivityInfo) enumspb.PendingActivityState {
	if ai.CancelRequested {
		return enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	}
	if ai.StartedEventId != common.EmptyEventID {
		return enumspb.PENDING_ACTIVITY_STATE_STARTED
	}
	return enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
}

func GetPendingActivityInfo(
	ctx context.Context, // only used as a passthrough to GetActivityType
	shardContext shard.Context,
	mutableState MutableState,
	ai *persistence.ActivityInfo,
) (*workflowpb.PendingActivityInfo, error) {
	now := shardContext.GetTimeSource().Now().UTC()

	p := &workflowpb.PendingActivityInfo{
		ActivityId: ai.ActivityId,
	}
	if ai.GetUseWorkflowBuildIdInfo() != nil {
		p.AssignedBuildId = &workflowpb.PendingActivityInfo_UseWorkflowBuildId{UseWorkflowBuildId: &emptypb.Empty{}}
	} else if ai.GetLastIndependentlyAssignedBuildId() != "" {
		p.AssignedBuildId = &workflowpb.PendingActivityInfo_LastIndependentlyAssignedBuildId{
			LastIndependentlyAssignedBuildId: ai.GetLastIndependentlyAssignedBuildId(),
		}
	}

	p.State = ActivityState(ai)

	p.LastAttemptCompleteTime = ai.LastAttemptCompleteTime
	if !ai.HasRetryPolicy {
		p.NextAttemptScheduleTime = nil
	} else {
		p.Attempt = ai.Attempt
		if p.State == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			scheduledTime := ai.ScheduledTime.AsTime()
			if now.Before(scheduledTime) {
				// in this case activity is waiting for a retry
				p.NextAttemptScheduleTime = ai.ScheduledTime
				currentRetryDuration := p.NextAttemptScheduleTime.AsTime().Sub(p.LastAttemptCompleteTime.AsTime())
				p.CurrentRetryInterval = durationpb.New(currentRetryDuration)
			} else {
				// in this case activity is at least scheduled
				p.NextAttemptScheduleTime = nil
				// we rely on the fact that ExponentialBackoffAlgorithm is deterministic, and  there's no random jitter
				interval := ExponentialBackoffAlgorithm(ai.RetryInitialInterval, ai.RetryBackoffCoefficient, p.Attempt)
				p.CurrentRetryInterval = durationpb.New(interval)
			}
		}
	}
	p.Attempt = max(p.Attempt, 1)

	if ai.LastHeartbeatUpdateTime != nil && !ai.LastHeartbeatUpdateTime.AsTime().IsZero() {
		p.LastHeartbeatTime = ai.LastHeartbeatUpdateTime
		p.HeartbeatDetails = ai.LastHeartbeatDetails
	}
	var err error
	p.ActivityType, err = mutableState.GetActivityType(ctx, ai)
	if err != nil {
		return nil, err
	}
	p.ScheduledTime = ai.ScheduledTime
	p.LastStartedTime = ai.StartedTime
	p.LastWorkerIdentity = ai.StartedIdentity
	if ai.HasRetryPolicy {
		p.ExpirationTime = ai.RetryExpirationTime
		p.MaximumAttempts = ai.RetryMaximumAttempts
		p.LastFailure = ai.RetryLastFailure
		if p.LastWorkerIdentity == "" && ai.RetryLastWorkerIdentity != "" {
			p.LastWorkerIdentity = ai.RetryLastWorkerIdentity
		}
	}

	return p, nil
}

func GetNextScheduledTime(ai *persistence.ActivityInfo) time.Time {
	// there are two possible cases:
	// * this is the first time activity was scheduled
	//  * in this case we should use current schedule time
	// * this is a retry
	//  * next scheduled time will be calculated, based on the retry policy and last time when activity was completed

	nextScheduledTime := ai.ScheduledTime.AsTime()
	if ai.Attempt > 1 {
		// calculate new schedule time
		interval := ExponentialBackoffAlgorithm(ai.RetryInitialInterval, ai.RetryBackoffCoefficient, ai.Attempt)

		if ai.RetryMaximumInterval.AsDuration() != 0 && (interval <= 0 || interval > ai.RetryMaximumInterval.AsDuration()) {
			interval = ai.RetryMaximumInterval.AsDuration()
		}

		if interval > 0 {
			nextScheduledTime = ai.LastAttemptCompleteTime.AsTime().Add(interval)
		}
	}
	return nextScheduledTime
}

func PauseActivityById(mutableState MutableState, activityId string) error {
	if !mutableState.IsWorkflowExecutionRunning() {
		return consts.ErrWorkflowCompleted
	}

	ai, activityFound := mutableState.GetActivityByActivityID(activityId)

	if !activityFound {
		return consts.ErrActivityNotFound
	}

	if ai.Paused {
		// do nothing
		return nil
	}

	// note - we are not increasing the stamp of the activity.
	// this is because if activity is actually running we should let it finish

	return mutableState.UpdateActivityWithCallback(ai, func(activityInfo *persistence.ActivityInfo, _ MutableState) {
		activityInfo.Paused = true
	})
}
