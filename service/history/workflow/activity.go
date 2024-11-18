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
	"go.temporal.io/server/api/historyservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func GetActivityState(ai *persistencepb.ActivityInfo) enumspb.PendingActivityState {
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
	ai *persistencepb.ActivityInfo,
	version int64,
	attempt int32,
	failure *failurepb.Failure,
	nextScheduledTime *timestamppb.Timestamp,
) *persistencepb.ActivityInfo {
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

func GetPendingActivityInfo(
	ctx context.Context, // only used as a passthrough to GetActivityType
	shardContext shard.Context,
	mutableState MutableState,
	ai *persistencepb.ActivityInfo,
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

	p.State = GetActivityState(ai)

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

func GetNextScheduledTime(ai *persistencepb.ActivityInfo) time.Time {
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

	return mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencepb.ActivityInfo, _ MutableState) error {
		// note - we are not increasing the stamp of the activity if it is running.
		// this is because if activity is actually running we should let it finish
		if GetActivityState(activityInfo) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			activityInfo.Stamp++
		}
		activityInfo.Paused = true
		return nil
	})
}
func ResetActivityById(
	shardContext shard.Context,
	mutableState MutableState,
	activityId string,
	scheduleNewRun bool,
	resetHeartbeats bool,
) error {
	if !mutableState.IsWorkflowExecutionRunning() {
		return consts.ErrWorkflowCompleted
	}
	ai, activityFound := mutableState.GetActivityByActivityID(activityId)

	if !activityFound {
		return consts.ErrActivityNotFound
	}

	if err := mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencepb.ActivityInfo, ms MutableState) {
		// reset the number of attempts
		ai.Attempt = 1

		// change stamp and regenerate the retry task if needed
		if needRegenerateRetryTask(ai, scheduleNewRun) {
			ai.Stamp++
			_ = ms.RegenerateActivityRetryTask(ai, shardContext.GetTimeSource().Now())
		}

		if resetHeartbeats {
			activityInfo.LastHeartbeatDetails = nil
			activityInfo.LastHeartbeatUpdateTime = nil
		}
	}); err != nil {
		return err
	}

	return nil
}

func UnpauseActivityWithResume(
	shardContext shard.Context,
	mutableState MutableState,
	ai *persistencepb.ActivityInfo,
	scheduleNewRun bool,
) (*historyservice.UnpauseActivityResponse, error) {

	if err := mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencepb.ActivityInfo, ms MutableState) {
		activityInfo.Stamp++
		activityInfo.Paused = false

		// change stamp and regenerate the retry task if needed
		if GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			scheduleTime := activityInfo.ScheduledTime.AsTime()
			if scheduleNewRun {
				scheduleTime = shardContext.GetTimeSource().Now().UTC()
			}
			_ = ms.RegenerateActivityRetryTask(activityInfo, scheduleTime)
		}
	}); err != nil {
		return nil, err
	}

	return &historyservice.UnpauseActivityResponse{}, nil
}

func UnpauseActivityWithReset(
	shardContext shard.Context,
	mutableState MutableState,
	ai *persistencepb.ActivityInfo,
	scheduleNewRun bool,
	resetHeartbeats bool,
) (*historyservice.UnpauseActivityResponse, error) {
	if err := mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencepb.ActivityInfo, ms MutableState) {
		activityInfo.Paused = false
		activityInfo.Stamp++

		// reset the number of attempts
		activityInfo.Attempt = 1

		if needRegenerateRetryTask(activityInfo, scheduleNewRun) {
			scheduleTime := activityInfo.ScheduledTime.AsTime()
			if scheduleNewRun {
				scheduleTime = shardContext.GetTimeSource().Now().UTC()
			}
			_ = ms.RegenerateActivityRetryTask(activityInfo, scheduleTime)

		}

		if resetHeartbeats {
			activityInfo.LastHeartbeatDetails = nil
			activityInfo.LastHeartbeatUpdateTime = nil
		}

	}); err != nil {
		return nil, err
	}

	return &historyservice.UnpauseActivityResponse{}, nil
}

func needRegenerateRetryTask(ai *persistencepb.ActivityInfo, scheduleNewRun bool) bool {
	// if activity is Paused - we don't need to do anything
	if ai.Paused {
		return false
	}

	// we need to update the Stamp:
	// * if scheduleNewRun flag is provided and activity is in retry
	//		- we need to prevent the retry from executing
	// * if scheduleNewRun flag is provided and activity is running
	//		- we need to prevent the previous run from succeeding
	// * if scheduleNewRun flag is not provided  and activity is in retry
	//		- retry may not happen  since the number of attempts changes
	//   	- because of that we need to generate new retry task
	//  	- because of that we need to increase the Stamp to block previous retry attempt (if the number of attempts doesn't change)
	// the only case when we don't need to increase the Stamp is when activity is running and there is no scheduleNewRun flag.

	if scheduleNewRun && GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
		// activity is waiting to start, scheduleNewRun flag is provided
		// we need to start the activity immediately
		// and increase the Stamp to prevent old retry from executing
		return true
	}

	if scheduleNewRun && GetActivityState(ai) != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
		// activity is running, scheduleNewRun flag is provided
		// we need to start the activity immediately
		// need to increase the Stamp to prevent old instance from succeeding
		return true
	}

	if !scheduleNewRun && GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
		// activity is waiting to start, scheduleNewRun flag is NOT provided
		// in this case we still need to increase the Stamp
		// this is because we reset the number of attempts, and this will prevent old retry from executing
		// we also need to start the activity immediately - no reason to wait
		// we still need to increase the stamp in case the number of attempts doesn't change
		return true
	}

	if !scheduleNewRun && GetActivityState(ai) != enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
		// activity is running, scheduleNewRun flag is NOT provided
		// in this case we don't need to do anything
		return false
	}

	return false
}
