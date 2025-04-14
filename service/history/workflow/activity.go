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
	"math/rand"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	rulespb "go.temporal.io/api/rules/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow/matcher"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func GetActivityState(ai *persistencespb.ActivityInfo) enumspb.PendingActivityState {
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

func UpdateActivityInfoForRetries(
	ai *persistencespb.ActivityInfo,
	version int64,
	attempt int32,
	failure *failurepb.Failure,
	nextScheduledTime *timestamppb.Timestamp,
) *persistencespb.ActivityInfo {
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
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	ai *persistencespb.ActivityInfo,
) (*workflowpb.PendingActivityInfo, error) {
	now := shardContext.GetTimeSource().Now().UTC()

	p := &workflowpb.PendingActivityInfo{
		ActivityId:                  ai.ActivityId,
		LastWorkerDeploymentVersion: ai.LastWorkerDeploymentVersion,
		Priority:                    ai.Priority,
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
	p.Paused = ai.Paused

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

	// adjust activity state for paused activities
	if ai.Paused {
		if p.State == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			// this state means activity is not running on the worker
			// if activity is paused on server and not running on worker - mark it as PAUSED
			p.State = enumspb.PENDING_ACTIVITY_STATE_PAUSED
		} else if p.State == enumspb.PENDING_ACTIVITY_STATE_STARTED {
			// this state means activity is running on the worker
			// if activity is paused on server, but still running on worker - mark it as PAUSE_REQUESTED
			p.State = enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
		} // if state is CANCEL_REQUESTEd - it is not modified
	}

	return p, nil
}

func GetNextScheduledTime(ai *persistencespb.ActivityInfo) time.Time {
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

func PauseActivity(mutableState historyi.MutableState, activityId string) error {
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

	return mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		// note - we are not increasing the stamp of the activity if it is running.
		// this is because if activity is actually running we should let it finish
		if GetActivityState(activityInfo) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			activityInfo.Stamp++
		}
		activityInfo.Paused = true
		return nil
	})
}

func ResetActivity(
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	activityId string,
	resetHeartbeats bool,
	keepPaused bool,
	jitter time.Duration,
) error {
	if !mutableState.IsWorkflowExecutionRunning() {
		return consts.ErrWorkflowCompleted
	}
	ai, activityFound := mutableState.GetActivityByActivityID(activityId)

	if !activityFound {
		return consts.ErrActivityNotFound
	}

	return mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
		// reset the number of attempts
		ai.Attempt = 1

		if resetHeartbeats {
			activityInfo.LastHeartbeatDetails = nil
			activityInfo.LastHeartbeatUpdateTime = nil
		}

		// if activity is running, or it is paused and we don't want to unpause - we don't need to do anything
		if GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_STARTED || (ai.Paused && keepPaused) {
			return nil
		}

		ai.Stamp++
		if ai.Paused && !keepPaused {
			ai.Paused = false
		}

		// if activity is not running - we need to regenerate the retry task as schedule activity immediately
		if GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			scheduleTime := shardContext.GetTimeSource().Now().UTC()
			if jitter != 0 {
				randomOffset := time.Duration(rand.Int63n(int64(jitter)))
				scheduleTime = scheduleTime.Add(randomOffset)
			}
			if err := ms.RegenerateActivityRetryTask(ai, scheduleTime); err != nil {
				return err
			}
		}

		return nil
	})
}

func UnpauseActivity(
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	ai *persistencespb.ActivityInfo,
	resetAttempts bool,
	resetHeartbeat bool,
	jitter time.Duration,
) error {
	if err := mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
		activityInfo.Paused = false
		activityInfo.Stamp++

		if resetAttempts {
			activityInfo.Attempt = 1
		}
		if resetHeartbeat {
			activityInfo.LastHeartbeatDetails = nil
			activityInfo.LastHeartbeatUpdateTime = nil
		}

		// if activity is not running - we need to regenerate the retry task as schedule activity immediately
		if GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			scheduleTime := shardContext.GetTimeSource().Now().UTC()
			if jitter != 0 {
				randomOffset := time.Duration(rand.Int63n(int64(jitter)))
				scheduleTime = scheduleTime.Add(randomOffset)
			}
			if err := ms.RegenerateActivityRetryTask(ai, scheduleTime); err != nil {
				return err
			}
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func UnpauseActivityWithResume(
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	ai *persistencespb.ActivityInfo,
	scheduleNewRun bool,
	jitter time.Duration,
) (*historyservice.UnpauseActivityResponse, error) {

	if err := mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
		activityInfo.Stamp++
		activityInfo.Paused = false

		// regenerate the retry task if needed
		if GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			if err := regenerateActivityRetryTask(activityInfo, scheduleNewRun, jitter, ms, shardContext); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &historyservice.UnpauseActivityResponse{}, nil
}

func UnpauseActivityWithReset(
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	ai *persistencespb.ActivityInfo,
	scheduleNewRun bool,
	resetHeartbeats bool,
	jitter time.Duration,
) (*historyservice.UnpauseActivityResponse, error) {
	if err := mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
		activityInfo.Paused = false
		activityInfo.Stamp++

		// reset the number of attempts
		activityInfo.Attempt = 1

		if needRegenerateRetryTask(activityInfo, scheduleNewRun) {
			if err := regenerateActivityRetryTask(activityInfo, scheduleNewRun, jitter, ms, shardContext); err != nil {
				return err
			}
		}

		if resetHeartbeats {
			activityInfo.LastHeartbeatDetails = nil
			activityInfo.LastHeartbeatUpdateTime = nil
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return &historyservice.UnpauseActivityResponse{}, nil
}

func regenerateActivityRetryTask(
	activityInfo *persistencespb.ActivityInfo,
	scheduleNewRun bool,
	jitter time.Duration,
	ms historyi.MutableState,
	shardContext historyi.ShardContext) error {
	scheduleTime := activityInfo.ScheduledTime.AsTime()
	if scheduleNewRun {
		scheduleTime = shardContext.GetTimeSource().Now().UTC()
		if jitter != 0 {
			randomOffset := time.Duration(rand.Int63n(int64(jitter)))
			scheduleTime = scheduleTime.Add(randomOffset)
		}
	}
	return ms.RegenerateActivityRetryTask(activityInfo, scheduleTime)
}

func needRegenerateRetryTask(ai *persistencespb.ActivityInfo, scheduleNewRun bool) bool {
	// if activity is Paused - we don't need to do anything
	if ai.Paused {
		return false
	}

	if scheduleNewRun {
		// we need to always regenerate the retry task if scheduleNewRun flag is provided
		// but for a different reasons.
		// if activity is waiting to start - to prevent old retry from executing
		// if activity is running - to prevent the current running activity from finishing
		return true
	}

	if GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
		// activity is waiting to start, scheduleNewRun flag is NOT provided
		// in this case we still need to increase the Stamp
		// this is because we reset the number of attempts, and this will prevent old retry from executing
		// we also need to start the activity immediately - no reason to wait
		// we still need to increase the stamp in case the number of attempts doesn't change
		return true
	}

	// activity is running, scheduleNewRun flag is NOT provided
	// in this case we don't need to do anything
	return false
}

func MatchWorkflowRule(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionState *persistencespb.WorkflowExecutionState,
	ai *persistencespb.ActivityInfo,
	rule *rulespb.WorkflowRuleSpec,
) (bool, error) {
	// match visibility query
	visibilityQuery := rule.GetVisibilityQuery()
	if visibilityQuery != "" {
		match, err := matcher.MatchMutableState(executionInfo, executionState, rule.GetVisibilityQuery())
		if err != nil || !match {
			return false, err
		}
	}

	// match activity query
	activityTrigger := rule.GetActivityStart()
	if activityTrigger == nil {
		return false, nil
	}
	return matcher.MatchActivity(ai, activityTrigger.GetPredicate())
}
