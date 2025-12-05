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

	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
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

func UpdateActivityInfoForRetries(
	ai *persistencespb.ActivityInfo,
	version int64,
	attempt int32,
	failure *failurepb.Failure,
	nextScheduledTime *timestamppb.Timestamp,
	isActivityRetryStampIncrementEnabled bool,
) *persistencespb.ActivityInfo {
	previousAttempt := ai.Attempt
	ai.Attempt = attempt
	ai.Version = version
	ai.ScheduledTime = nextScheduledTime
	ai.StartedEventId = common.EmptyEventID
	ai.StartVersion = common.EmptyVersion
	ai.RequestId = ""
	ai.StartedTime = nil
	ai.TimerTaskStatus = TimerTaskStatusNone
	ai.RetryLastWorkerIdentity = ai.StartedIdentity
	ai.RetryLastFailure = failure
	// this flag means the user resets the activity with "--reset-heartbeat" flag
	// server sends heartbeat details to the worker with the new activity attempt
	// if the current attempt was still running - worker can still send new heartbeats, and even complete the activity
	// so for the current activity attempt server continue to accept the heartbeats, but reset it for the new attempt
	if ai.ResetHeartbeats {
		ai.LastHeartbeatDetails = nil
		ai.LastHeartbeatUpdateTime = nil
	}
	ai.ActivityReset = false
	ai.ResetHeartbeats = false

	if isActivityRetryStampIncrementEnabled && attempt > previousAttempt {
		ai.Stamp++
	}

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
		LastDeploymentVersion:       ai.LastDeploymentVersion,
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
				interval := backoff.ExponentialBackoffAlgorithm(ai.RetryInitialInterval, ai.RetryBackoffCoefficient, p.Attempt)
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

	if ai.Paused {
		// adjust activity state for paused activities
		if p.State == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			// this state means activity is not running on the worker
			// if activity is paused on server and not running on worker - mark it as PAUSED
			p.State = enumspb.PENDING_ACTIVITY_STATE_PAUSED
		} else if p.State == enumspb.PENDING_ACTIVITY_STATE_STARTED {
			// this state means activity is running on the worker
			// if activity is paused on server, but still running on worker - mark it as PAUSE_REQUESTED
			p.State = enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED
		} // if state is CANCEL_REQUESTEd - it is not modified

		// fill activity pause info
		if ai.PauseInfo != nil {
			p.PauseInfo = &workflowpb.PendingActivityInfo_PauseInfo{
				PauseTime: ai.PauseInfo.PauseTime,
			}
			if ai.PauseInfo.GetManual() != nil {
				p.PauseInfo.PausedBy = &workflowpb.PendingActivityInfo_PauseInfo_Manual_{
					Manual: &workflowpb.PendingActivityInfo_PauseInfo_Manual{
						Identity: ai.PauseInfo.GetManual().Identity,
						Reason:   ai.PauseInfo.GetManual().Reason,
					},
				}
			} else {
				ruleId := ai.PauseInfo.GetRuleId()
				p.PauseInfo.PausedBy = &workflowpb.PendingActivityInfo_PauseInfo_Rule_{
					Rule: &workflowpb.PendingActivityInfo_PauseInfo_Rule{
						RuleId: ruleId,
					},
				}
				rule, ok := mutableState.GetNamespaceEntry().GetWorkflowRule(ruleId)
				if ok {
					p.PauseInfo.PausedBy.(*workflowpb.PendingActivityInfo_PauseInfo_Rule_).Rule.Identity = rule.CreatedByIdentity
					p.PauseInfo.PausedBy.(*workflowpb.PendingActivityInfo_PauseInfo_Rule_).Rule.Reason = rule.Description
				}
			}
		}
	}

	p.ActivityOptions = &activitypb.ActivityOptions{
		TaskQueue: &taskqueuepb.TaskQueue{
			// we may need to return sticky task queue name here
			Name:       ai.TaskQueue,
			NormalName: ai.TaskQueue,
		},
		ScheduleToCloseTimeout: ai.ScheduleToCloseTimeout,
		ScheduleToStartTimeout: ai.ScheduleToStartTimeout,
		StartToCloseTimeout:    ai.StartToCloseTimeout,
		HeartbeatTimeout:       ai.HeartbeatTimeout,
		Priority:               ai.Priority,

		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    ai.RetryInitialInterval,
			BackoffCoefficient: ai.RetryBackoffCoefficient,
			MaximumInterval:    ai.RetryMaximumInterval,
			MaximumAttempts:    ai.RetryMaximumAttempts,
		},
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
		interval := backoff.ExponentialBackoffAlgorithm(ai.RetryInitialInterval, ai.RetryBackoffCoefficient, ai.Attempt)

		if ai.RetryMaximumInterval.AsDuration() != 0 && (interval <= 0 || interval > ai.RetryMaximumInterval.AsDuration()) {
			interval = ai.RetryMaximumInterval.AsDuration()
		}

		if interval > 0 {
			nextScheduledTime = ai.LastAttemptCompleteTime.AsTime().Add(interval)
		}
	}
	return nextScheduledTime
}

func PauseActivity(
	mutableState historyi.MutableState,
	activityId string,
	pauseInfo *persistencespb.ActivityInfo_PauseInfo,
) error {
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
		activityInfo.PauseInfo = pauseInfo
		return nil
	})
}

func ResetActivity(
	ctx context.Context,
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	activityId string,
	resetHeartbeats bool,
	keepPaused bool,
	resetOptions bool,
	jitter time.Duration,
) error {
	if !mutableState.IsWorkflowExecutionRunning() {
		return consts.ErrWorkflowCompleted
	}
	ai, activityFound := mutableState.GetActivityByActivityID(activityId)

	if !activityFound {
		return consts.ErrActivityNotFound
	}

	var originalOptions *historypb.ActivityTaskScheduledEventAttributes
	if resetOptions {
		event, err := mutableState.GetActivityScheduledEvent(ctx, ai.ScheduledEventId)
		if err != nil {
			return serviceerror.NewInvalidArgumentf("ActivityTaskScheduledEvent not found, %v", err)
		}
		attrs, ok := event.Attributes.(*historypb.HistoryEvent_ActivityTaskScheduledEventAttributes)
		if !ok {
			return serviceerror.NewInvalidArgument("ActivityTaskScheduledEvent is invalid")
		}
		if attrs == nil || attrs.ActivityTaskScheduledEventAttributes == nil {
			return serviceerror.NewInvalidArgument("ActivityTaskScheduledEvent is incomplete")
		}

		originalOptions = attrs.ActivityTaskScheduledEventAttributes
	}

	return mutableState.UpdateActivity(ai.ScheduledEventId, func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
		// reset the number of attempts
		activityInfo.Attempt = 1
		activityInfo.ActivityReset = true
		if resetHeartbeats {
			activityInfo.ResetHeartbeats = true
		}

		if resetOptions {
			// update activity info with new options
			activityInfo.TaskQueue = originalOptions.TaskQueue.Name
			activityInfo.ScheduleToCloseTimeout = originalOptions.ScheduleToCloseTimeout
			activityInfo.ScheduleToStartTimeout = originalOptions.ScheduleToStartTimeout
			activityInfo.StartToCloseTimeout = originalOptions.StartToCloseTimeout
			activityInfo.HeartbeatTimeout = originalOptions.HeartbeatTimeout
			activityInfo.RetryMaximumInterval = originalOptions.RetryPolicy.MaximumInterval
			activityInfo.RetryBackoffCoefficient = originalOptions.RetryPolicy.BackoffCoefficient
			activityInfo.RetryInitialInterval = originalOptions.RetryPolicy.InitialInterval
			activityInfo.RetryMaximumAttempts = originalOptions.RetryPolicy.MaximumAttempts

			// move forward activity version
			activityInfo.Stamp++

			// invalidate timers
			activityInfo.TimerTaskStatus = TimerTaskStatusNone
		}

		// if activity is running, or it is paused and we don't want to unpause - we don't need to do anything
		if GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_STARTED || (ai.Paused && keepPaused) {
			return nil
		}

		activityInfo.Stamp++
		if activityInfo.Paused && !keepPaused {
			activityInfo.Paused = false
		}

		// if activity is not running - we need to regenerate the retry task as schedule activity immediately
		if GetActivityState(activityInfo) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			// we reset heartbeat was requested we also should reset heartbeat details and timer
			if resetHeartbeats {
				ai.LastHeartbeatDetails = nil
				ai.LastHeartbeatUpdateTime = nil
			}

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

func unpauseActivityInfo(ai *persistencespb.ActivityInfo) {
	ai.Paused = false
	ai.PauseInfo = nil
	ai.Stamp++

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
		unpauseActivityInfo(activityInfo)

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
		unpauseActivityInfo(activityInfo)

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
		unpauseActivityInfo(activityInfo)

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
