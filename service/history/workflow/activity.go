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
	if ai.GetCancelRequested() {
		return enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	}
	if ai.GetStartedEventId() != common.EmptyEventID {
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
) {
	previousAttempt := ai.GetAttempt()
	ai.SetAttempt(attempt)
	ai.SetVersion(version)
	ai.SetScheduledTime(nextScheduledTime)
	ai.SetStartedEventId(common.EmptyEventID)
	ai.SetStartVersion(common.EmptyVersion)
	ai.SetRequestId("")
	ai.ClearStartedTime()
	// Mark per-attempt timers for recreation.
	ai.SetTimerTaskStatus(ai.GetTimerTaskStatus() &^ (TimerTaskStatusCreatedHeartbeat | TimerTaskStatusCreatedStartToClose | TimerTaskStatusCreatedScheduleToStart))
	ai.SetRetryLastWorkerIdentity(ai.GetStartedIdentity())
	ai.SetRetryLastFailure(failure)
	// this flag means the user resets the activity with "--reset-heartbeat" flag
	// server sends heartbeat details to the worker with the new activity attempt
	// if the current attempt was still running - worker can still send new heartbeats, and even complete the activity
	// so for the current activity attempt server continue to accept the heartbeats, but reset it for the new attempt
	if ai.GetResetHeartbeats() {
		ai.ClearLastHeartbeatDetails()
		ai.ClearLastHeartbeatUpdateTime()
	}
	ai.SetActivityReset(false)
	ai.SetResetHeartbeats(false)

	if isActivityRetryStampIncrementEnabled && attempt > previousAttempt {
		ai.SetStamp(ai.GetStamp() + 1)
	}
}

func GetPendingActivityInfo(
	ctx context.Context, // only used as a passthrough to GetActivityType
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	ai *persistencespb.ActivityInfo,
) (*workflowpb.PendingActivityInfo, error) {
	now := shardContext.GetTimeSource().Now().UTC()

	p := workflowpb.PendingActivityInfo_builder{
		ActivityId:                  ai.GetActivityId(),
		LastWorkerDeploymentVersion: ai.GetLastWorkerDeploymentVersion(),
		LastDeploymentVersion:       ai.GetLastDeploymentVersion(),
		Priority:                    ai.GetPriority(),
	}.Build()
	if ai.GetUseWorkflowBuildIdInfo() != nil {
		p.SetUseWorkflowBuildId(&emptypb.Empty{})
	} else if ai.GetLastIndependentlyAssignedBuildId() != "" {
		p.SetLastIndependentlyAssignedBuildId(ai.GetLastIndependentlyAssignedBuildId())
	}

	p.SetState(GetActivityState(ai))

	p.SetLastAttemptCompleteTime(ai.GetLastAttemptCompleteTime())
	if !ai.GetHasRetryPolicy() {
		p.ClearNextAttemptScheduleTime()
	} else {
		p.SetAttempt(ai.GetAttempt())
		if p.GetState() == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			scheduledTime := ai.GetScheduledTime().AsTime()
			if now.Before(scheduledTime) {
				// in this case activity is waiting for a retry
				p.SetNextAttemptScheduleTime(ai.GetScheduledTime())
				currentRetryDuration := p.GetNextAttemptScheduleTime().AsTime().Sub(p.GetLastAttemptCompleteTime().AsTime())
				p.SetCurrentRetryInterval(durationpb.New(currentRetryDuration))
			} else {
				// in this case activity is at least scheduled
				p.ClearNextAttemptScheduleTime()
				// we rely on the fact that ExponentialBackoffAlgorithm is deterministic, and  there's no random jitter
				interval := backoff.ExponentialBackoffAlgorithm(ai.GetRetryInitialInterval(), ai.GetRetryBackoffCoefficient(), p.GetAttempt())
				p.SetCurrentRetryInterval(durationpb.New(interval))
			}
		}
	}
	p.SetAttempt(max(p.GetAttempt(), 1))
	p.SetPaused(ai.GetPaused())

	if ai.HasLastHeartbeatUpdateTime() && !ai.GetLastHeartbeatUpdateTime().AsTime().IsZero() {
		p.SetLastHeartbeatTime(ai.GetLastHeartbeatUpdateTime())
		p.SetHeartbeatDetails(ai.GetLastHeartbeatDetails())
	}
	activityType, err := mutableState.GetActivityType(ctx, ai)
	if err != nil {
		return nil, err
	}
	p.SetActivityType(activityType)
	p.SetScheduledTime(ai.GetScheduledTime())
	p.SetLastStartedTime(ai.GetStartedTime())
	p.SetLastWorkerIdentity(ai.GetStartedIdentity())
	if ai.GetHasRetryPolicy() {
		p.SetExpirationTime(ai.GetRetryExpirationTime())
		p.SetMaximumAttempts(ai.GetRetryMaximumAttempts())
		p.SetLastFailure(ai.GetRetryLastFailure())
		if p.GetLastWorkerIdentity() == "" && ai.GetRetryLastWorkerIdentity() != "" {
			p.SetLastWorkerIdentity(ai.GetRetryLastWorkerIdentity())
		}
	}

	if ai.GetPaused() {
		// adjust activity state for paused activities
		if p.GetState() == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			// this state means activity is not running on the worker
			// if activity is paused on server and not running on worker - mark it as PAUSED
			p.SetState(enumspb.PENDING_ACTIVITY_STATE_PAUSED)
		} else if p.GetState() == enumspb.PENDING_ACTIVITY_STATE_STARTED {
			// this state means activity is running on the worker
			// if activity is paused on server, but still running on worker - mark it as PAUSE_REQUESTED
			p.SetState(enumspb.PENDING_ACTIVITY_STATE_PAUSE_REQUESTED)
		} // if state is CANCEL_REQUESTEd - it is not modified

		// fill activity pause info
		if ai.HasPauseInfo() {
			p.SetPauseInfo(workflowpb.PendingActivityInfo_PauseInfo_builder{
				PauseTime: ai.GetPauseInfo().GetPauseTime(),
			}.Build())
			if ai.GetPauseInfo().GetManual() != nil {
				p.GetPauseInfo().SetManual(workflowpb.PendingActivityInfo_PauseInfo_Manual_builder{
					Identity: ai.GetPauseInfo().GetManual().GetIdentity(),
					Reason:   ai.GetPauseInfo().GetManual().GetReason(),
				}.Build())
			} else {
				ruleId := ai.GetPauseInfo().GetRuleId()
				ruleInfo := workflowpb.PendingActivityInfo_PauseInfo_Rule_builder{
					RuleId: ruleId,
				}.Build()
				rule, ok := mutableState.GetNamespaceEntry().GetWorkflowRule(ruleId)
				if ok {
					ruleInfo.SetIdentity(rule.GetCreatedByIdentity())
					ruleInfo.SetReason(rule.GetDescription())
				}
				p.GetPauseInfo().SetRule(ruleInfo)
			}
		}
	}

	p.SetActivityOptions(activitypb.ActivityOptions_builder{
		TaskQueue: taskqueuepb.TaskQueue_builder{
			// we may need to return sticky task queue name here
			Name:       ai.GetTaskQueue(),
			NormalName: ai.GetTaskQueue(),
		}.Build(),
		ScheduleToCloseTimeout: ai.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout: ai.GetScheduleToStartTimeout(),
		StartToCloseTimeout:    ai.GetStartToCloseTimeout(),
		HeartbeatTimeout:       ai.GetHeartbeatTimeout(),
		Priority:               ai.GetPriority(),

		RetryPolicy: commonpb.RetryPolicy_builder{
			InitialInterval:    ai.GetRetryInitialInterval(),
			BackoffCoefficient: ai.GetRetryBackoffCoefficient(),
			MaximumInterval:    ai.GetRetryMaximumInterval(),
			MaximumAttempts:    ai.GetRetryMaximumAttempts(),
		}.Build(),
	}.Build())

	return p, nil
}

func GetNextScheduledTime(ai *persistencespb.ActivityInfo) time.Time {
	// there are two possible cases:
	// * this is the first time activity was scheduled
	//  * in this case we should use current schedule time
	// * this is a retry
	//  * next scheduled time will be calculated, based on the retry policy and last time when activity was completed

	nextScheduledTime := ai.GetScheduledTime().AsTime()
	if ai.GetAttempt() > 1 {
		// calculate new schedule time
		interval := backoff.ExponentialBackoffAlgorithm(ai.GetRetryInitialInterval(), ai.GetRetryBackoffCoefficient(), ai.GetAttempt())

		if ai.GetRetryMaximumInterval().AsDuration() != 0 && (interval <= 0 || interval > ai.GetRetryMaximumInterval().AsDuration()) {
			interval = ai.GetRetryMaximumInterval().AsDuration()
		}

		if interval > 0 {
			nextScheduledTime = ai.GetLastAttemptCompleteTime().AsTime().Add(interval)
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

	if ai.GetPaused() {
		// do nothing
		return nil
	}

	return mutableState.UpdateActivity(ai.GetScheduledEventId(), func(activityInfo *persistencespb.ActivityInfo, _ historyi.MutableState) error {
		// note - we are not increasing the stamp of the activity if it is running.
		// this is because if activity is actually running we should let it finish
		if GetActivityState(activityInfo) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			activityInfo.SetStamp(activityInfo.GetStamp() + 1)
		}
		activityInfo.SetPaused(true)
		activityInfo.SetPauseInfo(pauseInfo)
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
		event, err := mutableState.GetActivityScheduledEvent(ctx, ai.GetScheduledEventId())
		if err != nil {
			return serviceerror.NewInvalidArgumentf("ActivityTaskScheduledEvent not found, %v", err)
		}
		if event.WhichAttributes() != historypb.HistoryEvent_ActivityTaskScheduledEventAttributes_case {
			return serviceerror.NewInvalidArgument("ActivityTaskScheduledEvent is invalid")
		}
		attrs := event.GetActivityTaskScheduledEventAttributes()
		if attrs == nil {
			return serviceerror.NewInvalidArgument("ActivityTaskScheduledEvent is incomplete")
		}

		originalOptions = attrs
	}

	return mutableState.UpdateActivity(ai.GetScheduledEventId(), func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
		// reset the number of attempts
		activityInfo.SetAttempt(1)
		activityInfo.SetActivityReset(true)
		if resetHeartbeats {
			activityInfo.SetResetHeartbeats(true)
		}

		if resetOptions {
			// update activity info with new options
			activityInfo.SetTaskQueue(originalOptions.GetTaskQueue().GetName())
			activityInfo.SetScheduleToCloseTimeout(originalOptions.GetScheduleToCloseTimeout())
			activityInfo.SetScheduleToStartTimeout(originalOptions.GetScheduleToStartTimeout())
			activityInfo.SetStartToCloseTimeout(originalOptions.GetStartToCloseTimeout())
			activityInfo.SetHeartbeatTimeout(originalOptions.GetHeartbeatTimeout())
			activityInfo.SetRetryMaximumInterval(originalOptions.GetRetryPolicy().GetMaximumInterval())
			activityInfo.SetRetryBackoffCoefficient(originalOptions.GetRetryPolicy().GetBackoffCoefficient())
			activityInfo.SetRetryInitialInterval(originalOptions.GetRetryPolicy().GetInitialInterval())
			activityInfo.SetRetryMaximumAttempts(originalOptions.GetRetryPolicy().GetMaximumAttempts())

			// move forward activity version
			activityInfo.SetStamp(activityInfo.GetStamp() + 1)

			// invalidate timers
			activityInfo.SetTimerTaskStatus(TimerTaskStatusNone)
		}

		// if activity is running, or it is paused and we don't want to unpause - we don't need to do anything
		if GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_STARTED || (ai.GetPaused() && keepPaused) {
			return nil
		}

		activityInfo.SetStamp(activityInfo.GetStamp() + 1)
		if activityInfo.GetPaused() && !keepPaused {
			activityInfo.SetPaused(false)
		}

		// if activity is not running - we need to regenerate the retry task as schedule activity immediately
		if GetActivityState(activityInfo) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
			// we reset heartbeat was requested we also should reset heartbeat details and timer
			if resetHeartbeats {
				ai.ClearLastHeartbeatDetails()
				ai.ClearLastHeartbeatUpdateTime()
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
	ai.SetPaused(false)
	ai.ClearPauseInfo()
	ai.SetStamp(ai.GetStamp() + 1)

}

func UnpauseActivity(
	shardContext historyi.ShardContext,
	mutableState historyi.MutableState,
	ai *persistencespb.ActivityInfo,
	resetAttempts bool,
	resetHeartbeat bool,
	jitter time.Duration,
) error {
	if err := mutableState.UpdateActivity(ai.GetScheduledEventId(), func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
		unpauseActivityInfo(activityInfo)

		if resetAttempts {
			activityInfo.SetAttempt(1)
		}
		if resetHeartbeat {
			activityInfo.ClearLastHeartbeatDetails()
			activityInfo.ClearLastHeartbeatUpdateTime()
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

	if err := mutableState.UpdateActivity(ai.GetScheduledEventId(), func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
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
	if err := mutableState.UpdateActivity(ai.GetScheduledEventId(), func(activityInfo *persistencespb.ActivityInfo, ms historyi.MutableState) error {
		unpauseActivityInfo(activityInfo)

		// reset the number of attempts
		activityInfo.SetAttempt(1)

		if needRegenerateRetryTask(activityInfo, scheduleNewRun) {
			if err := regenerateActivityRetryTask(activityInfo, scheduleNewRun, jitter, ms, shardContext); err != nil {
				return err
			}
		}

		if resetHeartbeats {
			activityInfo.ClearLastHeartbeatDetails()
			activityInfo.ClearLastHeartbeatUpdateTime()
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
	scheduleTime := activityInfo.GetScheduledTime().AsTime()
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
	if ai.GetPaused() {
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
