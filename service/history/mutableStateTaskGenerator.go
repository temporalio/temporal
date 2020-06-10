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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mutableStateTaskGenerator_mock.go

package history

import (
	"fmt"
	"time"

	commonpb "go.temporal.io/temporal-proto/common"
	eventpb "go.temporal.io/temporal-proto/event"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	mutableStateTaskGenerator interface {
		generateWorkflowStartTasks(
			now time.Time,
			startEvent *eventpb.HistoryEvent,
		) error
		generateWorkflowCloseTasks(
			now time.Time,
		) error
		generateRecordWorkflowStartedTasks(
			now time.Time,
			startEvent *eventpb.HistoryEvent,
		) error
		generateDelayedDecisionTasks(
			now time.Time,
			startEvent *eventpb.HistoryEvent,
		) error
		generateDecisionScheduleTasks(
			now time.Time,
			decisionScheduleID int64,
		) error
		generateDecisionStartTasks(
			now time.Time,
			decisionScheduleID int64,
		) error
		generateActivityTransferTasks(
			now time.Time,
			event *eventpb.HistoryEvent,
		) error
		generateActivityRetryTasks(
			activityScheduleID int64,
		) error
		generateChildWorkflowTasks(
			now time.Time,
			event *eventpb.HistoryEvent,
		) error
		generateRequestCancelExternalTasks(
			now time.Time,
			event *eventpb.HistoryEvent,
		) error
		generateSignalExternalTasks(
			now time.Time,
			event *eventpb.HistoryEvent,
		) error
		generateWorkflowSearchAttrTasks(
			now time.Time,
		) error
		generateWorkflowResetTasks(
			now time.Time,
		) error

		// these 2 APIs should only be called when mutable state transaction is being closed
		generateActivityTimerTasks(
			now time.Time,
		) error
		generateUserTimerTasks(
			now time.Time,
		) error
	}

	mutableStateTaskGeneratorImpl struct {
		namespaceCache cache.NamespaceCache
		logger         log.Logger

		mutableState mutableState
	}
)

const defaultWorkflowRetentionInDays int32 = 1

var _ mutableStateTaskGenerator = (*mutableStateTaskGeneratorImpl)(nil)

func newMutableStateTaskGenerator(
	namespaceCache cache.NamespaceCache,
	logger log.Logger,
	mutableState mutableState,
) *mutableStateTaskGeneratorImpl {

	return &mutableStateTaskGeneratorImpl{
		namespaceCache: namespaceCache,
		logger:         logger,

		mutableState: mutableState,
	}
}

func (r *mutableStateTaskGeneratorImpl) generateWorkflowStartTasks(
	now time.Time,
	startEvent *eventpb.HistoryEvent,
) error {

	attr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	firstDecisionDelayDuration := time.Duration(attr.GetFirstDecisionTaskBackoffSeconds()) * time.Second

	executionInfo := r.mutableState.GetExecutionInfo()
	startVersion := startEvent.GetVersion()

	runTimeoutDuration := time.Duration(executionInfo.WorkflowRunTimeout) * time.Second
	runTimeoutDuration = runTimeoutDuration + firstDecisionDelayDuration
	workflowExpirationTimestamp := now.Add(runTimeoutDuration)
	if !executionInfo.WorkflowExpirationTime.IsZero() && workflowExpirationTimestamp.After(executionInfo.WorkflowExpirationTime) {
		workflowExpirationTimestamp = executionInfo.WorkflowExpirationTime
	}
	r.mutableState.AddTimerTasks(&persistence.WorkflowTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: workflowExpirationTimestamp,
		Version:             startVersion,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateWorkflowCloseTasks(
	now time.Time,
) error {

	currentVersion := r.mutableState.GetCurrentVersion()
	executionInfo := r.mutableState.GetExecutionInfo()

	r.mutableState.AddTransferTasks(&persistence.CloseExecutionTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		Version:             currentVersion,
	})

	retentionInDays := defaultWorkflowRetentionInDays
	namespaceEntry, err := r.namespaceCache.GetNamespaceByID(executionInfo.NamespaceID)
	switch err.(type) {
	case nil:
		retentionInDays = namespaceEntry.GetRetentionDays(executionInfo.WorkflowID)
	case *serviceerror.NotFound:
		// namespace is not accessible, use default value above
	default:
		return err
	}

	retentionDuration := time.Duration(retentionInDays) * time.Hour * 24
	r.mutableState.AddTimerTasks(&persistence.DeleteHistoryEventTask{
		// TaskID is set by shard
		VisibilityTimestamp: now.Add(retentionDuration),
		Version:             currentVersion,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateDelayedDecisionTasks(
	now time.Time,
	startEvent *eventpb.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()

	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	decisionBackoffDuration := time.Duration(startAttr.GetFirstDecisionTaskBackoffSeconds()) * time.Second
	executionTimestamp := now.Add(decisionBackoffDuration)

	var firstDecisionDelayType int
	switch startAttr.GetInitiator() {
	case commonpb.CONTINUE_AS_NEW_INITIATOR_RETRY:
		firstDecisionDelayType = persistence.WorkflowBackoffTimeoutTypeRetry
	case commonpb.CONTINUE_AS_NEW_INITIATOR_CRON_SCHEDULE,
		commonpb.CONTINUE_AS_NEW_INITIATOR_DECIDER:
		firstDecisionDelayType = persistence.WorkflowBackoffTimeoutTypeCron
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown initiator: %v", startAttr.GetInitiator()))
	}

	r.mutableState.AddTimerTasks(&persistence.WorkflowBackoffTimerTask{
		// TaskID is set by shard
		// TODO EventID seems not used at all
		VisibilityTimestamp: executionTimestamp,
		TimeoutType:         firstDecisionDelayType,
		Version:             startVersion,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateRecordWorkflowStartedTasks(
	now time.Time,
	startEvent *eventpb.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()

	r.mutableState.AddTransferTasks(&persistence.RecordWorkflowStartedTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		Version:             startVersion,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateDecisionScheduleTasks(
	now time.Time,
	decisionScheduleID int64,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()
	decision, ok := r.mutableState.GetDecisionInfo(
		decisionScheduleID,
	)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending decision: %v", decisionScheduleID))
	}

	r.mutableState.AddTransferTasks(&persistence.DecisionTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		NamespaceID:         executionInfo.NamespaceID,
		TaskList:            decision.TaskList,
		ScheduleID:          decision.ScheduleID,
		Version:             decision.Version,
	})

	if r.mutableState.IsStickyTaskListEnabled() {
		scheduledTime := time.Unix(0, decision.ScheduledTimestamp)
		scheduleToStartTimeout := time.Duration(
			r.mutableState.GetExecutionInfo().StickyScheduleToStartTimeout,
		) * time.Second

		r.mutableState.AddTimerTasks(&persistence.DecisionTimeoutTask{
			// TaskID is set by shard
			VisibilityTimestamp: scheduledTime.Add(scheduleToStartTimeout),
			TimeoutType:         int(timerTypeScheduleToStart),
			EventID:             decision.ScheduleID,
			ScheduleAttempt:     decision.Attempt,
			Version:             decision.Version,
		})
	}

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateDecisionStartTasks(
	_ time.Time,
	decisionScheduleID int64,
) error {

	decision, ok := r.mutableState.GetDecisionInfo(
		decisionScheduleID,
	)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending decision: %v", decisionScheduleID))
	}

	startedTime := time.Unix(0, decision.StartedTimestamp)
	startToCloseTimeout := time.Duration(
		decision.DecisionTimeout,
	) * time.Second

	r.mutableState.AddTimerTasks(&persistence.DecisionTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: startedTime.Add(startToCloseTimeout),
		TimeoutType:         int(timerTypeStartToClose),
		EventID:             decision.ScheduleID,
		ScheduleAttempt:     decision.Attempt,
		Version:             decision.Version,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateActivityTransferTasks(
	now time.Time,
	event *eventpb.HistoryEvent,
) error {

	attr := event.GetActivityTaskScheduledEventAttributes()
	activityScheduleID := event.GetEventId()

	activityInfo, ok := r.mutableState.GetActivityInfo(activityScheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduleID))
	}

	var targetNamespaceID string
	var err error
	if activityInfo.NamespaceID != "" {
		targetNamespaceID = activityInfo.NamespaceID
	} else {
		// TODO remove this block after Mar, 1th, 2020
		//  previously, NamespaceID in activity info is not used, so need to get
		//  schedule event from DB checking whether activity to be scheduled
		//  belongs to this namespace
		targetNamespaceID, err = r.getTargetNamespaceID(attr.GetNamespace())
		if err != nil {
			return err
		}
	}

	r.mutableState.AddTransferTasks(&persistence.ActivityTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		NamespaceID:         targetNamespaceID,
		TaskList:            activityInfo.TaskList,
		ScheduleID:          activityInfo.ScheduleID,
		Version:             activityInfo.Version,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateActivityRetryTasks(
	activityScheduleID int64,
) error {

	ai, ok := r.mutableState.GetActivityInfo(activityScheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduleID))
	}

	r.mutableState.AddTimerTasks(&persistence.ActivityRetryTimerTask{
		// TaskID is set by shard
		Version:             ai.Version,
		VisibilityTimestamp: ai.ScheduledTime,
		EventID:             ai.ScheduleID,
		Attempt:             ai.Attempt,
	})
	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateChildWorkflowTasks(
	now time.Time,
	event *eventpb.HistoryEvent,
) error {

	attr := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	childWorkflowScheduleID := event.GetEventId()
	childWorkflowTargetNamespace := attr.GetNamespace()

	childWorkflowInfo, ok := r.mutableState.GetChildExecutionInfo(childWorkflowScheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending child workflow: %v", childWorkflowScheduleID))
	}

	targetNamespaceID, err := r.getTargetNamespaceID(childWorkflowTargetNamespace)
	if err != nil {
		return err
	}

	r.mutableState.AddTransferTasks(&persistence.StartChildExecutionTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		TargetNamespaceID:   targetNamespaceID,
		TargetWorkflowID:    childWorkflowInfo.StartedWorkflowID,
		InitiatedID:         childWorkflowInfo.InitiatedID,
		Version:             childWorkflowInfo.Version,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateRequestCancelExternalTasks(
	now time.Time,
	event *eventpb.HistoryEvent,
) error {

	attr := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()
	scheduleID := event.GetEventId()
	version := event.GetVersion()
	targetNamespace := attr.GetNamespace()
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	_, ok := r.mutableState.GetRequestCancelInfo(scheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending request cancel external workflow: %v", scheduleID))
	}

	targetNamespaceID, err := r.getTargetNamespaceID(targetNamespace)
	if err != nil {
		return err
	}

	r.mutableState.AddTransferTasks(&persistence.CancelExecutionTask{
		// TaskID is set by shard
		VisibilityTimestamp:     now,
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildOnly,
		InitiatedID:             scheduleID,
		Version:                 version,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateSignalExternalTasks(
	now time.Time,
	event *eventpb.HistoryEvent,
) error {

	attr := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()
	scheduleID := event.GetEventId()
	version := event.GetVersion()
	targetNamespace := attr.GetNamespace()
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	_, ok := r.mutableState.GetSignalInfo(scheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending signal external workflow: %v", scheduleID))
	}

	targetNamespaceID, err := r.getTargetNamespaceID(targetNamespace)
	if err != nil {
		return err
	}

	r.mutableState.AddTransferTasks(&persistence.SignalExecutionTask{
		// TaskID is set by shard
		VisibilityTimestamp:     now,
		TargetNamespaceID:       targetNamespaceID,
		TargetWorkflowID:        targetWorkflowID,
		TargetRunID:             targetRunID,
		TargetChildWorkflowOnly: targetChildOnly,
		InitiatedID:             scheduleID,
		Version:                 version,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateWorkflowSearchAttrTasks(
	now time.Time,
) error {

	currentVersion := r.mutableState.GetCurrentVersion()

	r.mutableState.AddTransferTasks(&persistence.UpsertWorkflowSearchAttributesTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		Version:             currentVersion, // task processing does not check this version
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateWorkflowResetTasks(
	now time.Time,
) error {

	currentVersion := r.mutableState.GetCurrentVersion()

	r.mutableState.AddTransferTasks(&persistence.ResetWorkflowTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		Version:             currentVersion,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateActivityTimerTasks(
	now time.Time,
) error {

	_, err := r.getTimerSequence(now).createNextActivityTimer()
	return err
}

func (r *mutableStateTaskGeneratorImpl) generateUserTimerTasks(
	now time.Time,
) error {

	_, err := r.getTimerSequence(now).createNextUserTimer()
	return err
}

func (r *mutableStateTaskGeneratorImpl) getTimerSequence(now time.Time) timerSequence {
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	return newTimerSequence(timeSource, r.mutableState)
}

func (r *mutableStateTaskGeneratorImpl) getTargetNamespaceID(
	targetNamespace string,
) (string, error) {

	targetNamespaceID := r.mutableState.GetExecutionInfo().NamespaceID
	if targetNamespace != "" {
		targetNamespaceEntry, err := r.namespaceCache.GetNamespace(targetNamespace)
		if err != nil {
			return "", err
		}
		targetNamespaceID = targetNamespaceEntry.GetInfo().Id
	}

	return targetNamespaceID, nil
}
