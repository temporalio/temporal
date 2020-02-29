// Copyright (c) 2019 Uber Technologies, Inc.
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

	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"
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
			startEvent *commonproto.HistoryEvent,
		) error
		generateWorkflowCloseTasks(
			now time.Time,
		) error
		generateRecordWorkflowStartedTasks(
			now time.Time,
			startEvent *commonproto.HistoryEvent,
		) error
		generateDelayedDecisionTasks(
			now time.Time,
			startEvent *commonproto.HistoryEvent,
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
			event *commonproto.HistoryEvent,
		) error
		generateActivityRetryTasks(
			activityScheduleID int64,
		) error
		generateChildWorkflowTasks(
			now time.Time,
			event *commonproto.HistoryEvent,
		) error
		generateRequestCancelExternalTasks(
			now time.Time,
			event *commonproto.HistoryEvent,
		) error
		generateSignalExternalTasks(
			now time.Time,
			event *commonproto.HistoryEvent,
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
		domainCache cache.DomainCache
		logger      log.Logger

		mutableState mutableState
	}
)

const defaultWorkflowRetentionInDays int32 = 1

var _ mutableStateTaskGenerator = (*mutableStateTaskGeneratorImpl)(nil)

func newMutableStateTaskGenerator(
	domainCache cache.DomainCache,
	logger log.Logger,
	mutableState mutableState,
) *mutableStateTaskGeneratorImpl {

	return &mutableStateTaskGeneratorImpl{
		domainCache: domainCache,
		logger:      logger,

		mutableState: mutableState,
	}
}

func (r *mutableStateTaskGeneratorImpl) generateWorkflowStartTasks(
	now time.Time,
	startEvent *commonproto.HistoryEvent,
) error {

	attr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	firstDecisionDelayDuration := time.Duration(attr.GetFirstDecisionTaskBackoffSeconds()) * time.Second

	executionInfo := r.mutableState.GetExecutionInfo()
	startVersion := startEvent.GetVersion()

	workflowTimeoutDuration := time.Duration(executionInfo.WorkflowTimeout) * time.Second
	workflowTimeoutDuration = workflowTimeoutDuration + firstDecisionDelayDuration
	workflowTimeoutTimestamp := now.Add(workflowTimeoutDuration)
	if !executionInfo.ExpirationTime.IsZero() && workflowTimeoutTimestamp.After(executionInfo.ExpirationTime) {
		workflowTimeoutTimestamp = executionInfo.ExpirationTime
	}
	r.mutableState.AddTimerTasks(&persistence.WorkflowTimeoutTask{
		// TaskID is set by shard
		VisibilityTimestamp: workflowTimeoutTimestamp,
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
	domainEntry, err := r.domainCache.GetDomainByID(executionInfo.DomainID)
	switch err.(type) {
	case nil:
		retentionInDays = domainEntry.GetRetentionDays(executionInfo.WorkflowID)
	case *serviceerror.NotFound:
		// domain is not accessible, use default value above
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
	startEvent *commonproto.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()

	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	decisionBackoffDuration := time.Duration(startAttr.GetFirstDecisionTaskBackoffSeconds()) * time.Second
	executionTimestamp := now.Add(decisionBackoffDuration)

	var firstDecisionDelayType int
	switch startAttr.GetInitiator() {
	// noParentWorkflow case
	case enums.ContinueAsNewInitiatorNotSet:
		firstDecisionDelayType = persistence.WorkflowBackoffTimeoutTypeCron
	// continue as new case
	case enums.ContinueAsNewInitiatorRetryPolicy:
		firstDecisionDelayType = persistence.WorkflowBackoffTimeoutTypeRetry
	case enums.ContinueAsNewInitiatorCronSchedule:
		firstDecisionDelayType = persistence.WorkflowBackoffTimeoutTypeCron
	case enums.ContinueAsNewInitiatorDecider:
		return serviceerror.NewInternal("encounter continue as new iterator & first decision delay not 0")
	default:
		return serviceerror.NewInternal(fmt.Sprintf("unknown iterator retry policy: %v", startAttr.GetInitiator()))
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
	startEvent *commonproto.HistoryEvent,
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
		DomainID:            executionInfo.DomainID,
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
	event *commonproto.HistoryEvent,
) error {

	attr := event.GetActivityTaskScheduledEventAttributes()
	activityScheduleID := event.GetEventId()

	activityInfo, ok := r.mutableState.GetActivityInfo(activityScheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduleID))
	}

	var targetDomainID string
	var err error
	if activityInfo.DomainID != "" {
		targetDomainID = activityInfo.DomainID
	} else {
		// TODO remove this block after Mar, 1th, 2020
		//  previously, DomainID in activity info is not used, so need to get
		//  schedule event from DB checking whether activity to be scheduled
		//  belongs to this domain
		targetDomainID, err = r.getTargetDomainID(attr.GetDomain())
		if err != nil {
			return err
		}
	}

	r.mutableState.AddTransferTasks(&persistence.ActivityTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		DomainID:            targetDomainID,
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
	event *commonproto.HistoryEvent,
) error {

	attr := event.GetStartChildWorkflowExecutionInitiatedEventAttributes()
	childWorkflowScheduleID := event.GetEventId()
	childWorkflowTargetDomain := attr.GetDomain()

	childWorkflowInfo, ok := r.mutableState.GetChildExecutionInfo(childWorkflowScheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending child workflow: %v", childWorkflowScheduleID))
	}

	targetDomainID, err := r.getTargetDomainID(childWorkflowTargetDomain)
	if err != nil {
		return err
	}

	r.mutableState.AddTransferTasks(&persistence.StartChildExecutionTask{
		// TaskID is set by shard
		VisibilityTimestamp: now,
		TargetDomainID:      targetDomainID,
		TargetWorkflowID:    childWorkflowInfo.StartedWorkflowID,
		InitiatedID:         childWorkflowInfo.InitiatedID,
		Version:             childWorkflowInfo.Version,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateRequestCancelExternalTasks(
	now time.Time,
	event *commonproto.HistoryEvent,
) error {

	attr := event.GetRequestCancelExternalWorkflowExecutionInitiatedEventAttributes()
	scheduleID := event.GetEventId()
	version := event.GetVersion()
	targetDomainName := attr.GetDomain()
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	_, ok := r.mutableState.GetRequestCancelInfo(scheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending request cancel external workflow: %v", scheduleID))
	}

	targetDomainID, err := r.getTargetDomainID(targetDomainName)
	if err != nil {
		return err
	}

	r.mutableState.AddTransferTasks(&persistence.CancelExecutionTask{
		// TaskID is set by shard
		VisibilityTimestamp:     now,
		TargetDomainID:          targetDomainID,
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
	event *commonproto.HistoryEvent,
) error {

	attr := event.GetSignalExternalWorkflowExecutionInitiatedEventAttributes()
	scheduleID := event.GetEventId()
	version := event.GetVersion()
	targetDomainName := attr.GetDomain()
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	_, ok := r.mutableState.GetSignalInfo(scheduleID)
	if !ok {
		return serviceerror.NewInternal(fmt.Sprintf("it could be a bug, cannot get pending signal external workflow: %v", scheduleID))
	}

	targetDomainID, err := r.getTargetDomainID(targetDomainName)
	if err != nil {
		return err
	}

	r.mutableState.AddTransferTasks(&persistence.SignalExecutionTask{
		// TaskID is set by shard
		VisibilityTimestamp:     now,
		TargetDomainID:          targetDomainID,
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

func (r *mutableStateTaskGeneratorImpl) getTargetDomainID(
	targetDomainName string,
) (string, error) {

	targetDomainID := r.mutableState.GetExecutionInfo().DomainID
	if targetDomainName != "" {
		targetDomainEntry, err := r.domainCache.GetDomain(targetDomainName)
		if err != nil {
			return "", err
		}
		targetDomainID = targetDomainEntry.GetInfo().ID
	}

	return targetDomainID, nil
}
