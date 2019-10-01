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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
)

type (
	mutableStateTaskGenerator interface {
		generateWorkflowStartTasks(
			now time.Time,
			startEvent *shared.HistoryEvent,
		) error
		generateWorkflowCloseTasks(
			now time.Time,
		) error
		generateRecordWorkflowStartedTasks(
			now time.Time,
			startEvent *shared.HistoryEvent,
		) error
		generateDelayedDecisionTasks(
			now time.Time,
			startEvent *shared.HistoryEvent,
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
			event *shared.HistoryEvent,
		) error
		generateActivityRetryTasks(
			activityScheduleID int64,
		) error
		generateChildWorkflowTasks(
			now time.Time,
			event *shared.HistoryEvent,
		) error
		generateRequestCancelExternalTasks(
			now time.Time,
			event *shared.HistoryEvent,
		) error
		generateSignalExternalTasks(
			now time.Time,
			event *shared.HistoryEvent,
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
	startEvent *shared.HistoryEvent,
) error {

	attr := startEvent.WorkflowExecutionStartedEventAttributes
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
	case *shared.EntityNotExistsError:
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
	startEvent *shared.HistoryEvent,
) error {

	startVersion := startEvent.GetVersion()

	startAttr := startEvent.WorkflowExecutionStartedEventAttributes
	decisionBackoffDuration := time.Duration(startAttr.GetFirstDecisionTaskBackoffSeconds()) * time.Second
	executionTimestamp := now.Add(decisionBackoffDuration)

	// noParentWorkflow case
	firstDecisionDelayType := persistence.WorkflowBackoffTimeoutTypeCron
	// continue as new case
	if startAttr.Initiator != nil {
		switch startAttr.GetInitiator() {
		case shared.ContinueAsNewInitiatorRetryPolicy:
			firstDecisionDelayType = persistence.WorkflowBackoffTimeoutTypeRetry
		case shared.ContinueAsNewInitiatorCronSchedule:
			firstDecisionDelayType = persistence.WorkflowBackoffTimeoutTypeCron
		case shared.ContinueAsNewInitiatorDecider:
			return &shared.InternalServiceError{
				Message: "encounter continue as new iterator & first decision delay not 0",
			}
		default:
			return &shared.InternalServiceError{
				Message: fmt.Sprintf("unknown iterator retry policy: %v", startAttr.GetInitiator()),
			}
		}
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
	startEvent *shared.HistoryEvent,
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
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("it could be a bug, cannot get pending decision: %v", decisionScheduleID),
		}
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
		timerTask := r.getTimerBuilder(now).AddScheduleToStartDecisionTimoutTask(
			decision.ScheduleID,
			decision.Attempt,
			executionInfo.StickyScheduleToStartTimeout,
		)
		timerTask.Version = decision.Version
		r.mutableState.AddTimerTasks(timerTask)
	}

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateDecisionStartTasks(
	now time.Time,
	decisionScheduleID int64,
) error {

	decision, ok := r.mutableState.GetDecisionInfo(
		decisionScheduleID,
	)
	if !ok {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("it could be a bug, cannot get pending decision: %v", decisionScheduleID),
		}
	}

	timerTask := r.getTimerBuilder(now).AddStartToCloseDecisionTimoutTask(
		decision.ScheduleID,
		decision.Attempt,
		decision.DecisionTimeout,
	)
	timerTask.Version = decision.Version
	r.mutableState.AddTimerTasks(timerTask)

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateActivityTransferTasks(
	now time.Time,
	event *shared.HistoryEvent,
) error {

	attr := event.ActivityTaskScheduledEventAttributes
	activityScheduleID := event.GetEventId()
	activityTargetDomain := attr.GetDomain()

	activityInfo, ok := r.mutableState.GetActivityInfo(activityScheduleID)
	if !ok {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduleID),
		}
	}

	targetDomainID, err := r.getTargetDomainID(activityTargetDomain)
	if err != nil {
		return err
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
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("it could be a bug, cannot get pending activity: %v", activityScheduleID),
		}
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
	event *shared.HistoryEvent,
) error {

	attr := event.StartChildWorkflowExecutionInitiatedEventAttributes
	childWorkflowScheduleID := event.GetEventId()
	childWorkflowTargetDomain := attr.GetDomain()

	childWorkflowInfo, ok := r.mutableState.GetChildExecutionInfo(childWorkflowScheduleID)
	if !ok {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("it could be a bug, cannot get pending child workflow: %v", childWorkflowScheduleID),
		}
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
	event *shared.HistoryEvent,
) error {

	attr := event.RequestCancelExternalWorkflowExecutionInitiatedEventAttributes
	scheduleID := event.GetEventId()
	targetDomainName := attr.GetDomain()
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	requestCancelExternalInfo, ok := r.mutableState.GetRequestCancelInfo(scheduleID)
	if !ok {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("it could be a bug, cannot get pending request cancel external workflow: %v", scheduleID),
		}
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
		Version:                 requestCancelExternalInfo.Version,
	})

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateSignalExternalTasks(
	now time.Time,
	event *shared.HistoryEvent,
) error {

	attr := event.SignalExternalWorkflowExecutionInitiatedEventAttributes
	scheduleID := event.GetEventId()
	targetDomainName := attr.GetDomain()
	targetWorkflowID := attr.GetWorkflowExecution().GetWorkflowId()
	targetRunID := attr.GetWorkflowExecution().GetRunId()
	targetChildOnly := attr.GetChildWorkflowOnly()

	signalExternalInfo, ok := r.mutableState.GetSignalInfo(scheduleID)
	if !ok {
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("it could be a bug, cannot get pending signal external workflow: %v", scheduleID),
		}
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
		Version:                 signalExternalInfo.Version,
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
		Version:             currentVersion,
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

	if timerTask := r.getTimerBuilder(now).GetActivityTimerTaskIfNeeded(
		r.mutableState,
	); timerTask != nil {
		// no need to set the version, since activity timer task
		// is just a trigger to check all activities
		r.mutableState.AddTimerTasks(timerTask)
	}

	return nil
}

func (r *mutableStateTaskGeneratorImpl) generateUserTimerTasks(
	now time.Time,
) error {

	if timerTask := r.getTimerBuilder(now).GetUserTimerTaskIfNeeded(
		r.mutableState,
	); timerTask != nil {
		// no need to set the version, since user timer task
		// is just a trigger to check all timers
		r.mutableState.AddTimerTasks(timerTask)
	}

	return nil
}

func (r *mutableStateTaskGeneratorImpl) getTimerBuilder(now time.Time) *timerBuilder {
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	return newTimerBuilder(timeSource)
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
