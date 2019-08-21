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

package history

import (
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/persistence"
)

var emptyTasks = []persistence.Task{}

type (
	mutableStateTaskRefresher interface {
		refreshTasks(now time.Time) error
	}

	mutableStateTaskRefresherImpl struct {
		domainCache cache.DomainCache
		eventsCache eventsCache
		logger      log.Logger

		mutableState  mutableState
		taskGenerator mutableStateTaskGenerator
	}
)

func newMutableStateTaskRefresher(
	domainCache cache.DomainCache,
	eventsCache eventsCache,
	logger log.Logger,
	mutableState mutableState,
) *mutableStateTaskRefresherImpl {

	return &mutableStateTaskRefresherImpl{
		domainCache: domainCache,
		eventsCache: eventsCache,
		logger:      logger,

		mutableState: mutableState,
		taskGenerator: newMutableStateTaskGenerator(
			domainCache,
			logger,
			mutableState,
		),
	}
}

func (r *mutableStateTaskRefresherImpl) refreshTasks(now time.Time) error {

	if err := r.refreshTasksForWorkflowStart(now); err != nil {
		return err
	}

	if err := r.refreshTasksForWorkflowClose(now); err != nil {
		return err
	}

	if err := r.refreshTasksForRecordWorkflowStarted(now); err != nil {
		return err
	}

	if err := r.refreshTasksForDecision(now); err != nil {
		return err
	}

	if err := r.refreshTasksForActivity(now); err != nil {
		return err
	}

	if err := r.refreshTasksForTimer(now); err != nil {
		return err
	}

	if err := r.refreshTasksForChildWorkflow(now); err != nil {
		return err
	}

	if err := r.refreshTasksForRequestCancelExternalWorkflow(now); err != nil {
		return err
	}

	if err := r.refreshTasksForSignalExternalWorkflow(now); err != nil {
		return err
	}

	if err := r.refreshTasksForWorkflowSearchAttr(now); err != nil {
		return err
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForWorkflowStart(
	now time.Time,
) error {

	startEvent, ok := r.mutableState.GetStartEvent()
	if !ok {
		return &shared.InternalServiceError{Message: "unable to load start event."}
	}

	if err := r.taskGenerator.generateWorkflowStartTasks(
		now,
		startEvent,
	); err != nil {
		return err
	}

	startAttr := startEvent.WorkflowExecutionStartedEventAttributes
	if !r.mutableState.HasProcessedOrPendingDecision() && startAttr.GetFirstDecisionTaskBackoffSeconds() > 0 {
		if err := r.taskGenerator.generateDelayedDecisionTasks(
			now,
			startEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForWorkflowClose(
	now time.Time,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()

	if executionInfo.CloseStatus != persistence.WorkflowCloseStatusNone {
		return r.taskGenerator.generateWorkflowCloseTasks(
			now,
		)
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForRecordWorkflowStarted(
	now time.Time,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()

	if executionInfo.CloseStatus == persistence.WorkflowCloseStatusNone {
		return r.taskGenerator.generateRecordWorkflowStartedTasks(
			now,
		)
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForDecision(
	now time.Time,
) error {

	if !r.mutableState.HasPendingDecision() {
		// no decision task at all
		return nil
	}

	decision, ok := r.mutableState.GetPendingDecision()
	if !ok {
		return &shared.InternalServiceError{Message: "it could be a bug, cannot get pending decision"}
	}

	// decision already started
	if decision.StartedID != common.EmptyEventID {
		return r.taskGenerator.generateDecisionStartTasks(
			now,
			decision.ScheduleID,
		)
	}

	// decision only scheduled
	return r.taskGenerator.generateDecisionScheduleTasks(
		now,
		decision.ScheduleID,
	)
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForActivity(
	now time.Time,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()
	pendingActivityInfos := r.mutableState.GetPendingActivityInfos()

Loop:
	for _, activityInfo := range pendingActivityInfos {
		// clear all activity timer task mask for later activity timer task re-generation
		activityInfo.TimerTaskStatus = TimerTaskStatusNone

		if activityInfo.StartedID != common.EmptyEventID {
			continue Loop
		}

		scheduleEvent, err := r.eventsCache.getEvent(
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			activityInfo.ScheduledEventBatchID,
			activityInfo.ScheduleID,
			r.mutableState.GetEventStoreVersion(),
			r.mutableState.GetCurrentBranch(),
		)
		if err != nil {
			return err
		}

		if err := r.taskGenerator.generateActivityTransferTasks(
			now,
			scheduleEvent,
		); err != nil {
			return err
		}
	}

	tBuilder := newTimerBuilder(r.logger, r.getTimeSource(now))
	if timerTask := tBuilder.GetActivityTimerTaskIfNeeded(
		r.mutableState,
	); timerTask != nil {
		// no need to set the version, since activity timer task
		// is just a trigger to check all activities
		r.mutableState.AddTimerTasks(timerTask)
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForTimer(
	now time.Time,
) error {

	pendingTimerInfos := r.mutableState.GetPendingTimerInfos()

	for _, timerInfo := range pendingTimerInfos {
		// clear all timer task mask for later timer task re-generation
		timerInfo.TaskID = TimerTaskStatusNone
	}

	tBuilder := newTimerBuilder(r.logger, r.getTimeSource(now))
	if timerTask := tBuilder.GetUserTimerTaskIfNeeded(
		r.mutableState,
	); timerTask != nil {
		// no need to set the version, since activity timer task
		// is just a trigger to check all activities
		r.mutableState.AddTimerTasks(timerTask)
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForChildWorkflow(
	now time.Time,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()
	pendingChildWorkflowInfos := r.mutableState.GetPendingChildExecutionInfos()

Loop:
	for _, childWorkflowInfo := range pendingChildWorkflowInfos {
		if childWorkflowInfo.StartedID != common.EmptyEventID {
			continue Loop
		}

		scheduleEvent, err := r.eventsCache.getEvent(
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			childWorkflowInfo.InitiatedEventBatchID,
			childWorkflowInfo.InitiatedID,
			r.mutableState.GetEventStoreVersion(),
			r.mutableState.GetCurrentBranch(),
		)
		if err != nil {
			return err
		}

		if err := r.taskGenerator.generateChildWorkflowTasks(
			now,
			scheduleEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForRequestCancelExternalWorkflow(
	now time.Time,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()
	pendingRequestCancelInfos := r.mutableState.GetPendingRequestCancelExternalInfos()

	for _, requestCancelInfo := range pendingRequestCancelInfos {
		initiateEvent, err := r.eventsCache.getEvent(
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			requestCancelInfo.InitiatedEventBatchID,
			requestCancelInfo.InitiatedID,
			r.mutableState.GetEventStoreVersion(),
			r.mutableState.GetCurrentBranch(),
		)
		if err != nil {
			return err
		}

		if err := r.taskGenerator.generateRequestCancelExternalTasks(
			now,
			initiateEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForSignalExternalWorkflow(
	now time.Time,
) error {

	executionInfo := r.mutableState.GetExecutionInfo()
	pendingSignalInfos := r.mutableState.GetPendingSignalExtrenalInfos()

	for _, signalInfo := range pendingSignalInfos {
		initiateEvent, err := r.eventsCache.getEvent(
			executionInfo.DomainID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			signalInfo.InitiatedEventBatchID,
			signalInfo.InitiatedID,
			r.mutableState.GetEventStoreVersion(),
			r.mutableState.GetCurrentBranch(),
		)
		if err != nil {
			return err
		}

		if err := r.taskGenerator.generateSignalExternalTasks(
			now,
			initiateEvent,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForWorkflowSearchAttr(
	now time.Time,
) error {

	return r.taskGenerator.generateWorkflowSearchAttrTasks(
		now,
	)
}

func (r *mutableStateTaskRefresherImpl) getTimeSource(
	now time.Time,
) clock.TimeSource {

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(now)
	return timeSource
}
