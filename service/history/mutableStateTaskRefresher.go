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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination mutableStateTaskRefresher_mock.go

package history

import (
	"time"

	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/clock"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/persistence"
)

var emptyTasks = []persistence.Task{}

type (
	mutableStateTaskRefresher interface {
		refreshTasks(now time.Time, mutableState mutableState) error
	}

	mutableStateTaskRefresherImpl struct {
		config         *Config
		namespaceCache cache.NamespaceCache
		eventsCache    eventsCache
		logger         log.Logger
	}
)

func newMutableStateTaskRefresher(
	config *Config,
	namespaceCache cache.NamespaceCache,
	eventsCache eventsCache,
	logger log.Logger,
) *mutableStateTaskRefresherImpl {

	return &mutableStateTaskRefresherImpl{
		config:         config,
		namespaceCache: namespaceCache,
		eventsCache:    eventsCache,
		logger:         logger,
	}
}

func (r *mutableStateTaskRefresherImpl) refreshTasks(
	now time.Time,
	mutableState mutableState,
) error {

	taskGenerator := newMutableStateTaskGenerator(
		r.namespaceCache,
		r.logger,
		mutableState,
	)

	if err := r.refreshTasksForWorkflowStart(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForWorkflowClose(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForRecordWorkflowStarted(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForDecision(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForActivity(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForTimer(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForChildWorkflow(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForRequestCancelExternalWorkflow(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if err := r.refreshTasksForSignalExternalWorkflow(
		now,
		mutableState,
		taskGenerator,
	); err != nil {
		return err
	}

	if r.config.AdvancedVisibilityWritingMode() != common.AdvancedVisibilityWritingModeOff {
		if err := r.refreshTasksForWorkflowSearchAttr(
			now,
			mutableState,
			taskGenerator,
		); err != nil {
			return err
		}
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForWorkflowStart(
	now time.Time,
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}

	if err := taskGenerator.generateWorkflowStartTasks(
		now,
		startEvent,
	); err != nil {
		return err
	}

	startAttr := startEvent.GetWorkflowExecutionStartedEventAttributes()
	if !mutableState.HasProcessedOrPendingDecision() && startAttr.GetFirstDecisionTaskBackoffSeconds() > 0 {
		if err := taskGenerator.generateDelayedDecisionTasks(
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
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()

	if executionInfo.CloseStatus != persistence.WorkflowCloseStatusRunning {
		return taskGenerator.generateWorkflowCloseTasks(
			now,
		)
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForRecordWorkflowStarted(
	now time.Time,
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	startEvent, err := mutableState.GetStartEvent()
	if err != nil {
		return err
	}

	executionInfo := mutableState.GetExecutionInfo()

	if executionInfo.CloseStatus == persistence.WorkflowCloseStatusRunning {
		return taskGenerator.generateRecordWorkflowStartedTasks(
			now,
			startEvent,
		)
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForDecision(
	now time.Time,
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	if !mutableState.HasPendingDecision() {
		// no decision task at all
		return nil
	}

	decision, ok := mutableState.GetPendingDecision()
	if !ok {
		return serviceerror.NewInternal("it could be a bug, cannot get pending decision")
	}

	// decision already started
	if decision.StartedID != common.EmptyEventID {
		return taskGenerator.generateDecisionStartTasks(
			now,
			decision.ScheduleID,
		)
	}

	// decision only scheduled
	return taskGenerator.generateDecisionScheduleTasks(
		now,
		decision.ScheduleID,
	)
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForActivity(
	now time.Time,
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	pendingActivityInfos := mutableState.GetPendingActivityInfos()

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

Loop:
	for _, activityInfo := range pendingActivityInfos {
		// clear all activity timer task mask for later activity timer task re-generation
		activityInfo.TimerTaskStatus = timerTaskStatusNone

		// need to update activity timer task mask for which task is generated
		if err := mutableState.UpdateActivity(
			activityInfo,
		); err != nil {
			return err
		}

		if activityInfo.StartedID != common.EmptyEventID {
			continue Loop
		}

		scheduleEvent, err := r.eventsCache.getEvent(
			executionInfo.NamespaceID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			activityInfo.ScheduledEventBatchID,
			activityInfo.ScheduleID,
			currentBranchToken,
		)
		if err != nil {
			return err
		}

		if err := taskGenerator.generateActivityTransferTasks(
			now,
			scheduleEvent,
		); err != nil {
			return err
		}
	}

	if _, err := newTimerSequence(
		r.getTimeSource(now),
		mutableState,
	).createNextActivityTimer(); err != nil {
		return err
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForTimer(
	now time.Time,
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	pendingTimerInfos := mutableState.GetPendingTimerInfos()

	for _, timerInfo := range pendingTimerInfos {
		// clear all timer task mask for later timer task re-generation
		timerInfo.TaskStatus = timerTaskStatusNone

		// need to update user timer task mask for which task is generated
		if err := mutableState.UpdateUserTimer(
			timerInfo,
		); err != nil {
			return err
		}
	}

	if _, err := newTimerSequence(
		r.getTimeSource(now),
		mutableState,
	).createNextUserTimer(); err != nil {
		return err
	}

	return nil
}

func (r *mutableStateTaskRefresherImpl) refreshTasksForChildWorkflow(
	now time.Time,
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	pendingChildWorkflowInfos := mutableState.GetPendingChildExecutionInfos()

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

Loop:
	for _, childWorkflowInfo := range pendingChildWorkflowInfos {
		if childWorkflowInfo.StartedID != common.EmptyEventID {
			continue Loop
		}

		scheduleEvent, err := r.eventsCache.getEvent(
			executionInfo.NamespaceID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			childWorkflowInfo.InitiatedEventBatchID,
			childWorkflowInfo.InitiatedID,
			currentBranchToken,
		)
		if err != nil {
			return err
		}

		if err := taskGenerator.generateChildWorkflowTasks(
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
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	pendingRequestCancelInfos := mutableState.GetPendingRequestCancelExternalInfos()

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	for _, requestCancelInfo := range pendingRequestCancelInfos {
		initiateEvent, err := r.eventsCache.getEvent(
			executionInfo.NamespaceID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			requestCancelInfo.GetInitiatedEventBatchId(),
			requestCancelInfo.GetInitiatedId(),
			currentBranchToken,
		)
		if err != nil {
			return err
		}

		if err := taskGenerator.generateRequestCancelExternalTasks(
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
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	executionInfo := mutableState.GetExecutionInfo()
	pendingSignalInfos := mutableState.GetPendingSignalExternalInfos()

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	for _, signalInfo := range pendingSignalInfos {
		initiateEvent, err := r.eventsCache.getEvent(
			executionInfo.NamespaceID,
			executionInfo.WorkflowID,
			executionInfo.RunID,
			signalInfo.GetInitiatedEventBatchId(),
			signalInfo.GetInitiatedId(),
			currentBranchToken,
		)
		if err != nil {
			return err
		}

		if err := taskGenerator.generateSignalExternalTasks(
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
	mutableState mutableState,
	taskGenerator mutableStateTaskGenerator,
) error {

	return taskGenerator.generateWorkflowSearchAttrTasks(
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
