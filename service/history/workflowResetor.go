// Copyright (c) 2017 Uber Technologies, Inc.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflowResetor_mock.go

package history

import (
	"context"
	"fmt"
	"time"

	"github.com/pborman/uuid"

	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	ce "github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/persistence"
)

type (
	// Deprecated: use workflowResetter instead when NDC is applies to all workflows
	workflowResetor interface {
		ResetWorkflowExecution(
			ctx context.Context,
			resetRequest *workflow.ResetWorkflowExecutionRequest,
			baseContext workflowExecutionContext,
			baseMutableState mutableState,
			currContext workflowExecutionContext,
			currMutableState mutableState,
		) (response *workflow.ResetWorkflowExecutionResponse, retError error)

		ApplyResetEvent(
			ctx context.Context,
			request *h.ReplicateEventsRequest,
			domainID, workflowID, currentRunID string,
		) (retError error)
	}

	workflowResetorImpl struct {
		eng        *historyEngineImpl
		timeSource clock.TimeSource
	}
)

var _ workflowResetor = (*workflowResetorImpl)(nil)

func newWorkflowResetor(historyEngine *historyEngineImpl) *workflowResetorImpl {
	return &workflowResetorImpl{
		eng:        historyEngine,
		timeSource: historyEngine.shard.GetTimeSource(),
	}
}

// ResetWorkflowExecution only allows resetting to decisionTaskCompleted, but exclude that batch of decisionTaskCompleted/decisionTaskFailed/decisionTaskTimeout.
// It will then fail the decision with cause of "reset_workflow"
func (w *workflowResetorImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *workflow.ResetWorkflowExecutionRequest,
	baseContext workflowExecutionContext,
	baseMutableState mutableState,
	currContext workflowExecutionContext,
	currMutableState mutableState,
) (*workflow.ResetWorkflowExecutionResponse, error) {

	domainEntry, retError := w.eng.shard.GetDomainCache().GetDomain(request.GetDomain())
	if retError != nil {
		return nil, retError
	}

	resetNewRunID := uuid.New()
	response := &workflow.ResetWorkflowExecutionResponse{
		RunId: common.StringPtr(resetNewRunID),
	}

	// before changing mutable state
	currPrevRunVersion, err := currMutableState.GetLastWriteVersion()
	if err != nil {
		return nil, err
	}
	// terminate the current run if it is running
	currTerminated, currCloseTask, currCleanupTask, retError := w.terminateIfCurrIsRunning(currMutableState, request.GetReason())
	if retError != nil {
		return response, retError
	}

	retError = w.validateResetWorkflowBeforeReplay(baseMutableState, currMutableState)
	if retError != nil {
		return response, retError
	}

	newMutableState, newHistorySize, newTransferTasks, newTimerTasks, retError := w.buildNewMutableStateForReset(
		ctx, domainEntry, baseMutableState, currMutableState,
		request.GetReason(), request.GetDecisionFinishEventId(), request.GetRequestId(), resetNewRunID,
	)
	if retError != nil {
		return response, retError
	}

	retError = w.checkDomainStatus(newMutableState, currPrevRunVersion, domainEntry.GetInfo().Name)
	if retError != nil {
		return response, retError
	}

	// update replication and generate replication task
	currReplicationTasks, newReplicationTasks, err := w.generateReplicationTasksForReset(
		currTerminated, currMutableState, newMutableState, domainEntry,
	)
	if err != nil {
		return nil, err
	}

	// finally, write to persistence
	retError = currContext.resetWorkflowExecution(
		currMutableState, currTerminated, currCloseTask, currCleanupTask,
		newMutableState, newHistorySize, newTransferTasks, newTimerTasks,
		currReplicationTasks, newReplicationTasks, baseMutableState.GetExecutionInfo().RunID,
		baseMutableState.GetNextEventID(),
	)

	return response, retError
}

func (w *workflowResetorImpl) checkDomainStatus(newMutableState mutableState, prevRunVersion int64, domain string) error {
	if newMutableState.GetReplicationState() != nil || newMutableState.GetVersionHistories() != nil {
		clusterMetadata := w.eng.shard.GetService().GetClusterMetadata()
		currentVersion := newMutableState.GetCurrentVersion()
		if currentVersion < prevRunVersion {
			return ce.NewDomainNotActiveError(
				domain,
				clusterMetadata.GetCurrentClusterName(),
				clusterMetadata.ClusterNameForFailoverVersion(prevRunVersion),
			)
		}
		activeCluster := clusterMetadata.ClusterNameForFailoverVersion(currentVersion)
		currentCluster := clusterMetadata.GetCurrentClusterName()
		if activeCluster != currentCluster {
			return ce.NewDomainNotActiveError(domain, currentCluster, activeCluster)
		}
	}
	return nil
}

func (w *workflowResetorImpl) validateResetWorkflowBeforeReplay(baseMutableState, currMutableState mutableState) error {
	if len(currMutableState.GetPendingChildExecutionInfos()) > 0 {
		return &workflow.BadRequestError{
			Message: fmt.Sprintf("reset is not allowed when current workflow has pending child workflow."),
		}
	}
	if currMutableState.IsWorkflowExecutionRunning() {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("current workflow should already been terminated"),
		}
	}
	return nil
}

func (w *workflowResetorImpl) validateResetWorkflowAfterReplay(newMutableState mutableState) error {
	if retError := newMutableState.CheckResettable(); retError != nil {
		return retError
	}
	if !newMutableState.HasInFlightDecision() {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("can't find the last started decision"),
		}
	}
	if newMutableState.HasBufferedEvents() {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("replay history shouldn't see any bufferred events"),
		}
	}
	if newMutableState.IsStickyTaskListEnabled() {
		return &workflow.InternalServiceError{
			Message: fmt.Sprintf("replay history shouldn't have stikyness"),
		}
	}
	return nil
}

// Fail the started activities
func (w *workflowResetorImpl) failStartedActivities(msBuilder mutableState) error {
	for _, ai := range msBuilder.GetPendingActivityInfos() {
		if ai.StartedID != common.EmptyEventID {
			// this means the activity has started but not completed, we need to fail the activity
			request := getRespondActivityTaskFailedRequestFromActivity(ai, "workflowReset")
			if _, err := msBuilder.AddActivityTaskFailedEvent(ai.ScheduleID, ai.StartedID, request); err != nil {
				// Unable to add ActivityTaskFailed event to history
				return &workflow.InternalServiceError{Message: "Unable to add ActivityTaskFailed event to mutableState."}
			}
		}
	}
	return nil
}

// Generate new transfer tasks to re-schedule task for scheduled(not started) activities.
// NOTE 1: activities with retry may have started but don't have the start event, we also re-schedule it)
// NOTE 2: ignore requestCancel/childWFs/singalExternal for now).
func (w *workflowResetorImpl) scheduleUnstartedActivities(msBuilder mutableState) ([]persistence.Task, error) {
	var tasks []persistence.Task
	exeInfo := msBuilder.GetExecutionInfo()
	// activities
	for _, ai := range msBuilder.GetPendingActivityInfos() {
		if ai.StartedID != common.EmptyEventID {
			return nil, &workflow.InternalServiceError{Message: "started activities should have been failed."}
		}
		t := &persistence.ActivityTask{
			DomainID:   exeInfo.DomainID,
			TaskList:   exeInfo.TaskList,
			ScheduleID: ai.ScheduleID,
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// TODO: @shreyassrivatsan reduce number of return parameters
func (w *workflowResetorImpl) buildNewMutableStateForReset(
	ctx context.Context,
	domainEntry *cache.DomainCacheEntry,
	baseMutableState, currMutableState mutableState,
	resetReason string,
	resetDecisionCompletedEventID int64,
	requestedID, newRunID string,
) (newMutableState mutableState, newHistorySize int64, newTransferTasks, newTimerTasks []persistence.Task, retError error) {

	domainID := baseMutableState.GetExecutionInfo().DomainID
	workflowID := baseMutableState.GetExecutionInfo().WorkflowID
	baseRunID := baseMutableState.GetExecutionInfo().RunID

	// replay history to reset point(exclusive) to rebuild mutableState
	forkEventVersion, wfTimeoutSecs, receivedSignals, continueRunID, newStateBuilder, historySize, retError := w.replayHistoryEvents(
		domainEntry, resetDecisionCompletedEventID, requestedID, baseMutableState, newRunID,
	)
	if retError != nil {
		return
	}
	newMutableState = newStateBuilder.(*stateBuilderImpl).mutableState

	// before this, the mutable state is in replay mode
	// need to close / flush the mutable state for new changes
	_, _, retError = newMutableState.CloseTransactionAsSnapshot(
		w.timeSource.Now(),
		transactionPolicyPassive,
	)
	if retError != nil {
		return
	}

	newHistorySize = historySize

	retError = w.validateResetWorkflowAfterReplay(newMutableState)
	if retError != nil {
		return
	}

	// set the new mutable state with the version in domain
	if retError = newMutableState.UpdateCurrentVersion(
		domainEntry.GetFailoverVersion(),
		false,
	); retError != nil {
		return
	}

	// failed the in-flight decision(started).
	// Note that we need to ensure DecisionTaskFailed event is appended right after DecisionTaskStarted event
	decision, _ := newMutableState.GetInFlightDecision()

	_, err := newMutableState.AddDecisionTaskFailedEvent(decision.ScheduleID, decision.StartedID, workflow.DecisionTaskFailedCauseResetWorkflow, nil,
		identityHistoryService, resetReason, "", baseRunID, newRunID, forkEventVersion)
	if err != nil {
		retError = &workflow.InternalServiceError{Message: "Failed to add decision failed event."}
		return
	}

	retError = w.failStartedActivities(newMutableState)
	if retError != nil {
		return
	}

	newTransferTasks, retError = w.scheduleUnstartedActivities(newMutableState)
	if retError != nil {
		return
	}

	// we will need a timer for the scheduled activities
	needActivityTimer := len(newTransferTasks) > 0

	// generate new timer tasks: we need 4 timers:
	// 1. WF timeout,
	// 2. user timers for timers started but not fired by reset
	// 3. activity timeout for scheduled but not started activities
	newTimerTasks, retError = w.generateTimerTasksForReset(newMutableState, wfTimeoutSecs, needActivityTimer)
	if retError != nil {
		return
	}
	// replay received signals back to mutableState/history:
	retError = w.replayReceivedSignals(ctx, receivedSignals, continueRunID, newMutableState, currMutableState)
	if retError != nil {
		return
	}

	// we always schedule a new decision after reset
	decision, err = newMutableState.AddDecisionTaskScheduledEvent(false)
	if err != nil {
		retError = &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
		return
	}

	// TODO workflow reset logic should use built-in task management
	newTransferTasks = append(newTransferTasks,
		&persistence.DecisionTask{
			DomainID:   domainID,
			TaskList:   decision.TaskList,
			ScheduleID: decision.ScheduleID,
		},
		&persistence.RecordWorkflowStartedTask{},
	)

	// fork a new history branch
	baseBranchToken, err := baseMutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, 0, nil, nil, err
	}
	forkResp, retError := w.eng.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      resetDecisionCompletedEventID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(domainID, workflowID, newRunID),
		ShardID:         common.IntPtr(w.eng.shard.GetShardID()),
	})
	if retError != nil {
		return
	}

	retError = newMutableState.SetCurrentBranchToken(forkResp.NewBranchToken)
	return
}

func (w *workflowResetorImpl) terminateIfCurrIsRunning(
	currMutableState mutableState,
	reason string,
) (terminateCurr bool, closeTask, cleanupTask persistence.Task, retError error) {

	if currMutableState.IsWorkflowExecutionRunning() {
		terminateCurr = true
		eventBatchFirstEventID := currMutableState.GetNextEventID()
		if _, retError = currMutableState.AddWorkflowExecutionTerminatedEvent(
			eventBatchFirstEventID,
			reason,
			nil,
			identityHistoryService,
		); retError != nil {
			return
		}
		closeTask, cleanupTask, retError = getWorkflowCleanupTasks(
			w.eng.shard.GetDomainCache(),
			currMutableState.GetExecutionInfo().DomainID,
			currMutableState.GetExecutionInfo().WorkflowID,
		)
		if retError != nil {
			return
		}
	}
	return
}

func (w *workflowResetorImpl) setEventIDsWithHistory(msBuilder mutableState) (int64, error) {
	history := msBuilder.GetHistoryBuilder().GetHistory().Events
	firstEvent := history[0]
	lastEvent := history[len(history)-1]
	msBuilder.GetExecutionInfo().SetLastFirstEventID(firstEvent.GetEventId())
	if msBuilder.GetReplicationState() != nil {
		msBuilder.UpdateReplicationStateLastEventID(lastEvent.GetVersion(), lastEvent.GetEventId())
	} else if msBuilder.GetVersionHistories() != nil {
		currentVersionHistory, err := msBuilder.GetVersionHistories().GetCurrentVersionHistory()
		if err != nil {
			return 0, err
		}
		if err := currentVersionHistory.AddOrUpdateItem(persistence.NewVersionHistoryItem(
			lastEvent.GetEventId(), lastEvent.GetVersion(),
		)); err != nil {
			return 0, err
		}
	}
	return firstEvent.GetEventId(), nil
}

func (w *workflowResetorImpl) generateReplicationTasksForReset(
	terminateCurr bool,
	currMutableState, newMutableState mutableState,
	domainEntry *cache.DomainCacheEntry,
) ([]persistence.Task, []persistence.Task, error) {
	var currRepTasks, insertRepTasks []persistence.Task
	if newMutableState.GetReplicationState() != nil || newMutableState.GetVersionHistories() != nil {
		if terminateCurr {
			// we will generate 2 replication tasks for this case
			firstEventIDForCurr, err := w.setEventIDsWithHistory(currMutableState)
			if err != nil {
				return nil, nil, err
			}
			if domainEntry.GetReplicationPolicy() == cache.ReplicationPolicyMultiCluster {
				currentBranchToken, err := currMutableState.GetCurrentBranchToken()
				if err != nil {
					return nil, nil, err
				}
				replicationTask := &persistence.HistoryReplicationTask{
					Version:      currMutableState.GetCurrentVersion(),
					FirstEventID: firstEventIDForCurr,
					NextEventID:  currMutableState.GetNextEventID(),
					BranchToken:  currentBranchToken,
				}
				if currMutableState.GetReplicationState() != nil {
					replicationTask.LastReplicationInfo = currMutableState.GetReplicationState().LastReplicationInfo
				} else if currMutableState.GetVersionHistories() != nil {
					replicationTask.LastReplicationInfo = nil
				}
				currRepTasks = append(currRepTasks, replicationTask)
			}
		}
		firstEventIDForNew, err := w.setEventIDsWithHistory(newMutableState)
		if err != nil {
			return nil, nil, err
		}
		if domainEntry.GetReplicationPolicy() == cache.ReplicationPolicyMultiCluster {
			newBranchToken, err := newMutableState.GetCurrentBranchToken()
			if err != nil {
				return nil, nil, err
			}
			replicationTask := &persistence.HistoryReplicationTask{
				Version:       newMutableState.GetCurrentVersion(),
				ResetWorkflow: true,
				FirstEventID:  firstEventIDForNew,
				NextEventID:   newMutableState.GetNextEventID(),
				BranchToken:   newBranchToken,
			}
			if newMutableState.GetReplicationState() != nil {
				replicationTask.LastReplicationInfo = newMutableState.GetReplicationState().LastReplicationInfo
			} else if newMutableState.GetVersionHistories() != nil {
				replicationTask.LastReplicationInfo = nil
			}
			insertRepTasks = append(insertRepTasks, replicationTask)
		}
	}
	return currRepTasks, insertRepTasks, nil
}

// replay signals in the base run, and also signals in all the runs along the chain of contineAsNew
func (w *workflowResetorImpl) replayReceivedSignals(
	ctx context.Context,
	receivedSignals []*workflow.HistoryEvent,
	continueRunID string,
	newMutableState, currMutableState mutableState,
) error {
	for _, se := range receivedSignals {
		sigReq := &workflow.SignalWorkflowExecutionRequest{
			SignalName: se.GetWorkflowExecutionSignaledEventAttributes().SignalName,
			Identity:   se.GetWorkflowExecutionSignaledEventAttributes().Identity,
			Input:      se.GetWorkflowExecutionSignaledEventAttributes().Input,
		}
		newMutableState.AddWorkflowExecutionSignaled( //nolint:errcheck
			sigReq.GetSignalName(), sigReq.GetInput(), sigReq.GetIdentity())
	}
	for {
		if len(continueRunID) == 0 {
			break
		}
		var continueMutableState mutableState
		if continueRunID == currMutableState.GetExecutionInfo().RunID {
			continueMutableState = currMutableState
		} else {
			continueExe := workflow.WorkflowExecution{
				WorkflowId: common.StringPtr(newMutableState.GetExecutionInfo().WorkflowID),
				RunId:      common.StringPtr(continueRunID),
			}
			continueContext, continueRelease, err := w.eng.historyCache.getOrCreateWorkflowExecution(ctx, newMutableState.GetExecutionInfo().DomainID, continueExe)
			if err != nil {
				return err
			}
			continueMutableState, err = continueContext.loadWorkflowExecution()
			if err != nil {
				return err
			}
			continueRelease(nil)
		}
		continueRunID = ""

		var nextPageToken []byte
		continueBranchToken, err := continueMutableState.GetCurrentBranchToken()
		if err != nil {
			return err
		}
		readReq := &persistence.ReadHistoryBranchRequest{
			BranchToken: continueBranchToken,
			MinEventID:  common.FirstEventID,
			// NOTE: read through history to the end so that we can collect all the received signals
			MaxEventID:    continueMutableState.GetNextEventID(),
			PageSize:      defaultHistoryPageSize,
			NextPageToken: nextPageToken,
			ShardID:       common.IntPtr(w.eng.shard.GetShardID()),
		}
		for {
			var readResp *persistence.ReadHistoryBranchByBatchResponse
			readResp, err := w.eng.historyV2Mgr.ReadHistoryBranchByBatch(readReq)
			if err != nil {
				return err
			}
			for _, batch := range readResp.History {
				for _, e := range batch.Events {
					if e.GetEventType() == workflow.EventTypeWorkflowExecutionSignaled {
						sigReq := &workflow.SignalWorkflowExecutionRequest{
							SignalName: e.GetWorkflowExecutionSignaledEventAttributes().SignalName,
							Identity:   e.GetWorkflowExecutionSignaledEventAttributes().Identity,
							Input:      e.GetWorkflowExecutionSignaledEventAttributes().Input,
						}
						newMutableState.AddWorkflowExecutionSignaled( //nolint:errcheck
							sigReq.GetSignalName(), sigReq.GetInput(), sigReq.GetIdentity())
					} else if e.GetEventType() == workflow.EventTypeWorkflowExecutionContinuedAsNew {
						attr := e.GetWorkflowExecutionContinuedAsNewEventAttributes()
						continueRunID = attr.GetNewExecutionRunId()
					}
				}
			}
			if len(readResp.NextPageToken) > 0 {
				readReq.NextPageToken = readResp.NextPageToken
			} else {
				break
			}
		}
	}
	return nil
}

func (w *workflowResetorImpl) generateTimerTasksForReset(
	msBuilder mutableState,
	wfTimeoutSecs int64,
	needActivityTimer bool,
) ([]persistence.Task, error) {
	timerTasks := []persistence.Task{}

	// WF timeout task
	duration := time.Duration(wfTimeoutSecs) * time.Second
	wfTimeoutTask := &persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: w.eng.shard.GetTimeSource().Now().Add(duration),
	}
	timerTasks = append(timerTasks, wfTimeoutTask)

	timerSequence := newTimerSequence(clock.NewRealTimeSource(), msBuilder)
	// user timer task
	if len(msBuilder.GetPendingTimerInfos()) > 0 {
		for _, timerInfo := range msBuilder.GetPendingTimerInfos() {
			timerInfo.TaskStatus = timerTaskStatusNone
			if err := msBuilder.UpdateUserTimer(timerInfo); err != nil {
				return nil, err
			}
		}
		if _, err := timerSequence.createNextUserTimer(); err != nil {
			return nil, err
		}
		// temporary hack to make the logic as is
		// this workflow resetor should be deleted once 2DC is deprecated
		timerTasks = append(timerTasks, msBuilder.(*mutableStateBuilder).insertTimerTasks...)
		msBuilder.(*mutableStateBuilder).insertTimerTasks = nil
	}

	// activity timer
	if needActivityTimer {
		for _, activityInfo := range msBuilder.GetPendingActivityInfos() {
			activityInfo.TimerTaskStatus = timerTaskStatusNone
			if err := msBuilder.UpdateActivity(activityInfo); err != nil {
				return nil, err
			}
		}
		if _, err := timerSequence.createNextActivityTimer(); err != nil {
			return nil, err
		}
		// temporary hack to make the logic as is
		// this workflow resetor should be deleted once 2DC is deprecated
		timerTasks = append(timerTasks, msBuilder.(*mutableStateBuilder).insertTimerTasks...)
		msBuilder.(*mutableStateBuilder).insertTimerTasks = nil
	}

	return timerTasks, nil
}

func getRespondActivityTaskFailedRequestFromActivity(ai *persistence.ActivityInfo, resetReason string) *workflow.RespondActivityTaskFailedRequest {
	return &workflow.RespondActivityTaskFailedRequest{
		Reason:   common.StringPtr(resetReason),
		Details:  ai.Details,
		Identity: common.StringPtr(ai.StartedIdentity),
	}
}

// TODO: @shreyassrivatsan reduce the number of return parameters from this method or return a struct
func (w *workflowResetorImpl) replayHistoryEvents(
	domainEntry *cache.DomainCacheEntry,
	decisionFinishEventID int64,
	requestID string,
	prevMutableState mutableState,
	newRunID string,
) (forkEventVersion, wfTimeoutSecs int64, receivedSignalsAfterReset []*workflow.HistoryEvent, continueRunID string, sBuilder stateBuilder, newHistorySize int64, retError error) {

	prevExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(prevMutableState.GetExecutionInfo().WorkflowID),
		RunId:      common.StringPtr(prevMutableState.GetExecutionInfo().RunID),
	}
	domainID := prevMutableState.GetExecutionInfo().DomainID
	var nextPageToken []byte
	prevBranchToken, err := prevMutableState.GetCurrentBranchToken()
	if err != nil {
		return 0, 0, nil, "", nil, 0, err
	}
	readReq := &persistence.ReadHistoryBranchRequest{
		BranchToken: prevBranchToken,
		MinEventID:  common.FirstEventID,
		// NOTE: read through history to the end so that we can keep the received signals
		MaxEventID:    prevMutableState.GetNextEventID(),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(w.eng.shard.GetShardID()),
	}
	var resetMutableState *mutableStateBuilder
	var lastBatch []*workflow.HistoryEvent

	for {
		var readResp *persistence.ReadHistoryBranchByBatchResponse
		readResp, retError = w.eng.historyV2Mgr.ReadHistoryBranchByBatch(readReq)
		if retError != nil {
			return
		}

		for _, batch := range readResp.History {
			history := batch.Events
			firstEvent := history[0]
			lastEvent := history[len(history)-1]

			// for saving received signals only
			if firstEvent.GetEventId() >= decisionFinishEventID {
				for _, e := range batch.Events {
					if e.GetEventType() == workflow.EventTypeWorkflowExecutionSignaled {
						receivedSignalsAfterReset = append(receivedSignalsAfterReset, e)
					}
					if e.GetEventType() == workflow.EventTypeWorkflowExecutionContinuedAsNew {
						attr := e.GetWorkflowExecutionContinuedAsNewEventAttributes()
						continueRunID = attr.GetNewExecutionRunId()
					}
				}
				continue
			}

			lastBatch = history
			if firstEvent.GetEventId() == common.FirstEventID {
				if firstEvent.GetEventType() != workflow.EventTypeWorkflowExecutionStarted {
					retError = &workflow.InternalServiceError{
						Message: fmt.Sprintf("first event type is not EventTypeWorkflowExecutionStarted: %v", firstEvent.GetEventType()),
					}
					return
				}
				wfTimeoutSecs = int64(firstEvent.GetWorkflowExecutionStartedEventAttributes().GetExecutionStartToCloseTimeoutSeconds())
				if prevMutableState.GetReplicationState() != nil {
					resetMutableState = newMutableStateBuilderWithReplicationState(
						w.eng.shard,
						w.eng.shard.GetEventsCache(),
						w.eng.logger,
						domainEntry,
					)
				} else if prevMutableState.GetVersionHistories() != nil {
					resetMutableState = newMutableStateBuilderWithVersionHistories(
						w.eng.shard,
						w.eng.shard.GetEventsCache(),
						w.eng.logger,
						domainEntry,
					)
				} else {
					resetMutableState = newMutableStateBuilder(w.eng.shard, w.eng.shard.GetEventsCache(), w.eng.logger, domainEntry)
				}

				sBuilder = newStateBuilder(
					w.eng.shard,
					w.eng.logger,
					resetMutableState,
					func(mutableState mutableState) mutableStateTaskGenerator {
						return newMutableStateTaskGenerator(w.eng.shard.GetDomainCache(), w.eng.logger, mutableState)
					},
				)
			}

			// avoid replay this event in stateBuilder which will run into NPE if WF doesn't enable XDC
			if lastEvent.GetEventType() == workflow.EventTypeWorkflowExecutionContinuedAsNew {
				retError = &workflow.BadRequestError{
					Message: fmt.Sprintf("wrong DecisionFinishEventId, cannot replay history to continueAsNew"),
				}
				return
			}

			_, retError = sBuilder.applyEvents(domainID, requestID, prevExecution, history, nil, false)
			if retError != nil {
				return
			}
		}
		newHistorySize += int64(readResp.Size)
		if len(readResp.NextPageToken) > 0 {
			readReq.NextPageToken = readResp.NextPageToken
		} else {
			break
		}
	}

	retError = validateLastBatchOfReset(lastBatch, decisionFinishEventID)
	if retError != nil {
		return
	}
	forkEventVersion = lastBatch[len(lastBatch)-1].GetVersion()

	startTime := w.timeSource.Now()
	resetMutableState.executionInfo.RunID = newRunID
	resetMutableState.executionInfo.StartTimestamp = startTime
	resetMutableState.executionInfo.LastUpdatedTimestamp = startTime
	resetMutableState.executionInfo.SetNextEventID(decisionFinishEventID)
	resetMutableState.ClearStickyness()
	return
}

func validateLastBatchOfReset(lastBatch []*workflow.HistoryEvent, decisionFinishEventID int64) error {
	firstEvent := lastBatch[0]
	lastEvent := lastBatch[len(lastBatch)-1]
	if decisionFinishEventID != lastEvent.GetEventId()+1 {
		return &workflow.BadRequestError{
			Message: fmt.Sprintf("wrong DecisionFinishEventId, it must be DecisionTaskStarted + 1: %v", lastEvent.GetEventId()),
		}
	}

	if lastEvent.GetEventType() != workflow.EventTypeDecisionTaskStarted {
		return &workflow.BadRequestError{
			Message: fmt.Sprintf("wrong DecisionFinishEventId, previous batch doesn't include EventTypeDecisionTaskStarted, lastFirstEventId: %v", firstEvent.GetEventId()),
		}
	}

	return nil
}

func validateResetReplicationTask(request *h.ReplicateEventsRequest) (*workflow.DecisionTaskFailedEventAttributes, error) {
	historyAfterReset := request.History.Events
	if len(historyAfterReset) == 0 || historyAfterReset[0].GetEventType() != workflow.EventTypeDecisionTaskFailed {
		return nil, errUnknownReplicationTask
	}
	firstEvent := historyAfterReset[0]
	if firstEvent.DecisionTaskFailedEventAttributes.GetCause() != workflow.DecisionTaskFailedCauseResetWorkflow {
		return nil, errUnknownReplicationTask
	}
	attr := firstEvent.DecisionTaskFailedEventAttributes
	if attr.GetNewRunId() != request.GetWorkflowExecution().GetRunId() {
		return nil, errUnknownReplicationTask
	}
	return attr, nil
}

func (w *workflowResetorImpl) ApplyResetEvent(
	ctx context.Context,
	request *h.ReplicateEventsRequest,
	domainID, workflowID, currentRunID string,
) error {
	var currContext workflowExecutionContext
	var baseMutableState, currMutableState, newMsBuilder mutableState
	var newHistorySize int64
	var newRunTransferTasks, newRunTimerTasks []persistence.Task

	resetAttr, retError := validateResetReplicationTask(request)
	historyAfterReset := request.History.Events
	lastEvent := historyAfterReset[len(historyAfterReset)-1]
	decisionFinishEventID := historyAfterReset[0].GetEventId()
	if retError != nil {
		return retError
	}
	baseExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(resetAttr.GetBaseRunId()),
	}

	baseContext, baseRelease, baseErr := w.eng.historyCache.getOrCreateWorkflowExecution(ctx, domainID, baseExecution)
	if baseErr != nil {
		return baseErr
	}
	defer func() { baseRelease(retError) }()
	baseMutableState, retError = baseContext.loadWorkflowExecution()
	if retError != nil {
		return retError
	}
	if baseMutableState.GetNextEventID() < decisionFinishEventID {
		// re-replicate the whole new run
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, resetAttr.GetNewRunId(), common.FirstEventID)
	}

	if currentRunID == resetAttr.GetBaseRunId() {
		currMutableState = baseMutableState
		currContext = baseContext
	} else {
		var currRelease releaseWorkflowExecutionFunc
		currExecution := workflow.WorkflowExecution{
			WorkflowId: baseExecution.WorkflowId,
			RunId:      common.StringPtr(currentRunID),
		}
		var currErr error
		currContext, currRelease, currErr = w.eng.historyCache.getOrCreateWorkflowExecution(ctx, domainID, currExecution)
		if currErr != nil {
			return currErr
		}
		defer func() { currRelease(retError) }()
		currMutableState, retError = currContext.loadWorkflowExecution()
		if retError != nil {
			return retError
		}
	}
	// before changing mutable state
	newMsBuilder, newHistorySize, newRunTransferTasks, newRunTimerTasks, retError = w.replicateResetEvent(baseMutableState, &baseExecution, historyAfterReset, resetAttr.GetForkEventVersion())
	if retError != nil {
		return retError
	}

	// fork a new history branch
	shardID := common.IntPtr(w.eng.shard.GetShardID())
	baseBranchToken, err := baseMutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}
	forkResp, retError := w.eng.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      decisionFinishEventID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(domainID, workflowID, resetAttr.GetNewRunId()),
		ShardID:         shardID,
	})
	if retError != nil {
		return retError
	}
	retError = newMsBuilder.SetCurrentBranchToken(forkResp.NewBranchToken)
	if retError != nil {
		return retError
	}

	// prepare to append history to new branch
	hBuilder := newHistoryBuilder(newMsBuilder, w.eng.logger)
	hBuilder.history = historyAfterReset
	newMsBuilder.SetHistoryBuilder(hBuilder)

	retError = currContext.resetWorkflowExecution(
		currMutableState,
		false,
		nil,
		nil,
		newMsBuilder,
		newHistorySize,
		newRunTransferTasks,
		newRunTimerTasks,
		nil,
		nil,
		baseExecution.GetRunId(),
		baseMutableState.GetNextEventID(),
	)
	if retError != nil {
		return retError
	}
	now := time.Unix(0, lastEvent.GetTimestamp())
	notify(w.eng.shard, request.GetSourceCluster(), now)
	return nil
}

// TODO: @shreyassrivatsan reduce number of return parameters from this method
func (w *workflowResetorImpl) replicateResetEvent(
	baseMutableState mutableState,
	baseExecution *workflow.WorkflowExecution,
	newRunHistory []*workflow.HistoryEvent,
	forkEventVersion int64,
) (newMsBuilder mutableState, newHistorySize int64, transferTasks, timerTasks []persistence.Task, retError error) {
	domainID := baseMutableState.GetExecutionInfo().DomainID
	workflowID := baseMutableState.GetExecutionInfo().WorkflowID
	firstEvent := newRunHistory[0]

	decisionFinishEventID := firstEvent.GetEventId()
	resetAttr := firstEvent.GetDecisionTaskFailedEventAttributes()

	requestID := uuid.New()
	var sBuilder stateBuilder
	var wfTimeoutSecs int64

	domainEntry, retError := w.eng.shard.GetDomainCache().GetDomainByID(domainID)
	if retError != nil {
		return
	}

	// replay old history from beginning of the baseRun upto decisionFinishEventID(exclusive)
	var nextPageToken []byte
	var lastEvent *workflow.HistoryEvent
	baseBranchToken, err := baseMutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, 0, nil, nil, err
	}
	readReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    decisionFinishEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nextPageToken,
		ShardID:       common.IntPtr(w.eng.shard.GetShardID()),
	}
	for {
		var readResp *persistence.ReadHistoryBranchByBatchResponse
		readResp, retError = w.eng.historyV2Mgr.ReadHistoryBranchByBatch(readReq)
		if retError != nil {
			return
		}
		for _, batch := range readResp.History {
			events := batch.Events
			firstEvent := events[0]
			lastEvent = events[len(events)-1]
			if firstEvent.GetEventId() == common.FirstEventID {
				wfTimeoutSecs = int64(firstEvent.GetWorkflowExecutionStartedEventAttributes().GetExecutionStartToCloseTimeoutSeconds())
				newMsBuilder = newMutableStateBuilderWithReplicationState(
					w.eng.shard,
					w.eng.shard.GetEventsCache(),
					w.eng.logger,
					domainEntry,
				)
				if err := newMsBuilder.UpdateCurrentVersion(
					firstEvent.GetVersion(),
					true,
				); err != nil {
					return nil, 0, nil, nil, err
				}

				sBuilder = newStateBuilder(
					w.eng.shard,
					w.eng.logger,
					newMsBuilder,
					func(mutableState mutableState) mutableStateTaskGenerator {
						return newMutableStateTaskGenerator(w.eng.shard.GetDomainCache(), w.eng.logger, mutableState)
					},
				)
			}
			_, retError = sBuilder.applyEvents(domainID, requestID, *baseExecution, events, nil, false)
			if retError != nil {
				return
			}
		}
		newHistorySize += int64(readResp.Size)
		if len(readResp.NextPageToken) > 0 {
			readReq.NextPageToken = readResp.NextPageToken
		} else {
			break
		}
	}
	if lastEvent.GetEventId() != decisionFinishEventID-1 || lastEvent.GetVersion() != forkEventVersion {
		// re-replicate the whole new run
		retError = newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, domainID, workflowID, resetAttr.GetNewRunId(), common.FirstEventID)
		return
	}
	startTime := time.Unix(0, firstEvent.GetTimestamp())
	newMsBuilder.GetExecutionInfo().RunID = resetAttr.GetNewRunId()
	newMsBuilder.GetExecutionInfo().StartTimestamp = startTime
	newMsBuilder.GetExecutionInfo().LastUpdatedTimestamp = startTime
	newMsBuilder.ClearStickyness()

	// before this, the mutable state is in replay mode
	// need to close / flush the mutable state for new changes
	_, _, retError = newMsBuilder.CloseTransactionAsSnapshot(
		time.Unix(0, lastEvent.GetTimestamp()),
		transactionPolicyPassive,
	)
	if retError != nil {
		return
	}

	// always enforce the attempt to zero so that we can always schedule a new decision(skip trasientDecision logic)
	decision, _ := newMsBuilder.GetInFlightDecision()
	decision.Attempt = 0
	newMsBuilder.UpdateDecision(decision)

	// before this, the mutable state is in replay mode
	// need to close / flush the mutable state for new changes
	_, _, retError = newMsBuilder.CloseTransactionAsSnapshot(
		time.Unix(0, lastEvent.GetTimestamp()),
		transactionPolicyPassive,
	)
	if retError != nil {
		return
	}

	lastEvent = newRunHistory[len(newRunHistory)-1]
	// replay new history (including decisionTaskScheduled)
	_, retError = sBuilder.applyEvents(domainID, requestID, *baseExecution, newRunHistory, nil, false)
	if retError != nil {
		return
	}
	newMsBuilder.GetExecutionInfo().SetNextEventID(lastEvent.GetEventId() + 1)

	actTasks, retError := w.scheduleUnstartedActivities(newMsBuilder)
	if retError != nil {
		return
	}
	transferTasks = append(transferTasks, actTasks...)
	timerTasks, retError = w.generateTimerTasksForReset(newMsBuilder, wfTimeoutSecs, len(actTasks) > 0)
	if retError != nil {
		return
	}

	// schedule new decision
	decisionScheduledID := newMsBuilder.GetExecutionInfo().DecisionScheduleID
	decision, _ = newMsBuilder.GetDecisionInfo(decisionScheduledID)
	transferTasks = append(transferTasks, &persistence.DecisionTask{
		DomainID:         domainID,
		TaskList:         decision.TaskList,
		ScheduleID:       decision.ScheduleID,
		RecordVisibility: true,
	})

	newMsBuilder.GetExecutionInfo().SetLastFirstEventID(firstEvent.GetEventId())
	newMsBuilder.UpdateReplicationStateLastEventID(lastEvent.GetVersion(), lastEvent.GetEventId())
	return
}

// FindAutoResetPoint returns the auto reset point
func FindAutoResetPoint(
	timeSource clock.TimeSource,
	badBinaries *workflow.BadBinaries,
	autoResetPoints *workflow.ResetPoints,
) (string, *workflow.ResetPointInfo) {
	if badBinaries == nil || badBinaries.Binaries == nil || autoResetPoints == nil || autoResetPoints.Points == nil {
		return "", nil
	}
	nowNano := timeSource.Now().UnixNano()
	for _, p := range autoResetPoints.Points {
		bin, ok := badBinaries.Binaries[p.GetBinaryChecksum()]
		if ok && p.GetResettable() {
			if p.GetExpiringTimeNano() > 0 && nowNano > p.GetExpiringTimeNano() {
				// reset point has expired and we may already deleted the history
				continue
			}
			return bin.GetReason(), p
		}
	}
	return "", nil
}

func getWorkflowCleanupTasks(
	domainCache cache.DomainCache,
	domainID string,
	workflowID string,
) (persistence.Task, persistence.Task, error) {

	var retentionInDays int32
	domainEntry, err := domainCache.GetDomainByID(domainID)
	if err != nil {
		if _, ok := err.(*workflow.EntityNotExistsError); !ok {
			return nil, nil, err
		}
	} else {
		retentionInDays = domainEntry.GetRetentionDays(workflowID)
	}
	deleteTask := createDeleteHistoryEventTimerTask(retentionInDays)
	return &persistence.CloseExecutionTask{}, deleteTask, nil
}

func createDeleteHistoryEventTimerTask(
	retentionInDays int32,
) *persistence.DeleteHistoryEventTask {

	retention := time.Duration(retentionInDays) * time.Hour * 24
	expiryTime := clock.NewRealTimeSource().Now().Add(retention)
	return &persistence.DeleteHistoryEventTask{
		VisibilityTimestamp: expiryTime,
	}
}
