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

package history

import (
	"context"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	h "github.com/uber/cadence/.gen/go/history"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	ce "github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/persistence"
)

type (
	workflowResetor interface {
		ResetWorkflowExecution(ctx context.Context, resetRequest *h.ResetWorkflowExecutionRequest) (response *workflow.ResetWorkflowExecutionResponse, retError error)
		ApplyResetEvent(ctx context.Context, request *h.ReplicateEventsRequest, domainID, workflowID, currentRunID string) (retError error)
	}

	workflowResetorImpl struct {
		eng        *historyEngineImpl
		replicator *historyReplicator
	}
)

var _ workflowResetor = (*workflowResetorImpl)(nil)

func newWorkflowResetor(historyEngine *historyEngineImpl, repl *historyReplicator) *workflowResetorImpl {
	return &workflowResetorImpl{
		eng:        historyEngine,
		replicator: repl,
	}
}

// ResetWorkflowExecution only allows resetting to decisionTaskCompleted, but exclude that batch of decisionTaskCompleted/decisionTaskFailed/decisionTaskTimeout.
// It will then fail the decision with cause of "reset_workflow"
func (w *workflowResetorImpl) ResetWorkflowExecution(ctx context.Context, resetRequest *h.ResetWorkflowExecutionRequest) (response *workflow.ResetWorkflowExecutionResponse, retError error) {
	domainEntry, retError := w.eng.getActiveDomainEntry(resetRequest.DomainUUID)
	if retError != nil {
		return
	}
	domainID := domainEntry.GetInfo().ID

	request := resetRequest.ResetRequest
	if request == nil || request.WorkflowExecution == nil || len(request.WorkflowExecution.GetRunId()) == 0 || len(request.WorkflowExecution.GetWorkflowId()) == 0 {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("Require workflowId and runId."),
		}
		return
	}
	if request.GetDecisionFinishEventId() <= common.FirstEventID {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("Decision finish ID must be > 1."),
		}
		return
	}
	baseExecution := workflow.WorkflowExecution{
		WorkflowId: request.WorkflowExecution.WorkflowId,
		RunId:      request.WorkflowExecution.RunId,
	}
	newRunID := uuid.New()
	response = &workflow.ResetWorkflowExecutionResponse{
		RunId: common.StringPtr(newRunID),
	}

	baseContext, baseRelease, retError := w.eng.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, baseExecution)
	if retError != nil {
		return
	}
	defer func() { baseRelease(retError) }()
	baseMutableState, retError := baseContext.loadWorkflowExecution()
	if retError != nil {
		return
	}

	// also load the current run of the workflow, it can be different from the base runID
	resp, retError := w.eng.shard.GetExecutionManager().GetCurrentExecution(&persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: request.WorkflowExecution.GetWorkflowId(),
	})
	if retError != nil {
		return
	}
	var currMutableState mutableState
	var currContext workflowExecutionContext
	var currExecution workflow.WorkflowExecution
	if resp.RunID == baseExecution.GetRunId() {
		currContext = baseContext
		currMutableState = baseMutableState
		currExecution = baseExecution
	} else {
		currExecution = workflow.WorkflowExecution{
			WorkflowId: request.WorkflowExecution.WorkflowId,
			RunId:      common.StringPtr(resp.RunID),
		}
		var currRelease func(err error)
		currContext, currRelease, retError = w.eng.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, currExecution)
		if retError != nil {
			return
		}
		defer func() { currRelease(retError) }()
		currMutableState, retError = currContext.loadWorkflowExecution()
		if retError != nil {
			return
		}
	}
	// dedup by requestID
	if currMutableState.GetExecutionInfo().CreateRequestID == request.GetRequestId() {
		response.RunId = currExecution.RunId
		w.eng.logger.WithFields(bark.Fields{
			logging.TagDomainID:            domainID,
			logging.TagWorkflowExecutionID: currExecution.GetWorkflowId(),
			logging.TagWorkflowRunID:       currExecution.GetRunId(),
		}).Info("Duplicated reset request")
		return
	}

	// before changing mutable state
	prevRunVersion := currMutableState.GetLastWriteVersion()
	// terminate the current run if it is running
	terminateCurr, closeTask, cleanupTask, retError := w.terminateIfCurrIsRunning(currMutableState, request.GetReason(), currExecution)
	if retError != nil {
		return
	}

	retError = validateResetWorkflowBeforeReplay(baseMutableState, currMutableState)
	if retError != nil {
		return
	}

	newMutableState, transferTasks, timerTasks, retError := w.buildNewMutableStateForReset(ctx, baseMutableState, currMutableState, request.GetReason(), request.GetDecisionFinishEventId(), request.GetRequestId(), newRunID)
	// complete the fork process at the end, it is OK even if this defer fails, because our timer task can still clean up correctly
	defer func() {
		if newMutableState != nil && len(newMutableState.GetExecutionInfo().GetCurrentBranch()) > 0 {
			w.eng.historyV2Mgr.CompleteForkBranch(&persistence.CompleteForkBranchRequest{
				BranchToken: newMutableState.GetExecutionInfo().GetCurrentBranch(),
				Success:     retError == nil,
			})
		}
	}()
	if retError != nil {
		return
	}

	retError = w.checkDomainStatus(newMutableState, prevRunVersion, request.GetDomain())
	if retError != nil {
		return
	}

	// update replication and generate replication task
	currReplicationTasks, insertReplicationTasks := w.generateReplicationTasksForReset(terminateCurr, currMutableState, newMutableState, domainEntry)

	// finally, write to persistence
	retError = currContext.resetWorkflowExecution(currMutableState, terminateCurr, closeTask, cleanupTask, newMutableState, transferTasks, timerTasks, currReplicationTasks, insertReplicationTasks, baseExecution.GetRunId(), baseMutableState.GetNextEventID(), prevRunVersion)

	if retError == nil {
		w.eng.txProcessor.NotifyNewTask(w.eng.currentClusterName, transferTasks)
		w.eng.timerProcessor.NotifyNewTimers(w.eng.currentClusterName, w.eng.shard.GetCurrentTime(w.eng.currentClusterName), timerTasks)
	}

	return
}

func (w *workflowResetorImpl) checkDomainStatus(newMutableState mutableState, prevRunVersion int64, domain string) (retError error) {
	if newMutableState.GetReplicationState() != nil {
		clusterMetadata := w.eng.shard.GetService().GetClusterMetadata()
		currentVersion := newMutableState.GetCurrentVersion()
		if currentVersion < prevRunVersion {
			retError = ce.NewDomainNotActiveError(
				domain,
				clusterMetadata.GetCurrentClusterName(),
				clusterMetadata.ClusterNameForFailoverVersion(prevRunVersion),
			)
			return
		}
		activeCluster := clusterMetadata.ClusterNameForFailoverVersion(currentVersion)
		currentCluster := clusterMetadata.GetCurrentClusterName()
		if activeCluster != currentCluster {
			retError = ce.NewDomainNotActiveError(domain, currentCluster, activeCluster)
			return
		}
	}
	return nil
}

func validateResetWorkflowBeforeReplay(baseMutableState, currMutableState mutableState) (retError error) {
	if baseMutableState.GetEventStoreVersion() != persistence.EventStoreVersionV2 {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("reset API is not supported for V1 history events"),
		}
		return
	}
	if len(baseMutableState.GetPendingChildExecutionInfos()) > 0 {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("reset is not allowed when workflow has pending child workflow. RunID: %v", baseMutableState.GetExecutionInfo().RunID),
		}
		return
	}
	if len(currMutableState.GetPendingChildExecutionInfos()) > 0 {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("reset is not allowed when workflow has pending child workflow. RunID: %v", currMutableState.GetExecutionInfo().RunID),
		}
		return
	}
	if currMutableState.IsWorkflowExecutionRunning() {
		retError = &workflow.InternalServiceError{
			Message: fmt.Sprintf("current workflow should already been terminated"),
		}
	}
	return
}

func validateResetWorkflowAfterReplay(newMutableState mutableState) (retError error) {
	if len(newMutableState.GetAllRequestCancels()) > 0 {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("it is not allowed resetting to a point that workflow has pending request cancel "),
		}
		return
	}
	if len(newMutableState.GetPendingChildExecutionInfos()) > 0 {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("it is not allowed resetting to a point that workflow has pending child workflow "),
		}
		return
	}
	if len(newMutableState.GetAllSignalsToSend()) > 0 {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("it is not allowed resetting to a point that workflow has pending signals to send, pending signal: %+v ", newMutableState.GetAllSignalsToSend()),
		}
		return
	}
	if !newMutableState.HasInFlightDecisionTask() {
		retError = &workflow.BadRequestError{
			Message: fmt.Sprintf("can't find the last started decision"),
		}
		return
	}
	if newMutableState.HasBufferedEvents() {
		retError = &workflow.InternalServiceError{
			Message: fmt.Sprintf("replay history shouldn't see any bufferred events"),
		}
	}
	if newMutableState.IsStickyTaskListEnabled() {
		retError = &workflow.InternalServiceError{
			Message: fmt.Sprintf("replay history shouldn't have stikyness"),
		}
	}
	return
}

// Fail the started activities
func (w *workflowResetorImpl) failStartedActivities(msBuilder mutableState) error {
	for _, ai := range msBuilder.GetPendingActivityInfos() {
		if ai.StartedID != common.EmptyEventID {
			// this means the activity has started but not completed, we need to fail the activity
			request := getRespondActivityTaskFailedRequestFromActivity(ai, "workflowReset")
			if msBuilder.AddActivityTaskFailedEvent(ai.ScheduleID, ai.StartedID, request) == nil {
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

func (w *workflowResetorImpl) buildNewMutableStateForReset(ctx context.Context, baseMutableState, currMutableState mutableState, resetReason string, resetDecisionCompletedEventID int64, requestedID, newRunID string) (newMutableState mutableState, transferTasks, timerTasks []persistence.Task, retError error) {
	domainID := baseMutableState.GetExecutionInfo().DomainID
	workflowID := baseMutableState.GetExecutionInfo().WorkflowID
	baseRunID := baseMutableState.GetExecutionInfo().RunID

	// replay history to reset point(exclusive) to rebuild mutableState
	forkEventVersion, wfTimeoutSecs, receivedSignals, continueRunID, newStateBuilder, retError := w.replayHistoryEvents(resetDecisionCompletedEventID, requestedID, baseMutableState, newRunID)
	if retError != nil {
		return
	}
	newMutableState = newStateBuilder.getMutableState()

	retError = validateResetWorkflowAfterReplay(newMutableState)
	if retError != nil {
		return
	}

	// failed the in-flight decision(started).
	// Note that we need to ensure DecisionTaskFailed event is appended right after DecisionTaskStarted event
	di, _ := newMutableState.GetInFlightDecisionTask()

	event := newMutableState.AddDecisionTaskFailedEvent(di.ScheduleID, di.StartedID, workflow.DecisionTaskFailedCauseResetWorkflow, nil,
		identityHistoryService, resetReason, baseRunID, newRunID, forkEventVersion)
	if event == nil {
		retError = &workflow.InternalServiceError{Message: "Failed to add decision failed event."}
	}

	retError = w.failStartedActivities(newMutableState)
	if retError != nil {
		return
	}

	transferTasks, retError = w.scheduleUnstartedActivities(newMutableState)
	if retError != nil {
		return
	}

	// we will need a timer for the scheduled activities
	needActivityTimer := len(transferTasks) > 0

	// generate new timer tasks: we need 4 timers:
	// 1. WF timeout,
	// 2. user timers for timers started but not fired by reset
	// 3. activity timeout for scheduled but not started activities
	timerTasks, retError = w.generateTimerTasksForReset(newMutableState, wfTimeoutSecs, needActivityTimer)
	if retError != nil {
		return
	}
	// replay received signals back to mutableState/history:
	retError = w.replayReceivedSignals(ctx, receivedSignals, continueRunID, newMutableState, currMutableState)
	if retError != nil {
		return
	}

	// we always schedule a new decision after reset
	di = newMutableState.AddDecisionTaskScheduledEvent()
	if di == nil {
		retError = &workflow.InternalServiceError{Message: "Failed to add decision scheduled event."}
		return
	}

	transferTasks = append(transferTasks, &persistence.DecisionTask{
		DomainID:         domainID,
		TaskList:         di.TaskList,
		ScheduleID:       di.ScheduleID,
		RecordVisibility: true,
	})

	// fork a new history branch
	forkResp, retError := w.eng.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseMutableState.GetCurrentBranch(),
		ForkNodeID:      resetDecisionCompletedEventID,
		Info:            historyGarbageCleanupInfo(domainID, workflowID, newRunID),
	})
	if retError != nil {
		return
	}
	newMutableState.GetExecutionInfo().BranchToken = forkResp.NewBranchToken
	return
}

func (w *workflowResetorImpl) terminateIfCurrIsRunning(currMutableState mutableState, reason string, currExecution workflow.WorkflowExecution) (terminateCurr bool, closeTask, cleanupTask persistence.Task, retError error) {
	if currMutableState.IsWorkflowExecutionRunning() {
		terminateCurr = true

		retError = failInFlightDecisionToClearBufferedEvents(currMutableState)
		if retError != nil {
			return
		}

		currMutableState.AddWorkflowExecutionTerminatedEvent(&workflow.TerminateWorkflowExecutionRequest{
			Reason:   common.StringPtr(reason),
			Details:  nil,
			Identity: common.StringPtr(identityHistoryService),
		})
		closeTask, cleanupTask, retError = w.eng.getWorkflowHistoryCleanupTasks(
			currMutableState.GetExecutionInfo().DomainID,
			currExecution.GetWorkflowId(),
			w.eng.getTimerBuilder(&currExecution))
		if retError != nil {
			return
		}
	}
	return
}

func historyGarbageCleanupInfo(domainID, workflowID, runID string) string {
	return fmt.Sprintf("%v:%v:%v", domainID, workflowID, runID)
}

func (w *workflowResetorImpl) setEventIDsWithHistory(msBuilder mutableState) int64 {
	clusterMetadata := w.eng.shard.GetService().GetClusterMetadata()
	history := msBuilder.GetHistoryBuilder().GetHistory().Events
	firstEvent := history[0]
	lastEvent := history[len(history)-1]
	msBuilder.GetExecutionInfo().SetLastFirstEventID(firstEvent.GetEventId())
	msBuilder.UpdateReplicationStateLastEventID(clusterMetadata.GetCurrentClusterName(), lastEvent.GetVersion(), lastEvent.GetEventId())
	return firstEvent.GetEventId()
}

func (w *workflowResetorImpl) generateReplicationTasksForReset(terminateCurr bool, currMutableState, newMutableState mutableState, domainEntry *cache.DomainCacheEntry) ([]persistence.Task, []persistence.Task) {
	var currRepTasks, insertRepTasks []persistence.Task
	if newMutableState.GetReplicationState() != nil {
		if terminateCurr {
			// we will generate 2 replication tasks for this case
			firstEventIDForCurr := w.setEventIDsWithHistory(currMutableState)
			if domainEntry.CanReplicateEvent() {
				replicationTask := &persistence.HistoryReplicationTask{
					Version:             currMutableState.GetCurrentVersion(),
					LastReplicationInfo: currMutableState.GetReplicationState().LastReplicationInfo,
					FirstEventID:        firstEventIDForCurr,
					NextEventID:         currMutableState.GetNextEventID(),
					EventStoreVersion:   currMutableState.GetEventStoreVersion(),
					BranchToken:         currMutableState.GetCurrentBranch(),
				}
				currRepTasks = append(currRepTasks, replicationTask)
			}
		}
		firstEventIDForNew := w.setEventIDsWithHistory(newMutableState)
		if domainEntry.CanReplicateEvent() {
			replicationTask := &persistence.HistoryReplicationTask{
				Version:             newMutableState.GetCurrentVersion(),
				LastReplicationInfo: newMutableState.GetReplicationState().LastReplicationInfo,
				ResetWorkflow:       true,
				FirstEventID:        firstEventIDForNew,
				NextEventID:         newMutableState.GetNextEventID(),
				EventStoreVersion:   newMutableState.GetEventStoreVersion(),
				BranchToken:         newMutableState.GetCurrentBranch(),
			}
			insertRepTasks = append(insertRepTasks, replicationTask)
		}
	}
	return currRepTasks, insertRepTasks
}

// replay signals in the base run, and also signals in all the runs along the chain of contineAsNew
func (w *workflowResetorImpl) replayReceivedSignals(ctx context.Context, receivedSignals []*workflow.HistoryEvent, continueRunID string, newMutableState, currMutableState mutableState) error {
	for _, se := range receivedSignals {
		sigReq := &workflow.SignalWorkflowExecutionRequest{
			SignalName: se.GetWorkflowExecutionSignaledEventAttributes().SignalName,
			Identity:   se.GetWorkflowExecutionSignaledEventAttributes().Identity,
			Input:      se.GetWorkflowExecutionSignaledEventAttributes().Input,
		}
		newMutableState.AddWorkflowExecutionSignaled(sigReq.GetSignalName(), sigReq.GetInput(), sigReq.GetIdentity())
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
			continueContext, continueRelease, err := w.eng.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, newMutableState.GetExecutionInfo().DomainID, continueExe)
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
		readReq := &persistence.ReadHistoryBranchRequest{
			BranchToken: continueMutableState.GetCurrentBranch(),
			MinEventID:  common.FirstEventID,
			// NOTE: read through history to the end so that we can collect all the received signals
			MaxEventID:    continueMutableState.GetNextEventID(),
			PageSize:      defaultHistoryPageSize,
			NextPageToken: nextPageToken,
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
						newMutableState.AddWorkflowExecutionSignaled(sigReq.GetSignalName(), sigReq.GetInput(), sigReq.GetIdentity())
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

func (w *workflowResetorImpl) generateTimerTasksForReset(msBuilder mutableState, wfTimeoutSecs int64, needActivityTimer bool) ([]persistence.Task, error) {
	timerTasks := []persistence.Task{}

	// WF timeout task
	duration := time.Duration(wfTimeoutSecs) * time.Second
	wfTimeoutTask := &persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: w.eng.shard.GetTimeSource().Now().Add(duration),
	}
	timerTasks = append(timerTasks, wfTimeoutTask)

	we := &workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(msBuilder.GetExecutionInfo().WorkflowID),
		RunId:      common.StringPtr(msBuilder.GetExecutionInfo().RunID),
	}
	tb := w.eng.getTimerBuilder(we)
	// user timer task
	if len(msBuilder.GetPendingTimerInfos()) > 0 {
		tb.loadUserTimers(msBuilder)
		tt := tb.firstTimerTaskWithoutChecking()
		timerTasks = append(timerTasks, tt)
	}

	// activity timer
	if needActivityTimer {
		tb.loadActivityTimers(msBuilder)
		tt := tb.firstActivityTimerTaskWithoutChecking()
		timerTasks = append(timerTasks, tt)
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

func (w *workflowResetorImpl) replayHistoryEvents(decisionFinishEventID int64, requestID string, prevMutableState mutableState, newRunID string) (forkEventVersion, wfTimeoutSecs int64, receivedSignalsAfterReset []*workflow.HistoryEvent, continueRunID string, sBuilder stateBuilder, retError error) {
	clusterMetadata := w.eng.shard.GetService().GetClusterMetadata()

	prevExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(prevMutableState.GetExecutionInfo().WorkflowID),
		RunId:      common.StringPtr(prevMutableState.GetExecutionInfo().RunID),
	}
	domainID := prevMutableState.GetExecutionInfo().DomainID
	var nextPageToken []byte
	readReq := &persistence.ReadHistoryBranchRequest{
		BranchToken: prevMutableState.GetCurrentBranch(),
		MinEventID:  common.FirstEventID,
		// NOTE: read through history to the end so that we can keep the received signals
		MaxEventID:    prevMutableState.GetNextEventID(),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nextPageToken,
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
						clusterMetadata.GetCurrentClusterName(),
						w.eng.shard,
						w.eng.shard.GetEventsCache(),
						w.eng.logger,
						firstEvent.GetVersion(),
					)
				} else {
					resetMutableState = newMutableStateBuilder(clusterMetadata.GetCurrentClusterName(), w.eng.shard,
						w.eng.shard.GetEventsCache(), w.eng.logger)
				}

				resetMutableState.executionInfo.EventStoreVersion = persistence.EventStoreVersionV2

				sBuilder = newStateBuilder(w.eng.shard, resetMutableState, w.eng.logger)
			}

			// avoid replay this event in stateBuilder which will run into NPE if WF doesn't enable XDC
			if firstEvent.GetEventType() == workflow.EventTypeWorkflowExecutionContinuedAsNew {
				retError = &workflow.BadRequestError{
					Message: fmt.Sprintf("wrong DecisionFinishEventId, cannot replay history to continueAsNew"),
				}
			}

			_, _, _, retError = sBuilder.applyEvents(domainID, requestID, prevExecution, history, nil, persistence.EventStoreVersionV2, persistence.EventStoreVersionV2)
			if retError != nil {
				return
			}
		}
		resetMutableState.IncrementHistorySize(readResp.Size)
		if len(readResp.NextPageToken) > 0 {
			readReq.NextPageToken = readResp.NextPageToken
		} else {
			break
		}
	}

	retError = validateLastBatchOfReset(lastBatch)
	if retError != nil {
		return
	}
	forkEventVersion = lastBatch[len(lastBatch)-1].GetVersion()

	startTime := time.Now()
	resetMutableState.executionInfo.RunID = newRunID
	resetMutableState.executionInfo.StartTimestamp = startTime
	resetMutableState.executionInfo.LastUpdatedTimestamp = startTime
	resetMutableState.executionInfo.SetNextEventID(decisionFinishEventID)
	resetMutableState.ClearStickyness()
	return
}

func validateLastBatchOfReset(lastBatch []*workflow.HistoryEvent) error {
	firstEvent := lastBatch[0]
	for _, event := range lastBatch {
		if event.GetEventType() == workflow.EventTypeDecisionTaskStarted {
			return nil
		}
	}
	return &workflow.BadRequestError{
		Message: fmt.Sprintf("wrong DecisionFinishEventId, previous batch doesn't include EventTypeDecisionTaskStarted, lastFirstEventId: %v", firstEvent.GetEventId()),
	}
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

func (w *workflowResetorImpl) ApplyResetEvent(ctx context.Context, request *h.ReplicateEventsRequest, domainID, workflowID, currentRunID string) (retError error) {
	var currContext workflowExecutionContext
	var baseMutableState, currMutableState, newMsBuilder mutableState
	var newRunTransferTasks, newRunTimerTasks []persistence.Task

	resetAttr, retError := validateResetReplicationTask(request)
	historyAfterReset := request.History.Events
	lastEvent := historyAfterReset[len(historyAfterReset)-1]
	decisionFinishEventID := historyAfterReset[0].GetEventId()
	if retError != nil {
		return
	}
	baseExecution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(resetAttr.GetBaseRunId()),
	}

	baseContext, baseRelease, baseErr := w.eng.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, baseExecution)
	if baseErr != nil {
		return baseErr
	}
	defer func() { baseRelease(retError) }()
	baseMutableState, retError = baseContext.loadWorkflowExecution()
	if retError != nil {
		return
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
		currContext, currRelease, currErr = w.eng.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx, domainID, currExecution)
		if currErr != nil {
			return currErr
		}
		defer func() { currRelease(retError) }()
		currMutableState, retError = currContext.loadWorkflowExecution()
		if retError != nil {
			return
		}
	}
	// before changing mutable state
	prevRunVersion := currMutableState.GetLastWriteVersion()
	newMsBuilder, newRunTransferTasks, newRunTimerTasks, retError = w.replicateResetEvent(baseMutableState, &baseExecution, historyAfterReset, resetAttr.GetForkEventVersion())
	if retError != nil {
		return
	}

	// fork a new history branch
	forkResp, retError := w.eng.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseMutableState.GetCurrentBranch(),
		ForkNodeID:      decisionFinishEventID,
		Info:            historyGarbageCleanupInfo(domainID, workflowID, resetAttr.GetNewRunId()),
	})
	if retError != nil {
		return
	}
	defer func() {
		w.eng.historyV2Mgr.CompleteForkBranch(&persistence.CompleteForkBranchRequest{
			BranchToken: newMsBuilder.GetExecutionInfo().GetCurrentBranch(),
			Success:     retError == nil,
		})
	}()
	newMsBuilder.GetExecutionInfo().BranchToken = forkResp.NewBranchToken

	// prepare to append history to new branch
	hBuilder := newHistoryBuilder(newMsBuilder, w.replicator.logger)
	hBuilder.history = historyAfterReset
	newMsBuilder.SetHistoryBuilder(hBuilder)

	retError = currContext.resetWorkflowExecution(currMutableState, false, nil, nil, newMsBuilder, newRunTransferTasks, newRunTimerTasks, nil, nil, baseExecution.GetRunId(), baseMutableState.GetNextEventID(), prevRunVersion)
	if retError != nil {
		return
	}
	now := time.Unix(0, lastEvent.GetTimestamp())
	w.replicator.notify(request.GetSourceCluster(), now, newRunTransferTasks, newRunTimerTasks)
	return nil
}

func (w *workflowResetorImpl) replicateResetEvent(baseMutableState mutableState, baseExecution *workflow.WorkflowExecution, newRunHistory []*workflow.HistoryEvent, forkEventVersion int64) (newMsBuilder mutableState, transferTasks, timerTasks []persistence.Task, retError error) {
	domainID := baseMutableState.GetExecutionInfo().DomainID
	workflowID := baseMutableState.GetExecutionInfo().WorkflowID
	firstEvent := newRunHistory[0]

	decisionFinishEventID := firstEvent.GetEventId()
	resetAttr := firstEvent.GetDecisionTaskFailedEventAttributes()

	clusterMetadata := w.eng.shard.GetService().GetClusterMetadata()
	requestID := uuid.New()
	var sBuilder stateBuilder
	var wfTimeoutSecs int64

	// replay old history from beginning of the baseRun upto decisionFinishEventID(exclusive)
	var nextPageToken []byte
	var lastEvent *workflow.HistoryEvent
	readReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseMutableState.GetCurrentBranch(),
		MinEventID:    common.FirstEventID,
		MaxEventID:    decisionFinishEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nextPageToken,
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
					clusterMetadata.GetCurrentClusterName(),
					w.eng.shard,
					w.eng.shard.GetEventsCache(),
					w.eng.logger,
					firstEvent.GetVersion(),
				)
				newMsBuilder.GetExecutionInfo().EventStoreVersion = persistence.EventStoreVersionV2
				sBuilder = newStateBuilder(w.eng.shard, newMsBuilder, w.eng.logger)
			}
			_, _, _, retError = sBuilder.applyEvents(domainID, requestID, *baseExecution, events, nil, persistence.EventStoreVersionV2, 0)
			if retError != nil {
				return
			}
		}
		newMsBuilder.IncrementHistorySize(readResp.Size)
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

	// always enforce the attempt to zero so that we can always schedule a new decision(skip trasientDecision logic)
	di, _ := newMsBuilder.GetInFlightDecisionTask()
	di.Attempt = 0
	newMsBuilder.UpdateDecision(di)

	lastEvent = newRunHistory[len(newRunHistory)-1]
	// replay new history (including decisionTaskScheduled)
	_, _, _, retError = sBuilder.applyEvents(domainID, requestID, *baseExecution, newRunHistory, nil, persistence.EventStoreVersionV2, 0)
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
	di, _ = newMsBuilder.GetPendingDecision(decisionScheduledID)
	transferTasks = append(transferTasks, &persistence.DecisionTask{
		DomainID:         domainID,
		TaskList:         di.TaskList,
		ScheduleID:       di.ScheduleID,
		RecordVisibility: true,
	})

	newMsBuilder.GetExecutionInfo().SetLastFirstEventID(firstEvent.GetEventId())
	newMsBuilder.UpdateReplicationStateLastEventID(clusterMetadata.GetCurrentClusterName(), lastEvent.GetVersion(), lastEvent.GetEventId())
	return
}
