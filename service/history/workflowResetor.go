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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflowResetor_mock.go

package history

import (
	"context"
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	// Deprecated: use workflowResetter instead when NDC is applies to all workflows
	workflowResetor interface {
		ResetWorkflowExecution(
			ctx context.Context,
			resetRequest *workflowservice.ResetWorkflowExecutionRequest,
			baseContext workflowExecutionContext,
			baseMutableState mutableState,
			currContext workflowExecutionContext,
			currMutableState mutableState,
		) (response *historyservice.ResetWorkflowExecutionResponse, retError error)

		ApplyResetEvent(
			ctx context.Context,
			request *historyservice.ReplicateEventsRequest,
			namespaceID, workflowID, currentRunID string,
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

// ResetWorkflowExecution only allows resetting to workflowTaskCompleted, but exclude that batch of workflowTaskCompleted/workflowTaskFailed/workflowTaskTimeout.
// It will then fail the workflow task with cause of "reset_workflow"
func (w *workflowResetorImpl) ResetWorkflowExecution(
	ctx context.Context,
	request *workflowservice.ResetWorkflowExecutionRequest,
	baseContext workflowExecutionContext,
	baseMutableState mutableState,
	currContext workflowExecutionContext,
	currMutableState mutableState,
) (*historyservice.ResetWorkflowExecutionResponse, error) {

	namespaceEntry, retError := w.eng.shard.GetNamespaceCache().GetNamespace(request.GetNamespace())
	if retError != nil {
		return nil, retError
	}

	resetNewRunID := uuid.New()
	response := &historyservice.ResetWorkflowExecutionResponse{
		RunId: resetNewRunID,
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
		ctx, namespaceEntry, baseMutableState, currMutableState,
		request.GetReason(), request.GetWorkflowTaskFinishEventId(), request.GetRequestId(), resetNewRunID,
	)
	if retError != nil {
		return response, retError
	}

	retError = w.checkNamespaceStatus(newMutableState, currPrevRunVersion, namespaceEntry.GetInfo().Name)
	if retError != nil {
		return response, retError
	}

	// update replication and generate replication task
	currReplicationTasks, newReplicationTasks, err := w.generateReplicationTasksForReset(
		currTerminated, currMutableState, newMutableState, namespaceEntry,
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

func (w *workflowResetorImpl) checkNamespaceStatus(newMutableState mutableState, prevRunVersion int64, namespace string) error {
	if newMutableState.GetReplicationState() != nil || newMutableState.GetVersionHistories() != nil {
		clusterMetadata := w.eng.shard.GetService().GetClusterMetadata()
		currentVersion := newMutableState.GetCurrentVersion()
		if currentVersion < prevRunVersion {
			return serviceerror.NewNamespaceNotActive(
				namespace,
				clusterMetadata.GetCurrentClusterName(),
				clusterMetadata.ClusterNameForFailoverVersion(prevRunVersion),
			)
		}
		activeCluster := clusterMetadata.ClusterNameForFailoverVersion(currentVersion)
		currentCluster := clusterMetadata.GetCurrentClusterName()
		if activeCluster != currentCluster {
			return serviceerror.NewNamespaceNotActive(namespace, currentCluster, activeCluster)
		}
	}
	return nil
}

func (w *workflowResetorImpl) validateResetWorkflowBeforeReplay(baseMutableState, currMutableState mutableState) error {
	if len(currMutableState.GetPendingChildExecutionInfos()) > 0 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("reset is not allowed when current workflow has pending child workflow."))
	}
	if currMutableState.IsWorkflowExecutionRunning() {
		return serviceerror.NewInternal(fmt.Sprintf("current workflow should already been terminated"))
	}
	return nil
}

func (w *workflowResetorImpl) validateResetWorkflowAfterReplay(newMutableState mutableState) error {
	if retError := newMutableState.CheckResettable(); retError != nil {
		return retError
	}
	if !newMutableState.HasInFlightWorkflowTask() {
		return serviceerror.NewInternal(fmt.Sprintf("can't find the last started workflow task"))
	}
	if newMutableState.HasBufferedEvents() {
		return serviceerror.NewInternal(fmt.Sprintf("replay history shouldn't see any bufferred events"))
	}
	if newMutableState.IsStickyTaskQueueEnabled() {
		return serviceerror.NewInternal(fmt.Sprintf("replay history shouldn't have stikyness"))
	}
	return nil
}

// Fail the started activities
func (w *workflowResetorImpl) failStartedActivities(msBuilder mutableState) error {
	for _, ai := range msBuilder.GetPendingActivityInfos() {
		if ai.StartedId != common.EmptyEventID {
			// this means the activity has started but not completed, we need to fail the activity
			if _, err := msBuilder.AddActivityTaskFailedEvent(
				ai.ScheduleId,
				ai.StartedId,
				failure.NewResetWorkflowFailure("reset workflow", ai.LastHeartbeatDetails),
				enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
				ai.StartedIdentity,
			); err != nil {
				// Unable to add ActivityTaskFailed event to history
				return serviceerror.NewInternal("Unable to add ActivityTaskFailed event to mutableState.")
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
		if ai.StartedId != common.EmptyEventID {
			return nil, serviceerror.NewInternal("started activities should have been failed.")
		}
		t := &persistence.ActivityTask{
			NamespaceID: exeInfo.NamespaceID,
			TaskQueue:   exeInfo.TaskQueue,
			ScheduleID:  ai.ScheduleId,
		}
		tasks = append(tasks, t)
	}
	return tasks, nil
}

// TODO: @shreyassrivatsan reduce number of return parameters
func (w *workflowResetorImpl) buildNewMutableStateForReset(
	ctx context.Context,
	namespaceEntry *cache.NamespaceCacheEntry,
	baseMutableState, currMutableState mutableState,
	resetReason string,
	resetWorkflowTaskCompletedEventID int64,
	requestedID, newRunID string,
) (newMutableState mutableState, newHistorySize int64, newTransferTasks, newTimerTasks []persistence.Task, retError error) {

	namespaceID := baseMutableState.GetExecutionInfo().NamespaceID
	workflowID := baseMutableState.GetExecutionInfo().WorkflowID
	baseRunID := baseMutableState.GetExecutionInfo().RunID

	// replay history to reset point(exclusive) to rebuild mutableState
	forkEventVersion, runTimeoutSecs, receivedSignals, continueRunID, newStateBuilder, historySize, retError := w.replayHistoryEvents(
		namespaceEntry, resetWorkflowTaskCompletedEventID, requestedID, baseMutableState, newRunID,
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

	// set the new mutable state with the version in namespace
	if retError = newMutableState.UpdateCurrentVersion(
		namespaceEntry.GetFailoverVersion(),
		false,
	); retError != nil {
		return
	}

	// failed the in-flight workflow task(started).
	// Note that we need to ensure WorkflowTaskFailed event is appended right after WorkflowTaskStarted event
	workflowTask, _ := newMutableState.GetInFlightWorkflowTask()

	_, err := newMutableState.AddWorkflowTaskFailedEvent(workflowTask.ScheduleID, workflowTask.StartedID, enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW, nil,
		identityHistoryService, resetReason, baseRunID, newRunID, forkEventVersion)
	if err != nil {
		retError = serviceerror.NewInternal("Failed to add workflowTask failed event.")
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
	newTimerTasks, retError = w.generateTimerTasksForReset(newMutableState, runTimeoutSecs, needActivityTimer)
	if retError != nil {
		return
	}
	// replay received signals back to mutableState/history:
	retError = w.replayReceivedSignals(ctx, resetWorkflowTaskCompletedEventID, receivedSignals, continueRunID, newMutableState, currMutableState)
	if retError != nil {
		return
	}

	// we always schedule a new workflowTask after reset
	workflowTask, err = newMutableState.AddWorkflowTaskScheduledEvent(false)
	if err != nil {
		retError = serviceerror.NewInternal("Failed to add workflowTask scheduled event.")
		return
	}

	// TODO workflow reset logic should use built-in task management
	newTransferTasks = append(newTransferTasks,
		&persistence.WorkflowTask{
			NamespaceID: namespaceID,
			TaskQueue:   workflowTask.TaskQueue.GetName(),
			ScheduleID:  workflowTask.ScheduleID,
		},
		&persistence.RecordWorkflowStartedTask{},
	)

	// fork a new history branch
	baseBranchToken, err := baseMutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, 0, nil, nil, err
	}

	shardId := w.eng.shard.GetShardID()
	forkResp, retError := w.eng.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      resetWorkflowTaskCompletedEventID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, newRunID),
		ShardID:         &shardId,
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
			w.eng.shard.GetNamespaceCache(),
			currMutableState.GetExecutionInfo().NamespaceID,
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
	namespaceEntry *cache.NamespaceCacheEntry,
) ([]persistence.Task, []persistence.Task, error) {
	var currRepTasks, insertRepTasks []persistence.Task
	if newMutableState.GetReplicationState() != nil || newMutableState.GetVersionHistories() != nil {
		if terminateCurr {
			// we will generate 2 replication tasks for this case
			firstEventIDForCurr, err := w.setEventIDsWithHistory(currMutableState)
			if err != nil {
				return nil, nil, err
			}
			if namespaceEntry.GetReplicationPolicy() == cache.ReplicationPolicyMultiCluster {
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
		if namespaceEntry.GetReplicationPolicy() == cache.ReplicationPolicyMultiCluster {
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

// replay signals in the base run, and also signals in all the runs along the chain of continueAsNew
func (w *workflowResetorImpl) replayReceivedSignals(
	ctx context.Context,
	workflowTaskFinishEventID int64,
	receivedSignals []*historypb.HistoryEvent,
	continueRunID string,
	newMutableState, currMutableState mutableState,
) error {
	for _, se := range receivedSignals {
		sigReq := &workflowservice.SignalWorkflowExecutionRequest{
			SignalName: se.GetWorkflowExecutionSignaledEventAttributes().SignalName,
			Identity:   se.GetWorkflowExecutionSignaledEventAttributes().Identity,
			Input:      se.GetWorkflowExecutionSignaledEventAttributes().Input,
		}
		newMutableState.AddWorkflowExecutionSignaled( // nolint:errcheck
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
			continueExe := commonpb.WorkflowExecution{
				WorkflowId: newMutableState.GetExecutionInfo().WorkflowID,
				RunId:      continueRunID,
			}
			continueContext, continueRelease, err := w.eng.historyCache.getOrCreateWorkflowExecution(ctx, newMutableState.GetExecutionInfo().NamespaceID, continueExe)
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
		shardId := w.eng.shard.GetShardID()
		readReq := &persistence.ReadHistoryBranchRequest{
			BranchToken: continueBranchToken,
			MinEventID:  common.FirstEventID,
			// NOTE: read through history to the end so that we can collect all the received signals
			MaxEventID:    continueMutableState.GetNextEventID(),
			PageSize:      defaultHistoryPageSize,
			NextPageToken: nextPageToken,
			ShardID:       &shardId,
		}
		for {
			var readResp *persistence.ReadHistoryBranchByBatchResponse
			readResp, err := w.eng.historyV2Mgr.ReadHistoryBranchByBatch(readReq)
			// Fail if we don't have enough events to perform the reset, otherwise continue with what we've got.
			if err != nil && (readResp == nil || readResp.LastEventID < workflowTaskFinishEventID) {
				return err
			}
			for _, batch := range readResp.History {
				for _, event := range batch.Events {
					e := event
					if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
						sigReq := &workflowservice.SignalWorkflowExecutionRequest{
							SignalName: e.GetWorkflowExecutionSignaledEventAttributes().SignalName,
							Identity:   e.GetWorkflowExecutionSignaledEventAttributes().Identity,
							Input:      e.GetWorkflowExecutionSignaledEventAttributes().Input,
						}
						newMutableState.AddWorkflowExecutionSignaled( // nolint:errcheck
							sigReq.GetSignalName(), sigReq.GetInput(), sigReq.GetIdentity())
					} else if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
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
	runTimeout time.Duration,
	needActivityTimer bool,
) ([]persistence.Task, error) {
	timerTasks := []persistence.Task{}

	// WF timeout task
	runTimeoutTask := &persistence.WorkflowTimeoutTask{
		VisibilityTimestamp: w.eng.shard.GetTimeSource().Now().Add(runTimeout),
	}
	timerTasks = append(timerTasks, runTimeoutTask)

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

// TODO: @shreyassrivatsan reduce the number of return parameters from this method or return a struct
func (w *workflowResetorImpl) replayHistoryEvents(
	namespaceEntry *cache.NamespaceCacheEntry,
	workflowTaskFinishEventID int64,
	requestID string,
	prevMutableState mutableState,
	newRunID string,
) (forkEventVersion int64, runTimeout time.Duration, receivedSignalsAfterReset []*historypb.HistoryEvent, continueRunID string, sBuilder stateBuilder, newHistorySize int64, retError error) {

	prevExecution := commonpb.WorkflowExecution{
		WorkflowId: prevMutableState.GetExecutionInfo().WorkflowID,
		RunId:      prevMutableState.GetExecutionInfo().RunID,
	}
	namespaceID := prevMutableState.GetExecutionInfo().NamespaceID
	var nextPageToken []byte
	prevBranchToken, err := prevMutableState.GetCurrentBranchToken()
	if err != nil {
		return 0, 0, nil, "", nil, 0, err
	}
	shardId := w.eng.shard.GetShardID()
	readReq := &persistence.ReadHistoryBranchRequest{
		BranchToken: prevBranchToken,
		MinEventID:  common.FirstEventID,
		// NOTE: read through history to the end so that we can keep the received signals
		MaxEventID:    prevMutableState.GetNextEventID(),
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nextPageToken,
		ShardID:       &shardId,
	}
	var resetMutableState *mutableStateBuilder
	var lastBatch []*historypb.HistoryEvent

	for {
		var readResp *persistence.ReadHistoryBranchByBatchResponse
		readResp, retError = w.eng.historyV2Mgr.ReadHistoryBranchByBatch(readReq)
		if retError != nil {
			if readResp == nil || readResp.LastEventID < workflowTaskFinishEventID {
				return
			}
			// Proceed with the reset if we've got sufficient number of events.
			retError = nil
		}

		for _, batch := range readResp.History {
			history := batch.Events
			firstEvent := history[0]
			lastEvent := history[len(history)-1]

			// for saving received signals only
			if firstEvent.GetEventId() >= workflowTaskFinishEventID {
				for _, e := range history {
					if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED {
						receivedSignalsAfterReset = append(receivedSignalsAfterReset, e)
					}
					if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
						attr := e.GetWorkflowExecutionContinuedAsNewEventAttributes()
						continueRunID = attr.GetNewExecutionRunId()
					}
				}
				continue
			}

			lastBatch = history
			if firstEvent.GetEventId() == common.FirstEventID {
				if firstEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
					retError = serviceerror.NewInternal(fmt.Sprintf("first event type is not WorkflowExecutionStarted: %v", firstEvent.GetEventType()))
					return
				}
				runTimeout = timestamp.DurationValue(firstEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowRunTimeout())
				if prevMutableState.GetReplicationState() != nil {
					resetMutableState = newMutableStateBuilderWithReplicationState(
						w.eng.shard,
						w.eng.shard.GetEventsCache(),
						w.eng.logger,
						namespaceEntry,
					)
				} else if prevMutableState.GetVersionHistories() != nil {
					resetMutableState = newMutableStateBuilderWithVersionHistories(
						w.eng.shard,
						w.eng.shard.GetEventsCache(),
						w.eng.logger,
						namespaceEntry,
					)
				} else {
					resetMutableState = newMutableStateBuilder(w.eng.shard, w.eng.shard.GetEventsCache(), w.eng.logger, namespaceEntry)
				}

				sBuilder = newStateBuilder(
					w.eng.shard,
					w.eng.logger,
					resetMutableState,
					func(mutableState mutableState) mutableStateTaskGenerator {
						return newMutableStateTaskGenerator(w.eng.shard.GetNamespaceCache(), w.eng.logger, mutableState)
					},
				)
			}

			// avoid replay this event in stateBuilder which will run into NPE if WF doesn't enable XDC
			if lastEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
				retError = serviceerror.NewInvalidArgument(fmt.Sprintf("wrong WorkflowTaskFinishEventId, cannot replay history to continueAsNew"))
				return
			}

			_, retError = sBuilder.applyEvents(namespaceID, requestID, prevExecution, history, nil, false)
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

	retError = validateLastBatchOfReset(lastBatch, workflowTaskFinishEventID)
	if retError != nil {
		return
	}
	forkEventVersion = lastBatch[len(lastBatch)-1].GetVersion()

	startTime := w.timeSource.Now()
	resetMutableState.executionInfo.RunID = newRunID
	resetMutableState.executionInfo.StartTimestamp = startTime
	resetMutableState.executionInfo.LastUpdatedTimestamp = startTime
	resetMutableState.executionInfo.SetNextEventID(workflowTaskFinishEventID)
	resetMutableState.ClearStickyness()
	return
}

func validateLastBatchOfReset(lastBatch []*historypb.HistoryEvent, workflowTaskFinishEventID int64) error {
	firstEvent := lastBatch[0]
	lastEvent := lastBatch[len(lastBatch)-1]
	if workflowTaskFinishEventID != lastEvent.GetEventId()+1 {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("wrong WorkflowTaskFinishEventId, it must be WorkflowTaskStarted + 1: %v", lastEvent.GetEventId()))
	}

	if lastEvent.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("wrong WorkflowTaskFinishEventId, previous batch doesn't include WorkflowTaskStarted, lastFirstEventId: %v", firstEvent.GetEventId()))
	}

	return nil
}

func validateResetReplicationTask(request *historyservice.ReplicateEventsRequest) (*historypb.WorkflowTaskFailedEventAttributes, error) {
	historyAfterReset := request.History.Events
	if len(historyAfterReset) == 0 || historyAfterReset[0].GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
		return nil, errUnknownReplicationTask
	}
	firstEvent := historyAfterReset[0]
	if firstEvent.GetWorkflowTaskFailedEventAttributes().GetCause() != enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW {
		return nil, errUnknownReplicationTask
	}
	attr := firstEvent.GetWorkflowTaskFailedEventAttributes()
	if attr.GetNewRunId() != request.GetWorkflowExecution().GetRunId() {
		return nil, errUnknownReplicationTask
	}
	return attr, nil
}

func (w *workflowResetorImpl) ApplyResetEvent(
	ctx context.Context,
	request *historyservice.ReplicateEventsRequest,
	namespaceID, workflowID, currentRunID string,
) error {
	var currContext workflowExecutionContext
	var baseMutableState, currMutableState, newMsBuilder mutableState
	var newHistorySize int64
	var newRunTransferTasks, newRunTimerTasks []persistence.Task

	resetAttr, retError := validateResetReplicationTask(request)
	historyAfterReset := request.History.Events
	lastEvent := historyAfterReset[len(historyAfterReset)-1]
	workflowTaskFinishEventID := historyAfterReset[0].GetEventId()
	if retError != nil {
		return retError
	}
	baseExecution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      resetAttr.GetBaseRunId(),
	}

	baseContext, baseRelease, baseErr := w.eng.historyCache.getOrCreateWorkflowExecution(ctx, namespaceID, baseExecution)
	if baseErr != nil {
		return baseErr
	}
	defer func() { baseRelease(retError) }()
	baseMutableState, retError = baseContext.loadWorkflowExecution()
	if retError != nil {
		return retError
	}
	if baseMutableState.GetNextEventID() < workflowTaskFinishEventID {
		// re-replicate the whole new run
		return serviceerrors.NewRetryTask(ErrWorkflowNotFoundMsg, namespaceID, workflowID, resetAttr.GetNewRunId(), common.FirstEventID)
	}

	if currentRunID == resetAttr.GetBaseRunId() {
		currMutableState = baseMutableState
		currContext = baseContext
	} else {
		var currRelease releaseWorkflowExecutionFunc
		currExecution := commonpb.WorkflowExecution{
			WorkflowId: baseExecution.WorkflowId,
			RunId:      currentRunID,
		}
		var currErr error
		currContext, currRelease, currErr = w.eng.historyCache.getOrCreateWorkflowExecution(ctx, namespaceID, currExecution)
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
	shardID := w.eng.shard.GetShardID()
	baseBranchToken, err := baseMutableState.GetCurrentBranchToken()
	if err != nil {
		return err
	}
	forkResp, retError := w.eng.historyV2Mgr.ForkHistoryBranch(&persistence.ForkHistoryBranchRequest{
		ForkBranchToken: baseBranchToken,
		ForkNodeID:      workflowTaskFinishEventID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID, workflowID, resetAttr.GetNewRunId()),
		ShardID:         &shardID,
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
	now := timestamp.TimeValue(lastEvent.GetEventTime())
	notify(w.eng.shard, request.GetSourceCluster(), now)
	return nil
}

// TODO: @shreyassrivatsan reduce number of return parameters from this method
func (w *workflowResetorImpl) replicateResetEvent(
	baseMutableState mutableState,
	baseExecution *commonpb.WorkflowExecution,
	newRunHistory []*historypb.HistoryEvent,
	forkEventVersion int64,
) (newMsBuilder mutableState, newHistorySize int64, transferTasks, timerTasks []persistence.Task, retError error) {
	namespaceID := baseMutableState.GetExecutionInfo().NamespaceID
	workflowID := baseMutableState.GetExecutionInfo().WorkflowID
	firstEvent := newRunHistory[0]

	workflowTaskFinishEventID := firstEvent.GetEventId()
	resetAttr := firstEvent.GetWorkflowTaskFailedEventAttributes()

	requestID := uuid.New()
	var sBuilder stateBuilder
	var runTimeout time.Duration

	namespaceEntry, retError := w.eng.shard.GetNamespaceCache().GetNamespaceByID(namespaceID)
	if retError != nil {
		return
	}

	// replay old history from beginning of the baseRun upto workflowTaskFinishEventID(exclusive)
	var nextPageToken []byte
	var lastEvent *historypb.HistoryEvent
	baseBranchToken, err := baseMutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, 0, nil, nil, err
	}
	shardId := w.eng.shard.GetShardID()
	readReq := &persistence.ReadHistoryBranchRequest{
		BranchToken:   baseBranchToken,
		MinEventID:    common.FirstEventID,
		MaxEventID:    workflowTaskFinishEventID,
		PageSize:      defaultHistoryPageSize,
		NextPageToken: nextPageToken,
		ShardID:       &shardId,
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
				runTimeout = timestamp.DurationValue(firstEvent.GetWorkflowExecutionStartedEventAttributes().GetWorkflowRunTimeout())
				newMsBuilder = newMutableStateBuilderWithReplicationState(
					w.eng.shard,
					w.eng.shard.GetEventsCache(),
					w.eng.logger,
					namespaceEntry,
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
						return newMutableStateTaskGenerator(w.eng.shard.GetNamespaceCache(), w.eng.logger, mutableState)
					},
				)
			}
			_, retError = sBuilder.applyEvents(namespaceID, requestID, *baseExecution, events, nil, false)
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
	if lastEvent.GetEventId() != workflowTaskFinishEventID-1 || lastEvent.GetVersion() != forkEventVersion {
		// re-replicate the whole new run
		retError = serviceerrors.NewRetryTask(ErrWorkflowNotFoundMsg, namespaceID, workflowID, resetAttr.GetNewRunId(), common.FirstEventID)
		return
	}
	startTime := timestamp.TimeValue(firstEvent.GetEventTime())
	newMsBuilder.GetExecutionInfo().RunID = resetAttr.GetNewRunId()
	newMsBuilder.GetExecutionInfo().StartTimestamp = startTime
	newMsBuilder.GetExecutionInfo().LastUpdatedTimestamp = startTime
	newMsBuilder.ClearStickyness()

	// before this, the mutable state is in replay mode
	// need to close / flush the mutable state for new changes
	_, _, retError = newMsBuilder.CloseTransactionAsSnapshot(
		timestamp.TimeValue(lastEvent.GetEventTime()),
		transactionPolicyPassive,
	)
	if retError != nil {
		return
	}

	// always enforce the attempt to 1 so that we can always schedule a new workflow task(skip trasientWorkflowTask logic)
	workflowTask, _ := newMsBuilder.GetInFlightWorkflowTask()
	workflowTask.Attempt = 1
	newMsBuilder.UpdateWorkflowTask(workflowTask)

	// before this, the mutable state is in replay mode
	// need to close / flush the mutable state for new changes
	_, _, retError = newMsBuilder.CloseTransactionAsSnapshot(
		timestamp.TimeValue(lastEvent.GetEventTime()),
		transactionPolicyPassive,
	)
	if retError != nil {
		return
	}

	lastEvent = newRunHistory[len(newRunHistory)-1]
	// replay new history (including workflowTaskScheduled)
	_, retError = sBuilder.applyEvents(namespaceID, requestID, *baseExecution, newRunHistory, nil, false)
	if retError != nil {
		return
	}
	newMsBuilder.GetExecutionInfo().SetNextEventID(lastEvent.GetEventId() + 1)

	actTasks, retError := w.scheduleUnstartedActivities(newMsBuilder)
	if retError != nil {
		return
	}
	transferTasks = append(transferTasks, actTasks...)
	timerTasks, retError = w.generateTimerTasksForReset(newMsBuilder, runTimeout, len(actTasks) > 0)
	if retError != nil {
		return
	}

	// schedule new workflowTask
	workflowTaskScheduledID := newMsBuilder.GetExecutionInfo().WorkflowTaskScheduleID
	workflowTask, _ = newMsBuilder.GetWorkflowTaskInfo(workflowTaskScheduledID)
	transferTasks = append(transferTasks, &persistence.WorkflowTask{
		NamespaceID:      namespaceID,
		TaskQueue:        workflowTask.TaskQueue.GetName(),
		ScheduleID:       workflowTask.ScheduleID,
		RecordVisibility: true,
	})

	newMsBuilder.GetExecutionInfo().SetLastFirstEventID(firstEvent.GetEventId())
	newMsBuilder.UpdateReplicationStateLastEventID(lastEvent.GetVersion(), lastEvent.GetEventId())
	return
}

// FindAutoResetPoint returns the auto reset point
func FindAutoResetPoint(
	timeSource clock.TimeSource,
	badBinaries *namespacepb.BadBinaries,
	autoResetPoints *workflowpb.ResetPoints,
) (string, *workflowpb.ResetPointInfo) {
	if badBinaries == nil || badBinaries.Binaries == nil || autoResetPoints == nil || autoResetPoints.Points == nil {
		return "", nil
	}
	now := timeSource.Now()
	for _, p := range autoResetPoints.Points {
		bin, ok := badBinaries.Binaries[p.GetBinaryChecksum()]
		if ok && p.GetResettable() {
			expireTime := timestamp.TimeValue(p.GetExpireTime())
			if !expireTime.IsZero() && now.After(expireTime) {
				// reset point has expired and we may already deleted the history
				continue
			}
			return bin.GetReason(), p
		}
	}
	return "", nil
}

func getWorkflowCleanupTasks(
	namespaceCache cache.NamespaceCache,
	namespaceID string,
	workflowID string,
) (persistence.Task, persistence.Task, error) {

	var retentionInDays int32
	namespaceEntry, err := namespaceCache.GetNamespaceByID(namespaceID)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); !ok {
			return nil, nil, err
		}
	} else {
		retentionInDays = namespaceEntry.GetRetentionDays(workflowID)
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
