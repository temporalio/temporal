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
	ctx "context"
	"time"

	"github.com/pborman/uuid"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	nDCBranchMgrProvider func(
		context workflowExecutionContext,
		mutableState mutableState,
		logger log.Logger,
	) nDCBranchMgr

	nDCStateRebuilderProvider func(
		mutableState mutableState,
		branchIndex int,
		logger log.Logger,
	) nDCStateRebuilder

	nDCHistoryReplicator struct {
		shard             ShardContext
		historyEngine     *historyEngineImpl
		historyCache      *historyCache
		domainCache       cache.DomainCache
		historySerializer persistence.PayloadSerializer
		historyV2Mgr      persistence.HistoryV2Manager
		clusterMetadata   cluster.Metadata
		metricsClient     metrics.Client
		logger            log.Logger
		resetor           workflowResetor

		getNewBranchMgr      nDCBranchMgrProvider
		getNewStateRebuilder nDCStateRebuilderProvider
		getNewStateBuilder   stateBuilderProvider
		getNewMutableState   mutableStateProvider
	}
)

func newNDCHistoryReplicator(
	shard ShardContext,
	historyEngine *historyEngineImpl,
	historyCache *historyCache,
	domainCache cache.DomainCache,
	historyV2Mgr persistence.HistoryV2Manager,
	logger log.Logger,
) *nDCHistoryReplicator {

	replicator := &nDCHistoryReplicator{
		shard:             shard,
		historyEngine:     historyEngine,
		historyCache:      historyCache,
		domainCache:       domainCache,
		historySerializer: persistence.NewPayloadSerializer(),
		historyV2Mgr:      historyV2Mgr,
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		metricsClient:     shard.GetMetricsClient(),
		logger:            logger.WithTags(tag.ComponentHistoryReplicator),

		getNewBranchMgr: func(context workflowExecutionContext, mutableState mutableState, logger log.Logger) nDCBranchMgr {
			return newNDCBranchMgr(context, mutableState, shard, historyV2Mgr, logger)
		},
		getNewStateRebuilder: func(mutableState mutableState, branchIndex int, logger log.Logger) nDCStateRebuilder {
			return newNDCStateRebuilder(mutableState, branchIndex, shard, historyV2Mgr, logger)
		},
		getNewStateBuilder: func(msBuilder mutableState, logger log.Logger) stateBuilder {
			return newStateBuilder(shard, msBuilder, logger)
		},
		getNewMutableState: func(version int64, logger log.Logger) mutableState {
			return newMutableStateBuilderWithVersionHistories(
				shard.GetService().GetClusterMetadata().GetCurrentClusterName(),
				shard,
				shard.GetEventsCache(),
				logger,
				version,
			)
		},
	}
	replicator.resetor = nil // TODO wire v2 history replicator with workflow reseter

	return replicator
}

func (r *nDCHistoryReplicator) ApplyEvents(
	ctx ctx.Context,
	request *h.ReplicateEventsRequest,
) (retError error) {

	startTime := time.Now()
	task, err := newNDCReplicationTask(r.clusterMetadata, startTime, r.logger, request)
	if err != nil {
		return err
	}

	context, release, err := r.historyCache.getOrCreateWorkflowExecutionWithTimeout(
		ctx,
		task.domainID,
		task.execution,
	)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() { release(retError) }()

	return r.applyEvents(ctx, context, task)
}

func (r *nDCHistoryReplicator) applyEvents(
	ctx ctx.Context,
	context workflowExecutionContext,
	task nDCReplicationTask,
) error {

	switch task.getFirstEvent().GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		_, err := context.loadWorkflowExecution()
		switch err.(type) {
		case nil:
			r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
			return nil
		case *shared.EntityNotExistsError:
			// mutable state not created, proceed
			return r.applyStartEvents(ctx, context, task)
		default:
			// unable to get mutable state, return err so we can retry the task later
			return err
		}

	default:
		// apply events, other than simple start workflow execution
		// the continue as new + start workflow execution combination will also be processed here
		mutableState, err := context.loadWorkflowExecution()
		switch err.(type) {
		case nil:
			branchIndex, err := r.applyNonStartEventsBranchChecking(ctx, context, mutableState, task)
			if err != nil {
				return err
			}

			doContinue, err := r.applyNonStartEventsEventIDChecking(ctx, context, mutableState, branchIndex, task)
			if err != nil || !doContinue {
				return err
			}

			mutableState, err = r.applyNonStartEventsPrepareMutableState(ctx, context, mutableState, branchIndex, task)
			if err != nil {
				return err
			}

			if mutableState.GetVersionHistories().GetCurrentBranchIndex() == branchIndex {
				return r.applyNonStartEventsToCurrentBranch(ctx, context, mutableState, task)
			}
			return r.applyNonStartEventsToNoneCurrentBranch(ctx, context, mutableState, branchIndex, task)
		case *shared.EntityNotExistsError:
			// mutable state not created, proceed
			return r.applyNonStartEventsMissingMutableState(ctx, context, task)
		default:
			// unable to get mutable state, return err so we can retry the task later
			return err
		}
	}
}

func (r *nDCHistoryReplicator) applyStartEvents(
	ctx ctx.Context,
	context workflowExecutionContext,
	task nDCReplicationTask,
) (retError error) {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	msBuilder := r.getNewMutableState(task.getVersion(), task.getLogger())
	stateBuilder := r.getNewStateBuilder(msBuilder, task.getLogger())

	// use state builder for workflow mutable state mutation
	_, _, _, err := stateBuilder.applyEvents(
		task.getDomainID(),
		requestID,
		task.getExecution(),
		task.getEvents(),
		task.getNewRunEvents(),
		persistence.EventStoreVersionV2, // 3+ DC will only use event store version 2
		persistence.EventStoreVersionV2, // 3+ DC will only use event store version 2
	)
	if err != nil {
		return err
	}

	_, _, err = context.appendFirstBatchEventsForStandby(msBuilder, task.getEvents())
	if err != nil {
		return err
	}

	// workflow passive side logic should not generate any replication task
	var replicationTasks []persistence.Task // passive side generates no replication tasks
	transferTasks := stateBuilder.getTransferTasks()
	timerTasks := stateBuilder.getTimerTasks()
	defer func() {
		if retError == nil {
			r.notify(task.getSourceCluster(), task.getEventTime(), transferTasks, timerTasks)
		}
	}()

	// try to create the workflow execution
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = context.createWorkflowExecution(
		msBuilder, task.getSourceCluster(), nDCCreateReplicationTask, task.getEventTime(),
		transferTasks, replicationTasks, timerTasks,
		createMode, prevRunID, prevLastWriteVersion,
	)
	if err == nil {
		return nil
	}
	if _, ok := err.(*persistence.WorkflowExecutionAlreadyStartedError); !ok {
		return err
	}

	// we have WorkflowExecutionAlreadyStartedError
	errExist := err.(*persistence.WorkflowExecutionAlreadyStartedError)
	currentRunID := errExist.RunID
	if currentRunID == task.getRunID() {
		return nil
	}

	currentLastEventID, currentLastWriteVersion, currentLastEventTaskID, err := r.getWorkflowVersionTaskID(
		ctx,
		task.getDomainID(),
		task.getWorkflowID(),
		currentRunID,
	)
	if err != nil {
		return err
	}

	if currentLastWriteVersion > task.getVersion() {
		// workflow to be created has lower version than current workflow
		if err := r.convertWorkflowToZombie(msBuilder.GetExecutionInfo()); err != nil {
			return err
		}
		return context.createWorkflowExecution(
			msBuilder, task.getSourceCluster(), nDCCreateReplicationTask, task.getEventTime(),
			transferTasks, replicationTasks, timerTasks,
			createMode, prevRunID, prevLastWriteVersion,
		)
	}
	if currentLastWriteVersion == task.getVersion() {
		// we need to check the current workflow execution
		if currentLastEventTaskID > task.getLastEvent().GetTaskId() {
			// although the versions are the same, the event task ID indicates that
			// the workflow to be created is before the current workflow
			if err := r.convertWorkflowToZombie(msBuilder.GetExecutionInfo()); err != nil {
				return err
			}
			return context.createWorkflowExecution(
				msBuilder, task.getSourceCluster(), nDCCreateReplicationTask, task.getEventTime(),
				transferTasks, replicationTasks, timerTasks,
				createMode, prevRunID, prevLastWriteVersion,
			)
		}
		return newRetryTaskErrorWithHint(ErrRetryExistingWorkflowMsg, task.getDomainID(),
			task.getWorkflowID(), currentRunID, currentLastEventID+1)
	}

	// currentStartVersion < incomingVersion
	// this can happen during the failover; since we have no idea
	// whether the remote active cluster is aware of the current running workflow,
	// the only thing we can do is to terminate the current workflow and
	// start the new workflow from the request
	_, err = r.terminateOrZombifyWorkflow(ctx, task.getDomainID(), task.getWorkflowID(), currentRunID, task.getVersion())
	if err != nil {
		return err
	}

	createMode = persistence.CreateWorkflowModeWorkflowIDReuse
	prevRunID = currentRunID
	prevLastWriteVersion = task.getVersion()
	return context.createWorkflowExecution(
		msBuilder, task.getSourceCluster(), nDCCreateReplicationTask, task.getEventTime(),
		transferTasks, replicationTasks, timerTasks,
		createMode, prevRunID, prevLastWriteVersion,
	)
}

func (r *nDCHistoryReplicator) applyNonStartEventsBranchChecking(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	task nDCReplicationTask,
) (int, error) {

	incomingVersionHistory := task.getVersionHistory()
	branchMgr := r.getNewBranchMgr(context, mutableState, task.getLogger())
	versionHistoryIndex, err := branchMgr.prepareVersionHistory(
		ctx,
		incomingVersionHistory,
	)
	if err != nil {
		return 0, err
	}
	return versionHistoryIndex, nil

}

func (r *nDCHistoryReplicator) applyNonStartEventsEventIDChecking(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	branchIndex int,
	task nDCReplicationTask,
) (bool, error) {

	versionHistories := mutableState.GetVersionHistories()
	versionHistory, err := versionHistories.GetVersionHistory(branchIndex)
	if err != nil {
		return false, err
	}
	lastVersionHistoryItem, err := versionHistory.GetLastItem()
	if err != nil {
		return false, err
	}
	nextEventID := lastVersionHistoryItem.GetEventID() + 1

	if task.getFirstEvent().GetEventId() < nextEventID {
		// duplicate replication task
		r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
		return false, nil
	}
	if task.getFirstEvent().GetEventId() > nextEventID {
		return false, newRetryTaskErrorWithHint(
			ErrRetryBufferEventsMsg,
			task.getDomainID(),
			task.getWorkflowID(),
			task.getRunID(),
			lastVersionHistoryItem.GetEventID()+1,
		)
	}
	// task.getFirstEvent().GetEventId() == nextEventID
	return true, nil
}

func (r *nDCHistoryReplicator) applyNonStartEventsPrepareMutableState(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	branchIndex int,
	task nDCReplicationTask,
) (mutableState, error) {

	versionHistories := mutableState.GetVersionHistories()
	currentBranchIndex := versionHistories.GetCurrentBranchIndex()

	// replication task to be applied to current branch
	if branchIndex == currentBranchIndex {
		return mutableState, nil
	}

	currentVersionHistory, err := versionHistories.GetVersionHistory(currentBranchIndex)
	if err != nil {
		return nil, err
	}
	currentLastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return nil, err
	}

	if task.getVersion() < currentLastItem.GetVersion() {
		return mutableState, nil
	}
	if task.getVersion() == currentLastItem.GetVersion() {
		return nil, &shared.BadRequestError{
			Message: "replication task with version == current branch last write version cannot be applied to non current branch",
		}
	}

	// task.getVersion() > currentLastItem
	// incoming replication task, after application, will become the current branch
	// (because higher version wins), we need to rebuild the mutable state for that
	stateRebuilder := r.getNewStateRebuilder(mutableState, branchIndex, task.getLogger())
	rebuildMutableState, err := stateRebuilder.rebuild(ctx, uuid.New())
	if err != nil {
		return nil, err
	}
	return rebuildMutableState, nil
}

func (r *nDCHistoryReplicator) applyNonStartEventsToCurrentBranch(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	task nDCReplicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.getNewStateBuilder(mutableState, task.getLogger())
	_, _, newRunMutableState, err := stateBuilder.applyEvents(
		task.getDomainID(),
		requestID,
		task.getExecution(),
		task.getEvents(),
		task.getNewRunEvents(),
		nDCMutableStateEventStoreVersion,
		nDCMutableStateEventStoreVersion,
	)
	if err != nil {
		return err
	}

	// we need to check the current workflow execution
	currentRunID, err := r.getCurrentWorkflowRunID(ctx,
		task.getDomainID(),
		task.getWorkflowID(),
	)
	if err != nil {
		return err
	}
	_, currentLastWriteVersion, currentLastEventTaskID, err := r.getWorkflowVersionTaskID(
		ctx,
		task.getDomainID(),
		task.getWorkflowID(),
		currentRunID,
	)
	if err != nil {
		return err
	}

	if task.getVersion() > currentLastWriteVersion ||
		(task.getVersion() == currentLastWriteVersion && task.getLastEvent().GetTaskId() > currentLastEventTaskID) {
		// replication task happens after current workflow
		// after application of the replication task, current workflow should be terminated or zombified
		_, err = r.terminateOrZombifyWorkflow(
			ctx,
			task.getDomainID(),
			task.getWorkflowID(),
			currentRunID,
			task.getVersion(),
		)
		if err != nil {
			return err
		}
	} else {
		// task.getVersion() < currentLastWriteVersion ||
		// (task.getVersion() == currentLastWriteVersion && task.getLastEvent().GetTaskId() < currentLastEventTaskID)
		// replication task happens before current workflow
		// this workflow should be a zombie
		if err := r.convertWorkflowToZombie(mutableState.GetExecutionInfo()); err != nil {
			return err
		}
	}

	// TODO we need to handle zombie workflow which tries to complete and do continue as new

	// TODO need to handle compare and swap current record

	// TODO logic below is placeholder ONLY
	if newRunMutableState != nil {
		// Generate a transaction ID for appending events to history
		transactionID, err := r.shard.GetNextTransferTaskID()
		if err != nil {
			return err
		}
		// continueAsNew
		err = context.appendFirstBatchHistoryForContinueAsNew(newRunMutableState, transactionID)
		if err != nil {
			return err
		}
	}

	err = context.replicateWorkflowExecution(
		task.getRequest(),
		stateBuilder.getTransferTasks(),
		stateBuilder.getTimerTasks(),
		task.getLastEvent().GetEventId(),
		task.getEventTime(),
	)
	if err == nil {
		r.notify(task.getSourceCluster(), task.getEventTime(), stateBuilder.getTransferTasks(), stateBuilder.getTimerTasks())
	}
	return err
}

func (r *nDCHistoryReplicator) applyNonStartEventsToNoneCurrentBranch(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	branchIndex int,
	task nDCReplicationTask,
) error {

	// TODO handle continue as new
	transactionID, err := r.shard.GetNextTransferTaskID()
	if err != nil {
		return err
	}
	doLastEventValidation := true
	if _, _, err = context.appendHistoryEvents(
		task.getEvents(),
		transactionID,
		doLastEventValidation,
		nDCCreateReplicationTask,
		nil,
	); err != nil {
		return err
	}

	versionHistoryItem := persistence.NewVersionHistoryItem(
		task.getLastEvent().GetEventId(),
		task.getLastEvent().GetVersion(),
	)
	versionHistory, err := mutableState.GetVersionHistories().GetVersionHistory(branchIndex)
	if err != nil {
		return err
	}
	if err = versionHistory.AddOrUpdateItem(versionHistoryItem); err != nil {
		return err
	}

	// TODO persist mutable state by bypassing current record
	panic("")
}

func (r *nDCHistoryReplicator) applyNonStartEventsMissingMutableState(
	ctx ctx.Context,
	context workflowExecutionContext,
	task nDCReplicationTask,
) error {

	resendTaskErr := newRetryTaskErrorWithHint(
		ErrWorkflowNotFoundMsg,
		task.getDomainID(),
		task.getWorkflowID(),
		task.getRunID(),
		common.FirstEventID,
	)

	// for non reset workflow execution replication task, just do re-application
	if !task.getRequest().GetResetWorkflow() {
		return resendTaskErr
	}

	// reset workflow execution, this requires some special handling

	// we need to check the current workflow execution
	currentRunID, err := r.getCurrentWorkflowRunID(ctx,
		task.getDomainID(),
		task.getWorkflowID(),
	)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return err
		}
		// err is EntityNotExistsError
		return resendTaskErr
	}
	_, currentMutableState, currentRelease, err := r.getWorkflowContextMutableState(
		ctx,
		task.getDomainID(),
		task.getWorkflowID(),
		currentRunID,
	)
	if err != nil {
		return err
	}
	defer func() { currentRelease(nil) }() // code below only only do read operation

	currentVersionHistory, err := currentMutableState.GetVersionHistories().GetVersionHistory(
		currentMutableState.GetVersionHistories().GetCurrentBranchIndex(),
	)
	if err != nil {
		return err
	}
	currentLastItem, err := currentVersionHistory.GetLastItem()
	if err != nil {
		return err
	}
	currentLastEventID := currentLastItem.GetEventID()
	currentLastWriteVersion := currentLastItem.GetVersion()
	currentLastEventTaskID := currentMutableState.GetExecutionInfo().LastEventTaskID
	currentStillRunning := currentMutableState.IsWorkflowExecutionRunning()
	currentRelease(nil) // all variables are read, release lock

	if currentLastWriteVersion > task.getVersion() {
		// regard reset workflow replication as normal backfill, since task version is smaller
		return resendTaskErr
	}
	if currentLastWriteVersion < task.getVersion() {
		if currentStillRunning {
			if _, err := r.terminateOrZombifyWorkflow(
				ctx,
				task.getDomainID(),
				task.getWorkflowID(),
				currentRunID,
				task.getVersion(),
			); err != nil {
				return err
			}
		}
		return r.resetor.ApplyResetEvent(ctx, task.getRequest(), task.getDomainID(), task.getWorkflowID(), currentRunID)
	}

	// currentLastWriteVersion == incomingVersion
	if task.getLastEvent().GetTaskId() < currentLastEventTaskID {
		// regard reset workflow replication as normal backfill, since task ID is smaller
		return resendTaskErr
	}

	// task.getLastEvent().GetTaskId() >= currentLastEventTaskID
	if currentStillRunning {
		return newRetryTaskErrorWithHint(
			ErrWorkflowNotFoundMsg,
			task.getDomainID(),
			task.getWorkflowID(),
			currentRunID,
			currentLastEventID+1,
		)
	}
	return r.resetor.ApplyResetEvent(ctx, task.getRequest(), task.getDomainID(), task.getWorkflowID(), currentRunID)
}

func (r *nDCHistoryReplicator) terminateOrZombifyWorkflow(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
	incomingVersion int64,
) (bool, error) {

	// TODO if the workflow to be terminated has last write version being local active
	//  then use active logic to terminate this workflow
	// TODO if the workflow to be terminated's last write version being remote active
	//  then turn this workflow into a zombie

	return false, nil
}

func (r *nDCHistoryReplicator) getWorkflowVersionTaskID(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (int64, int64, int64, error) {

	_, mutableState, release, err := r.getWorkflowContextMutableState(
		ctx, domainID, workflowID, runID,
	)
	if err != nil {
		return 0, 0, 0, err
	}
	defer func() { release(nil) }() // code below only only do read operation

	versionHistory, err := mutableState.GetVersionHistories().GetVersionHistory(
		mutableState.GetVersionHistories().GetCurrentBranchIndex(),
	)
	if err != nil {
		return 0, 0, 0, err
	}
	lastItem, err := versionHistory.GetLastItem()
	if err != nil {
		return 0, 0, 0, err
	}
	lastEventTaskID := mutableState.GetExecutionInfo().LastEventTaskID

	return lastItem.GetEventID(), lastItem.GetVersion(), lastEventTaskID, nil
}

func (r *nDCHistoryReplicator) convertWorkflowToZombie(
	executionInfo *persistence.WorkflowExecutionInfo,
) error {

	return executionInfo.UpdateWorkflowStateCloseStatus(
		persistence.WorkflowStateZombie,
		persistence.WorkflowCloseStatusNone,
	)
}

func (r *nDCHistoryReplicator) getCurrentWorkflowRunID(
	ctx ctx.Context,
	domainID string,
	workflowID string,
) (string, error) {

	resp, err := r.shard.GetExecutionManager().GetCurrentExecution(
		&persistence.GetCurrentExecutionRequest{
			DomainID:   domainID,
			WorkflowID: workflowID,
		},
	)
	if err != nil {
		return "", err
	}
	return resp.RunID, nil
}

func (r *nDCHistoryReplicator) getWorkflowContextMutableState(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (workflowExecutionContext, mutableState, releaseWorkflowExecutionFunc, error) {

	// we need to check the current workflow execution
	context, release, err := r.historyCache.getOrCreateWorkflowExecutionWithTimeout(
		ctx,
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		// no matter what error happen, we need to retry
		release(err)
		return nil, nil, nil, err
	}
	return context, msBuilder, release, nil
}

func (r *nDCHistoryReplicator) notify(
	clusterName string,
	now time.Time,
	transferTasks []persistence.Task,
	timerTasks []persistence.Task,
) {

	now = now.Add(-r.shard.GetConfig().StandbyClusterDelay())

	r.shard.SetCurrentTime(clusterName, now)
	r.historyEngine.txProcessor.NotifyNewTask(clusterName, transferTasks)
	r.historyEngine.timerProcessor.NotifyNewTimers(clusterName, now, timerTasks)
}
