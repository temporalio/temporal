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

var (
	// ErrInvalidDomainID is returned if domain ID is invalid
	ErrInvalidDomainID = &shared.BadRequestError{Message: "invalid domain ID"}
	// ErrInvalidExecution is returned if execution is invalid
	ErrInvalidExecution = &shared.BadRequestError{Message: "invalid execution"}
	// ErrInvalidRunID is returned if run ID is invalid
	ErrInvalidRunID = &shared.BadRequestError{Message: "invalid run ID"}
	// ErrEventIDMismatch is returned if event ID mis-matched
	ErrEventIDMismatch = &shared.BadRequestError{Message: "event ID mismatch"}
	// ErrEventVersionMismatch is returned if event version mis-matched
	ErrEventVersionMismatch = &shared.BadRequestError{Message: "event version mismatch"}
)

type (
	historyReplicatorV2 struct {
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

		getNewConflictResolver conflictResolverProvider
		getNewStateBuilder     stateBuilderProvider
		getNewMutableState     mutableStateProvider
	}
)

func newHistoryReplicatorV2(shard ShardContext, historyEngine *historyEngineImpl, historyCache *historyCache, domainCache cache.DomainCache,
	historyV2Mgr persistence.HistoryV2Manager, logger log.Logger) *historyReplicatorV2 {
	replicator := &historyReplicatorV2{
		shard:             shard,
		historyEngine:     historyEngine,
		historyCache:      historyCache,
		domainCache:       domainCache,
		historySerializer: persistence.NewPayloadSerializer(),
		historyV2Mgr:      historyV2Mgr,
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		metricsClient:     shard.GetMetricsClient(),
		logger:            logger.WithTags(tag.ComponentHistoryReplicator),

		getNewConflictResolver: func(context workflowExecutionContext, logger log.Logger) conflictResolver {
			return nil // TODO this is a skeleton implementation
		},
		getNewStateBuilder: func(msBuilder mutableState, logger log.Logger) stateBuilder {
			return nil // TODO this is a skeleton implementation
		},
		getNewMutableState: func(version int64, logger log.Logger) mutableState {
			return newMutableStateBuilderWithReplicationState(
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

func (r *historyReplicatorV2) ApplyEvents(ctx ctx.Context, request *h.ReplicateEventsRequest) (retError error) {

	startTime := time.Now()
	task, err := newReplicationTask(r.clusterMetadata, startTime, r.logger, request)
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

func (r *historyReplicatorV2) applyEvents(ctx ctx.Context, context workflowExecutionContext,
	task historyReplicationTask) (retError error) {

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
			mutableState, err = r.applyNonStartEventsBranchChecking(ctx, context, mutableState, task)
			if err != nil || mutableState == nil {
				return err
			}
			doContinue, err := r.applyNonStartEventsEventIDChecking(ctx, context, mutableState, task)
			if err != nil || !doContinue {
				return err
			}
			return r.applyNonStartEvents(ctx, context, mutableState, task)
		case *shared.EntityNotExistsError:
			// mutable state not created, proceed
			return r.applyNonStartEventsMissingMutableState(ctx, context, task)
		default:
			// unable to get mutable state, return err so we can retry the task later
			return err
		}
	}
}

func (r *historyReplicatorV2) applyStartEvents(ctx ctx.Context, context workflowExecutionContext,
	task historyReplicationTask) (retError error) {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	msBuilder := r.getNewMutableState(task.getVersion(), task.getLogger())
	sBuilder := r.getNewStateBuilder(msBuilder, task.getLogger())

	// directly use stateBuilder to apply events for other events(including continueAsNew)
	_, _, _, err := sBuilder.applyEvents(
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
	createReplicationTask := false
	transferTasks := sBuilder.getTransferTasks()
	var replicationTasks []persistence.Task // passive side generates no replication tasks
	timerTasks := sBuilder.getTimerTasks()
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
		msBuilder, task.getSourceCluster(), createReplicationTask, task.getEventTime(), transferTasks, replicationTasks, timerTasks,
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
	currentState := errExist.State
	currentLastWriteVersion := errExist.LastWriteVersion

	if currentRunID == task.getRunID() {
		return nil
	}

	// current workflow is completed
	if currentState == persistence.WorkflowStateCompleted {
		// allow the application of workflow creation if currentLastWriteVersion > incomingVersion
		// because this can be caused by missing replication events
		// proceed to create workflow
		createMode = persistence.CreateWorkflowModeWorkflowIDReuse
		prevRunID = currentRunID
		prevLastWriteVersion = currentLastWriteVersion
		return context.createWorkflowExecution(
			msBuilder, task.getSourceCluster(), createReplicationTask, task.getEventTime(), transferTasks, replicationTasks, timerTasks,
			createMode, prevRunID, prevLastWriteVersion,
		)
	}

	// current workflow is still running
	if currentLastWriteVersion > task.getVersion() {
		// TODO do backfill workflow, with zombie state
		return nil
	}
	if currentLastWriteVersion == task.getVersion() {
		// TODO should the LastEventTaskID be included in the current workflow execution record?

		_, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(
			ctx,
			task.getDomainID(),
			task.getWorkflowID(),
		)
		if err != nil {
			return err
		}
		currentRunID := currentMutableState.GetExecutionInfo().RunID
		currentLastEventTaskID := currentMutableState.GetExecutionInfo().LastEventTaskID
		currentNextEventID := currentMutableState.GetNextEventID()
		currentRelease(nil)

		if msBuilder.GetExecutionInfo().LastEventTaskID < currentLastEventTaskID {
			// TODO do backfill workflow, with zombie state
			return nil
		}
		return newRetryTaskErrorWithHint(ErrRetryExistingWorkflowMsg, task.getDomainID(),
			task.getWorkflowID(), currentRunID, currentNextEventID)
	}

	// currentStartVersion < incomingVersion && current workflow still running
	// this can happen during the failover; since we have no idea
	// whether the remote active cluster is aware of the current running workflow,
	// the only thing we can do is to terminate the current workflow and
	// start the new workflow from the request

	// same workflow ID, same shard
	_, err = r.terminateWorkflow(ctx, task.getDomainID(),
		task.getWorkflowID(), currentRunID, task.getVersion(), task.getEventTime())
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return err
		}
		// if workflow is completed just when the call is made, will get EntityNotExistsError
		// we are not sure whether the workflow to be terminated ends with continue as new or not
		// so when encounter EntityNotExistsError, just contiue to execute, if err occurs,
		// there will be retry on the worker level
	}
	createMode = persistence.CreateWorkflowModeWorkflowIDReuse
	prevRunID = currentRunID
	prevLastWriteVersion = task.getVersion()
	return context.createWorkflowExecution(
		msBuilder, task.getSourceCluster(), createReplicationTask, task.getEventTime(), transferTasks, replicationTasks, timerTasks,
		createMode, prevRunID, prevLastWriteVersion,
	)
}

func (r *historyReplicatorV2) applyNonStartEventsBranchChecking(ctx ctx.Context, context workflowExecutionContext, mutableState mutableState,
	task historyReplicationTask) (mutableState, error) {

	// TODO do workflow history lowest common ancestor checking
	return nil, nil
}

func (r *historyReplicatorV2) applyNonStartEventsEventIDChecking(ctx ctx.Context, context workflowExecutionContext, mutableState mutableState,
	task historyReplicationTask) (doContinue bool, retError error) {

	nextEventID := mutableState.GetNextEventID()
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
			nextEventID,
		)
	}
	return true, nil
}

func (r *historyReplicatorV2) applyNonStartEvents(ctx ctx.Context, context workflowExecutionContext, mutableState mutableState,
	task historyReplicationTask) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	sBuilder := r.getNewStateBuilder(mutableState, task.getLogger())

	// directly use stateBuilder to apply events for other events(including continueAsNew)
	_, _, newRunMutableState, err := sBuilder.applyEvents(
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
		sBuilder.getTransferTasks(),
		sBuilder.getTimerTasks(),
		task.getLastEvent().GetEventId(),
		task.getEventTime(),
	)
	if err == nil {
		r.notify(task.getSourceCluster(), task.getEventTime(), sBuilder.getTransferTasks(), sBuilder.getTimerTasks())
	}

	return err
}

func (r *historyReplicatorV2) applyNonStartEventsMissingMutableState(ctx ctx.Context, context workflowExecutionContext,
	task historyReplicationTask) error {

	// we need to check the current workflow execution
	_, currentMutableState, currentRelease, err := r.getCurrentWorkflowMutableState(
		ctx,
		task.getDomainID(),
		task.getWorkflowID(),
	)
	if err != nil {
		if _, ok := err.(*shared.EntityNotExistsError); !ok {
			return err
		}
		// err is EntityNotExistsError
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, task.getDomainID(), task.getWorkflowID(), task.getRunID(), common.FirstEventID)
	}

	currentRunID := currentMutableState.GetExecutionInfo().RunID
	currentLastEventTaskID := currentMutableState.GetExecutionInfo().LastEventTaskID
	currentNextEventID := currentMutableState.GetNextEventID()
	currentLastWriteVersion := currentMutableState.GetLastWriteVersion()
	currentStillRunning := currentMutableState.IsWorkflowExecutionRunning()
	currentRelease(nil)

	if currentLastWriteVersion > task.getVersion() {
		// return retry task error for backfill
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, task.getDomainID(), task.getWorkflowID(), task.getRunID(), common.FirstEventID)
	}
	if currentLastWriteVersion < task.getVersion() {
		if currentStillRunning {
			_, err := r.terminateWorkflow(ctx, task.getDomainID(), task.getWorkflowID(), currentRunID, task.getVersion(), task.getEventTime())
			if err != nil {
				if _, ok := err.(*shared.EntityNotExistsError); !ok {
					return err
				}
				// if workflow is completed just when the call is made, will get EntityNotExistsError
				// we are not sure whether the workflow to be terminated ends with continue as new or not
				// so when encounter EntityNotExistsError, just continue to execute, if err occurs,
				// there will be retry on the worker level
			}
			// TODO if local was not active, better return a retry task error backfill all history, in case kafka lost events.
		}
		if task.getRequest().GetResetWorkflow() {
			return r.resetor.ApplyResetEvent(ctx, task.getRequest(), task.getDomainID(), task.getWorkflowID(), currentRunID)
		}
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, task.getDomainID(), task.getWorkflowID(), task.getRunID(), common.FirstEventID)
	}

	// currentLastWriteVersion == incomingVersion
	if currentStillRunning {
		if task.getLastEvent().GetTaskId() < currentLastEventTaskID {
			// return retry task error for backfill
			return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, task.getDomainID(), task.getWorkflowID(), task.getRunID(), common.FirstEventID)
		}
		return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, task.getDomainID(), task.getWorkflowID(), currentRunID, currentNextEventID)
	}

	if task.getRequest().GetResetWorkflow() {
		//Note that at this point, current run is already closed and currentLastWriteVersion <= incomingVersion
		return r.resetor.ApplyResetEvent(ctx, task.getRequest(), task.getDomainID(), task.getWorkflowID(), currentRunID)
	}
	return newRetryTaskErrorWithHint(ErrWorkflowNotFoundMsg, task.getDomainID(), task.getWorkflowID(), task.getRunID(), common.FirstEventID)
}

// func (r *historyReplicator) getCurrentWorkflowInfo(domainID string, workflowID string) (runID string, lastWriteVersion int64, closeStatus int, retError error) {
func (r *historyReplicatorV2) getCurrentWorkflowMutableState(ctx ctx.Context, domainID string,
	workflowID string) (workflowExecutionContext, mutableState, releaseWorkflowExecutionFunc, error) {

	// TODO refactor for case if context is already current workflow

	// we need to check the current workflow execution
	context, release, err := r.historyCache.getOrCreateWorkflowExecutionWithTimeout(ctx,
		domainID,
		// only use the workflow ID, to get the current running one
		shared.WorkflowExecution{WorkflowId: common.StringPtr(workflowID)},
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

func (r *historyReplicatorV2) terminateWorkflow(ctx ctx.Context, domainID string, workflowID string, runID string,
	terminationEventVersion int64, now time.Time) (bool, error) {

	// TODO should this workflow termination event be replicated?
	// TODO 2DC vs 3+DC, different logic

	// TODO if the workflow to be terminated has last write version being local active
	// terminate with last write version and generate replication task
	// TODO if the workflow to be terminated's last write version being remote active
	// terminate with incoming version

	return false, nil
}

func (r *historyReplicatorV2) notify(clusterName string, now time.Time,
	transferTasks []persistence.Task, timerTasks []persistence.Task) {

	now = now.Add(-r.shard.GetConfig().StandbyClusterDelay())

	r.shard.SetCurrentTime(clusterName, now)
	r.historyEngine.txProcessor.NotifyNewTask(clusterName, transferTasks)
	r.historyEngine.timerProcessor.NotifyNewTimers(clusterName, now, timerTasks)
}
