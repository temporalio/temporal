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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination context_mock.go

package workflow

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"

	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow/update"
)

const (
	LockPriorityHigh LockPriority = 0
	LockPriorityLow  LockPriority = 1
)

type (
	LockPriority int

	Context interface {
		GetWorkflowKey() definition.WorkflowKey

		LoadMutableState(ctx context.Context, shardContext shard.Context) (MutableState, error)
		LoadExecutionStats(ctx context.Context, shardContext shard.Context) (*persistencespb.ExecutionStats, error)
		Clear()

		Lock(ctx context.Context, lockPriority LockPriority) error
		Unlock(lockPriority LockPriority)

		IsDirty() bool

		ReapplyEvents(
			ctx context.Context,
			shardContext shard.Context,
			eventBatches []*persistence.WorkflowEvents,
		) error

		PersistWorkflowEvents(
			ctx context.Context,
			shardContext shard.Context,
			workflowEventsSlice ...*persistence.WorkflowEvents,
		) (int64, error)

		CreateWorkflowExecution(
			ctx context.Context,
			shardContext shard.Context,
			createMode persistence.CreateWorkflowMode,
			prevRunID string,
			prevLastWriteVersion int64,
			newMutableState MutableState,
			newWorkflow *persistence.WorkflowSnapshot,
			newWorkflowEvents []*persistence.WorkflowEvents,
		) error
		ConflictResolveWorkflowExecution(
			ctx context.Context,
			shardContext shard.Context,
			conflictResolveMode persistence.ConflictResolveWorkflowMode,
			resetMutableState MutableState,
			newContext Context,
			newMutableState MutableState,
			currentContext Context,
			currentMutableState MutableState,
			resetWorkflowTransactionPolicy TransactionPolicy,
			newWorkflowTransactionPolicy *TransactionPolicy,
			currentTransactionPolicy *TransactionPolicy,
		) error
		UpdateWorkflowExecutionAsActive(
			ctx context.Context,
			shardContext shard.Context,
		) error
		UpdateWorkflowExecutionWithNewAsActive(
			ctx context.Context,
			shardContext shard.Context,
			newContext Context,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionAsPassive(
			ctx context.Context,
			shardContext shard.Context,
		) error
		UpdateWorkflowExecutionWithNewAsPassive(
			ctx context.Context,
			shardContext shard.Context,
			newContext Context,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionWithNew(
			ctx context.Context,
			shardContext shard.Context,
			updateMode persistence.UpdateWorkflowMode,
			newContext Context,
			newMutableState MutableState,
			updateWorkflowTransactionPolicy TransactionPolicy,
			newWorkflowTransactionPolicy *TransactionPolicy,
		) error
		// SetWorkflowExecution is an alias to SubmitClosedWorkflowSnapshot with TransactionPolicyPassive.
		SetWorkflowExecution(
			ctx context.Context,
			shardContext shard.Context,
		) error
		// SubmitClosedWorkflowSnapshot closes the current mutable state transaction with the given
		// transactionPolicy and updates the workflow execution record in the DB. Does not check the "current"
		// run status for the execution.
		// Closes the transaction as snapshot, which errors out if there are any buffered events that need
		// flushing and generally does not expect new history events to be generated (expected for closed
		// workflows).
		// NOTE: in the future, we'd like to have the ability to close the transaction as mutation to avoid the
		// overhead of overwriting the entire DB record.
		SubmitClosedWorkflowSnapshot(
			ctx context.Context,
			shardContext shard.Context,
			transactionPolicy TransactionPolicy,
		) error
		// TODO (alex-update): move this from workflow context.
		UpdateRegistry(ctx context.Context, ms MutableState) update.Registry
	}
)

type (
	ContextImpl struct {
		workflowKey     definition.WorkflowKey
		logger          log.Logger
		throttledLogger log.ThrottledLogger
		metricsHandler  metrics.Handler
		config          *configs.Config

		mutex          locks.PriorityMutex
		MutableState   MutableState
		updateRegistry update.Registry
	}
)

var _ Context = (*ContextImpl)(nil)

func NewContext(
	config *configs.Config,
	workflowKey definition.WorkflowKey,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	metricsHandler metrics.Handler,
) *ContextImpl {
	return &ContextImpl{
		workflowKey:     workflowKey,
		logger:          logger,
		throttledLogger: throttledLogger,
		metricsHandler:  metricsHandler.WithTags(metrics.OperationTag(metrics.WorkflowContextScope)),
		config:          config,
		mutex:           locks.NewPriorityMutex(),
	}
}

func (c *ContextImpl) Lock(
	ctx context.Context,
	lockPriority LockPriority,
) error {
	switch lockPriority {
	case LockPriorityHigh:
		return c.mutex.LockHigh(ctx)
	case LockPriorityLow:
		return c.mutex.LockLow(ctx)
	default:
		panic(fmt.Sprintf("unknown lock priority: %v", lockPriority))
	}
}

func (c *ContextImpl) Unlock(
	lockPriority LockPriority,
) {
	switch lockPriority {
	case LockPriorityHigh:
		c.mutex.UnlockHigh()
	case LockPriorityLow:
		c.mutex.UnlockLow()
	default:
		panic(fmt.Sprintf("unknown lock priority: %v", lockPriority))
	}
}

func (c *ContextImpl) IsDirty() bool {
	if c.MutableState == nil {
		return false
	}
	return c.MutableState.IsDirty()
}

func (c *ContextImpl) Clear() {
	metrics.WorkflowContextCleared.With(c.metricsHandler).Record(1)
	if c.MutableState != nil {
		c.MutableState.GetQueryRegistry().Clear()
		c.MutableState = nil
	}
	if c.updateRegistry != nil {
		c.updateRegistry.Clear()
		c.updateRegistry = nil
	}
}

func (c *ContextImpl) GetWorkflowKey() definition.WorkflowKey {
	return c.workflowKey
}

func (c *ContextImpl) GetNamespace(shardContext shard.Context) namespace.Name {
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(c.workflowKey.NamespaceID),
	)
	if err != nil {
		return ""
	}
	return namespaceEntry.Name()
}

func (c *ContextImpl) LoadExecutionStats(ctx context.Context, shardContext shard.Context) (*persistencespb.ExecutionStats, error) {
	_, err := c.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}
	return c.MutableState.GetExecutionInfo().ExecutionStats, nil
}

func (c *ContextImpl) LoadMutableState(ctx context.Context, shardContext shard.Context) (MutableState, error) {
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(c.workflowKey.NamespaceID),
	)
	if err != nil {
		return nil, err
	}

	if c.MutableState == nil {
		response, err := getWorkflowExecution(ctx, shardContext, &persistence.GetWorkflowExecutionRequest{
			ShardID:     shardContext.GetShardID(),
			NamespaceID: c.workflowKey.NamespaceID,
			WorkflowID:  c.workflowKey.WorkflowID,
			RunID:       c.workflowKey.RunID,
		})
		if err != nil {
			return nil, err
		}

		c.MutableState, err = NewMutableStateFromDB(
			shardContext,
			shardContext.GetEventsCache(),
			c.logger,
			namespaceEntry,
			response.State,
			response.DBRecordVersion,
		)
		if err != nil {
			return nil, err
		}
	}

	flushBeforeReady, err := c.MutableState.StartTransaction(namespaceEntry)
	if err != nil {
		return nil, err
	}
	if !flushBeforeReady {
		return c.MutableState, nil
	}

	if err = c.UpdateWorkflowExecutionAsActive(
		ctx,
		shardContext,
	); err != nil {
		return nil, err
	}

	flushBeforeReady, err = c.MutableState.StartTransaction(namespaceEntry)
	if err != nil {
		return nil, err
	}
	if flushBeforeReady {
		return nil, serviceerror.NewInternal("Context counter flushBeforeReady status after loading mutable state from DB")
	}

	return c.MutableState, nil
}

func (c *ContextImpl) PersistWorkflowEvents(
	ctx context.Context,
	shardContext shard.Context,
	workflowEventsSlice ...*persistence.WorkflowEvents,
) (int64, error) {
	return PersistWorkflowEvents(ctx, shardContext, workflowEventsSlice...)
}

func (c *ContextImpl) CreateWorkflowExecution(
	ctx context.Context,
	shardContext shard.Context,
	createMode persistence.CreateWorkflowMode,
	prevRunID string,
	prevLastWriteVersion int64,
	newMutableState MutableState,
	newWorkflow *persistence.WorkflowSnapshot,
	newWorkflowEvents []*persistence.WorkflowEvents,
) (retError error) {

	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	createRequest := &persistence.CreateWorkflowExecutionRequest{
		ShardID: shardContext.GetShardID(),
		// workflow create mode & prev run ID & version
		Mode:                     createMode,
		PreviousRunID:            prevRunID,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *newWorkflow,
		NewWorkflowEvents:   newWorkflowEvents,
	}

	_, err := createWorkflowExecution(
		ctx,
		shardContext,
		newMutableState.GetCurrentVersion(),
		createRequest,
	)
	if err != nil {
		return err
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	NotifyWorkflowSnapshotTasks(engine, newWorkflow)
	emitStateTransitionCount(c.metricsHandler, shardContext.GetClusterMetadata(), newMutableState)

	return nil
}

func (c *ContextImpl) ConflictResolveWorkflowExecution(
	ctx context.Context,
	shardContext shard.Context,
	conflictResolveMode persistence.ConflictResolveWorkflowMode,
	resetMutableState MutableState,
	newContext Context,
	newMutableState MutableState,
	currentContext Context,
	currentMutableState MutableState,
	resetWorkflowTransactionPolicy TransactionPolicy,
	newWorkflowTransactionPolicy *TransactionPolicy,
	currentTransactionPolicy *TransactionPolicy,
) (retError error) {

	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	resetWorkflow, resetWorkflowEventsSeq, err := resetMutableState.CloseTransactionAsSnapshot(
		resetWorkflowTransactionPolicy,
	)
	if err != nil {
		return err
	}

	var newWorkflow *persistence.WorkflowSnapshot
	var newWorkflowEventsSeq []*persistence.WorkflowEvents
	if newContext != nil && newMutableState != nil && newWorkflowTransactionPolicy != nil {

		defer func() {
			if retError != nil {
				newContext.Clear()
			}
		}()

		newWorkflow, newWorkflowEventsSeq, err = newMutableState.CloseTransactionAsSnapshot(
			*newWorkflowTransactionPolicy,
		)
		if err != nil {
			return err
		}
	}

	var currentWorkflow *persistence.WorkflowMutation
	var currentWorkflowEventsSeq []*persistence.WorkflowEvents
	if currentContext != nil && currentMutableState != nil && currentTransactionPolicy != nil {

		defer func() {
			if retError != nil {
				currentContext.Clear()
			}
		}()

		currentWorkflow, currentWorkflowEventsSeq, err = currentMutableState.CloseTransactionAsMutation(
			*currentTransactionPolicy,
		)
		if err != nil {
			return err
		}
	}

	if err := c.conflictResolveEventReapply(
		ctx,
		shardContext,
		conflictResolveMode,
		resetWorkflowEventsSeq,
		newWorkflowEventsSeq,
		// current workflow events will not participate in the events reapplication
	); err != nil {
		return err
	}

	if _, _, _, err := NewTransaction(shardContext).ConflictResolveWorkflowExecution(
		ctx,
		conflictResolveMode,
		resetMutableState.GetCurrentVersion(),
		resetWorkflow,
		resetWorkflowEventsSeq,
		MutableStateFailoverVersion(newMutableState),
		newWorkflow,
		newWorkflowEventsSeq,
		MutableStateFailoverVersion(currentMutableState),
		currentWorkflow,
		currentWorkflowEventsSeq,
	); err != nil {
		return err
	}

	emitStateTransitionCount(c.metricsHandler, shardContext.GetClusterMetadata(), resetMutableState)
	emitStateTransitionCount(c.metricsHandler, shardContext.GetClusterMetadata(), newMutableState)
	emitStateTransitionCount(c.metricsHandler, shardContext.GetClusterMetadata(), currentMutableState)

	return nil
}

func (c *ContextImpl) UpdateWorkflowExecutionAsActive(
	ctx context.Context,
	shardContext shard.Context,
) error {

	// We only perform this check on active cluster for the namespace
	historySizeForceTerminate, err := c.enforceHistorySizeCheck(ctx, shardContext)
	if err != nil {
		return err
	}
	historyCountForceTerminate := false
	if !historySizeForceTerminate {
		historyCountForceTerminate, err = c.enforceHistoryCountCheck(ctx, shardContext)
		if err != nil {
			return err
		}
	}
	msForceTerminate := false
	if !historySizeForceTerminate && !historyCountForceTerminate {
		msForceTerminate, err = c.enforceMutableStateSizeCheck(ctx, shardContext)
		if err != nil {
			return err
		}
	}

	err = c.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		TransactionPolicyActive,
		nil,
	)
	if err != nil {
		return err
	}

	// Returns ResourceExhausted error back to caller after workflow execution is forced terminated
	// Retrying the operation will give appropriate semantics operation should expect in the case of workflow
	// execution being closed.
	if historySizeForceTerminate {
		return consts.ErrHistorySizeExceedsLimit
	}
	if historyCountForceTerminate {
		return consts.ErrHistoryCountExceedsLimit
	}
	if msForceTerminate {
		return consts.ErrMutableStateSizeExceedsLimit
	}

	return nil
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsActive(
	ctx context.Context,
	shardContext shard.Context,
	newContext Context,
	newMutableState MutableState,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		newContext,
		newMutableState,
		TransactionPolicyActive,
		TransactionPolicyActive.Ptr(),
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionAsPassive(
	ctx context.Context,
	shardContext shard.Context,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		TransactionPolicyPassive,
		nil,
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsPassive(
	ctx context.Context,
	shardContext shard.Context,
	newContext Context,
	newMutableState MutableState,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		newContext,
		newMutableState,
		TransactionPolicyPassive,
		TransactionPolicyPassive.Ptr(),
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNew(
	ctx context.Context,
	shardContext shard.Context,
	updateMode persistence.UpdateWorkflowMode,
	newContext Context,
	newMutableState MutableState,
	updateWorkflowTransactionPolicy TransactionPolicy,
	newWorkflowTransactionPolicy *TransactionPolicy,
) (retError error) {

	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	updateWorkflow, updateWorkflowEventsSeq, err := c.MutableState.CloseTransactionAsMutation(
		updateWorkflowTransactionPolicy,
	)
	if err != nil {
		return err
	}

	var newWorkflow *persistence.WorkflowSnapshot
	var newWorkflowEventsSeq []*persistence.WorkflowEvents
	if newContext != nil && newMutableState != nil && newWorkflowTransactionPolicy != nil {
		defer func() {
			if retError != nil {
				newContext.Clear()
			}
		}()

		newWorkflow, newWorkflowEventsSeq, err = newMutableState.CloseTransactionAsSnapshot(
			*newWorkflowTransactionPolicy,
		)
		if err != nil {
			return err
		}
	}

	if err := c.mergeUpdateWithNewReplicationTasks(
		updateWorkflow,
		newWorkflow,
	); err != nil {
		return err
	}

	if err := c.updateWorkflowExecutionEventReapply(
		ctx,
		shardContext,
		updateMode,
		updateWorkflowEventsSeq,
		newWorkflowEventsSeq,
	); err != nil {
		return err
	}

	if _, _, err := NewTransaction(shardContext).UpdateWorkflowExecution(
		ctx,
		updateMode,
		c.MutableState.GetCurrentVersion(),
		updateWorkflow,
		updateWorkflowEventsSeq,
		MutableStateFailoverVersion(newMutableState),
		newWorkflow,
		newWorkflowEventsSeq,
	); err != nil {
		return err
	}

	emitStateTransitionCount(c.metricsHandler, shardContext.GetClusterMetadata(), c.MutableState)
	emitStateTransitionCount(c.metricsHandler, shardContext.GetClusterMetadata(), newMutableState)

	// finally emit session stats
	emitWorkflowHistoryStats(
		c.metricsHandler,
		c.GetNamespace(shardContext),
		c.MutableState.GetExecutionState().State,
		int(c.MutableState.GetExecutionInfo().ExecutionStats.HistorySize),
		int(c.MutableState.GetNextEventID()-1),
	)

	return nil
}

func (c *ContextImpl) SetWorkflowExecution(
	ctx context.Context,
	shardContext shard.Context,
) (retError error) {
	return c.SubmitClosedWorkflowSnapshot(ctx, shardContext, TransactionPolicyPassive)
}

func (c *ContextImpl) SubmitClosedWorkflowSnapshot(
	ctx context.Context,
	shardContext shard.Context,
	transactionPolicy TransactionPolicy,
) (retError error) {
	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	resetWorkflowSnapshot, resetWorkflowEventsSeq, err := c.MutableState.CloseTransactionAsSnapshot(
		transactionPolicy,
	)
	if err != nil {
		return err
	}
	if len(resetWorkflowEventsSeq) != 0 {
		metrics.ClosedWorkflowBufferEventCount.With(c.metricsHandler).Record(1)
		c.logger.Warn("SetWorkflowExecution encountered new events")
	}

	return NewTransaction(shardContext).SetWorkflowExecution(
		ctx,
		resetWorkflowSnapshot,
	)
}

func (c *ContextImpl) mergeUpdateWithNewReplicationTasks(
	currentWorkflowMutation *persistence.WorkflowMutation,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
) error {

	if newWorkflowSnapshot == nil {
		return nil
	}

	if currentWorkflowMutation.ExecutionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW &&
		!c.config.ReplicationEnableUpdateWithNewTaskMerge() {
		// we need to make sure target cell is able to handle updateWithNew
		// for non continuedAsNew case before enabling this feature
		return nil
	}

	// namespace could be local or global with only one cluster,
	// or current cluster is standby cluster for the namespace,
	// so no replication task is generated.
	//
	// TODO: What does the following comment mean?
	// it is possible that continue as new is done as part of passive logic
	numCurrentReplicationTasks := len(currentWorkflowMutation.Tasks[tasks.CategoryReplication])
	numNewReplicationTasks := len(newWorkflowSnapshot.Tasks[tasks.CategoryReplication])

	if numCurrentReplicationTasks == 0 && numNewReplicationTasks == 0 {
		return nil
	}
	if numCurrentReplicationTasks == 0 {
		c.logger.Info("Current workflow has no replication task, while new workflow has replication task",
			tag.WorkflowNamespaceID(c.workflowKey.NamespaceID),
			tag.WorkflowID(c.workflowKey.WorkflowID),
			tag.WorkflowRunID(c.workflowKey.RunID),
			tag.WorkflowNewRunID(newWorkflowSnapshot.ExecutionState.RunId),
		)
		return nil
	}
	if numNewReplicationTasks == 0 {
		c.logger.Info("New workflow has no replication task, while current workflow has replication task",
			tag.WorkflowNamespaceID(c.workflowKey.NamespaceID),
			tag.WorkflowID(c.workflowKey.WorkflowID),
			tag.WorkflowRunID(c.workflowKey.RunID),
			tag.WorkflowNewRunID(newWorkflowSnapshot.ExecutionState.RunId),
		)
		return nil
	}
	if numNewReplicationTasks > 1 {
		// This could happen when importing a workflow and current running workflow is being terminated.
		// TODO: support more than one replication tasks (batch of events) in the new workflow
		c.logger.Info("Skipped merging replication tasks because new run has more than one replication tasks",
			tag.WorkflowNamespaceID(c.workflowKey.NamespaceID),
			tag.WorkflowID(c.workflowKey.WorkflowID),
			tag.WorkflowRunID(c.workflowKey.RunID),
			tag.WorkflowNewRunID(newWorkflowSnapshot.ExecutionState.RunId),
		)
		return nil
	}

	// current workflow is closing with new workflow
	// merge the new run's replication task to current workflow's replication task
	// so that they can be applied transactionally in the standby cluster.
	// TODO: this logic should be more generic so that the first replication task
	// in the new run doesn't have to be HistoryReplicationTask
	newRunTask := newWorkflowSnapshot.Tasks[tasks.CategoryReplication][0].(*tasks.HistoryReplicationTask)
	delete(newWorkflowSnapshot.Tasks, tasks.CategoryReplication)

	newRunBranchToken := newRunTask.BranchToken
	newRunID := newRunTask.RunID
	taskUpdated := false
	for idx := numCurrentReplicationTasks - 1; idx >= 0; idx-- {
		replicationTask := currentWorkflowMutation.Tasks[tasks.CategoryReplication][idx]
		if task, ok := replicationTask.(*tasks.HistoryReplicationTask); ok {
			taskUpdated = true
			task.NewRunBranchToken = newRunBranchToken
			task.NewRunID = newRunID
			break
		}
	}
	if !taskUpdated {
		return serviceerror.NewInternal("unable to find HistoryReplicationTask from current workflow for update-with-new replication")
	}
	return nil
}

func (c *ContextImpl) updateWorkflowExecutionEventReapply(
	ctx context.Context,
	shardContext shard.Context,
	updateMode persistence.UpdateWorkflowMode,
	eventBatch1 []*persistence.WorkflowEvents,
	eventBatch2 []*persistence.WorkflowEvents,
) error {

	if updateMode != persistence.UpdateWorkflowModeBypassCurrent {
		return nil
	}

	var eventBatches []*persistence.WorkflowEvents
	eventBatches = append(eventBatches, eventBatch1...)
	eventBatches = append(eventBatches, eventBatch2...)
	return c.ReapplyEvents(ctx, shardContext, eventBatches)
}

func (c *ContextImpl) conflictResolveEventReapply(
	ctx context.Context,
	shardContext shard.Context,
	conflictResolveMode persistence.ConflictResolveWorkflowMode,
	eventBatch1 []*persistence.WorkflowEvents,
	eventBatch2 []*persistence.WorkflowEvents,
) error {

	if conflictResolveMode != persistence.ConflictResolveWorkflowModeBypassCurrent {
		return nil
	}

	var eventBatches []*persistence.WorkflowEvents
	eventBatches = append(eventBatches, eventBatch1...)
	eventBatches = append(eventBatches, eventBatch2...)
	return c.ReapplyEvents(ctx, shardContext, eventBatches)
}

func (c *ContextImpl) ReapplyEvents(
	ctx context.Context,
	shardContext shard.Context,
	eventBatches []*persistence.WorkflowEvents,
) error {

	// NOTE: this function should only be used to workflow which is
	// not the caller, or otherwise deadlock will appear

	if len(eventBatches) == 0 {
		return nil
	}

	namespaceID := namespace.ID(eventBatches[0].NamespaceID)
	workflowID := eventBatches[0].WorkflowID
	runID := eventBatches[0].RunID
	var reapplyEvents []*historypb.HistoryEvent
	for _, events := range eventBatches {
		if namespace.ID(events.NamespaceID) != namespaceID ||
			events.WorkflowID != workflowID {
			return serviceerror.NewInternal("Context encountered mismatch namespaceID / workflowID in events reapplication.")
		}

		for _, e := range events.Events {
			event := e
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED:

				reapplyEvents = append(reapplyEvents, event)
			}
		}
	}
	if len(reapplyEvents) == 0 {
		return nil
	}

	// Reapply events only reapply to the current run.
	// The run id is only used for reapply event de-duplication
	execution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      runID,
	}
	namespaceRegistry := shardContext.GetNamespaceRegistry()
	serializer := shardContext.GetPayloadSerializer()
	namespaceEntry, err := namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	activeCluster := namespaceEntry.ActiveClusterName()
	if activeCluster == shardContext.GetClusterMetadata().GetCurrentClusterName() {
		engine, err := shardContext.GetEngine(ctx)
		if err != nil {
			return err
		}
		return engine.ReapplyEvents(
			ctx,
			namespaceID,
			workflowID,
			runID,
			reapplyEvents,
		)
	}

	// The active cluster of the namespace is the same as current cluster.
	// Use the history from the same cluster to reapply events
	reapplyEventsDataBlob, err := serializer.SerializeEvents(reapplyEvents, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return err
	}
	// The active cluster of the namespace is differ from the current cluster
	// Use frontend client to route this request to the active cluster
	// Reapplication only happens in active cluster
	sourceAdminClient, err := shardContext.GetRemoteAdminClient(activeCluster)
	if err != nil {
		return err
	}
	if sourceAdminClient == nil {
		// TODO: will this ever happen?
		return serviceerror.NewInternal(fmt.Sprintf("cannot find cluster config %v to do reapply", activeCluster))
	}

	_, err = sourceAdminClient.ReapplyEvents(
		ctx,
		&adminservice.ReapplyEventsRequest{
			NamespaceId:       namespaceEntry.ID().String(),
			WorkflowExecution: execution,
			Events:            reapplyEventsDataBlob,
		},
	)

	return err
}

// TODO: remove `fallbackMutableState` parameter again (added since it's not possible to initialize a new Context with a specific MutableState)
func (c *ContextImpl) UpdateRegistry(ctx context.Context, fallbackMutableState MutableState) update.Registry {
	ms := c.MutableState
	if ms == nil {
		if fallbackMutableState == nil {
			panic("both c.MutableState and fallbackMutableState are nil")
		}
		ms = fallbackMutableState
	}

	if c.updateRegistry != nil && c.updateRegistry.FailoverVersion() != ms.GetCurrentVersion() {
		c.updateRegistry.Clear()
		c.updateRegistry = nil
	}

	if c.updateRegistry == nil {
		nsIDStr := ms.GetNamespaceEntry().ID().String()

		c.updateRegistry = update.NewRegistry(
			ms,
			update.WithLogger(c.logger),
			update.WithMetrics(c.metricsHandler),
			update.WithTracerProvider(trace.SpanFromContext(ctx).TracerProvider()),
			update.WithInFlightLimit(
				func() int {
					return c.config.WorkflowExecutionMaxInFlightUpdates(nsIDStr)
				},
			),
			update.WithTotalLimit(
				func() int {
					return c.config.WorkflowExecutionMaxTotalUpdates(nsIDStr)
				},
			),
		)
	}
	return c.updateRegistry
}

// Returns true if execution is forced terminated
func (c *ContextImpl) enforceHistorySizeCheck(
	ctx context.Context,
	shardContext shard.Context,
) (bool, error) {
	// Hard terminate workflow if still running and breached history size limit
	if c.maxHistorySizeExceeded(shardContext) {
		if err := c.forceTerminateWorkflow(ctx, shardContext, common.FailureReasonHistorySizeExceedsLimit); err != nil {
			return false, err
		}
		// Return true to caller to indicate workflow state is overwritten to force terminate execution on update
		return true, nil
	}
	return false, nil
}

// Returns true if the workflow is running and history size should trigger a forced termination
// Prints a log message if history size is over the error or warn limits
func (c *ContextImpl) maxHistorySizeExceeded(shardContext shard.Context) bool {
	namespaceName := c.GetNamespace(shardContext).String()
	historySizeLimitWarn := c.config.HistorySizeLimitWarn(namespaceName)
	historySizeLimitError := c.config.HistorySizeLimitError(namespaceName)
	historySize := int(c.MutableState.GetExecutionInfo().ExecutionStats.HistorySize)

	if historySize > historySizeLimitError && c.MutableState.IsWorkflowExecutionRunning() {
		c.logger.Warn("history size exceeds error limit.",
			tag.WorkflowNamespaceID(c.workflowKey.NamespaceID),
			tag.WorkflowID(c.workflowKey.WorkflowID),
			tag.WorkflowRunID(c.workflowKey.RunID),
			tag.WorkflowHistorySize(historySize))

		return true
	}

	if historySize > historySizeLimitWarn {
		c.throttledLogger.Warn("history size exceeds warn limit.",
			tag.WorkflowNamespaceID(c.MutableState.GetExecutionInfo().NamespaceId),
			tag.WorkflowID(c.MutableState.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(c.MutableState.GetExecutionState().RunId),
			tag.WorkflowHistorySize(historySize))
	}

	return false
}

func (c *ContextImpl) enforceHistoryCountCheck(
	ctx context.Context,
	shardContext shard.Context,
) (bool, error) {
	// Hard terminate workflow if still running and breached history count limit
	if c.maxHistoryCountExceeded(shardContext) {
		if err := c.forceTerminateWorkflow(ctx, shardContext, common.FailureReasonHistoryCountExceedsLimit); err != nil {
			return false, err
		}
		// Return true to caller to indicate workflow state is overwritten to force terminate execution on update
		return true, nil
	}
	return false, nil
}

// Returns true if the workflow is running and history event count should trigger a forced termination
// Prints a log message if history event count is over the error or warn limits
func (c *ContextImpl) maxHistoryCountExceeded(shardContext shard.Context) bool {
	namespaceName := c.GetNamespace(shardContext).String()
	historyCountLimitWarn := c.config.HistoryCountLimitWarn(namespaceName)
	historyCountLimitError := c.config.HistoryCountLimitError(namespaceName)
	historyCount := int(c.MutableState.GetNextEventID() - 1)

	if historyCount > historyCountLimitError && c.MutableState.IsWorkflowExecutionRunning() {
		c.logger.Warn("history count exceeds error limit.",
			tag.WorkflowNamespaceID(c.workflowKey.NamespaceID),
			tag.WorkflowID(c.workflowKey.WorkflowID),
			tag.WorkflowRunID(c.workflowKey.RunID),
			tag.WorkflowEventCount(historyCount))

		return true
	}

	if historyCount > historyCountLimitWarn {
		c.throttledLogger.Warn("history count exceeds warn limit.",
			tag.WorkflowNamespaceID(c.MutableState.GetExecutionInfo().NamespaceId),
			tag.WorkflowID(c.MutableState.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(c.MutableState.GetExecutionState().RunId),
			tag.WorkflowEventCount(historyCount))
	}

	return false
}

// Returns true if execution is forced terminated
// TODO: ideally this check should be after closing mutable state tx, but that would require a large refactor
func (c *ContextImpl) enforceMutableStateSizeCheck(ctx context.Context, shardContext shard.Context) (bool, error) {
	if c.maxMutableStateSizeExceeded() {
		if err := c.forceTerminateWorkflow(ctx, shardContext, common.FailureReasonMutableStateSizeExceedsLimit); err != nil {
			return false, err
		}
		// Return true to caller to indicate workflow state is overwritten to force terminate execution on update
		return true, nil
	}
	return false, nil
}

// Returns true if the workflow is running and mutable state size should trigger a forced termination
// Prints a log message if mutable state size is over the error or warn limits
func (c *ContextImpl) maxMutableStateSizeExceeded() bool {
	mutableStateSizeLimitError := c.config.MutableStateSizeLimitError()
	mutableStateSizeLimitWarn := c.config.MutableStateSizeLimitWarn()

	mutableStateSize := c.MutableState.GetApproximatePersistedSize()
	metrics.PersistedMutableStateSize.With(c.metricsHandler).Record(int64(mutableStateSize))

	if mutableStateSize > mutableStateSizeLimitError {
		c.logger.Warn("mutable state size exceeds error limit.",
			tag.WorkflowNamespaceID(c.workflowKey.NamespaceID),
			tag.WorkflowID(c.workflowKey.WorkflowID),
			tag.WorkflowRunID(c.workflowKey.RunID),
			tag.WorkflowMutableStateSize(mutableStateSize))

		return true
	}

	if mutableStateSize > mutableStateSizeLimitWarn {
		c.throttledLogger.Warn("mutable state size exceeds warn limit.",
			tag.WorkflowNamespaceID(c.MutableState.GetExecutionInfo().NamespaceId),
			tag.WorkflowID(c.MutableState.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(c.MutableState.GetExecutionState().RunId),
			tag.WorkflowMutableStateSize(mutableStateSize))
	}

	return false
}

func (c *ContextImpl) forceTerminateWorkflow(
	ctx context.Context,
	shardContext shard.Context,
	failureReason string,
) error {
	if !c.MutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// Abort updates before clearing context.
	// MS is not persisted yet, but this code is executed only when something
	// really bad happened with workflow, and it won't make any progress anyway.
	c.UpdateRegistry(ctx, nil).Abort(update.AbortReasonWorkflowCompleted)

	// Discard pending changes in MutableState so we can apply terminate state transition
	c.Clear()

	// Reload mutable state
	mutableState, err := c.LoadMutableState(ctx, shardContext)
	if err != nil {
		return err
	}

	return TerminateWorkflow(
		mutableState,
		failureReason,
		nil,
		consts.IdentityHistoryService,
		false,
	)
}

// CacheSize estimates the in-memory size of the object for cache limits. For proto objects, it uses proto.Size()
// which returns the serialized size. Note: In-memory size will be slightly larger than the serialized size.
func (c *ContextImpl) CacheSize() int {
	if !c.config.HistoryCacheLimitSizeBased {
		return 1
	}
	size := len(c.workflowKey.WorkflowID) + len(c.workflowKey.RunID) + len(c.workflowKey.NamespaceID)
	if c.MutableState != nil {
		size += c.MutableState.GetApproximatePersistedSize()
	}
	if c.updateRegistry != nil {
		size += c.updateRegistry.GetSize()
	}
	return size
}

func emitStateTransitionCount(
	metricsHandler metrics.Handler,
	clusterMetadata cluster.Metadata,
	mutableState MutableState,
) {
	if mutableState == nil {
		return
	}
	namespaceEntry := mutableState.GetNamespaceEntry()
	handler := metricsHandler.WithTags(
		metrics.NamespaceTag(namespaceEntry.Name().String()),
		metrics.NamespaceStateTag(namespaceState(clusterMetadata, util.Ptr(mutableState.GetCurrentVersion()))),
	)
	metrics.StateTransitionCount.With(handler).Record(
		mutableState.GetExecutionInfo().StateTransitionCount,
	)
	if mutableState.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		metrics.StateTransitionCount.With(handler).Record(
			mutableState.GetExecutionInfo().StateTransitionCount,
			metrics.OperationTag(metrics.WorkflowCompletionStatsScope),
		)
	}
}

const (
	namespaceStateActive  = "active"
	namespaceStatePassive = "passive"
	namespaceStateUnknown = "_unknown_"
)

func namespaceState(
	clusterMetadata cluster.Metadata,
	mutableStateCurrentVersion *int64,
) string {

	if mutableStateCurrentVersion == nil {
		return namespaceStateUnknown
	}

	// default value, need to special handle
	if *mutableStateCurrentVersion == 0 {
		return namespaceStateActive
	}

	if clusterMetadata.IsVersionFromSameCluster(
		clusterMetadata.GetClusterID(),
		*mutableStateCurrentVersion,
	) {
		return namespaceStateActive
	}
	return namespaceStatePassive
}

func MutableStateFailoverVersion(
	mutableState MutableState,
) *int64 {
	if mutableState == nil {
		return nil
	}
	return util.Ptr(mutableState.GetCurrentVersion())
}
