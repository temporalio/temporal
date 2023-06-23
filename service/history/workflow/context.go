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
	"time"

	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow/update"
)

const (
	defaultRemoteCallTimeout = 30 * time.Second
)

const (
	LockPriorityHigh LockPriority = 0
	LockPriorityLow  LockPriority = 1
)

type (
	LockPriority int

	Context interface {
		GetWorkflowKey() definition.WorkflowKey

		LoadMutableState(ctx context.Context) (MutableState, error)
		LoadExecutionStats(ctx context.Context) (*persistencespb.ExecutionStats, error)
		Clear()

		Lock(ctx context.Context, lockPriority LockPriority) error
		Unlock(lockPriority LockPriority)

		ReapplyEvents(
			ctx context.Context,
			eventBatches []*persistence.WorkflowEvents,
		) error

		PersistWorkflowEvents(
			ctx context.Context,
			workflowEventsSlice ...*persistence.WorkflowEvents,
		) (int64, error)

		CreateWorkflowExecution(
			ctx context.Context,
			createMode persistence.CreateWorkflowMode,
			prevRunID string,
			prevLastWriteVersion int64,
			newMutableState MutableState,
			newWorkflow *persistence.WorkflowSnapshot,
			newWorkflowEvents []*persistence.WorkflowEvents,
		) error
		ConflictResolveWorkflowExecution(
			ctx context.Context,
			conflictResolveMode persistence.ConflictResolveWorkflowMode,
			resetMutableState MutableState,
			newContext Context,
			newMutableState MutableState,
			currentContext Context,
			currentMutableState MutableState,
			currentTransactionPolicy *TransactionPolicy,
		) error
		UpdateWorkflowExecutionAsActive(
			ctx context.Context,
		) error
		UpdateWorkflowExecutionWithNewAsActive(
			ctx context.Context,
			newContext Context,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionAsPassive(
			ctx context.Context,
		) error
		UpdateWorkflowExecutionWithNewAsPassive(
			ctx context.Context,
			newContext Context,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionWithNew(
			ctx context.Context,
			updateMode persistence.UpdateWorkflowMode,
			newContext Context,
			newMutableState MutableState,
			currentWorkflowTransactionPolicy TransactionPolicy,
			newWorkflowTransactionPolicy *TransactionPolicy,
		) error
		SetWorkflowExecution(
			ctx context.Context,
		) error
		// TODO (alex-update): move this from workflow context.
		UpdateRegistry(ctx context.Context) update.Registry
	}
)

type (
	ContextImpl struct {
		shard           shard.Context
		workflowKey     definition.WorkflowKey
		logger          log.Logger
		throttledLogger log.ThrottledLogger
		metricsHandler  metrics.Handler
		clusterMetadata cluster.Metadata
		timeSource      clock.TimeSource
		config          *configs.Config
		transaction     Transaction

		mutex          locks.PriorityMutex
		MutableState   MutableState
		updateRegistry update.Registry
	}
)

var _ Context = (*ContextImpl)(nil)

func NewContext(
	shard shard.Context,
	workflowKey definition.WorkflowKey,
	logger log.Logger,
) *ContextImpl {
	return &ContextImpl{
		shard:           shard,
		workflowKey:     workflowKey,
		logger:          logger,
		throttledLogger: shard.GetThrottledLogger(),
		metricsHandler:  shard.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.WorkflowContextScope)),
		clusterMetadata: shard.GetClusterMetadata(),
		timeSource:      shard.GetTimeSource(),
		config:          shard.GetConfig(),
		mutex:           locks.NewPriorityMutex(),
		transaction:     NewTransaction(shard),
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

func (c *ContextImpl) Clear() {
	c.metricsHandler.Counter(metrics.WorkflowContextCleared.GetMetricName()).Record(1)
	if c.MutableState != nil {
		c.MutableState.GetQueryRegistry().Clear()
	}
	c.MutableState = nil
}

func (c *ContextImpl) GetWorkflowKey() definition.WorkflowKey {
	return c.workflowKey
}

func (c *ContextImpl) GetNamespace() namespace.Name {
	namespaceEntry, err := c.shard.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(c.workflowKey.NamespaceID),
	)
	if err != nil {
		return ""
	}
	return namespaceEntry.Name()
}

func (c *ContextImpl) LoadExecutionStats(ctx context.Context) (*persistencespb.ExecutionStats, error) {
	_, err := c.LoadMutableState(ctx)
	if err != nil {
		return nil, err
	}
	return c.MutableState.GetExecutionInfo().ExecutionStats, nil
}

func (c *ContextImpl) LoadMutableState(ctx context.Context) (MutableState, error) {
	namespaceEntry, err := c.shard.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(c.workflowKey.NamespaceID),
	)
	if err != nil {
		return nil, err
	}

	if c.MutableState == nil {
		response, err := getWorkflowExecution(ctx, c.shard, &persistence.GetWorkflowExecutionRequest{
			ShardID:     c.shard.GetShardID(),
			NamespaceID: c.workflowKey.NamespaceID,
			WorkflowID:  c.workflowKey.WorkflowID,
			RunID:       c.workflowKey.RunID,
		})
		if err != nil {
			return nil, err
		}

		c.MutableState, err = newMutableStateFromDB(
			c.shard,
			c.shard.GetEventsCache(),
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
	workflowEventsSlice ...*persistence.WorkflowEvents,
) (int64, error) {
	return PersistWorkflowEvents(ctx, c.shard, workflowEventsSlice...)
}

func (c *ContextImpl) CreateWorkflowExecution(
	ctx context.Context,
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
		ShardID: c.shard.GetShardID(),
		// workflow create mode & prev run ID & version
		Mode:                     createMode,
		PreviousRunID:            prevRunID,
		PreviousLastWriteVersion: prevLastWriteVersion,

		NewWorkflowSnapshot: *newWorkflow,
		NewWorkflowEvents:   newWorkflowEvents,
	}

	_, err := createWorkflowExecution(
		ctx,
		c.shard,
		newMutableState.GetCurrentVersion(),
		createRequest,
	)
	if err != nil {
		return err
	}

	engine, err := c.shard.GetEngine(ctx)
	if err != nil {
		return err
	}
	NotifyWorkflowSnapshotTasks(engine, newWorkflow)
	emitStateTransitionCount(c.metricsHandler, c.clusterMetadata, newMutableState)

	return nil
}

func (c *ContextImpl) ConflictResolveWorkflowExecution(
	ctx context.Context,
	conflictResolveMode persistence.ConflictResolveWorkflowMode,
	resetMutableState MutableState,
	newContext Context,
	newMutableState MutableState,
	currentContext Context,
	currentMutableState MutableState,
	currentTransactionPolicy *TransactionPolicy,
) (retError error) {

	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	resetWorkflow, resetWorkflowEventsSeq, err := resetMutableState.CloseTransactionAsSnapshot(
		TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}

	var newWorkflow *persistence.WorkflowSnapshot
	var newWorkflowEventsSeq []*persistence.WorkflowEvents
	if newContext != nil && newMutableState != nil {

		defer func() {
			if retError != nil {
				newContext.Clear()
			}
		}()

		newWorkflow, newWorkflowEventsSeq, err = newMutableState.CloseTransactionAsSnapshot(
			TransactionPolicyPassive,
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
		conflictResolveMode,
		resetWorkflowEventsSeq,
		newWorkflowEventsSeq,
		// current workflow events will not participate in the events reapplication
	); err != nil {
		return err
	}

	if _, _, _, err := c.transaction.ConflictResolveWorkflowExecution(
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

	emitStateTransitionCount(c.metricsHandler, c.clusterMetadata, resetMutableState)
	emitStateTransitionCount(c.metricsHandler, c.clusterMetadata, newMutableState)
	emitStateTransitionCount(c.metricsHandler, c.clusterMetadata, currentMutableState)

	return nil
}

func (c *ContextImpl) UpdateWorkflowExecutionAsActive(
	ctx context.Context,
) error {

	// We only perform this check on active cluster for the namespace
	historyForceTerminate, err := c.enforceHistorySizeCheck(ctx)
	if err != nil {
		return err
	}
	msForceTerminate := false
	if !historyForceTerminate {
		msForceTerminate, err = c.enforceMutableStateSizeCheck(ctx)
		if err != nil {
			return err
		}
	}

	err = c.UpdateWorkflowExecutionWithNew(
		ctx,
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
	if historyForceTerminate {
		return consts.ErrHistorySizeExceedsLimit
	}
	if msForceTerminate {
		return consts.ErrMutableStateSizeExceedsLimit
	}

	return nil
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsActive(
	ctx context.Context,
	newContext Context,
	newMutableState MutableState,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		persistence.UpdateWorkflowModeUpdateCurrent,
		newContext,
		newMutableState,
		TransactionPolicyActive,
		TransactionPolicyActive.Ptr(),
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionAsPassive(
	ctx context.Context,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		TransactionPolicyPassive,
		nil,
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsPassive(
	ctx context.Context,
	newContext Context,
	newMutableState MutableState,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		persistence.UpdateWorkflowModeUpdateCurrent,
		newContext,
		newMutableState,
		TransactionPolicyPassive,
		TransactionPolicyPassive.Ptr(),
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNew(
	ctx context.Context,
	updateMode persistence.UpdateWorkflowMode,
	newContext Context,
	newMutableState MutableState,
	currentWorkflowTransactionPolicy TransactionPolicy,
	newWorkflowTransactionPolicy *TransactionPolicy,
) (retError error) {

	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	currentWorkflow, currentWorkflowEventsSeq, err := c.MutableState.CloseTransactionAsMutation(
		currentWorkflowTransactionPolicy,
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

	if err := c.mergeContinueAsNewReplicationTasks(
		updateMode,
		currentWorkflow,
		newWorkflow,
	); err != nil {
		return err
	}

	if err := c.updateWorkflowExecutionEventReapply(
		ctx,
		updateMode,
		currentWorkflowEventsSeq,
		newWorkflowEventsSeq,
	); err != nil {
		return err
	}

	if _, _, err := c.transaction.UpdateWorkflowExecution(
		ctx,
		updateMode,
		c.MutableState.GetCurrentVersion(),
		currentWorkflow,
		currentWorkflowEventsSeq,
		MutableStateFailoverVersion(newMutableState),
		newWorkflow,
		newWorkflowEventsSeq,
	); err != nil {
		return err
	}

	emitStateTransitionCount(c.metricsHandler, c.clusterMetadata, c.MutableState)
	emitStateTransitionCount(c.metricsHandler, c.clusterMetadata, newMutableState)

	// finally emit session stats
	namespace := c.GetNamespace()
	emitWorkflowHistoryStats(
		c.metricsHandler,
		namespace,
		int(c.MutableState.GetExecutionInfo().ExecutionStats.HistorySize),
		int(c.MutableState.GetNextEventID()-1),
	)

	return nil
}

func (c *ContextImpl) SetWorkflowExecution(
	ctx context.Context,
) (retError error) {
	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	resetWorkflowSnapshot, resetWorkflowEventsSeq, err := c.MutableState.CloseTransactionAsSnapshot(
		TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}
	if len(resetWorkflowEventsSeq) != 0 {
		c.metricsHandler.Counter(metrics.ClosedWorkflowBufferEventCount.GetMetricName()).Record(1)
		c.logger.Warn("SetWorkflowExecution encountered new events")
	}

	return c.transaction.SetWorkflowExecution(
		ctx,
		resetWorkflowSnapshot,
	)
}

func (c *ContextImpl) mergeContinueAsNewReplicationTasks(
	updateMode persistence.UpdateWorkflowMode,
	currentWorkflowMutation *persistence.WorkflowMutation,
	newWorkflowSnapshot *persistence.WorkflowSnapshot,
) error {

	if currentWorkflowMutation.ExecutionState.Status != enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
		return nil
	} else if updateMode == persistence.UpdateWorkflowModeBypassCurrent && newWorkflowSnapshot == nil {
		// update current workflow as zombie & continue as new without new zombie workflow
		// this case can be valid if new workflow is already created by resend
		return nil
	}

	// current workflow is doing continue as new

	// it is possible that continue as new is done as part of passive logic
	if len(currentWorkflowMutation.Tasks[tasks.CategoryReplication]) == 0 {
		return nil
	}

	if newWorkflowSnapshot == nil || len(newWorkflowSnapshot.Tasks[tasks.CategoryReplication]) != 1 {
		return serviceerror.NewInternal("unable to find replication task from new workflow for continue as new replication")
	}

	// merge the new run first event batch replication task
	// to current event batch replication task
	newRunTask := newWorkflowSnapshot.Tasks[tasks.CategoryReplication][0].(*tasks.HistoryReplicationTask)
	delete(newWorkflowSnapshot.Tasks, tasks.CategoryReplication)

	newRunBranchToken := newRunTask.BranchToken
	newRunID := newRunTask.RunID
	taskUpdated := false
	for _, replicationTask := range currentWorkflowMutation.Tasks[tasks.CategoryReplication] {
		if task, ok := replicationTask.(*tasks.HistoryReplicationTask); ok {
			taskUpdated = true
			task.NewRunBranchToken = newRunBranchToken
			task.NewRunID = newRunID
		}
	}
	if !taskUpdated {
		return serviceerror.NewInternal("unable to find replication task from current workflow for continue as new replication")
	}
	return nil
}

func (c *ContextImpl) updateWorkflowExecutionEventReapply(
	ctx context.Context,
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
	return c.ReapplyEvents(ctx, eventBatches)
}

func (c *ContextImpl) conflictResolveEventReapply(
	ctx context.Context,
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
	return c.ReapplyEvents(ctx, eventBatches)
}

func (c *ContextImpl) ReapplyEvents(
	ctx context.Context,
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
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
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
	namespaceRegistry := c.shard.GetNamespaceRegistry()
	serializer := c.shard.GetPayloadSerializer()
	namespaceEntry, err := namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}

	activeCluster := namespaceEntry.ActiveClusterName()
	if activeCluster == c.shard.GetClusterMetadata().GetCurrentClusterName() {
		engine, err := c.shard.GetEngine(ctx)
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
	sourceAdminClient, err := c.shard.GetRemoteAdminClient(activeCluster)
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

func (c *ContextImpl) UpdateRegistry(ctx context.Context) update.Registry {
	if c.updateRegistry == nil {
		nsIDStr := c.MutableState.GetNamespaceEntry().ID().String()
		c.updateRegistry = update.NewRegistry(
			func() update.UpdateStore { return c.MutableState },
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
) (bool, error) {
	// Hard terminate workflow if still running and breached history size or history count limits
	if c.maxHistorySizeExceeded() {
		if err := c.forceTerminateWorkflow(ctx, common.FailureReasonHistorySizeExceedsLimit); err != nil {
			return false, err
		}
		// Return true to caller to indicate workflow state is overwritten to force terminate execution on update
		return true, nil
	}
	return false, nil
}

// Returns true if the workflow is running and history size or event count should trigger a forced termination
// Prints a log message if history size or history event count are over the error or warn limits
func (c *ContextImpl) maxHistorySizeExceeded() bool {
	namespaceName := c.GetNamespace().String()
	historySizeLimitWarn := c.config.HistorySizeLimitWarn(namespaceName)
	historySizeLimitError := c.config.HistorySizeLimitError(namespaceName)
	historyCountLimitWarn := c.config.HistoryCountLimitWarn(namespaceName)
	historyCountLimitError := c.config.HistoryCountLimitError(namespaceName)

	historySize := int(c.MutableState.GetExecutionInfo().ExecutionStats.HistorySize)
	historyCount := int(c.MutableState.GetNextEventID() - 1)

	if (historySize > historySizeLimitError || historyCount > historyCountLimitError) &&
		c.MutableState.IsWorkflowExecutionRunning() {
		c.logger.Warn("history size exceeds error limit.",
			tag.WorkflowNamespaceID(c.workflowKey.NamespaceID),
			tag.WorkflowID(c.workflowKey.WorkflowID),
			tag.WorkflowRunID(c.workflowKey.RunID),
			tag.WorkflowHistorySize(historySize),
			tag.WorkflowEventCount(historyCount))

		return true
	}

	if historySize > historySizeLimitWarn || historyCount > historyCountLimitWarn {
		c.throttledLogger.Warn("history size exceeds warn limit.",
			tag.WorkflowNamespaceID(c.MutableState.GetExecutionInfo().NamespaceId),
			tag.WorkflowID(c.MutableState.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(c.MutableState.GetExecutionState().RunId),
			tag.WorkflowHistorySize(historySize),
			tag.WorkflowEventCount(historyCount))
	}

	return false
}

// Returns true if execution is forced terminated
// TODO: ideally this check should be after closing mutable state tx, but that would require a large refactor
func (c *ContextImpl) enforceMutableStateSizeCheck(ctx context.Context) (bool, error) {
	if c.maxMutableStateSizeExceeded() {
		if err := c.forceTerminateWorkflow(ctx, common.FailureReasonMutableStateSizeExceedsLimit); err != nil {
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
	failureReason string,
) error {
	if !c.MutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// Discard pending changes in MutableState so we can apply terminate state transition
	c.Clear()

	// Reload mutable state
	mutableState, err := c.LoadMutableState(ctx)
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

func emitStateTransitionCount(
	metricsHandler metrics.Handler,
	clusterMetadata cluster.Metadata,
	mutableState MutableState,
) {
	if mutableState == nil {
		return
	}

	namespaceEntry := mutableState.GetNamespaceEntry()
	metricsHandler.Histogram(
		metrics.StateTransitionCount.GetMetricName(),
		metrics.StateTransitionCount.GetMetricUnit(),
	).Record(
		mutableState.GetExecutionInfo().StateTransitionCount,
		metrics.NamespaceTag(namespaceEntry.Name().String()),
		metrics.NamespaceStateTag(namespaceState(clusterMetadata, convert.Int64Ptr(mutableState.GetCurrentVersion()))),
	)
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
	return convert.Int64Ptr(mutableState.GetCurrentVersion())
}
