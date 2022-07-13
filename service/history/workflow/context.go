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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const (
	defaultRemoteCallTimeout = 30 * time.Second
)

const (
	CallerTypeAPI  CallerType = 0
	CallerTypeTask CallerType = 1
)

type (
	CallerType int

	Context interface {
		GetNamespace() namespace.Name
		GetNamespaceID() namespace.ID
		GetWorkflowID() string
		GetRunID() string

		LoadWorkflowExecution(ctx context.Context) (MutableState, error)
		LoadExecutionStats(ctx context.Context) (*persistencespb.ExecutionStats, error)
		Clear()

		Lock(ctx context.Context, caller CallerType) error
		Unlock(caller CallerType)

		GetHistorySize() int64
		SetHistorySize(size int64)

		ReapplyEvents(
			eventBatches []*persistence.WorkflowEvents,
		) error

		PersistWorkflowEvents(
			ctx context.Context,
			workflowEvents *persistence.WorkflowEvents,
		) (int64, error)

		CreateWorkflowExecution(
			ctx context.Context,
			now time.Time,
			createMode persistence.CreateWorkflowMode,
			prevRunID string,
			prevLastWriteVersion int64,
			newMutableState MutableState,
			newWorkflow *persistence.WorkflowSnapshot,
			newWorkflowEvents []*persistence.WorkflowEvents,
		) error
		ConflictResolveWorkflowExecution(
			ctx context.Context,
			now time.Time,
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
			now time.Time,
		) error
		UpdateWorkflowExecutionWithNewAsActive(
			ctx context.Context,
			now time.Time,
			newContext Context,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionAsPassive(
			ctx context.Context,
			now time.Time,
		) error
		UpdateWorkflowExecutionWithNewAsPassive(
			ctx context.Context,
			now time.Time,
			newContext Context,
			newMutableState MutableState,
		) error
		UpdateWorkflowExecutionWithNew(
			ctx context.Context,
			now time.Time,
			updateMode persistence.UpdateWorkflowMode,
			newContext Context,
			newMutableState MutableState,
			currentWorkflowTransactionPolicy TransactionPolicy,
			newWorkflowTransactionPolicy *TransactionPolicy,
		) error
		SetWorkflowExecution(
			ctx context.Context,
			now time.Time,
		) error
	}
)

type (
	ContextImpl struct {
		shard         shard.Context
		workflowKey   definition.WorkflowKey
		logger        log.Logger
		metricsClient metrics.Client
		timeSource    clock.TimeSource
		config        *configs.Config
		transaction   Transaction

		mutex        locks.PriorityMutex
		MutableState MutableState
		stats        *persistencespb.ExecutionStats
	}
)

var _ Context = (*ContextImpl)(nil)

var (
	PersistenceOperationRetryPolicy = common.CreatePersistenceRetryPolicy()
)

func NewContext(
	shard shard.Context,
	workflowKey definition.WorkflowKey,
	logger log.Logger,
) *ContextImpl {
	return &ContextImpl{
		shard:         shard,
		workflowKey:   workflowKey,
		logger:        logger,
		metricsClient: shard.GetMetricsClient(),
		timeSource:    shard.GetTimeSource(),
		config:        shard.GetConfig(),
		mutex:         locks.NewPriorityMutex(),
		transaction:   NewTransaction(shard),
		stats: &persistencespb.ExecutionStats{
			HistorySize: 0,
		},
	}
}

func (c *ContextImpl) Lock(
	ctx context.Context,
	caller CallerType,
) error {
	switch caller {
	case CallerTypeAPI:
		return c.mutex.LockHigh(ctx)
	case CallerTypeTask:
		return c.mutex.LockLow(ctx)
	default:
		panic(fmt.Sprintf("unknown caller type: %v", caller))
	}
}

func (c *ContextImpl) Unlock(
	caller CallerType,
) {
	switch caller {
	case CallerTypeAPI:
		c.mutex.UnlockHigh()
	case CallerTypeTask:
		c.mutex.UnlockLow()
	default:
		panic(fmt.Sprintf("unknown caller type: %v", caller))
	}
}

func (c *ContextImpl) Clear() {
	c.metricsClient.IncCounter(metrics.WorkflowContextScope, metrics.WorkflowContextCleared)
	if c.MutableState != nil {
		c.MutableState.GetQueryRegistry().Clear()
	}
	c.MutableState = nil
	c.stats = &persistencespb.ExecutionStats{
		HistorySize: 0,
	}
}

func (c *ContextImpl) GetNamespaceID() namespace.ID {
	return namespace.ID(c.workflowKey.NamespaceID)
}

func (c *ContextImpl) GetWorkflowID() string {
	return c.workflowKey.WorkflowID
}

func (c *ContextImpl) GetRunID() string {
	return c.workflowKey.RunID
}

func (c *ContextImpl) GetNamespace() namespace.Name {
	namespaceEntry, err := c.shard.GetNamespaceRegistry().GetNamespaceByID(c.GetNamespaceID())
	if err != nil {
		return ""
	}
	return namespaceEntry.Name()
}

func (c *ContextImpl) GetHistorySize() int64 {
	return c.stats.HistorySize
}

func (c *ContextImpl) SetHistorySize(size int64) {
	c.stats.HistorySize = size
}

func (c *ContextImpl) LoadExecutionStats(ctx context.Context) (*persistencespb.ExecutionStats, error) {
	_, err := c.LoadWorkflowExecution(ctx)
	if err != nil {
		return nil, err
	}
	return c.stats, nil
}

func (c *ContextImpl) LoadWorkflowExecution(ctx context.Context) (MutableState, error) {
	namespaceEntry, err := c.shard.GetNamespaceRegistry().GetNamespaceByID(c.GetNamespaceID())
	if err != nil {
		return nil, err
	}

	if c.MutableState == nil {
		response, err := getWorkflowExecutionWithRetry(ctx, c.shard, &persistence.GetWorkflowExecutionRequest{
			ShardID:     c.shard.GetShardID(),
			NamespaceID: c.workflowKey.NamespaceID,
			WorkflowID:  c.workflowKey.WorkflowID,
			RunID:       c.workflowKey.RunID,
		})
		if err != nil {
			return nil, err
		}

		c.MutableState, err = newMutableStateBuilderFromDB(
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

		c.stats = response.State.ExecutionInfo.ExecutionStats
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
		c.shard.GetTimeSource().Now(),
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
	workflowEvents *persistence.WorkflowEvents,
) (int64, error) {
	return PersistWorkflowEvents(ctx, c.shard, workflowEvents)
}

func (c *ContextImpl) CreateWorkflowExecution(
	ctx context.Context,
	_ time.Time,
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

	resp, err := createWorkflowExecutionWithRetry(
		ctx,
		c.shard,
		createRequest,
	)
	if err != nil {
		return err
	}
	c.SetHistorySize(int64(resp.NewMutableStateStats.HistoryStatistics.SizeDiff))

	engine, err := c.shard.GetEngine(ctx)
	if err != nil {
		return err
	}
	NotifyWorkflowSnapshotTasks(engine, newWorkflow, newMutableState.GetNamespaceEntry().ActiveClusterName())
	emitStateTransitionCount(c.metricsClient, newMutableState)

	return nil
}

func (c *ContextImpl) ConflictResolveWorkflowExecution(
	ctx context.Context,
	now time.Time,
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
		now,
		TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}
	resetWorkflow.ExecutionInfo.ExecutionStats = &persistencespb.ExecutionStats{
		HistorySize: c.GetHistorySize(),
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
			now,
			TransactionPolicyPassive,
		)
		if err != nil {
			return err
		}
		newWorkflow.ExecutionInfo.ExecutionStats = &persistencespb.ExecutionStats{
			HistorySize: newContext.GetHistorySize(),
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
			now,
			*currentTransactionPolicy,
		)
		if err != nil {
			return err
		}
		currentWorkflow.ExecutionInfo.ExecutionStats = &persistencespb.ExecutionStats{
			HistorySize: currentContext.GetHistorySize(),
		}
	}

	if err := c.conflictResolveEventReapply(
		conflictResolveMode,
		resetWorkflowEventsSeq,
		newWorkflowEventsSeq,
		// current workflow events will not participate in the events reapplication
	); err != nil {
		return err
	}

	if resetWorkflowSizeDiff, newWorkflowSizeDiff, currentWorkflowSizeDiff, err := c.transaction.ConflictResolveWorkflowExecution(
		ctx,
		conflictResolveMode,
		resetWorkflow,
		resetWorkflowEventsSeq,
		newWorkflow,
		newWorkflowEventsSeq,
		currentWorkflow,
		currentWorkflowEventsSeq,
		resetMutableState.GetNamespaceEntry().ActiveClusterName(),
	); err != nil {
		return err
	} else {
		c.SetHistorySize(c.GetHistorySize() + resetWorkflowSizeDiff)
		if newContext != nil {
			newContext.SetHistorySize(newContext.GetHistorySize() + newWorkflowSizeDiff)
		}
		if currentContext != nil {
			currentContext.SetHistorySize(currentContext.GetHistorySize() + currentWorkflowSizeDiff)
		}
	}

	emitStateTransitionCount(c.metricsClient, resetMutableState)
	emitStateTransitionCount(c.metricsClient, newMutableState)
	emitStateTransitionCount(c.metricsClient, currentMutableState)

	return nil
}

func (c *ContextImpl) UpdateWorkflowExecutionAsActive(
	ctx context.Context,
	now time.Time,
) error {

	// We only perform this check on active cluster for the namespace
	forceTerminate, err := c.enforceSizeCheck(ctx)
	if err != nil {
		return err
	}

	if err := c.UpdateWorkflowExecutionWithNew(
		ctx,
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		TransactionPolicyActive,
		nil,
	); err != nil {
		return err
	}

	if forceTerminate {
		// Returns ResourceExhausted error back to caller after workflow execution is forced terminated
		// Retrying the operation will give appropriate semantics operation should expect in the case of workflow
		// execution being closed.
		return consts.ErrSizeExceedsLimit
	}

	return nil
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsActive(
	ctx context.Context,
	now time.Time,
	newContext Context,
	newMutableState MutableState,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		newContext,
		newMutableState,
		TransactionPolicyActive,
		TransactionPolicyActive.Ptr(),
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionAsPassive(
	ctx context.Context,
	now time.Time,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		nil,
		nil,
		TransactionPolicyPassive,
		nil,
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsPassive(
	ctx context.Context,
	now time.Time,
	newContext Context,
	newMutableState MutableState,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		newContext,
		newMutableState,
		TransactionPolicyPassive,
		TransactionPolicyPassive.Ptr(),
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNew(
	ctx context.Context,
	now time.Time,
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
		now,
		currentWorkflowTransactionPolicy,
	)
	if err != nil {
		return err
	}
	currentWorkflow.ExecutionInfo.ExecutionStats = &persistencespb.ExecutionStats{
		HistorySize: c.GetHistorySize(),
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
			now,
			*newWorkflowTransactionPolicy,
		)
		if err != nil {
			return err
		}
		newWorkflow.ExecutionInfo.ExecutionStats = &persistencespb.ExecutionStats{
			HistorySize: newContext.GetHistorySize(),
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
		updateMode,
		currentWorkflowEventsSeq,
		newWorkflowEventsSeq,
	); err != nil {
		return err
	}

	if currentWorkflowSizeDiff, newWorkflowSizeDiff, err := c.transaction.UpdateWorkflowExecution(
		ctx,
		updateMode,
		currentWorkflow,
		currentWorkflowEventsSeq,
		newWorkflow,
		newWorkflowEventsSeq,
		c.MutableState.GetNamespaceEntry().ActiveClusterName(),
	); err != nil {
		return err
	} else {
		c.SetHistorySize(c.GetHistorySize() + currentWorkflowSizeDiff)
		if newContext != nil {
			newContext.SetHistorySize(newContext.GetHistorySize() + newWorkflowSizeDiff)
		}
	}

	emitStateTransitionCount(c.metricsClient, c.MutableState)
	emitStateTransitionCount(c.metricsClient, newMutableState)

	// finally emit session stats
	namespace := c.GetNamespace()
	emitWorkflowHistoryStats(
		c.metricsClient,
		namespace,
		int(c.GetHistorySize()),
		int(c.MutableState.GetNextEventID()-1),
	)

	return nil
}

func (c *ContextImpl) SetWorkflowExecution(ctx context.Context, now time.Time) (retError error) {
	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	resetWorkflowSnapshot, resetWorkflowEventsSeq, err := c.MutableState.CloseTransactionAsSnapshot(
		now,
		TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}
	if len(resetWorkflowEventsSeq) != 0 {
		return serviceerror.NewInternal("SetWorkflowExecution encountered new events")
	}

	resetWorkflowSnapshot.ExecutionInfo.ExecutionStats = &persistencespb.ExecutionStats{
		HistorySize: c.GetHistorySize(),
	}

	return c.transaction.SetWorkflowExecution(
		ctx,
		resetWorkflowSnapshot,
		c.MutableState.GetNamespaceEntry().ActiveClusterName(),
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
	taskUpdated := false
	for _, replicationTask := range currentWorkflowMutation.Tasks[tasks.CategoryReplication] {
		if task, ok := replicationTask.(*tasks.HistoryReplicationTask); ok {
			taskUpdated = true
			task.NewRunBranchToken = newRunBranchToken
		}
	}
	if !taskUpdated {
		return serviceerror.NewInternal("unable to find replication task from current workflow for continue as new replication")
	}
	return nil
}

func (c *ContextImpl) updateWorkflowExecutionEventReapply(
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
	return c.ReapplyEvents(eventBatches)
}

func (c *ContextImpl) conflictResolveEventReapply(
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
	return c.ReapplyEvents(eventBatches)
}

func (c *ContextImpl) ReapplyEvents(
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

	// TODO: should we pass in a context instead of using the default one?
	ctx, cancel := context.WithTimeout(context.Background(), defaultRemoteCallTimeout)
	defer cancel()

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
	sourceCluster, err := c.shard.GetRemoteAdminClient(activeCluster)
	if err != nil {
		return err
	}
	if sourceCluster == nil {
		return serviceerror.NewInternal(fmt.Sprintf("cannot find cluster config %v to do reapply", activeCluster))
	}
	ctx2, cancel2 := rpc.NewContextWithTimeoutAndHeaders(defaultRemoteCallTimeout)
	defer cancel2()
	_, err = sourceCluster.ReapplyEvents(
		ctx2,
		&adminservice.ReapplyEventsRequest{
			Namespace:         namespaceEntry.Name().String(),
			NamespaceId:       namespaceEntry.ID().String(),
			WorkflowExecution: execution,
			Events:            reapplyEventsDataBlob,
		},
	)

	return err
}

// Returns true if execution is forced terminated
func (c *ContextImpl) enforceSizeCheck(
	ctx context.Context,
) (bool, error) {
	namespaceName := c.GetNamespace().String()
	historySizeLimitWarn := c.config.HistorySizeLimitWarn(namespaceName)
	historySizeLimitError := c.config.HistorySizeLimitError(namespaceName)
	historyCountLimitWarn := c.config.HistoryCountLimitWarn(namespaceName)
	historyCountLimitError := c.config.HistoryCountLimitError(namespaceName)

	historySize := int(c.GetHistorySize())
	historyCount := int(c.MutableState.GetNextEventID() - 1)

	// Hard terminate workflow if still running and breached size or count limit
	if (historySize > historySizeLimitError || historyCount > historyCountLimitError) &&
		c.MutableState.IsWorkflowExecutionRunning() {
		c.logger.Error("history size exceeds error limit.",
			tag.WorkflowNamespaceID(c.workflowKey.NamespaceID),
			tag.WorkflowID(c.workflowKey.WorkflowID),
			tag.WorkflowRunID(c.workflowKey.RunID),
			tag.WorkflowHistorySize(historySize),
			tag.WorkflowEventCount(historyCount))

		// Discard pending changes in MutableState so we can apply terminate state transition
		c.Clear()

		// Reload mutable state
		mutableState, err := c.LoadWorkflowExecution(ctx)
		if err != nil {
			return false, err
		}

		// Terminate workflow is written as a separate batch and might result in more than one event as we close the
		// outstanding workflow task before terminating the workflow
		eventBatchFirstEventID := mutableState.GetNextEventID()
		if err := TerminateWorkflow(
			mutableState,
			eventBatchFirstEventID,
			common.FailureReasonSizeExceedsLimit,
			nil,
			consts.IdentityHistoryService,
			false,
		); err != nil {
			return false, err
		}

		// Return true to caller to indicate workflow state is overwritten to force terminate execution on update
		return true, nil
	}

	if historySize > historySizeLimitWarn || historyCount > historyCountLimitWarn {
		c.logger.Warn("history size exceeds warn limit.",
			tag.WorkflowNamespaceID(c.MutableState.GetExecutionInfo().NamespaceId),
			tag.WorkflowID(c.MutableState.GetExecutionInfo().WorkflowId),
			tag.WorkflowRunID(c.MutableState.GetExecutionState().RunId),
			tag.WorkflowHistorySize(historySize),
			tag.WorkflowEventCount(historyCount))
	}

	return false, nil
}

func emitStateTransitionCount(
	metricsClient metrics.Client,
	mutableState MutableState,
) {
	if mutableState == nil {
		return
	}

	metricsClient.Scope(
		metrics.WorkflowContextScope,
		metrics.NamespaceTag(mutableState.GetNamespaceEntry().Name().String()),
	).RecordDistribution(metrics.StateTransitionCount, int(mutableState.GetExecutionInfo().StateTransitionCount))
}
