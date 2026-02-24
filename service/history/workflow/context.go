package workflow

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow/update"
)

type (
	ContextImpl struct {
		workflowKey     definition.WorkflowKey
		archetypeID     chasm.ArchetypeID
		logger          log.Logger
		throttledLogger log.ThrottledLogger
		metricsHandler  metrics.Handler
		config          *configs.Config

		lock           locks.PrioritySemaphore
		MutableState   historyi.MutableState
		updateRegistry update.Registry
	}
)

var _ historyi.WorkflowContext = (*ContextImpl)(nil)

func NewContext(
	config *configs.Config,
	workflowKey definition.WorkflowKey,
	archetypeID chasm.ArchetypeID,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	metricsHandler metrics.Handler,
) *ContextImpl {
	tags := func() []tag.Tag {
		return []tag.Tag{
			tag.WorkflowNamespaceID(workflowKey.NamespaceID),
			tag.WorkflowID(workflowKey.WorkflowID),
			tag.WorkflowRunID(workflowKey.RunID),
		}
	}
	contextImpl := &ContextImpl{
		workflowKey:     workflowKey,
		archetypeID:     archetypeID,
		logger:          log.NewLazyLogger(logger, tags),
		throttledLogger: log.NewLazyLogger(throttledLogger, tags),
		metricsHandler:  metricsHandler.WithTags(metrics.OperationTag(metrics.WorkflowContextScope)),
		config:          config,
		lock:            locks.NewPrioritySemaphore(1),
	}
	softassert.That(
		contextImpl.throttledLogger,
		contextImpl.archetypeID != chasm.UnspecifiedArchetypeID,
		"Creating execution context with unspecified archetype ID",
	)

	return contextImpl
}

func (c *ContextImpl) Lock(
	ctx context.Context,
	lockPriority locks.Priority,
) error {
	return c.lock.Acquire(ctx, lockPriority, 1)
}

func (c *ContextImpl) Unlock() {
	c.lock.Release(1)
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
		c.MutableState.RemoveSpeculativeWorkflowTaskTimeoutTask()
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

func (c *ContextImpl) GetNamespace(shardContext historyi.ShardContext) namespace.Name {
	namespaceEntry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(
		namespace.ID(c.workflowKey.NamespaceID),
	)
	if err != nil {
		return ""
	}
	return namespaceEntry.Name()
}

func (c *ContextImpl) LoadExecutionStats(ctx context.Context, shardContext historyi.ShardContext) (*persistencespb.ExecutionStats, error) {
	_, err := c.LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}
	return c.MutableState.GetExecutionInfo().ExecutionStats, nil
}

func (c *ContextImpl) LoadMutableState(ctx context.Context, shardContext historyi.ShardContext) (historyi.MutableState, error) {
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
			ArchetypeID: c.archetypeID,
		})
		if err != nil {
			return nil, err
		}

		mutableState, err := NewMutableStateFromDB(
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

		// NOTE: we can't not assigned the result directly to c.MutableState like below
		// c.MutableState, err = NewMutableStateFromDB(...)
		// Otherwise c.MutableState (an interface) will not be nil, but can point to a nil *MutableStateImpl pointer
		// returned by NewMutableStateFromDB().
		// Thus causing NPE (e.g. when calling c.Clear()) or other unexpected behavior.
		c.MutableState = mutableState
	}

	mutableStateArchetypeID := c.MutableState.ChasmTree().ArchetypeID()
	if c.archetypeID != chasm.UnspecifiedArchetypeID && c.archetypeID != mutableStateArchetypeID {
		chasmRegistry := shardContext.ChasmRegistry()
		contextArchetype, ok := chasmRegistry.ComponentFqnByID(c.archetypeID)
		if !ok {
			contextArchetype = strconv.FormatUint(uint64(c.archetypeID), 10)
		}

		mutableStateArchetype, ok := chasmRegistry.ComponentFqnByID(mutableStateArchetypeID)
		if !ok {
			mutableStateArchetype = strconv.FormatUint(uint64(mutableStateArchetypeID), 10)
		}

		c.logger.Warn("Potential ID conflict across different archetypes",
			tag.Archetype(contextArchetype),
			tag.String("mutable-state-archetype", mutableStateArchetype),
		)
		return nil, serviceerror.NewNotFoundf(
			"CHASM Archetype missmatch for %v, expected: %s, actual: %s",
			c.workflowKey,
			contextArchetype,
			mutableStateArchetype,
		)
	}
	c.archetypeID = mutableStateArchetypeID

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
	shardContext historyi.ShardContext,
	workflowEventsSlice ...*persistence.WorkflowEvents,
) (int64, error) {
	return PersistWorkflowEvents(ctx, shardContext, workflowEventsSlice...)
}

func (c *ContextImpl) CreateWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	createMode persistence.CreateWorkflowMode,
	prevRunID string,
	prevLastWriteVersion int64,
	newMutableState historyi.MutableState,
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

		ArchetypeID: c.archetypeID,

		NewWorkflowSnapshot: *newWorkflow,
		NewWorkflowEvents:   newWorkflowEvents,
	}

	_, err := createWorkflowExecution(
		ctx,
		shardContext,
		newMutableState.GetCurrentVersion(),
		createRequest,
		newMutableState.IsWorkflow(),
	)
	if err != nil {
		return err
	}

	engine, err := shardContext.GetEngine(ctx)
	if err != nil {
		return err
	}
	NotifyOnExecutionSnapshot(engine, newWorkflow)
	emitStateTransitionCount(c.metricsHandler, shardContext.GetClusterMetadata(), newMutableState)

	return nil
}

func (c *ContextImpl) ConflictResolveWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	conflictResolveMode persistence.ConflictResolveWorkflowMode,
	resetMutableState historyi.MutableState,
	newContext historyi.WorkflowContext,
	newMutableState historyi.MutableState,
	currentContext historyi.WorkflowContext,
	currentMutableState historyi.MutableState,
	resetWorkflowTransactionPolicy historyi.TransactionPolicy,
	newWorkflowTransactionPolicy *historyi.TransactionPolicy,
	currentTransactionPolicy *historyi.TransactionPolicy,
) (retError error) {

	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	resetWorkflow, resetWorkflowEventsSeq, err := resetMutableState.CloseTransactionAsSnapshot(
		ctx,
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
			ctx,
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
			ctx,
			*currentTransactionPolicy,
		)
		if err != nil {
			return err
		}
	}

	eventsToReapply := resetWorkflowEventsSeq
	if len(resetWorkflowEventsSeq) == 0 {
		if reapplyCandidateEvents := resetMutableState.GetReapplyCandidateEvents(); len(reapplyCandidateEvents) != 0 {
			eventsToReapply = []*persistence.WorkflowEvents{
				{
					NamespaceID: c.workflowKey.NamespaceID,
					WorkflowID:  c.workflowKey.WorkflowID,
					RunID:       c.workflowKey.RunID,
					Events:      reapplyCandidateEvents,
				},
			}
		}
	}

	if err := c.conflictResolveEventReapply(
		ctx,
		shardContext,
		conflictResolveMode,
		eventsToReapply,
		// The new run is created by applying events so the history builder in newMutableState contains the events be re-applied.
		// So we can use newWorkflowEventsSeq directly to reapply events.
		newWorkflowEventsSeq,
		// current workflow events will not participate in the events reapplication
	); err != nil {
		return err
	}

	if _, _, _, err := NewTransaction(shardContext).ConflictResolveWorkflowExecution(
		ctx,
		conflictResolveMode,
		c.archetypeID,
		resetMutableState.GetCurrentVersion(),
		resetWorkflow,
		resetWorkflowEventsSeq,
		MutableStateFailoverVersion(newMutableState),
		newWorkflow,
		newWorkflowEventsSeq,
		MutableStateFailoverVersion(currentMutableState),
		currentWorkflow,
		currentWorkflowEventsSeq,
		resetMutableState.IsWorkflow(),
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
	shardContext historyi.ShardContext,
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

	updateMode, err := c.updateWorkflowMode()
	if err != nil {
		return err
	}

	err = c.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		updateMode,
		nil,
		nil,
		historyi.TransactionPolicyActive,
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
	shardContext historyi.ShardContext,
	newContext historyi.WorkflowContext,
	newMutableState historyi.MutableState,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		newContext,
		newMutableState,
		historyi.TransactionPolicyActive,
		historyi.TransactionPolicyActive.Ptr(),
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionAsPassive(
	ctx context.Context,
	shardContext historyi.ShardContext,
) error {

	updateMode, err := c.updateWorkflowMode()
	if err != nil {
		return err
	}

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		updateMode,
		nil,
		nil,
		historyi.TransactionPolicyPassive,
		nil,
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNewAsPassive(
	ctx context.Context,
	shardContext historyi.ShardContext,
	newContext historyi.WorkflowContext,
	newMutableState historyi.MutableState,
) error {

	return c.UpdateWorkflowExecutionWithNew(
		ctx,
		shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		newContext,
		newMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
	)
}

func (c *ContextImpl) UpdateWorkflowExecutionWithNew(
	ctx context.Context,
	shardContext historyi.ShardContext,
	updateMode persistence.UpdateWorkflowMode,
	newContext historyi.WorkflowContext,
	newMutableState historyi.MutableState,
	updateWorkflowTransactionPolicy historyi.TransactionPolicy,
	newWorkflowTransactionPolicy *historyi.TransactionPolicy,
) (retError error) {

	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	if newContext != nil && newMutableState != nil && newWorkflowTransactionPolicy != nil {
		c.MutableState.SetSuccessorRunID(newMutableState.GetExecutionState().RunId)
	}

	updateWorkflow, updateWorkflowEventsSeq, err := c.MutableState.CloseTransactionAsMutation(
		ctx,
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
			ctx,
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

	eventsToReapply := updateWorkflowEventsSeq
	if len(updateWorkflowEventsSeq) == 0 {
		if reapplyCandidateEvents := c.MutableState.GetReapplyCandidateEvents(); len(reapplyCandidateEvents) != 0 {
			eventsToReapply = []*persistence.WorkflowEvents{
				{
					NamespaceID: c.workflowKey.NamespaceID,
					WorkflowID:  c.workflowKey.WorkflowID,
					RunID:       c.workflowKey.RunID,
					Events:      reapplyCandidateEvents,
				},
			}
		}
	}

	if err := c.updateWorkflowExecutionEventReapply(
		ctx,
		shardContext,
		updateMode,
		eventsToReapply,
		// The new run is created by applying events so the history builder in newMutableState contains the events be re-applied.
		// So we can use newWorkflowEventsSeq directly to reapply events.
		newWorkflowEventsSeq,
	); err != nil {
		return err
	}

	if _, _, err := NewTransaction(shardContext).UpdateWorkflowExecution(
		ctx,
		updateMode,
		c.archetypeID,
		c.MutableState.GetCurrentVersion(),
		updateWorkflow,
		updateWorkflowEventsSeq,
		MutableStateFailoverVersion(newMutableState),
		newWorkflow,
		newWorkflowEventsSeq,
		c.MutableState.IsWorkflow(),
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
	shardContext historyi.ShardContext,
) (retError error) {
	return c.SubmitClosedWorkflowSnapshot(ctx, shardContext, historyi.TransactionPolicyPassive)
}

func (c *ContextImpl) SubmitClosedWorkflowSnapshot(
	ctx context.Context,
	shardContext historyi.ShardContext,
	transactionPolicy historyi.TransactionPolicy,
) (retError error) {
	defer func() {
		if retError != nil {
			c.Clear()
		}
	}()

	resetWorkflowSnapshot, resetWorkflowEventsSeq, err := c.MutableState.CloseTransactionAsSnapshot(
		ctx,
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
		c.archetypeID,
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
			tag.WorkflowNewRunID(newWorkflowSnapshot.ExecutionState.RunId),
		)
		return nil
	}
	if numNewReplicationTasks == 0 {
		c.logger.Info("New workflow has no replication task, while current workflow has replication task",
			tag.WorkflowNewRunID(newWorkflowSnapshot.ExecutionState.RunId),
		)
		return nil
	}
	if numNewReplicationTasks > 1 {
		// This could happen when importing a workflow and current running workflow is being terminated.
		// TODO: support more than one replication tasks (batch of events) in the new workflow
		c.logger.Info("Skipped merging replication tasks because new run has more than one replication tasks",
			tag.WorkflowNewRunID(newWorkflowSnapshot.ExecutionState.RunId),
		)
		return nil
	}

	// current workflow is closing with new workflow
	// merge the new run's replication task to current workflow's replication task
	// so that they can be applied transactionally in the standby cluster.
	// TODO: this logic should be more generic so that the first replication task
	// in the new run doesn't have to be HistoryReplicationTask
	var newRunBranchToken []byte
	var newRunID string
	newRunTask := newWorkflowSnapshot.Tasks[tasks.CategoryReplication][0]
	delete(newWorkflowSnapshot.Tasks, tasks.CategoryReplication)

	switch task := newRunTask.(type) {
	case *tasks.HistoryReplicationTask:
		// Handle HistoryReplicationTask specifically
		newRunBranchToken = task.BranchToken
		newRunID = task.RunID
	case *tasks.SyncVersionedTransitionTask:
		// Handle SyncVersionedTransitionTask specifically
		newRunID = task.RunID
	default:
		// Handle unexpected types or log an error if this case is not expected
		return serviceerror.NewInternalf("unexpected replication task type for new run task %T", newRunTask)
	}
	taskUpdated := false

	updateTask := func(task any) bool {
		switch t := task.(type) {
		case *tasks.HistoryReplicationTask:
			t.NewRunBranchToken = newRunBranchToken
			t.NewRunID = newRunID
			return true
		case *tasks.SyncVersionedTransitionTask:
			t.NewRunID = newRunID
			taskEquivalents := t.TaskEquivalents
			taskEquivalentsUpdated := false
			for idx := len(taskEquivalents) - 1; idx >= 0; idx-- {
				// For state based, we should update a sync versioned transition task and update a history task inside task equivalent.
				if historyTask, ok := taskEquivalents[idx].(*tasks.HistoryReplicationTask); ok {
					historyTask.NewRunBranchToken = newRunBranchToken
					historyTask.NewRunID = newRunID
					taskEquivalentsUpdated = true
					break
				}
			}
			if !taskEquivalentsUpdated {
				c.logger.Error("SyncVersionedTransitionTask has no HistoryReplicationTask equivalent to update")
			}
			return taskEquivalentsUpdated
		default:
		}
		return false
	}

	for idx := numCurrentReplicationTasks - 1; idx >= 0; idx-- {
		replicationTask := currentWorkflowMutation.Tasks[tasks.CategoryReplication][idx]
		if updateTask(replicationTask) {
			taskUpdated = true
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
	shardContext historyi.ShardContext,
	updateMode persistence.UpdateWorkflowMode,
	eventBatch1 []*persistence.WorkflowEvents,
	eventBatch2 []*persistence.WorkflowEvents,
) error {
	if updateMode == persistence.UpdateWorkflowModeIgnoreCurrent {
		if len(eventBatch1) != 0 || len(eventBatch2) != 0 {
			return serviceerror.NewInternal("encountered events reapplication without knowing if workflow is current. Events generated for a close workflow?")
		}
		return nil
	}

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
	shardContext historyi.ShardContext,
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

func (c *ContextImpl) updateWorkflowMode() (persistence.UpdateWorkflowMode, error) {
	if !c.config.EnableUpdateWorkflowModeIgnoreCurrent() {
		return persistence.UpdateWorkflowModeUpdateCurrent, nil
	}

	if c.MutableState.IsCurrentWorkflowGuaranteed() {
		return persistence.UpdateWorkflowModeUpdateCurrent, nil
	}

	guaranteed, err := c.MutableState.IsNonCurrentWorkflowGuaranteed()
	if err != nil {
		return 0, err
	}
	if guaranteed {
		return persistence.UpdateWorkflowModeBypassCurrent, nil
	}
	return persistence.UpdateWorkflowModeIgnoreCurrent, nil
}

func (c *ContextImpl) ReapplyEvents(
	ctx context.Context,
	shardContext historyi.ShardContext,
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
			if shouldReapplyEvent(shardContext.StateMachineRegistry(), event) {
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

	activeCluster := namespaceEntry.ActiveClusterName(workflowID)
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
	reapplyEventsDataBlob, err := serializer.SerializeEvents(reapplyEvents)
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
		return serviceerror.NewInternalf("cannot find cluster config %v to do reapply", activeCluster)
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

func (c *ContextImpl) RefreshTasks(
	ctx context.Context,
	shardContext historyi.ShardContext,
) error {
	mutableState, err := c.LoadMutableState(ctx, shardContext)
	if err != nil {
		return err
	}

	if err := NewTaskRefresher(shardContext).Refresh(ctx, mutableState, false); err != nil {
		return err
	}

	if c.config.EnableUpdateWorkflowModeIgnoreCurrent() {
		return c.UpdateWorkflowExecutionAsPassive(ctx, shardContext)
	}

	// TODO: remove following code once EnableUpdateWorkflowModeIgnoreCurrent config is deprecated.
	if !mutableState.IsWorkflowExecutionRunning() {
		// Can't use UpdateWorkflowExecutionAsPassive since it updates the current run,
		// and we are operating on a closed workflow.
		return c.SubmitClosedWorkflowSnapshot(ctx, shardContext, historyi.TransactionPolicyPassive)
	}
	return c.UpdateWorkflowExecutionAsPassive(ctx, shardContext)
}

func (c *ContextImpl) UpdateRegistry(ctx context.Context) update.Registry {
	if c.updateRegistry != nil && c.updateRegistry.FailoverVersion() != c.MutableState.GetCurrentVersion() {
		c.updateRegistry.Clear()
		c.updateRegistry = nil
	}

	if c.updateRegistry == nil {
		nsName := c.MutableState.GetNamespaceEntry().Name().String()

		c.updateRegistry = update.NewRegistry(
			c.MutableState,
			update.WithNamespace(nsName),
			update.WithLogger(c.logger),
			update.WithMetrics(c.metricsHandler),
			update.WithTracerProvider(trace.SpanFromContext(ctx).TracerProvider()),
			update.WithInFlightLimit(
				func() int {
					return c.config.WorkflowExecutionMaxInFlightUpdates(nsName)
				},
			),
			update.WithInFlightSizeLimit(
				func() int {
					return c.config.WorkflowExecutionMaxInFlightUpdatePayloads(nsName)
				},
			),
			update.WithTotalLimit(
				func() int {
					return c.config.WorkflowExecutionMaxTotalUpdates(nsName)
				},
			),
			update.WithTotalLimitSuggestCAN(
				func() float64 {
					return c.config.WorkflowExecutionMaxTotalUpdatesSuggestContinueAsNewThreshold(nsName)
				},
			),
		)
	}
	return c.updateRegistry
}

// Returns true if execution is forced terminated
func (c *ContextImpl) enforceHistorySizeCheck(
	ctx context.Context,
	shardContext historyi.ShardContext,
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
func (c *ContextImpl) maxHistorySizeExceeded(shardContext historyi.ShardContext) bool {
	namespaceName := c.GetNamespace(shardContext).String()
	historySizeLimitWarn := c.config.HistorySizeLimitWarn(namespaceName)
	historySizeLimitError := c.config.HistorySizeLimitError(namespaceName)
	historySize := int(c.MutableState.GetExecutionInfo().ExecutionStats.HistorySize)

	if historySize > historySizeLimitError && c.MutableState.IsWorkflowExecutionRunning() {
		c.logger.Warn("history size exceeds error limit.",
			tag.WorkflowHistorySize(historySize))

		return true
	}

	if historySize > historySizeLimitWarn {
		c.throttledLogger.Warn("history size exceeds warn limit.",
			tag.WorkflowHistorySize(historySize))
	}

	return false
}

func (c *ContextImpl) enforceHistoryCountCheck(
	ctx context.Context,
	shardContext historyi.ShardContext,
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
func (c *ContextImpl) maxHistoryCountExceeded(shardContext historyi.ShardContext) bool {
	namespaceName := c.GetNamespace(shardContext).String()
	historyCountLimitWarn := c.config.HistoryCountLimitWarn(namespaceName)
	historyCountLimitError := c.config.HistoryCountLimitError(namespaceName)
	historyCount := int(c.MutableState.GetNextEventID() - 1)

	if historyCount > historyCountLimitError && c.MutableState.IsWorkflowExecutionRunning() {
		c.logger.Warn("history count exceeds error limit.",
			tag.WorkflowEventCount(historyCount))

		return true
	}

	if historyCount > historyCountLimitWarn {
		c.throttledLogger.Warn("history count exceeds warn limit.",
			tag.WorkflowEventCount(historyCount))
	}

	return false
}

// Returns true if execution is forced terminated
// TODO: ideally this check should be after closing mutable state tx, but that would require a large refactor
func (c *ContextImpl) enforceMutableStateSizeCheck(ctx context.Context, shardContext historyi.ShardContext) (bool, error) {
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
			tag.WorkflowMutableStateSize(mutableStateSize))

		return true
	}

	if mutableStateSize > mutableStateSizeLimitWarn {
		c.throttledLogger.Warn("mutable state size exceeds warn limit.",
			tag.WorkflowMutableStateSize(mutableStateSize))
	}

	return false
}

func (c *ContextImpl) forceTerminateWorkflow(
	ctx context.Context,
	shardContext historyi.ShardContext,
	failureReason string,
) error {
	if !c.MutableState.IsWorkflowExecutionRunning() {
		return nil
	}

	// Abort updates before clearing context.
	// MS is not persisted yet, but this code is executed only when something
	// really bad happened with workflow, and it won't make any progress anyway.
	c.UpdateRegistry(ctx).Abort(update.AbortReasonWorkflowCompleted)

	// Discard pending changes in MutableState so we can apply terminate state transition
	c.Clear()

	// Reload mutable state
	mutableState, err := c.LoadMutableState(ctx, shardContext)
	if err != nil {
		return err
	}

	if !mutableState.IsWorkflow() {
		return mutableState.ChasmTree().Terminate(chasm.TerminateComponentRequest{
			Identity:  consts.IdentityHistoryService,
			Reason:    failureReason,
			Details:   nil,
			RequestID: primitives.NewUUID().String(),
		})
	}

	return TerminateWorkflow(
		mutableState,
		failureReason,
		nil,
		consts.IdentityHistoryService,
		false,
		nil, // No links necessary.
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
	mutableState historyi.MutableState,
) {
	if mutableState == nil {
		return
	}
	namespaceEntry := mutableState.GetNamespaceEntry()
	metrics.StateTransitionCount.With(metricsHandler).Record(
		mutableState.GetExecutionInfo().StateTransitionCount,
		metrics.NamespaceTag(namespaceEntry.Name().String()),
		metrics.NamespaceStateTag(namespaceState(clusterMetadata, util.Ptr(mutableState.GetCurrentVersion()))),
	)
}

func namespaceState(
	clusterMetadata cluster.Metadata,
	mutableStateCurrentVersion *int64,
) string {

	if mutableStateCurrentVersion == nil {
		return metrics.UnknownNamespaceStateTagValue
	}

	// default value, need to special handle
	if *mutableStateCurrentVersion == 0 {
		return metrics.ActiveNamespaceStateTagValue
	}

	if clusterMetadata.IsVersionFromSameCluster(
		clusterMetadata.GetClusterID(),
		*mutableStateCurrentVersion,
	) {
		return metrics.ActiveNamespaceStateTagValue
	}
	return metrics.PassiveNamespaceStateTagValue
}

func MutableStateFailoverVersion(
	mutableState historyi.MutableState,
) *int64 {
	if mutableState == nil {
		return nil
	}
	return util.Ptr(mutableState.GetCurrentVersion())
}
