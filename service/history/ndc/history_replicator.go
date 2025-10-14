package ndc

import (
	"context"
	"errors"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

const (
	mutableStateMissingMessage = "Resend events due to missing mutable state"
)

type (
	mutableStateRebuilderProvider func(
		mutableState historyi.MutableState,
		logger log.Logger,
	) workflow.MutableStateRebuilder

	bufferEventFlusherProvider func(
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
		logger log.Logger,
	) BufferEventFlusher

	branchMgrProvider func(
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
		logger log.Logger,
	) BranchMgr

	conflictResolverProvider func(
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
		logger log.Logger,
	) ConflictResolver

	workflowResetterProvider func(
		namespaceID namespace.ID,
		workflowID string,
		baseRunID string,
		newContext historyi.WorkflowContext,
		newRunID string,
		logger log.Logger,
	) resetter

	EventBlobs struct {
		CurrentRunEvents *commonpb.DataBlob
		NewRunEvents     *commonpb.DataBlob
	}

	HistoryReplicator interface {
		ApplyEvents(
			ctx context.Context,
			request *historyservice.ReplicateEventsV2Request,
		) error
		// ReplicateHistoryEvents is the batch version of ApplyEvents
		// NOTE:
		//  1. all history events should have the same version
		//  2. all history events should share the same version history
		ReplicateHistoryEvents(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
			baseExecutionInfo *workflowspb.BaseExecutionInfo,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			events [][]*historypb.HistoryEvent,
			newEvents []*historypb.HistoryEvent,
			newRunID string,
		) error
		BackfillHistoryEvents(ctx context.Context, request *historyi.BackfillHistoryEventsRequest) error
	}

	HistoryReplicatorImpl struct {
		shardContext      historyi.ShardContext
		clusterMetadata   cluster.Metadata
		historySerializer serialization.Serializer
		metricsHandler    metrics.Handler
		namespaceRegistry namespace.Registry
		workflowCache     wcache.Cache
		eventsReapplier   EventsReapplier
		transactionMgr    TransactionManager
		logger            log.Logger

		mutableStateMapper *MutableStateMapperImpl
		newResetter        workflowResetterProvider
	}

	rawHistoryData struct {
		rawHistory *commonpb.DataBlob
		nodeID     int64
	}
)

var errPanic = serviceerror.NewInternal("encountered panic")

func NewHistoryReplicator(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
	eventsReapplier EventsReapplier,
	eventSerializer serialization.Serializer,
	logger log.Logger,
) *HistoryReplicatorImpl {

	logger = log.With(logger, tag.ComponentHistoryReplicator)
	transactionMgr := NewTransactionManager(shardContext, workflowCache, eventsReapplier, logger, false)
	replicator := &HistoryReplicatorImpl{
		shardContext:      shardContext,
		clusterMetadata:   shardContext.GetClusterMetadata(),
		historySerializer: eventSerializer,
		metricsHandler:    shardContext.GetMetricsHandler(),
		namespaceRegistry: shardContext.GetNamespaceRegistry(),
		workflowCache:     workflowCache,
		transactionMgr:    transactionMgr,
		eventsReapplier:   eventsReapplier,
		logger:            logger,

		mutableStateMapper: NewMutableStateMapping(
			shardContext,
			func(
				wfContext historyi.WorkflowContext,
				mutableState historyi.MutableState,
				logger log.Logger,
			) BufferEventFlusher {
				return NewBufferEventFlusher(shardContext, wfContext, mutableState, logger)
			},
			func(
				wfContext historyi.WorkflowContext,
				mutableState historyi.MutableState,
				logger log.Logger,
			) BranchMgr {
				return NewBranchMgr(shardContext, wfContext, mutableState, logger)
			},
			func(
				wfContext historyi.WorkflowContext,
				mutableState historyi.MutableState,
				logger log.Logger,
			) ConflictResolver {
				return NewConflictResolver(shardContext, wfContext, mutableState, logger)
			},
			func(
				state historyi.MutableState,
				logger log.Logger,
			) workflow.MutableStateRebuilder {
				return workflow.NewMutableStateRebuilder(
					shardContext,
					logger,
					state,
				)
			},
		),
		newResetter: func(
			namespaceID namespace.ID,
			workflowID string,
			baseRunID string,
			newContext historyi.WorkflowContext,
			newRunID string,
			logger log.Logger,
		) resetter {
			return NewResetter(shardContext, transactionMgr, namespaceID, workflowID, baseRunID, newContext, newRunID, logger)
		},
	}

	return replicator
}

func (r *HistoryReplicatorImpl) ApplyEvents(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
) (retError error) {

	task, err := newReplicationTaskFromRequest(
		r.clusterMetadata,
		r.historySerializer,
		r.logger,
		request,
	)
	if err != nil {
		return err
	}

	return r.doApplyEvents(ctx, task)
}

func (r *HistoryReplicatorImpl) BackfillHistoryEvents(
	ctx context.Context,
	request *historyi.BackfillHistoryEventsRequest,
) error {
	task, err := newReplicationTaskFromBatch(
		r.clusterMetadata,
		r.logger,
		request.WorkflowKey,
		request.BaseExecutionInfo,
		request.VersionHistoryItems,
		request.Events,
		request.NewEvents,
		request.NewRunID,
		request.VersionedHistory,
		true,
	)
	if err != nil {
		return err
	}

	return r.doApplyBackfillEvents(ctx, task, r.applyBackfillEvents)
}

func (r *HistoryReplicatorImpl) doApplyBackfillEvents(
	ctx context.Context,
	task replicationTask,
	action func(context.Context, historyi.MutableState, historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, replicationTask) error,
) (retError error) {
	wfContext, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		task.getNamespaceID(),
		task.getExecution(),
		locks.PriorityHigh,
	)
	if err != nil {
		return err
	}
	defer func() {
		if rec := recover(); rec != nil {
			releaseFn(errPanic)
			panic(rec)
		}
		releaseFn(retError)
	}()

	mutableState, err := wfContext.LoadMutableState(ctx, r.shardContext)
	switch err.(type) {
	case nil:
		return action(ctx, mutableState, wfContext, releaseFn, task)
	case *serviceerror.NotFound:
		return serviceerrors.NewSyncState(
			mutableStateMissingMessage,
			task.getNamespaceID().String(),
			task.getWorkflowID(),
			task.getRunID(),
			task.getVersionedTransition(),
			nil,
		)
	default:
		return err
	}
}

func (r *HistoryReplicatorImpl) applyBackfillEvents(
	ctx context.Context,
	mutableState historyi.MutableState,
	wfContext historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	task replicationTask,
) (retError error) {
	versionedTransition := task.getVersionedTransition()
	if versionedTransition == nil {
		return serviceerror.NewInvalidArgument("versioned transition is required for backfill task")
	}

	if task.getFirstEvent().GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		return serviceerror.NewInvalidArgument("workflow execution started event is not expected in backfill task")
	}

	transitionHistory := mutableState.GetExecutionInfo().GetTransitionHistory()
	if len(transitionHistory) != 0 {
		if transitionhistory.Compare(versionedTransition, transitionhistory.LastVersionedTransition(transitionHistory)) > 0 {
			return serviceerrors.NewSyncState(
				mutableStateMissingMessage,
				task.getNamespaceID().String(),
				task.getWorkflowID(),
				task.getRunID(),
				task.getVersionedTransition(),
				mutableState.GetExecutionInfo().VersionHistories,
			)
		}
		err := transitionhistory.StalenessCheck(transitionHistory, versionedTransition)
		if err == nil {
			return consts.ErrDuplicate
		}
	}

	mutableState, prepareHistoryBranchOut, err := r.mutableStateMapper.GetOrCreateHistoryBranch(ctx, wfContext, mutableState, task)
	if err != nil {
		return err
	} else if !prepareHistoryBranchOut.DoContinue {
		metrics.DuplicateReplicationEventsCounter.With(r.metricsHandler).Record(
			1,
			metrics.OperationTag(metrics.BackfillHistoryEventsTaskScope))
		return consts.ErrDuplicate
	}

	if mutableState.GetExecutionInfo().GetVersionHistories().GetCurrentVersionHistoryIndex() == prepareHistoryBranchOut.BranchIndex {
		// for backfill, we should create a new branch even if the branch is current
		mutableState, prepareHistoryBranchOut, err = r.mutableStateMapper.CreateHistoryBranch(ctx, wfContext, mutableState, task)
		if err != nil {
			return err
		}
	}

	err = task.skipDuplicatedEvents(prepareHistoryBranchOut.EventsApplyIndex)
	if err != nil {
		return err
	}

	if len(task.getNewEvents()) != 0 {
		return r.applyBackfillEventsWithNew(
			ctx,
			wfContext,
			releaseFn,
			task,
		)
	}
	return r.applyBackfillEventsWithoutNew(
		ctx,
		wfContext,
		mutableState,
		prepareHistoryBranchOut.BranchIndex,
		releaseFn,
		task,
	)
}

func (r *HistoryReplicatorImpl) applyBackfillEventsWithNew(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	task replicationTask,
) (retError error) {
	wfContext.Clear()
	releaseFn(nil)

	task, newTask, err := task.splitTask()
	if err != nil {
		return err
	}

	if err := r.doApplyEvents(ctx, newTask); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create new workflow when applyBackfillEvents",
			tag.Error(err),
		)
		return err
	}

	if err := r.doApplyBackfillEvents(ctx, task, r.applyBackfillEvents); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create target workflow when applyBackfillEvents",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *HistoryReplicatorImpl) applyBackfillEventsWithoutNew(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	branchIndex int32,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	task replicationTask,
) (retError error) {
	return r.applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(
		ctx,
		wfContext,
		mutableState,
		branchIndex,
		releaseFn,
		task,
	)
}

func (r *HistoryReplicatorImpl) ReplicateHistoryEvents(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	baseExecutionInfo *workflowspb.BaseExecutionInfo,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	eventsSlice [][]*historypb.HistoryEvent,
	newEvents []*historypb.HistoryEvent,
	newRunID string,
) error {
	task, err := newReplicationTaskFromBatch(
		r.clusterMetadata,
		r.logger,
		workflowKey,
		baseExecutionInfo,
		versionHistoryItems,
		eventsSlice,
		newEvents,
		newRunID,
		nil,
		false,
	)
	if err != nil {
		return err
	}

	return r.doApplyEvents(ctx, task)
}

func (r *HistoryReplicatorImpl) doApplyEvents(
	ctx context.Context,
	task replicationTask,
) (retError error) {

	wfContext, releaseFn, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		r.shardContext,
		task.getNamespaceID(),
		task.getExecution(),
		locks.PriorityHigh,
	)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() {
		if rec := recover(); rec != nil {
			releaseFn(errPanic)
			panic(rec)
		} else {
			releaseFn(retError)
		}
	}()

	if task.getFirstEvent().GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		err = r.applyStartEvents(ctx, wfContext, releaseFn, task)
		if !errors.Is(err, consts.ErrDuplicate) { // if ErrDuplicate is returned from creation, we should also look if every event is applied in the mutable state
			return err
		}
	}
	// apply events, other than simple start workflow execution
	// the update + start workflow execution combination will also be processed here
	mutableState, err := wfContext.LoadMutableState(ctx, r.shardContext)
	switch err.(type) {
	case nil:
		mutableState, _, err = r.mutableStateMapper.FlushBufferEvents(ctx, wfContext, mutableState, task)
		if err != nil {
			return err
		}
		mutableState, prepareHistoryBranchOut, err := r.mutableStateMapper.GetOrCreateHistoryBranch(ctx, wfContext, mutableState, task)
		if err != nil {
			return err
		} else if !prepareHistoryBranchOut.DoContinue {
			metrics.DuplicateReplicationEventsCounter.With(r.metricsHandler).Record(
				1,
				metrics.OperationTag(metrics.ReplicateHistoryEventsScope))
			return consts.ErrDuplicate
		}
		err = task.skipDuplicatedEvents(prepareHistoryBranchOut.EventsApplyIndex)
		if err != nil {
			return err
		}

		mutableState, isRebuilt, err := r.mutableStateMapper.GetOrRebuildCurrentMutableState(
			ctx,
			wfContext,
			mutableState,
			GetOrRebuildMutableStateIn{replicationTask: task, BranchIndex: prepareHistoryBranchOut.BranchIndex},
		)
		if err != nil {
			return err
		}
		if mutableState.GetExecutionInfo().GetVersionHistories().GetCurrentVersionHistoryIndex() == prepareHistoryBranchOut.BranchIndex {
			return r.applyNonStartEventsToCurrentBranch(ctx, wfContext, mutableState, isRebuilt, releaseFn, task)
		}
		return r.applyNonStartEventsToNonCurrentBranch(ctx, wfContext, mutableState, prepareHistoryBranchOut.BranchIndex, releaseFn, task)

	case *serviceerror.NotFound:
		// mutable state not created, check if is workflow reset
		mutableState, err := r.applyNonStartEventsMissingMutableState(ctx, wfContext, task)
		if err != nil {
			return err
		}

		return r.applyNonStartEventsResetWorkflow(ctx, wfContext, mutableState, task)

	default:
		// unable to get mutable state, return err, so we can retry the task later
		return err
	}
}

func (r *HistoryReplicatorImpl) applyStartEvents(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	task replicationTask,
) error {
	namespaceEntry, err := r.namespaceRegistry.GetNamespaceByID(task.getNamespaceID())
	if err != nil {
		return err
	}
	var mutableState historyi.MutableState = workflow.NewMutableState(
		r.shardContext,
		r.shardContext.GetEventsCache(),
		task.getLogger(),
		namespaceEntry,
		task.getWorkflowID(),
		task.getRunID(),
		timestamp.TimeValue(task.getFirstEvent().GetEventTime()),
	)
	mutableState, newMutableState, err := r.mutableStateMapper.ApplyEvents(ctx, wfContext, mutableState, task)
	if err != nil {
		return err
	}
	if newMutableState != nil {
		task.getLogger().Error(
			"HistoryReplicator::applyStartEvents encountered create workflow with continue as new case",
			tag.Error(err),
		)
	}

	if task.stateBased() {
		mutableState.InitTransitionHistory()
	}

	err = r.transactionMgr.CreateWorkflow(
		ctx,
		NewWorkflow(
			r.clusterMetadata,
			wfContext,
			mutableState,
			releaseFn,
		),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to create workflow when applyStartEvents",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *HistoryReplicatorImpl) applyNonStartEventsToCurrentBranch(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	isRebuilt bool,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	task replicationTask,
) error {
	mutableState, newMutableState, err := r.mutableStateMapper.ApplyEvents(ctx, wfContext, mutableState, task)
	if err != nil {
		return err
	}

	targetWorkflow := NewWorkflow(
		r.clusterMetadata,
		wfContext,
		mutableState,
		releaseFn,
	)
	var newWorkflow Workflow
	if newMutableState != nil {
		newExecutionInfo := newMutableState.GetExecutionInfo()
		newExecutionState := newMutableState.GetExecutionState()
		newContext := workflow.NewContext(
			r.shardContext.GetConfig(),
			definition.NewWorkflowKey(
				newExecutionInfo.NamespaceId,
				newExecutionInfo.WorkflowId,
				newExecutionState.RunId,
			),
			r.logger,
			r.shardContext.GetThrottledLogger(),
			r.shardContext.GetMetricsHandler(),
		)

		newWorkflow = NewWorkflow(
			r.clusterMetadata,
			newContext,
			newMutableState,
			wcache.NoopReleaseFn,
		)
	}

	err = r.transactionMgr.UpdateWorkflow(
		ctx,
		isRebuilt,
		targetWorkflow,
		newWorkflow,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to update workflow when applyNonStartEventsToCurrentBranch",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *HistoryReplicatorImpl) applyNonStartEventsToNonCurrentBranch(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	branchIndex int32,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	task replicationTask,
) error {

	if len(task.getNewEvents()) != 0 {
		return r.applyNonStartEventsToNonCurrentBranchWithContinueAsNew(
			ctx,
			wfContext,
			releaseFn,
			task,
		)
	}

	return r.applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(
		ctx,
		wfContext,
		mutableState,
		branchIndex,
		releaseFn,
		task,
	)
}

func (r *HistoryReplicatorImpl) applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	branchIndex int32,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	task replicationTask,
) error {

	versionHistoryItem := versionhistory.NewVersionHistoryItem(
		task.getLastEvent().GetEventId(),
		task.getLastEvent().GetVersion(),
	)
	versionHistory, err := versionhistory.GetVersionHistory(mutableState.GetExecutionInfo().GetVersionHistories(), branchIndex)
	if err != nil {
		return err
	}
	if err = versionhistory.AddOrUpdateVersionHistoryItem(versionHistory, versionHistoryItem); err != nil {
		return err
	}

	transactionIDs, err := r.shardContext.GenerateTaskIDs(len(task.getEvents()))
	if err != nil {
		return err
	}

	eventsSlice := make([]*persistence.WorkflowEvents, len(task.getEvents()))
	for i, events := range task.getEvents() {
		eventsSlice[i] = &persistence.WorkflowEvents{
			NamespaceID: task.getNamespaceID().String(),
			WorkflowID:  task.getExecution().GetWorkflowId(),
			RunID:       task.getExecution().GetRunId(),
			BranchToken: versionHistory.GetBranchToken(),
			PrevTxnID:   0, // TODO @wxing1292 events chaining will not work for backfill case
			TxnID:       transactionIDs[i],
			Events:      events,
		}
	}
	err = r.transactionMgr.BackfillWorkflow(
		ctx,
		NewWorkflow(
			r.clusterMetadata,
			wfContext,
			mutableState,
			releaseFn,
		),
		eventsSlice...,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to backfill workflow when applyNonStartEventsToNonCurrentBranch",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *HistoryReplicatorImpl) applyNonStartEventsToNonCurrentBranchWithContinueAsNew(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	releaseFn historyi.ReleaseWorkflowContextFunc,
	task replicationTask,
) error {

	// workflow backfill to non current branch with continue as new
	// first, release target workflow lock & create the new workflow as zombie
	// NOTE: need to release target workflow due to target workflow
	//  can potentially be the current workflow causing deadlock

	// 1. clear all in memory changes & release target workflow Lock
	// 2. apply new workflow first
	// 3. apply target workflow

	// step 1
	wfContext.Clear()
	releaseFn(nil)

	// step 2
	task, newTask, err := task.splitTask()
	if err != nil {
		return err
	}
	if err := r.doApplyEvents(ctx, newTask); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create new workflow when applyNonStartEventsToNonCurrentBranchWithContinueAsNew",
			tag.Error(err),
		)
		return err
	}

	// step 3
	if err := r.doApplyEvents(ctx, task); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create target workflow when applyNonStartEventsToNonCurrentBranchWithContinueAsNew",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *HistoryReplicatorImpl) applyNonStartEventsMissingMutableState(
	ctx context.Context,
	newWFContext historyi.WorkflowContext,
	task replicationTask,
) (historyi.MutableState, error) {

	// for non reset workflow execution replication task, just do re-replication
	if !task.isWorkflowReset() {
		startEventId := common.EmptyEventID
		startEventVersion := common.EmptyVersion
		if task.getBaseWorkflowInfo() != nil {
			startEventId = task.getBaseWorkflowInfo().LowestCommonAncestorEventId
			startEventVersion = task.getBaseWorkflowInfo().LowestCommonAncestorEventVersion
		}
		firstEvent := task.getFirstEvent()
		endEventId := firstEvent.GetEventId()
		endEventVersion := firstEvent.GetVersion()
		return nil, serviceerrors.NewRetryReplication(
			mutableStateMissingMessage,
			task.getNamespaceID().String(),
			task.getWorkflowID(),
			task.getRunID(),
			startEventId,
			startEventVersion,
			endEventId,
			endEventVersion,
		)
	}

	baseWorkflowInfo := task.getBaseWorkflowInfo()
	baseRunID := baseWorkflowInfo.RunId
	baseEventID := baseWorkflowInfo.LowestCommonAncestorEventId
	baseEventVersion := baseWorkflowInfo.LowestCommonAncestorEventVersion
	newRunID := newWFContext.GetWorkflowKey().RunID

	workflowResetter := r.newResetter(
		task.getNamespaceID(),
		task.getWorkflowID(),
		baseRunID,
		newWFContext,
		newRunID,
		task.getLogger(),
	)

	resetMutableState, err := workflowResetter.resetWorkflow(
		ctx,
		task.getEventTime(),
		baseEventID,
		baseEventVersion,
		task.getFirstEvent().GetEventId(),
		task.getVersion(),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to reset workflow when applyNonStartEventsMissingMutableState",
			tag.Error(err),
		)
		return nil, err
	}
	return resetMutableState, nil
}

func (r *HistoryReplicatorImpl) applyNonStartEventsResetWorkflow(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	task replicationTask,
) error {
	mutableState, newMutableState, err := r.mutableStateMapper.ApplyEvents(ctx, wfContext, mutableState, task)
	if err != nil {
		return err
	}
	if newMutableState != nil {
		task.getLogger().Error(
			"HistoryReplicator::applyNonStartEventsResetWorkflow encountered reset workflow with continue as new case",
			tag.Error(err),
		)
	}

	targetWorkflow := NewWorkflow(
		r.clusterMetadata,
		wfContext,
		mutableState,
		wcache.NoopReleaseFn,
	)

	err = r.transactionMgr.CreateWorkflow(
		ctx,
		targetWorkflow,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to create workflow when applyNonStartEventsResetWorkflow",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *HistoryReplicatorImpl) notify(
	clusterName string,
	now time.Time,
) {
	if clusterName == r.clusterMetadata.GetCurrentClusterName() {
		// this is a valid use case for testing, but not for production
		r.logger.Warn("nDCHistoryReplicator applying events generated by current cluster")
		return
	}
	now = now.Add(-r.shardContext.GetConfig().StandbyClusterDelay())
	r.shardContext.SetCurrentTime(clusterName, now)
}
