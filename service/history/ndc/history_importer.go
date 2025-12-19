package ndc

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	HistoryImporter interface {
		ImportWorkflow(
			ctx context.Context,
			workflowKey definition.WorkflowKey,
			versionHistoryItems []*historyspb.VersionHistoryItem,
			events [][]*historypb.HistoryEvent,
			token []byte,
		) ([]byte, bool, error)
	}

	HistoryImporterImpl struct {
		shardContext   historyi.ShardContext
		namespaceCache namespace.Registry
		workflowCache  wcache.Cache
		taskRefresher  workflow.TaskRefresher
		transactionMgr TransactionManager
		logger         log.Logger

		mutableStateInitializer *MutableStateInitializerImpl
		mutableStateMapper      *MutableStateMapperImpl
	}
)

func NewHistoryImporter(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
	logger log.Logger,
) *HistoryImporterImpl {
	logger = log.With(logger, tag.ComponentHistoryImporter)
	backfiller := &HistoryImporterImpl{
		shardContext:   shardContext,
		namespaceCache: shardContext.GetNamespaceRegistry(),
		workflowCache:  workflowCache,
		taskRefresher:  workflow.NewTaskRefresher(shardContext),
		transactionMgr: NewTransactionManager(shardContext, workflowCache, nil, logger, true),
		logger:         logger,

		mutableStateInitializer: NewMutableStateInitializer(
			shardContext,
			workflowCache,
			logger,
		),
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
	}
	return backfiller
}

func (r *HistoryImporterImpl) ImportWorkflow(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	eventsSlice [][]*historypb.HistoryEvent,
	token []byte,
) (_ []byte, _ bool, retError error) {
	if len(eventsSlice) == 0 && len(token) == 0 {
		return nil, false, serviceerror.NewInvalidArgument("ImportWorkflowExecution cannot import empty history events")
	}

	ndcWorkflow, mutableStateSpec, err := r.mutableStateInitializer.Initialize(ctx, workflowKey, token)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		// it is ok to clear everytime this function is invoked
		// mutable state will be at most initialized once from shard mutable state cache
		// mutable state will usually be initialized from input token
		ndcWorkflow.GetContext().Clear()
		ndcWorkflow.GetReleaseFn()(retError)
	}()

	if len(eventsSlice) != 0 {
		return r.applyEvents(
			ctx,
			ndcWorkflow,
			mutableStateSpec,
			versionHistoryItems,
			eventsSlice,
			len(token) == 0,
		)
	}

	if err := r.commit(
		ctx,
		ndcWorkflow,
		mutableStateSpec,
	); err != nil {
		return nil, false, err
	}
	return nil, false, nil
}

func (r *HistoryImporterImpl) applyEvents(
	ctx context.Context,
	ndcWorkflow Workflow,
	mutableStateSpec MutableStateInitializationSpec,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	eventsSlice [][]*historypb.HistoryEvent,
	createNewBranch bool,
) (_ []byte, _ bool, retError error) {

	wfContext := ndcWorkflow.GetContext()
	mutableState := ndcWorkflow.GetMutableState()
	task, err := newReplicationTaskFromBatch(
		r.shardContext.GetClusterMetadata(),
		r.logger,
		wfContext.GetWorkflowKey(),
		nil,
		versionHistoryItems,
		eventsSlice,
		nil,
		"",
		nil,
		false,
	)
	if err != nil {
		return nil, false, err
	}

	if mutableStateSpec.IsBrandNew {
		if task.getFirstEvent().GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			err := serviceerror.NewInternal("mutable state is brand new, but events are not imported from beginning")
			task.getLogger().Error("HistoryImporter::applyEvents encountered mutable state vs events mismatch", tag.Error(err))
			return nil, false, err
		}
		return r.applyStartEventsAndSerialize(
			ctx,
			wfContext,
			mutableState,
			mutableStateSpec,
			task,
		)
	}
	return r.applyNonStartEventsAndSerialize(
		ctx,
		wfContext,
		mutableState,
		mutableStateSpec,
		task,
		createNewBranch,
	)
}

func (r *HistoryImporterImpl) applyStartEventsAndSerialize(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	mutableStateSpec MutableStateInitializationSpec,
	task replicationTask,
) ([]byte, bool, error) {
	mutableState, newMutableState, err := r.mutableStateMapper.ApplyEvents(
		ctx,
		wfContext,
		mutableState,
		task,
	)
	if err != nil {
		return nil, false, err
	}
	if newMutableState != nil {
		task.getLogger().Error(
			"HistoryImporter::applyStartEventsAndSerialize encountered create workflow with continue as new case",
			tag.Error(err),
		)
	}
	token, err := r.persistHistoryAndSerializeMutableState(ctx, mutableState, mutableStateSpec)
	return token, err == nil, err
}

func (r *HistoryImporterImpl) applyNonStartEventsAndSerialize(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	mutableStateSpec MutableStateInitializationSpec,
	task replicationTask,
	createNewBranch bool,
) ([]byte, bool, error) {
	prepareBranchFn := r.mutableStateMapper.GetOrCreateHistoryBranch
	if createNewBranch {
		prepareBranchFn = r.mutableStateMapper.CreateHistoryBranch
	}

	mutableState, prepareHistoryBranchOut, err := prepareBranchFn(ctx, wfContext, mutableState, task)
	if err != nil {
		return nil, false, err
	} else if !prepareHistoryBranchOut.DoContinue {
		token, err := r.persistHistoryAndSerializeMutableState(ctx, mutableState, mutableStateSpec)
		return token, false, err
	} else if createNewBranch && prepareHistoryBranchOut.BranchIndex == 0 {
		// sanity check
		return nil, false, serviceerror.NewInternal("HistoryImporter unable to correctly create new branch")
	}

	mutableState, _, err = r.mutableStateMapper.GetOrRebuildMutableState(
		ctx,
		wfContext,
		mutableState,
		GetOrRebuildMutableStateIn{replicationTask: task, BranchIndex: prepareHistoryBranchOut.BranchIndex},
	)
	if err != nil {
		return nil, false, err
	}
	mutableState, newMutableState, err := r.mutableStateMapper.ApplyEvents(
		ctx,
		wfContext,
		mutableState,
		task,
	)
	if err != nil {
		return nil, false, err
	}

	if newMutableState != nil {
		task.getLogger().Error(
			"HistoryImporter::applyNonStartEventsAndSerialize encountered create workflow with continue as new case",
			tag.Error(err),
		)
	}
	token, err := r.persistHistoryAndSerializeMutableState(ctx, mutableState, mutableStateSpec)
	return token, err == nil, err
}

func (r *HistoryImporterImpl) persistHistoryAndSerializeMutableState(
	ctx context.Context,
	mutableState historyi.MutableState,
	mutableStateSpec MutableStateInitializationSpec,
) ([]byte, error) {
	targetWorkflowSnapshot, targetWorkflowEventsSeq, err := mutableState.CloseTransactionAsSnapshot(
		ctx,
		historyi.TransactionPolicyPassive,
	)
	if err != nil {
		return nil, err
	}

	sizeSiff, err := workflow.PersistWorkflowEvents(ctx, r.shardContext, targetWorkflowEventsSeq...)
	if err != nil {
		return nil, err
	}
	mutableState.AddHistorySize(sizeSiff)

	mutableStateRow := &persistencespb.WorkflowMutableState{
		ActivityInfos:       targetWorkflowSnapshot.ActivityInfos,
		TimerInfos:          targetWorkflowSnapshot.TimerInfos,
		ChildExecutionInfos: targetWorkflowSnapshot.ChildExecutionInfos,
		RequestCancelInfos:  targetWorkflowSnapshot.RequestCancelInfos,
		SignalInfos:         targetWorkflowSnapshot.SignalInfos,
		SignalRequestedIds:  convert.StringSetToSlice(targetWorkflowSnapshot.SignalRequestedIDs),
		ExecutionInfo:       targetWorkflowSnapshot.ExecutionInfo,
		ExecutionState:      targetWorkflowSnapshot.ExecutionState,
		NextEventId:         targetWorkflowSnapshot.NextEventID,
		BufferedEvents:      nil,
		Checksum:            targetWorkflowSnapshot.Checksum,
	}
	return r.mutableStateInitializer.serializeBackfillToken(
		mutableStateRow,
		mutableStateSpec.DBRecordVersion,
		mutableStateSpec.DBHistorySize,
		mutableStateSpec.ExistsInDB,
	)
}

func (r *HistoryImporterImpl) commit(
	ctx context.Context,
	memNDCWorkflow Workflow,
	mutableStateSpec MutableStateInitializationSpec,
) (retError error) {
	if mutableStateSpec.IsBrandNew {
		return serviceerror.NewInvalidArgument("HistoryImporter::commit cannot create workflow without events")
	}

	if !mutableStateSpec.ExistsInDB {
		// refresh tasks to be generated
		if err := r.taskRefresher.Refresh(
			ctx,
			memNDCWorkflow.GetMutableState(),
			false,
		); err != nil {
			return err
		}
		memMutableState := memNDCWorkflow.GetMutableState()
		nextEventID, _ := memMutableState.GetUpdateCondition()
		memMutableState.SetUpdateCondition(nextEventID, mutableStateSpec.DBRecordVersion)
		if err := r.transactionMgr.CreateWorkflow(
			ctx,
			chasm.WorkflowArchetypeID,
			memNDCWorkflow,
		); err != nil {
			r.logger.Error("HistoryImporter::commit encountered error", tag.Error(err))
			return err
		}
		return nil
	}

	workflowKey := memNDCWorkflow.GetContext().GetWorkflowKey()
	dbNDCWorkflow, err := r.transactionMgr.LoadWorkflow(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		workflowKey.WorkflowID,
		workflowKey.RunID,
		chasm.WorkflowArchetypeID,
	)
	if err != nil {
		r.logger.Error("HistoryImporter::commit unable to find workflow in DB", tag.Error(err))
		return err
	}
	defer func() {
		if rec := recover(); rec != nil {
			dbNDCWorkflow.GetReleaseFn()(errPanic)
			panic(rec)
		} else {
			dbNDCWorkflow.GetReleaseFn()(retError)
		}
	}()

	dbCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		dbNDCWorkflow.GetMutableState().GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		r.logger.Error("HistoryImporter::commit unable to find current version history from DB", tag.Error(err))
		return err
	}
	memCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(
		memNDCWorkflow.GetMutableState().GetExecutionInfo().GetVersionHistories(),
	)
	if err != nil {
		r.logger.Error("HistoryImporter::commit unable to find current version history from DB", tag.Error(err))
		return err
	}
	cmpResult, err := versionhistory.CompareVersionHistory(memCurrentVersionHistory, dbCurrentVersionHistory)
	if err != nil {
		r.logger.Error("HistoryImporter::commit unable to compare current version history between mem vs DB", tag.Error(err))
		return err
	}
	if cmpResult == 0 {
		// version history from mem mutable state == db mutable state, dedup
		r.logger.Info("HistoryImporter::commit skip, current version history between mem == DB")
		return nil
	}

	if cmpResult < 0 {
		// imported events does not belong to current branch, update DB mutable state with new version history
		updated, _, err := versionhistory.AddAndSwitchVersionHistory(
			dbNDCWorkflow.GetMutableState().GetExecutionInfo().GetVersionHistories(),
			memCurrentVersionHistory,
		)
		if err != nil {
			r.logger.Error("HistoryImporter::commit unable to update version history from DB", tag.Error(err))
			return err
		}
		if updated {
			err = serviceerror.NewInternal("current version history should not be updated")
			r.logger.Error("HistoryImporter::commit unable to update version history from DB", tag.Error(err))
			return err
		}
		sizeDiff := memNDCWorkflow.GetMutableState().GetHistorySize() - mutableStateSpec.DBHistorySize
		dbNDCWorkflow.GetMutableState().AddHistorySize(sizeDiff)
		if err := dbNDCWorkflow.GetContext().SetWorkflowExecution(ctx, r.shardContext); err != nil {
			r.logger.Error("HistoryImporter::commit encountered error", tag.Error(err))
		}
		return nil
	}

	// cmpResult > 0
	dbNDCWorkflow.GetContext().Clear()
	// imported events is the new current branch, update write to DB
	// refresh tasks to be generated
	if err := r.taskRefresher.Refresh(
		ctx,
		memNDCWorkflow.GetMutableState(),
		false,
	); err != nil {
		return err
	}
	memMutableState := memNDCWorkflow.GetMutableState()
	nextEventID, _ := memMutableState.GetUpdateCondition()
	memMutableState.SetUpdateCondition(nextEventID, mutableStateSpec.DBRecordVersion)
	if err := r.transactionMgr.UpdateWorkflow(
		ctx,
		true,
		chasm.WorkflowArchetypeID,
		memNDCWorkflow,
		nil,
	); err != nil {
		r.logger.Error("HistoryImporter::commit encountered error", tag.Error(err))
		return err
	}
	return nil
}
