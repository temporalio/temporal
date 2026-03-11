package ndc

import (
	"context"

	"github.com/google/uuid"
	"go.temporal.io/server/common/log/tag"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	historyi "go.temporal.io/server/service/history/interfaces"
)

type (
	MutableStateMapper[Input any, Output any] func(
		ctx context.Context,
		wfContext historyi.WorkflowContext,
		mutableState historyi.MutableState,
		input Input,
	) (historyi.MutableState, Output, error)

	MutableStateMapperImpl struct {
		shardContext             historyi.ShardContext
		newBufferEventFlusher    bufferEventFlusherProvider
		newBranchMgr             branchMgrProvider
		newConflictResolver      conflictResolverProvider
		newMutableStateRebuilder mutableStateRebuilderProvider
	}

	PrepareHistoryBranchOut struct {
		DoContinue       bool  // whether to continue applying events
		BranchIndex      int32 // branch index on version histories
		EventsApplyIndex int   // index of events that should start applying from
	}

	GetOrRebuildMutableStateIn struct {
		replicationTask
		BranchIndex int32
	}
)

var _ MutableStateMapper[replicationTask, struct{}] = (*MutableStateMapperImpl)(nil).FlushBufferEvents
var _ MutableStateMapper[replicationTask, PrepareHistoryBranchOut] = (*MutableStateMapperImpl)(nil).GetOrCreateHistoryBranch
var _ MutableStateMapper[GetOrRebuildMutableStateIn, bool] = (*MutableStateMapperImpl)(nil).GetOrRebuildCurrentMutableState
var _ MutableStateMapper[GetOrRebuildMutableStateIn, bool] = (*MutableStateMapperImpl)(nil).GetOrRebuildMutableState
var _ MutableStateMapper[replicationTask, historyi.MutableState] = (*MutableStateMapperImpl)(nil).ApplyEvents

func NewMutableStateMapping(
	shardContext historyi.ShardContext,
	newBufferEventFlusher bufferEventFlusherProvider,
	newBranchMgr branchMgrProvider,
	newConflictResolver conflictResolverProvider,
	newMutableStateRebuilder mutableStateRebuilderProvider,
) *MutableStateMapperImpl {
	return &MutableStateMapperImpl{
		shardContext:             shardContext,
		newBufferEventFlusher:    newBufferEventFlusher,
		newBranchMgr:             newBranchMgr,
		newConflictResolver:      newConflictResolver,
		newMutableStateRebuilder: newMutableStateRebuilder,
	}
}

func (m *MutableStateMapperImpl) FlushBufferEvents(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	task replicationTask,
) (historyi.MutableState, struct{}, error) {
	flusher := m.newBufferEventFlusher(wfContext, mutableState, task.getLogger())
	_, mutableState, err := flusher.flush(ctx)
	if err != nil {
		task.getLogger().Error(
			"MutableStateMapping::FlushBufferEvents unable to flush buffer events",
			tag.Error(err),
		)
		return nil, struct{}{}, err
	}
	return mutableState, struct{}{}, err
}

func (m *MutableStateMapperImpl) GetOrCreateHistoryBranch(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	task replicationTask,
) (historyi.MutableState, PrepareHistoryBranchOut, error) {
	branchMgr := m.newBranchMgr(wfContext, mutableState, task.getLogger())
	incomingVersionHistory := task.getVersionHistory()
	eventBatches := task.getEvents()
	eventBatchApplyIndex := 0
	doContinue := false
	var versionHistoryIndex int32
	var err error
	for index, eventBatch := range eventBatches {
		doContinueCurrentBatch, versionHistoryIndexCurrentBatch, errCurrentBatch := branchMgr.GetOrCreate(
			ctx,
			incomingVersionHistory,
			eventBatch[0].GetEventId(),
			eventBatch[0].GetVersion(),
		)
		if doContinueCurrentBatch || errCurrentBatch != nil {
			eventBatchApplyIndex = index
			doContinue = doContinueCurrentBatch
			err = errCurrentBatch
			versionHistoryIndex = versionHistoryIndexCurrentBatch
			break
		}
	}

	switch err.(type) {
	case nil:
		return mutableState, PrepareHistoryBranchOut{
			DoContinue:       doContinue,
			BranchIndex:      versionHistoryIndex,
			EventsApplyIndex: eventBatchApplyIndex,
		}, nil
	case *serviceerrors.RetryReplication:
		// replication message can arrive out of order
		// do not log
		return nil, PrepareHistoryBranchOut{}, err
	default:
		task.getLogger().Error(
			"MutableStateMapping::GetOrCreateHistoryBranch unable to prepare version history",
			tag.Error(err),
		)
		return nil, PrepareHistoryBranchOut{}, err
	}
}

func (m *MutableStateMapperImpl) CreateHistoryBranch(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	task replicationTask,
) (historyi.MutableState, PrepareHistoryBranchOut, error) {
	branchMgr := m.newBranchMgr(wfContext, mutableState, task.getLogger())
	incomingVersionHistory := task.getVersionHistory()
	doContinue, versionHistoryIndex, err := branchMgr.Create(
		ctx,
		incomingVersionHistory,
		task.getFirstEvent().GetEventId(),
		task.getFirstEvent().GetVersion(),
	)
	switch err.(type) {
	case nil:
		return mutableState, PrepareHistoryBranchOut{
			DoContinue:  doContinue,
			BranchIndex: versionHistoryIndex,
		}, nil
	case *serviceerrors.RetryReplication:
		// replication message can arrive out of order
		// do not log
		return nil, PrepareHistoryBranchOut{}, err
	default:
		task.getLogger().Error(
			"MutableStateMapping::GetOrCreateHistoryBranch unable to prepare version history",
			tag.Error(err),
		)
		return nil, PrepareHistoryBranchOut{}, err
	}
}

func (m *MutableStateMapperImpl) GetOrRebuildCurrentMutableState(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	task GetOrRebuildMutableStateIn,
) (historyi.MutableState, bool, error) {
	conflictResolver := m.newConflictResolver(wfContext, mutableState, task.getLogger())
	incomingVersion := task.getVersion()
	mutableState, isRebuilt, err := conflictResolver.GetOrRebuildCurrentMutableState(
		ctx,
		task.BranchIndex,
		incomingVersion,
	)
	if err != nil {
		task.getLogger().Error(
			"MutableStateMapping::PrepareMutableState unable to prepare mutable state",
			tag.Error(err),
		)
	}
	return mutableState, isRebuilt, err
}

func (m *MutableStateMapperImpl) GetOrRebuildMutableState(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	task GetOrRebuildMutableStateIn,
) (historyi.MutableState, bool, error) {
	conflictResolver := m.newConflictResolver(wfContext, mutableState, task.getLogger())
	mutableState, isRebuilt, err := conflictResolver.GetOrRebuildMutableState(
		ctx,
		task.BranchIndex,
	)
	if err != nil {
		task.getLogger().Error(
			"MutableStateMapping::PrepareMutableState unable to prepare mutable state",
			tag.Error(err),
		)
	}
	return mutableState, isRebuilt, err
}

func (m *MutableStateMapperImpl) ApplyEvents(
	ctx context.Context,
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
	task replicationTask,
) (historyi.MutableState, historyi.MutableState, error) {
	mutableStateRebuilder := m.newMutableStateRebuilder(mutableState, task.getLogger())
	newMutableState, err := mutableStateRebuilder.ApplyEvents(
		ctx,
		task.getNamespaceID(),
		uuid.New().String(),
		task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
		task.getNewRunID(),
	)
	if err != nil {
		task.getLogger().Error(
			"MutableStateMapping::ApplyEvents unable to apply events",
			tag.Error(err),
		)
		return nil, nil, err
	}
	return mutableState, newMutableState, nil
}
