package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

// histReplResetterStub is a hand written stub implementing the `resetter` interface,
// which has no generated gomock mock in this package.
type histReplResetterStub struct {
	resetFn func(
		ctx context.Context,
		now time.Time,
		baseLastEventID int64,
		baseLastEventVersion int64,
		incomingFirstEventID int64,
		incomingFirstEventVersion int64,
	) (historyi.MutableState, error)
}

func (s *histReplResetterStub) resetWorkflow(
	ctx context.Context,
	now time.Time,
	baseLastEventID int64,
	baseLastEventVersion int64,
	incomingFirstEventID int64,
	incomingFirstEventVersion int64,
) (historyi.MutableState, error) {
	return s.resetFn(ctx, now, baseLastEventID, baseLastEventVersion, incomingFirstEventID, incomingFirstEventVersion)
}

type histReplSuite struct {
	suite.Suite
	*require.Assertions

	controller *gomock.Controller
	mockShard  *shard.ContextTest

	mockWorkflowCache    *wcache.MockCache
	mockClusterMetadata  *cluster.MockMetadata
	mockTransactionMgr   *MockTransactionManager
	mockEventsReapplier  *MockEventsReapplier
	mockBufferFlusher    *MockBufferEventFlusher
	mockBranchMgr        *MockBranchMgr
	mockConflictResolver *MockConflictResolver
	mockMSRebuilder      *workflow.MockMutableStateRebuilder
	mockNamespaceCache   *namespace.MockRegistry

	logger     log.Logger
	serializer serialization.Serializer

	namespaceID   namespace.ID
	namespaceName namespace.Name
	workflowID    string
	runID         string

	// reset stub controls
	resetReturn historyi.MutableState
	resetErr    error

	replicator *HistoryReplicatorImpl
}

func TestHistReplSuite(t *testing.T) {
	suite.Run(t, new(histReplSuite))
}

func (s *histReplSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{ShardId: 10, RangeId: 1},
		tests.NewDynamicConfig(),
	)

	reg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(reg))
	s.mockShard.SetStateMachineRegistry(reg)

	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	// source cluster differs from current cluster -> notify() goes to SetCurrentTime path.
	s.mockClusterMetadata.EXPECT().
		ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).
		Return(cluster.TestAlternativeClusterName).AnyTimes()

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockWorkflowCache = wcache.NewMockCache(s.controller)
	s.mockTransactionMgr = NewMockTransactionManager(s.controller)
	s.mockEventsReapplier = NewMockEventsReapplier(s.controller)
	s.mockBufferFlusher = NewMockBufferEventFlusher(s.controller)
	s.mockBranchMgr = NewMockBranchMgr(s.controller)
	s.mockConflictResolver = NewMockConflictResolver(s.controller)
	s.mockMSRebuilder = workflow.NewMockMutableStateRebuilder(s.controller)

	s.logger = s.mockShard.GetLogger()
	s.serializer = serialization.NewSerializer()

	s.namespaceID = tests.NamespaceID
	s.namespaceName = tests.Namespace
	s.workflowID = "hist-repl-wf-id"
	s.runID = uuid.NewString()

	mapper := NewMutableStateMapping(
		s.mockShard,
		func(historyi.WorkflowContext, historyi.MutableState, log.Logger) BufferEventFlusher {
			return s.mockBufferFlusher
		},
		func(historyi.WorkflowContext, historyi.MutableState, log.Logger) BranchMgr {
			return s.mockBranchMgr
		},
		func(historyi.WorkflowContext, historyi.MutableState, log.Logger) ConflictResolver {
			return s.mockConflictResolver
		},
		func(historyi.MutableState, log.Logger) workflow.MutableStateRebuilder {
			return s.mockMSRebuilder
		},
	)

	s.replicator = &HistoryReplicatorImpl{
		shardContext:       s.mockShard,
		clusterMetadata:    s.mockClusterMetadata,
		historySerializer:  s.serializer,
		metricsHandler:     s.mockShard.GetMetricsHandler(),
		namespaceRegistry:  s.mockNamespaceCache,
		workflowCache:      s.mockWorkflowCache,
		eventsReapplier:    s.mockEventsReapplier,
		transactionMgr:     s.mockTransactionMgr,
		logger:             s.logger,
		mutableStateMapper: mapper,
		newResetter: func(
			_ namespace.ID,
			_ string,
			_ string,
			_ historyi.WorkflowContext,
			_ string,
			_ log.Logger,
		) resetter {
			return &histReplResetterStub{
				resetFn: func(context.Context, time.Time, int64, int64, int64, int64) (historyi.MutableState, error) {
					return s.resetReturn, s.resetErr
				},
			}
		},
	}
}

func (s *histReplSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

// ---------- helpers ----------

func (s *histReplSuite) workflowKey() definition.WorkflowKey {
	return definition.NewWorkflowKey(s.namespaceID.String(), s.workflowID, s.runID)
}

func (s *histReplSuite) execution() *commonpb.WorkflowExecution {
	return &commonpb.WorkflowExecution{WorkflowId: s.workflowID, RunId: s.runID}
}

func histReplEvent(eventID int64, eventType enumspb.EventType) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventId:   eventID,
		Version:   common.EmptyVersion,
		EventType: eventType,
	}
}

// task builds a non-start-event replication task.
func (s *histReplSuite) nonStartTask(events [][]*historypb.HistoryEvent) replicationTask {
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(events[len(events)-1][len(events[len(events)-1])-1].GetEventId(), common.EmptyVersion)},
		events,
		nil,
		"",
		nil,
		false,
	)
	s.NoError(err)
	return task
}

func (s *histReplSuite) mutableStateWithVersionHistories(currentIndex int32) *historyi.MockMutableState {
	ms := historyi.NewMockMutableState(s.controller)
	vh := versionhistory.NewVersionHistory([]byte("branch-token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(1, common.EmptyVersion),
	})
	histories := versionhistory.NewVersionHistories(vh)
	histories.CurrentVersionHistoryIndex = currentIndex
	execInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID.String(),
		WorkflowId:       s.workflowID,
		VersionHistories: histories,
	}
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()
	return ms
}

// mutableStateWithTwoBranches builds a mutable state with two version history branches,
// current index set to currentIndex. Both branches end at event id 1.
func (s *histReplSuite) mutableStateWithTwoBranches(currentIndex int32) *historyi.MockMutableState {
	ms := historyi.NewMockMutableState(s.controller)
	vh0 := versionhistory.NewVersionHistory([]byte("branch-token-0"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(1, common.EmptyVersion),
	})
	vh1 := versionhistory.NewVersionHistory([]byte("branch-token-1"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(1, common.EmptyVersion),
	})
	histories := versionhistory.NewVersionHistories(vh0)
	_, err := versionhistory.AddVersionHistory(histories, vh1)
	s.NoError(err)
	histories.CurrentVersionHistoryIndex = currentIndex
	execInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID.String(),
		WorkflowId:       s.workflowID,
		VersionHistories: histories,
	}
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()
	return ms
}

func (s *histReplSuite) noopRelease() historyi.ReleaseWorkflowContextFunc {
	return func(error) {}
}

// ---------- constructor ----------

func (s *histReplSuite) TestNewHistoryReplicator() {
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	r := NewHistoryReplicator(
		s.mockShard,
		s.mockWorkflowCache,
		s.mockEventsReapplier,
		s.serializer,
		s.logger,
	)
	s.NotNil(r)
	s.NotNil(r.transactionMgr)
	s.NotNil(r.mutableStateMapper)
	s.NotNil(r.newResetter)
	// exercise the resetter provider closure.
	s.NotNil(r.newResetter(s.namespaceID, s.workflowID, "base", historyi.NewMockWorkflowContext(s.controller), s.runID, s.logger))

	// exercise the BufferEventFlusher provider closure captured by NewHistoryReplicator
	// (the other mapper provider closures wire deep real components and are not driven here).
	ctx := context.Background()
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	flushMS := historyi.NewMockMutableState(s.controller)
	flushMS.EXPECT().HasBufferedEvents().Return(false)
	flushMS.EXPECT().HasStartedWorkflowTask().Return(false)
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	_, _, err := r.mutableStateMapper.FlushBufferEvents(ctx, wfContext, flushMS, task)
	s.NoError(err)
}

// ---------- ApplyEvents ----------

func (s *histReplSuite) TestApplyEvents_TaskBuildError() {
	// invalid namespace ID -> newReplicationTaskFromRequest fails.
	err := s.replicator.ApplyEvents(context.Background(), &historyservice.ReplicateEventsV2Request{
		NamespaceId: "not-a-uuid",
	})
	s.Error(err)
}

func (s *histReplSuite) TestApplyEvents_Success_DelegatesToDoApplyEvents() {
	ctx := context.Background()
	events := []*historypb.HistoryEvent{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}
	blob, err := s.serializer.SerializeEvents(events)
	s.NoError(err)

	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId:       s.namespaceID.String(),
		WorkflowExecution: s.execution(),
		Events:            blob,
		VersionHistoryItems: []*historyspb.VersionHistoryItem{
			versionhistory.NewVersionHistoryItem(2, common.EmptyVersion),
		},
	}

	// drive doApplyEvents: get context fails fast to keep this test focused on delegation.
	wfErr := serviceerror.NewUnavailable("boom")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, wfErr)

	err = s.replicator.ApplyEvents(ctx, request)
	s.Equal(wfErr, err)
}

// ---------- ReplicateHistoryEvents ----------

func (s *histReplSuite) TestReplicateHistoryEvents_TaskBuildError() {
	// empty events slice -> newReplicationTaskFromBatch fails.
	err := s.replicator.ReplicateHistoryEvents(
		context.Background(),
		s.workflowKey(),
		nil,
		nil,
		nil,
		nil,
		"",
	)
	s.Error(err)
}

func (s *histReplSuite) TestReplicateHistoryEvents_Success() {
	ctx := context.Background()
	events := [][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}

	wfErr := serviceerror.NewUnavailable("boom")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, wfErr)

	err := s.replicator.ReplicateHistoryEvents(
		ctx,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		events,
		nil,
		"",
	)
	s.Equal(wfErr, err)
}

// ---------- BackfillHistoryEvents ----------

func (s *histReplSuite) TestBackfillHistoryEvents_TaskBuildError() {
	err := s.replicator.BackfillHistoryEvents(context.Background(), &historyi.BackfillHistoryEventsRequest{
		WorkflowKey: s.workflowKey(),
		Events:      nil, // empty -> error
	})
	s.Error(err)
}

func (s *histReplSuite) TestBackfillHistoryEvents_Success_DelegatesToDoApplyBackfill() {
	ctx := context.Background()
	events := [][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}

	wfErr := serviceerror.NewUnavailable("boom")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, wfErr)

	err := s.replicator.BackfillHistoryEvents(ctx, &historyi.BackfillHistoryEventsRequest{
		WorkflowKey:         s.workflowKey(),
		VersionHistoryItems: []*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		Events:              events,
		VersionedHistory:    &persistencespb.VersionedTransition{NamespaceFailoverVersion: common.EmptyVersion, TransitionCount: 1},
	})
	s.Equal(wfErr, err)
}

// ---------- doApplyBackfillEvents ----------

func (s *histReplSuite) TestDoApplyBackfillEvents_GetContextError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	wfErr := serviceerror.NewUnavailable("boom")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, wfErr)

	err := s.replicator.doApplyBackfillEvents(ctx, task, s.replicator.applyBackfillEvents)
	s.Equal(wfErr, err)
}

func (s *histReplSuite) TestDoApplyBackfillEvents_MutableStateNotFound() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(nil, serviceerror.NewNotFound("not found"))

	err := s.replicator.doApplyBackfillEvents(ctx, task, s.replicator.applyBackfillEvents)
	s.IsType(&serviceerrors.SyncState{}, err)
}

func (s *histReplSuite) TestDoApplyBackfillEvents_LoadMutableStateOtherError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	loadErr := serviceerror.NewUnavailable("db down")
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(nil, loadErr)

	err := s.replicator.doApplyBackfillEvents(ctx, task, s.replicator.applyBackfillEvents)
	s.Equal(loadErr, err)
}

func (s *histReplSuite) TestDoApplyBackfillEvents_ActionInvoked() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(ms, nil)

	sentinel := serviceerror.NewInternal("action ran")
	action := func(context.Context, historyi.MutableState, historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, replicationTask) error {
		return sentinel
	}
	err := s.replicator.doApplyBackfillEvents(ctx, task, action)
	s.Equal(sentinel, err)
}

func (s *histReplSuite) TestDoApplyBackfillEvents_PanicReleasesAndRepanics() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	released := false
	releaseFn := func(err error) {
		released = true
		s.Equal(errPanic, err)
	}
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, historyi.ReleaseWorkflowContextFunc(releaseFn), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(ms, nil)

	action := func(context.Context, historyi.MutableState, historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, replicationTask) error {
		panic("kaboom")
	}
	s.Panics(func() {
		_ = s.replicator.doApplyBackfillEvents(ctx, task, action)
	})
	s.True(released)
}

// ---------- applyBackfillEvents ----------

func (s *histReplSuite) TestApplyBackfillEvents_NilVersionedTransition() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := historyi.NewMockMutableState(s.controller)

	err := s.replicator.applyBackfillEvents(ctx, ms, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
}

func (s *histReplSuite) backfillTask(events [][]*historypb.HistoryEvent, vt *persistencespb.VersionedTransition) replicationTask {
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(events[len(events)-1][len(events[len(events)-1])-1].GetEventId(), common.EmptyVersion)},
		events,
		nil,
		"",
		vt,
		true,
	)
	s.NoError(err)
	return task
}

func (s *histReplSuite) TestApplyBackfillEvents_StartEventNotAllowed() {
	ctx := context.Background()
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: common.EmptyVersion, TransitionCount: 1}
	task := s.backfillTask([][]*historypb.HistoryEvent{{histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)}}, vt)
	ms := historyi.NewMockMutableState(s.controller)

	err := s.replicator.applyBackfillEvents(ctx, ms, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
}

func (s *histReplSuite) TestApplyBackfillEvents_TransitionHistoryStale_SyncState() {
	ctx := context.Background()
	// incoming versioned transition newer than mutable state's last -> SyncState
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 100}
	task := s.backfillTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}, vt)

	ms := historyi.NewMockMutableState(s.controller)
	execInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 1},
		},
		VersionHistories: versionhistory.NewVersionHistories(versionhistory.NewVersionHistory(nil, nil)),
	}
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()

	err := s.replicator.applyBackfillEvents(ctx, ms, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	s.IsType(&serviceerrors.SyncState{}, err)
}

func (s *histReplSuite) TestApplyBackfillEvents_TransitionHistoryDuplicate() {
	ctx := context.Background()
	// incoming versioned transition already covered by mutable state -> ErrDuplicate
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
	task := s.backfillTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}, vt)

	ms := historyi.NewMockMutableState(s.controller)
	execInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 10},
		},
		VersionHistories: versionhistory.NewVersionHistories(versionhistory.NewVersionHistory(nil, nil)),
	}
	ms.EXPECT().GetExecutionInfo().Return(execInfo).AnyTimes()

	err := s.replicator.applyBackfillEvents(ctx, ms, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	s.Equal(consts.ErrDuplicate, err)
}

func (s *histReplSuite) TestApplyBackfillEvents_GetOrCreateBranchError() {
	ctx := context.Background()
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
	task := s.backfillTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}, vt)

	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()

	branchErr := serviceerror.NewUnavailable("branch fail")
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(false, int32(0), branchErr)

	err := s.replicator.applyBackfillEvents(ctx, ms, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	s.Equal(branchErr, err)
}

func (s *histReplSuite) TestApplyBackfillEvents_DoNotContinue_Duplicate() {
	ctx := context.Background()
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
	task := s.backfillTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}, vt)

	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{}).AnyTimes()

	// GetOrCreate returns doContinue=false for every batch -> EventsApplyIndex stays 0, DoContinue false.
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(false, int32(0), nil)

	err := s.replicator.applyBackfillEvents(ctx, ms, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	s.Equal(consts.ErrDuplicate, err)
}

func (s *histReplSuite) TestApplyBackfillEvents_CurrentBranch_CreatesNewBranch_ThenBackfill() {
	ctx := context.Background()
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
	lastEvent := histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)
	task := s.backfillTask([][]*historypb.HistoryEvent{{lastEvent}}, vt)

	ms := s.mutableStateWithTwoBranches(0)
	wfContext := historyi.NewMockWorkflowContext(s.controller)

	// GetOrCreateHistoryBranch: continue, branch index 0 (== current index).
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)
	// CreateHistoryBranch: returns new branch index 1.
	s.mockBranchMgr.EXPECT().Create(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(1), nil)

	// applyBackfillEventsWithoutNew -> applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew
	s.mockTransactionMgr.EXPECT().BackfillWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.applyBackfillEvents(ctx, ms, wfContext, s.noopRelease(), task)
	s.NoError(err)
}

func (s *histReplSuite) TestApplyBackfillEvents_CreateHistoryBranchError() {
	ctx := context.Background()
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 1}
	task := s.backfillTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}, vt)

	ms := s.mutableStateWithVersionHistories(0)
	wfContext := historyi.NewMockWorkflowContext(s.controller)

	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)
	createErr := serviceerror.NewUnavailable("create fail")
	s.mockBranchMgr.EXPECT().Create(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(false, int32(0), createErr)

	err := s.replicator.applyBackfillEvents(ctx, ms, wfContext, s.noopRelease(), task)
	s.Equal(createErr, err)
}

func (s *histReplSuite) TestApplyBackfillEvents_NonCurrentBranch_WithNew_SplitsTask() {
	ctx := context.Background()
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: common.EmptyVersion, TransitionCount: 1}
	events := [][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}
	newEvents := []*historypb.HistoryEvent{histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)}
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		events,
		newEvents,
		uuid.NewString(),
		vt,
		true,
	)
	s.NoError(err)

	ms := s.mutableStateWithVersionHistories(1) // branch index 0 != current 1 -> non current branch
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)

	// applyBackfillEventsWithNew clears, releases, splits, then re-applies.
	// newTask goes through doApplyEvents (start event) and target through doApplyBackfillEvents.
	// Make both inner calls fail fast at GetOrCreateWorkflowExecution to keep this focused.
	wfErr := serviceerror.NewUnavailable("inner")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, wfErr)

	err = s.replicator.applyBackfillEvents(ctx, ms, wfContext, s.noopRelease(), task)
	s.Equal(wfErr, err)
}

func (s *histReplSuite) TestApplyBackfillEvents_NonCurrentBranch_WithoutNew() {
	ctx := context.Background()
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: common.EmptyVersion, TransitionCount: 1}
	task := s.backfillTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}, vt)

	ms := s.mutableStateWithVersionHistories(1) // branch index 0 != current 1
	wfContext := historyi.NewMockWorkflowContext(s.controller)

	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)
	s.mockTransactionMgr.EXPECT().BackfillWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.applyBackfillEvents(ctx, ms, wfContext, s.noopRelease(), task)
	s.NoError(err)
}

// ---------- applyBackfillEventsWithNew error paths ----------

func (s *histReplSuite) TestApplyBackfillEventsWithNew_SplitError() {
	ctx := context.Background()
	// task without new events -> splitTask returns ErrNoNewRunHistory
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	err := s.replicator.applyBackfillEventsWithNew(ctx, wfContext, s.noopRelease(), task)
	s.Equal(ErrNoNewRunHistory, err)
}

// withNewTask builds a backfill task with new run events, so splitTask succeeds.
func (s *histReplSuite) withNewTask() replicationTask {
	events := [][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}
	newEvents := []*historypb.HistoryEvent{histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)}
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		events,
		newEvents,
		uuid.NewString(),
		nil,
		false,
	)
	s.NoError(err)
	return task
}

func (s *histReplSuite) TestApplyBackfillEventsWithNew_NewSucceeds_TargetFails() {
	ctx := context.Background()
	task := s.withNewTask()
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	// step 2: doApplyEvents(newTask) - new run starts with a start event and succeeds.
	newWfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(newWfContext, s.noopRelease(), nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	// step 3: doApplyBackfillEvents(target) fails at GetOrCreateWorkflowExecution.
	targetErr := serviceerror.NewUnavailable("target fail")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, targetErr)

	err := s.replicator.applyBackfillEventsWithNew(ctx, wfContext, s.noopRelease(), task)
	s.Equal(targetErr, err)
}

func (s *histReplSuite) TestNonCurrentBranchWithCAN_NewSucceeds_TargetFails() {
	ctx := context.Background()
	task := s.withNewTask()
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	// step 2: new run succeeds via start event create.
	newWfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(newWfContext, s.noopRelease(), nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	// step 3: target doApplyEvents fails fast.
	targetErr := serviceerror.NewUnavailable("target fail")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, targetErr)

	err := s.replicator.applyNonStartEventsToNonCurrentBranchWithContinueAsNew(ctx, wfContext, s.noopRelease(), task)
	s.Equal(targetErr, err)
}

func (s *histReplSuite) TestDoApplyEvents_StartEvent_Success_ReturnsNil() {
	ctx := context.Background()
	task := s.startTask(false)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)

	// applyStartEvents succeeds (CreateWorkflow returns nil) -> doApplyEvents returns nil directly.
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.doApplyEvents(ctx, task)
	s.NoError(err)
}

func (s *histReplSuite) TestNonCurrentBranchWithCAN_FullSuccess() {
	ctx := context.Background()
	task := s.withNewTask()
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	// step 2: new run start event create succeeds -> doApplyEvents(newTask) returns nil.
	newWfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(newWfContext, s.noopRelease(), nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	// step 3: target doApplyEvents - non-start event, loads ms, flushes, branch, rebuild, non-current backfill.
	targetWfContext := historyi.NewMockWorkflowContext(s.controller)
	targetMS := s.mutableStateWithVersionHistories(1) // branch 0 != current 1
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(targetWfContext, s.noopRelease(), nil)
	targetWfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(targetMS, nil)
	s.mockBufferFlusher.EXPECT().flush(ctx).Return(targetWfContext, targetMS, nil)
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)
	s.mockConflictResolver.EXPECT().GetOrRebuildCurrentMutableState(ctx, int32(0), gomock.Any()).Return(targetMS, false, nil)
	s.mockTransactionMgr.EXPECT().BackfillWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.applyNonStartEventsToNonCurrentBranchWithContinueAsNew(ctx, wfContext, s.noopRelease(), task)
	s.NoError(err)
}

func (s *histReplSuite) TestApplyBackfillEventsWithNew_FullSuccess() {
	ctx := context.Background()
	// state-based task with new events and versioned transition so the split target
	// can flow through applyBackfillEvents to a non-current branch backfill.
	events := [][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}
	newEvents := []*historypb.HistoryEvent{histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)}
	vt := &persistencespb.VersionedTransition{NamespaceFailoverVersion: common.EmptyVersion, TransitionCount: 1}
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		events,
		newEvents,
		uuid.NewString(),
		vt,
		true,
	)
	s.NoError(err)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	// step 2: new run (start event) create succeeds.
	newWfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(newWfContext, s.noopRelease(), nil)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	// step 3: target doApplyBackfillEvents -> applyBackfillEvents -> non-current branch backfill.
	targetWfContext := historyi.NewMockWorkflowContext(s.controller)
	targetMS := s.mutableStateWithVersionHistories(1) // empty transition history, branch 0 != current 1
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(targetWfContext, s.noopRelease(), nil)
	targetWfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(targetMS, nil)
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)
	s.mockTransactionMgr.EXPECT().BackfillWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err = s.replicator.applyBackfillEventsWithNew(ctx, wfContext, s.noopRelease(), task)
	s.NoError(err)
}

func (s *histReplSuite) TestApplyBackfillEventsWithNew_NewFails() {
	ctx := context.Background()
	task := s.withNewTask()
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	// step 2: doApplyEvents(newTask) fails fast at GetOrCreateWorkflowExecution.
	newErr := serviceerror.NewUnavailable("new fail")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, newErr)

	err := s.replicator.applyBackfillEventsWithNew(ctx, wfContext, s.noopRelease(), task)
	s.Equal(newErr, err)
}

// ---------- doApplyEvents ----------

func (s *histReplSuite) TestDoApplyEvents_GetContextError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	wfErr := serviceerror.NewUnavailable("boom")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, wfErr)

	err := s.replicator.doApplyEvents(ctx, task)
	s.Equal(wfErr, err)
}

func (s *histReplSuite) TestDoApplyEvents_PanicRepanics() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	released := false
	releaseFn := func(err error) {
		released = true
		s.Equal(errPanic, err)
	}
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, historyi.ReleaseWorkflowContextFunc(releaseFn), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).DoAndReturn(func(context.Context, historyi.ShardContext) (historyi.MutableState, error) {
		panic("kaboom")
	})

	s.Panics(func() {
		_ = s.replicator.doApplyEvents(ctx, task)
	})
	s.True(released)
}

func (s *histReplSuite) TestDoApplyEvents_LoadMutableStateOtherError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	loadErr := serviceerror.NewUnavailable("db down")
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(nil, loadErr)

	err := s.replicator.doApplyEvents(ctx, task)
	s.Equal(loadErr, err)
}

func (s *histReplSuite) TestDoApplyEvents_FlushBufferError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(ms, nil)

	flushErr := serviceerror.NewUnavailable("flush fail")
	s.mockBufferFlusher.EXPECT().flush(ctx).Return(nil, nil, flushErr)

	err := s.replicator.doApplyEvents(ctx, task)
	s.Equal(flushErr, err)
}

func (s *histReplSuite) TestDoApplyEvents_GetOrCreateBranchError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(ms, nil)
	s.mockBufferFlusher.EXPECT().flush(ctx).Return(wfContext, ms, nil)

	branchErr := serviceerror.NewUnavailable("branch fail")
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(false, int32(0), branchErr)

	err := s.replicator.doApplyEvents(ctx, task)
	s.Equal(branchErr, err)
}

func (s *histReplSuite) TestDoApplyEvents_DoNotContinue_Duplicate() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(ms, nil)
	s.mockBufferFlusher.EXPECT().flush(ctx).Return(wfContext, ms, nil)
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(false, int32(0), nil)

	err := s.replicator.doApplyEvents(ctx, task)
	s.Equal(consts.ErrDuplicate, err)
}

func (s *histReplSuite) TestDoApplyEvents_GetOrRebuildError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(ms, nil)
	s.mockBufferFlusher.EXPECT().flush(ctx).Return(wfContext, ms, nil)
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)

	rebuildErr := serviceerror.NewUnavailable("rebuild fail")
	s.mockConflictResolver.EXPECT().GetOrRebuildCurrentMutableState(ctx, int32(0), gomock.Any()).Return(nil, false, rebuildErr)

	err := s.replicator.doApplyEvents(ctx, task)
	s.Equal(rebuildErr, err)
}

func (s *histReplSuite) TestDoApplyEvents_CurrentBranch_Success() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := s.mutableStateWithVersionHistories(0)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(ms, nil)
	s.mockBufferFlusher.EXPECT().flush(ctx).Return(wfContext, ms, nil)
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)
	s.mockConflictResolver.EXPECT().GetOrRebuildCurrentMutableState(ctx, int32(0), gomock.Any()).Return(ms, false, nil)

	// applyNonStartEventsToCurrentBranch
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().UpdateWorkflow(ctx, false, gomock.Any(), gomock.Any(), nil).Return(nil)

	err := s.replicator.doApplyEvents(ctx, task)
	s.NoError(err)
}

func (s *histReplSuite) TestDoApplyEvents_NonCurrentBranch_Success() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := s.mutableStateWithVersionHistories(1) // branch 0 != current 1
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(ms, nil)
	s.mockBufferFlusher.EXPECT().flush(ctx).Return(wfContext, ms, nil)
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), int64(2), gomock.Any()).Return(true, int32(0), nil)
	s.mockConflictResolver.EXPECT().GetOrRebuildCurrentMutableState(ctx, int32(0), gomock.Any()).Return(ms, false, nil)

	s.mockTransactionMgr.EXPECT().BackfillWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.doApplyEvents(ctx, task)
	s.NoError(err)
}

func (s *histReplSuite) TestDoApplyEvents_MissingMutableState_ResetWorkflow() {
	ctx := context.Background()
	// workflow reset task: baseWorkflowInfo.LowestCommonAncestorEventId+1 == firstEvent.EventId
	firstEvent := histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)
	baseInfo := &workflowspb.BaseExecutionInfo{
		RunId:                            uuid.NewString(),
		LowestCommonAncestorEventId:      1,
		LowestCommonAncestorEventVersion: common.EmptyVersion,
	}
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		baseInfo,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		[][]*historypb.HistoryEvent{{firstEvent}},
		nil,
		"",
		nil,
		false,
	)
	s.NoError(err)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().GetWorkflowKey().Return(s.workflowKey()).AnyTimes()
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(nil, serviceerror.NewNotFound("not found"))

	resetMS := s.mutableStateWithVersionHistories(0)
	s.resetReturn = resetMS
	s.resetErr = nil

	// applyNonStartEventsResetWorkflow
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err = s.replicator.doApplyEvents(ctx, task)
	s.NoError(err)
}

func (s *histReplSuite) TestDoApplyEvents_MissingMutableState_NonReset_RetryReplication() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(nil, serviceerror.NewNotFound("not found"))

	err := s.replicator.doApplyEvents(ctx, task)
	s.IsType(&serviceerrors.RetryReplication{}, err)
}

func (s *histReplSuite) TestDoApplyEvents_StartEvent_DuplicateThenContinue() {
	ctx := context.Background()
	// First event is a start event. applyStartEvents returns ErrDuplicate (CreateWorkflow dup),
	// so doApplyEvents continues to load mutable state path.
	startEvent := histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(1, common.EmptyVersion)},
		[][]*historypb.HistoryEvent{{startEvent}},
		nil,
		"",
		nil,
		false,
	)
	s.NoError(err)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)

	// applyStartEvents
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(consts.ErrDuplicate)

	// then continues to load mutable state -> NotFound -> non-reset -> RetryReplication
	wfContext.EXPECT().LoadMutableState(ctx, s.mockShard).Return(nil, serviceerror.NewNotFound("not found"))

	err = s.replicator.doApplyEvents(ctx, task)
	s.IsType(&serviceerrors.RetryReplication{}, err)
}

func (s *histReplSuite) TestDoApplyEvents_StartEvent_NonDuplicateError() {
	ctx := context.Background()
	startEvent := histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(1, common.EmptyVersion)},
		[][]*historypb.HistoryEvent{{startEvent}},
		nil,
		"",
		nil,
		false,
	)
	s.NoError(err)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(wfContext, s.noopRelease(), nil)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	createErr := serviceerror.NewUnavailable("create fail")
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(createErr)

	err = s.replicator.doApplyEvents(ctx, task)
	s.Equal(createErr, err)
}

// ---------- applyStartEvents ----------

func (s *histReplSuite) startTask(stateBased bool) replicationTask {
	startEvent := histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(1, common.EmptyVersion)},
		[][]*historypb.HistoryEvent{{startEvent}},
		nil,
		"",
		nil,
		stateBased,
	)
	s.NoError(err)
	return task
}

func (s *histReplSuite) TestApplyStartEvents_GetNamespaceError() {
	ctx := context.Background()
	task := s.startTask(false)
	nsErr := serviceerror.NewUnavailable("ns fail")
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(nil, nsErr)

	err := s.replicator.applyStartEvents(ctx, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	s.Equal(nsErr, err)
}

func (s *histReplSuite) TestApplyStartEvents_ApplyEventsError() {
	ctx := context.Background()
	task := s.startTask(false)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	applyErr := serviceerror.NewUnavailable("apply fail")
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, applyErr)

	err := s.replicator.applyStartEvents(ctx, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	s.Equal(applyErr, err)
}

func (s *histReplSuite) TestApplyStartEvents_Success_Notifies() {
	ctx := context.Background()
	task := s.startTask(false)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.applyStartEvents(ctx, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	s.NoError(err)
}

func (s *histReplSuite) TestApplyStartEvents_StateBased_NewMutableStateLogged() {
	ctx := context.Background()
	task := s.startTask(true)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(tests.GlobalNamespaceEntry, nil)
	// returning a non-nil newMutableState exercises the logging branch.
	newMS := historyi.NewMockMutableState(s.controller)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(newMS, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.applyStartEvents(ctx, historyi.NewMockWorkflowContext(s.controller), s.noopRelease(), task)
	s.NoError(err)
}

// ---------- applyNonStartEventsToCurrentBranch ----------

func (s *histReplSuite) TestApplyNonStartEventsToCurrentBranch_ApplyError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := historyi.NewMockMutableState(s.controller)
	applyErr := serviceerror.NewUnavailable("apply fail")
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, applyErr)

	err := s.replicator.applyNonStartEventsToCurrentBranch(ctx, historyi.NewMockWorkflowContext(s.controller), ms, false, s.noopRelease(), task)
	s.Equal(applyErr, err)
}

func (s *histReplSuite) TestApplyNonStartEventsToCurrentBranch_WithNewMutableState() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := historyi.NewMockMutableState(s.controller)

	newMS := historyi.NewMockMutableState(s.controller)
	newMS.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: s.namespaceID.String(),
		WorkflowId:  s.workflowID,
	}).AnyTimes()
	newMS.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: uuid.NewString()}).AnyTimes()
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(newMS, nil)
	s.mockTransactionMgr.EXPECT().UpdateWorkflow(ctx, true, gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.applyNonStartEventsToCurrentBranch(ctx, historyi.NewMockWorkflowContext(s.controller), ms, true, s.noopRelease(), task)
	s.NoError(err)
}

func (s *histReplSuite) TestApplyNonStartEventsToCurrentBranch_UpdateError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := historyi.NewMockMutableState(s.controller)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	updateErr := serviceerror.NewUnavailable("update fail")
	s.mockTransactionMgr.EXPECT().UpdateWorkflow(ctx, false, gomock.Any(), gomock.Any(), nil).Return(updateErr)

	err := s.replicator.applyNonStartEventsToCurrentBranch(ctx, historyi.NewMockWorkflowContext(s.controller), ms, false, s.noopRelease(), task)
	s.Equal(updateErr, err)
}

// ---------- applyNonStartEventsToNonCurrentBranch ----------

func (s *histReplSuite) TestApplyNonStartEventsToNonCurrentBranch_WithContinueAsNew() {
	ctx := context.Background()
	events := [][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}
	newEvents := []*historypb.HistoryEvent{histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)}
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		events,
		newEvents,
		uuid.NewString(),
		nil,
		false,
	)
	s.NoError(err)

	ms := historyi.NewMockMutableState(s.controller)
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	// inner doApplyEvents fails fast.
	wfErr := serviceerror.NewUnavailable("inner")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, wfErr)

	err = s.replicator.applyNonStartEventsToNonCurrentBranch(ctx, wfContext, ms, int32(0), s.noopRelease(), task)
	s.Equal(wfErr, err)
}

func (s *histReplSuite) TestApplyNonStartEventsToNonCurrentBranch_WithoutContinueAsNew() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := s.mutableStateWithVersionHistories(0)
	s.mockTransactionMgr.EXPECT().BackfillWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.applyNonStartEventsToNonCurrentBranch(ctx, historyi.NewMockWorkflowContext(s.controller), ms, int32(0), s.noopRelease(), task)
	s.NoError(err)
}

// ---------- applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew ----------

func (s *histReplSuite) TestNonCurrentBranchWithoutCAN_GetVersionHistoryError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := s.mutableStateWithVersionHistories(0)
	// branch index out of range -> GetVersionHistory error.
	err := s.replicator.applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(ctx, historyi.NewMockWorkflowContext(s.controller), ms, int32(99), s.noopRelease(), task)
	s.Error(err)
}

func (s *histReplSuite) TestNonCurrentBranchWithoutCAN_BackfillError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := s.mutableStateWithVersionHistories(0)
	backfillErr := serviceerror.NewUnavailable("backfill fail")
	s.mockTransactionMgr.EXPECT().BackfillWorkflow(ctx, gomock.Any(), gomock.Any()).Return(backfillErr)

	err := s.replicator.applyNonStartEventsToNonCurrentBranchWithoutContinueAsNew(ctx, historyi.NewMockWorkflowContext(s.controller), ms, int32(0), s.noopRelease(), task)
	s.Equal(backfillErr, err)
}

// ---------- applyNonStartEventsToNonCurrentBranchWithContinueAsNew ----------

func (s *histReplSuite) TestNonCurrentBranchWithCAN_SplitError() {
	ctx := context.Background()
	// no new events -> splitTask error.
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	err := s.replicator.applyNonStartEventsToNonCurrentBranchWithContinueAsNew(ctx, wfContext, s.noopRelease(), task)
	s.Equal(ErrNoNewRunHistory, err)
}

func (s *histReplSuite) TestNonCurrentBranchWithCAN_NewWorkflowError() {
	ctx := context.Background()
	events := [][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}}
	newEvents := []*historypb.HistoryEvent{histReplEvent(1, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED)}
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		nil,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		events,
		newEvents,
		uuid.NewString(),
		nil,
		false,
	)
	s.NoError(err)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear()

	wfErr := serviceerror.NewUnavailable("inner new")
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx, s.mockShard, s.namespaceID, gomock.Any(), locks.PriorityHigh,
	).Return(nil, nil, wfErr)

	err = s.replicator.applyNonStartEventsToNonCurrentBranchWithContinueAsNew(ctx, wfContext, s.noopRelease(), task)
	s.Equal(wfErr, err)
}

// ---------- applyNonStartEventsMissingMutableState ----------

func (s *histReplSuite) TestApplyNonStartEventsMissingMutableState_NonReset_NoBaseInfo() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})

	ms, err := s.replicator.applyNonStartEventsMissingMutableState(ctx, historyi.NewMockWorkflowContext(s.controller), task)
	s.Nil(ms)
	s.IsType(&serviceerrors.RetryReplication{}, err)
}

func (s *histReplSuite) TestApplyNonStartEventsMissingMutableState_NonReset_WithBaseInfo() {
	ctx := context.Background()
	// base info present but not a reset (LCA+1 != firstEvent.EventId)
	baseInfo := &workflowspb.BaseExecutionInfo{
		RunId:                            uuid.NewString(),
		LowestCommonAncestorEventId:      10,
		LowestCommonAncestorEventVersion: common.EmptyVersion,
	}
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		baseInfo,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		[][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}},
		nil,
		"",
		nil,
		false,
	)
	s.NoError(err)

	ms, err := s.replicator.applyNonStartEventsMissingMutableState(ctx, historyi.NewMockWorkflowContext(s.controller), task)
	s.Nil(ms)
	s.IsType(&serviceerrors.RetryReplication{}, err)
}

func (s *histReplSuite) resetTask() replicationTask {
	firstEvent := histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)
	baseInfo := &workflowspb.BaseExecutionInfo{
		RunId:                            uuid.NewString(),
		LowestCommonAncestorEventId:      1,
		LowestCommonAncestorEventVersion: common.EmptyVersion,
	}
	task, err := newReplicationTask(
		s.mockClusterMetadata,
		s.logger,
		s.workflowKey(),
		baseInfo,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(2, common.EmptyVersion)},
		[][]*historypb.HistoryEvent{{firstEvent}},
		nil,
		"",
		nil,
		false,
	)
	s.NoError(err)
	return task
}

func (s *histReplSuite) TestApplyNonStartEventsMissingMutableState_Reset_Success() {
	ctx := context.Background()
	task := s.resetTask()
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().GetWorkflowKey().Return(s.workflowKey()).AnyTimes()

	resetMS := historyi.NewMockMutableState(s.controller)
	s.resetReturn = resetMS
	s.resetErr = nil

	ms, err := s.replicator.applyNonStartEventsMissingMutableState(ctx, wfContext, task)
	s.NoError(err)
	s.Equal(resetMS, ms)
}

func (s *histReplSuite) TestApplyNonStartEventsMissingMutableState_Reset_Error() {
	ctx := context.Background()
	task := s.resetTask()
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().GetWorkflowKey().Return(s.workflowKey()).AnyTimes()

	resetErr := serviceerror.NewUnavailable("reset fail")
	s.resetReturn = nil
	s.resetErr = resetErr

	ms, err := s.replicator.applyNonStartEventsMissingMutableState(ctx, wfContext, task)
	s.Nil(ms)
	s.Equal(resetErr, err)
}

// ---------- applyNonStartEventsResetWorkflow ----------

func (s *histReplSuite) TestApplyNonStartEventsResetWorkflow_ApplyError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := historyi.NewMockMutableState(s.controller)
	applyErr := serviceerror.NewUnavailable("apply fail")
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, applyErr)

	err := s.replicator.applyNonStartEventsResetWorkflow(ctx, historyi.NewMockWorkflowContext(s.controller), ms, task)
	s.Equal(applyErr, err)
}

func (s *histReplSuite) TestApplyNonStartEventsResetWorkflow_Success_WithNewMutableState() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := historyi.NewMockMutableState(s.controller)
	newMS := historyi.NewMockMutableState(s.controller)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(newMS, nil)
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(nil)

	err := s.replicator.applyNonStartEventsResetWorkflow(ctx, historyi.NewMockWorkflowContext(s.controller), ms, task)
	s.NoError(err)
}

func (s *histReplSuite) TestApplyNonStartEventsResetWorkflow_CreateError() {
	ctx := context.Background()
	task := s.nonStartTask([][]*historypb.HistoryEvent{{histReplEvent(2, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED)}})
	ms := historyi.NewMockMutableState(s.controller)
	s.mockMSRebuilder.EXPECT().ApplyEvents(ctx, s.namespaceID, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
	createErr := serviceerror.NewUnavailable("create fail")
	s.mockTransactionMgr.EXPECT().CreateWorkflow(ctx, gomock.Any(), gomock.Any()).Return(createErr)

	err := s.replicator.applyNonStartEventsResetWorkflow(ctx, historyi.NewMockWorkflowContext(s.controller), ms, task)
	s.Equal(createErr, err)
}

// ---------- notify ----------

func (s *histReplSuite) TestNotify_CurrentCluster_NoOp() {
	// notify to the current cluster name short-circuits.
	s.replicator.notify(cluster.TestCurrentClusterName, time.Now())
}

func (s *histReplSuite) TestNotify_RemoteCluster_SetsCurrentTime() {
	s.replicator.notify(cluster.TestAlternativeClusterName, time.Now().Add(-time.Hour))
}
