package ndc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	histImportSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockShard          *shard.ContextTest
		mockWorkflowCache  *wcache.MockCache
		mockNamespaceCache *namespace.MockRegistry
		mockTxMgr          *MockTransactionManager
		mockTaskRefresher  *workflow.MockTaskRefresher

		mockBranchMgr        *MockBranchMgr
		mockConflictResolver *MockConflictResolver
		mockBufferFlusher    *MockBufferEventFlusher
		mockRebuilder        *workflow.MockMutableStateRebuilder

		logger log.Logger

		importer *HistoryImporterImpl
	}
)

func TestHistImportSuite(t *testing.T) {
	suite.Run(t, new(histImportSuite))
}

func (s *histImportSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	config := tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		config,
	)
	reg := hsm.NewRegistry()
	s.NoError(workflow.RegisterStateMachine(reg))
	s.mockShard.SetStateMachineRegistry(reg)
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(gomock.Any(), gomock.Any()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.MockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockWorkflowCache = wcache.NewMockCache(s.controller)
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockTxMgr = NewMockTransactionManager(s.controller)
	s.mockTaskRefresher = workflow.NewMockTaskRefresher(s.controller)
	s.mockBranchMgr = NewMockBranchMgr(s.controller)
	s.mockConflictResolver = NewMockConflictResolver(s.controller)
	s.mockBufferFlusher = NewMockBufferEventFlusher(s.controller)
	s.mockRebuilder = workflow.NewMockMutableStateRebuilder(s.controller)
	s.logger = s.mockShard.GetLogger()

	// Build via the real constructor (covers NewHistoryImporter), then swap
	// concrete collaborators with mock-backed equivalents for deterministic tests.
	s.importer = NewHistoryImporter(s.mockShard, s.mockWorkflowCache, s.logger)
	s.importer.transactionMgr = s.mockTxMgr
	s.importer.taskRefresher = s.mockTaskRefresher
	s.importer.mutableStateInitializer = NewMutableStateInitializer(s.mockShard, s.mockWorkflowCache, s.logger)
	s.importer.mutableStateMapper = NewMutableStateMapping(
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
			return s.mockRebuilder
		},
	)
}

func (s *histImportSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

// ---- helpers ----

func (s *histImportSuite) startEvents() [][]*historypb.HistoryEvent {
	return [][]*historypb.HistoryEvent{
		{
			{
				EventId:   1,
				Version:   100,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				EventTime: timestampNow(),
			},
		},
	}
}

func (s *histImportSuite) nonStartEvents() [][]*historypb.HistoryEvent {
	return [][]*historypb.HistoryEvent{
		{
			{
				EventId:   5,
				Version:   100,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				EventTime: timestampNow(),
			},
		},
	}
}

func (s *histImportSuite) versionHistoryItems() []*historyspb.VersionHistoryItem {
	return []*historyspb.VersionHistoryItem{{EventId: 1, Version: 100}}
}

func timestampNow() *timestamppb.Timestamp {
	return timestamppb.New(time.Now().UTC())
}

// newTask builds a replication task directly (mirrors newReplicationTaskFromBatch
// success), used to exercise the apply*/persist methods in isolation.
func (s *histImportSuite) newTask(eventsSlice [][]*historypb.HistoryEvent) replicationTask {
	task, err := newReplicationTaskFromBatch(
		s.mockShard.GetClusterMetadata(),
		s.logger,
		tests.WorkflowKey,
		nil,
		s.versionHistoryItems(),
		eventsSlice,
		nil,
		"",
		nil,
		false,
	)
	s.NoError(err)
	return task
}

// newMockWorkflow builds a MockWorkflow returning the provided context / mutable state.
func (s *histImportSuite) newMockWorkflow(
	wfContext historyi.WorkflowContext,
	mutableState historyi.MutableState,
) *MockWorkflow {
	wf := NewMockWorkflow(s.controller)
	wf.EXPECT().GetContext().Return(wfContext).AnyTimes()
	wf.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	return wf
}

// successfulPersistMutableState returns a MockMutableState configured so that
// persistHistoryAndSerializeMutableState succeeds (empty events => no DB write).
func (s *histImportSuite) successfulPersistMutableState() *historyi.MockMutableState {
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().CloseTransactionAsSnapshot(gomock.Any(), historyi.TransactionPolicyPassive).Return(
		&persistence.WorkflowSnapshot{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{},
			ExecutionState: &persistencespb.WorkflowExecutionState{},
		},
		[]*persistence.WorkflowEvents{}, // no events => PersistWorkflowEvents returns (0, nil)
		nil,
	)
	ms.EXPECT().AddHistorySize(int64(0))
	return ms
}

// ---- NewHistoryImporter ----

func (s *histImportSuite) TestNewHistoryImporter() {
	importer := NewHistoryImporter(s.mockShard, s.mockWorkflowCache, s.logger)
	s.NotNil(importer)
	s.NotNil(importer.mutableStateInitializer)
	s.NotNil(importer.mutableStateMapper)
	s.NotNil(importer.taskRefresher)
	s.NotNil(importer.transactionMgr)
	s.Equal(s.mockShard, importer.shardContext)
}

// TestNewHistoryImporter_BufferFlusherProvider exercises the bufferEventFlusher
// provider closure wired by NewHistoryImporter (constructs a real flusher and runs
// it against a mutable state with no buffered events).
func (s *histImportSuite) TestNewHistoryImporter_BufferFlusherProvider() {
	importer := NewHistoryImporter(s.mockShard, s.mockWorkflowCache, s.logger)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().HasBufferedEvents().Return(false)
	ms.EXPECT().HasStartedWorkflowTask().Return(false)
	task := s.newTask(s.nonStartEvents())

	outMS, _, err := importer.mutableStateMapper.FlushBufferEvents(context.Background(), wfContext, ms, task)
	s.NoError(err)
	s.Equal(ms, outMS)
}

// TestNewHistoryImporter_BranchMgrProvider exercises the branchMgr provider closure
// (constructs a real BranchMgr; empty local version histories make it return early).
func (s *histImportSuite) TestNewHistoryImporter_BranchMgrProvider() {
	importer := NewHistoryImporter(s.mockShard, s.mockWorkflowCache, s.logger)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	// local branch diverges from the incoming task's version history at event 1 ->
	// FindLCAVersionHistoryItemAndIndex returns an error, short-circuiting the real
	// BranchMgr without further mutable-state interaction.
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: s.versionHistories(int64(1), int64(999)),
	}).AnyTimes()
	task := s.newTask(s.nonStartEvents())

	_, _, err := importer.mutableStateMapper.GetOrCreateHistoryBranch(context.Background(), wfContext, ms, task)
	s.Error(err)
}

// TestNewHistoryImporter_ConflictResolverProvider exercises the conflictResolver
// provider closure (constructs a real ConflictResolver; branchIndex == current index
// makes it return the current mutable state directly).
func (s *histImportSuite) TestNewHistoryImporter_ConflictResolverProvider() {
	importer := NewHistoryImporter(s.mockShard, s.mockWorkflowCache, s.logger)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: &historyspb.VersionHistories{CurrentVersionHistoryIndex: 0},
	}).AnyTimes()
	task := GetOrRebuildMutableStateIn{replicationTask: s.newTask(s.nonStartEvents()), BranchIndex: 0}

	outMS, isRebuilt, err := importer.mutableStateMapper.GetOrRebuildMutableState(context.Background(), wfContext, ms, task)
	s.NoError(err)
	s.False(isRebuilt)
	s.Equal(ms, outMS)
}

// ---- ImportWorkflow ----

func (s *histImportSuite) TestImportWorkflow_EmptyEventsAndToken() {
	_, success, err := s.importer.ImportWorkflow(
		context.Background(),
		tests.WorkflowKey,
		nil,
		nil,
		nil,
	)
	s.False(success)
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
}

func (s *histImportSuite) TestImportWorkflow_InitializeError() {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(nil, serviceerror.NewInternal("ns error"))

	_, success, err := s.importer.ImportWorkflow(
		context.Background(),
		tests.WorkflowKey,
		s.versionHistoryItems(),
		s.startEvents(),
		nil,
	)
	s.False(success)
	s.Error(err)
}

func (s *histImportSuite) TestImportWorkflow_ApplyEvents_BrandNewDispatch() {
	// Drive Initialize via DB-not-found path -> IsBrandNew == true.
	wfContext, releaseFn := s.expectInitializeFromDBNotFound()
	_ = wfContext

	// applyEvents -> brand new -> applyStartEventsAndSerialize -> mapper.ApplyEvents errors,
	// returning before any persistence is required.
	s.mockRebuilder.EXPECT().ApplyEvents(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, serviceerror.NewInternal("apply error"))

	_, success, err := s.importer.ImportWorkflow(
		context.Background(),
		tests.WorkflowKey,
		s.versionHistoryItems(),
		s.startEvents(),
		nil,
	)
	s.False(success)
	s.Error(err)
	s.True(releaseFn.called)
}

func (s *histImportSuite) TestImportWorkflow_Commit_NoEventsWithToken() {
	// Token present, no events -> commit branch. Use a brand-new spec to hit the
	// IsBrandNew commit guard, which returns deterministically without DB access.
	token := s.buildToken(true /*isBrandNew via existsInDB false won't set IsBrandNew*/)
	// commit with IsBrandNew requires DB-not-found path which needs no token; so for
	// the token path IsBrandNew is always false. ExistsInDB=false triggers CreateWorkflow.
	s.expectInitializeFromTokenSucceeds(false /*existsInDB*/)

	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), false).Return(nil)
	s.mockTxMgr.EXPECT().CreateWorkflow(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil)

	_, success, err := s.importer.ImportWorkflow(
		context.Background(),
		tests.WorkflowKey,
		s.versionHistoryItems(),
		nil,
		token,
	)
	s.False(success)
	s.NoError(err)
}

func (s *histImportSuite) TestImportWorkflow_Commit_Error() {
	s.expectInitializeFromTokenSucceeds(false /*existsInDB*/)

	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), false).Return(serviceerror.NewInternal("refresh err"))

	_, success, err := s.importer.ImportWorkflow(
		context.Background(),
		tests.WorkflowKey,
		s.versionHistoryItems(),
		nil,
		s.buildToken(false),
	)
	s.False(success)
	s.Error(err)
}

// ---- applyEvents ----

func (s *histImportSuite) TestApplyEvents_InvalidEvents() {
	// version mismatch within a slice -> newReplicationTaskFromBatch error
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	wf := s.newMockWorkflow(wfContext, historyi.NewMockMutableState(s.controller))

	badEvents := [][]*historypb.HistoryEvent{
		{
			{EventId: 1, Version: 1},
			{EventId: 2, Version: 2},
		},
	}
	_, success, err := s.importer.applyEvents(
		context.Background(),
		wf,
		MutableStateInitializationSpec{},
		s.versionHistoryItems(),
		badEvents,
		false,
	)
	s.False(success)
	s.Error(err)
}

func (s *histImportSuite) TestApplyEvents_BrandNew_NonStartFirstEvent() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	wf := s.newMockWorkflow(wfContext, historyi.NewMockMutableState(s.controller))

	_, success, err := s.importer.applyEvents(
		context.Background(),
		wf,
		MutableStateInitializationSpec{IsBrandNew: true},
		s.versionHistoryItems(),
		s.nonStartEvents(), // first event is not workflow started
		false,
	)
	s.False(success)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
}

func (s *histImportSuite) TestApplyEvents_BrandNew_StartDispatch() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms := s.successfulPersistMutableState()
	wf := s.newMockWorkflow(wfContext, ms)

	s.mockRebuilder.EXPECT().ApplyEvents(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, nil)

	token, success, err := s.importer.applyEvents(
		context.Background(),
		wf,
		MutableStateInitializationSpec{IsBrandNew: true},
		s.versionHistoryItems(),
		s.startEvents(),
		false,
	)
	s.True(success)
	s.NoError(err)
	s.NotNil(token)
}

func (s *histImportSuite) TestApplyEvents_NonStartDispatch() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	ms := historyi.NewMockMutableState(s.controller)
	wf := s.newMockWorkflow(wfContext, ms)

	// non-brand-new -> applyNonStartEventsAndSerialize; createNewBranch=false ->
	// GetOrCreateHistoryBranch with DoContinue=false -> persist.
	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, int32(0), nil)
	ms.EXPECT().CloseTransactionAsSnapshot(gomock.Any(), historyi.TransactionPolicyPassive).Return(
		&persistence.WorkflowSnapshot{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{},
			ExecutionState: &persistencespb.WorkflowExecutionState{},
		},
		[]*persistence.WorkflowEvents{},
		nil,
	)
	ms.EXPECT().AddHistorySize(int64(0))

	_, success, err := s.importer.applyEvents(
		context.Background(),
		wf,
		MutableStateInitializationSpec{IsBrandNew: false},
		s.versionHistoryItems(),
		s.nonStartEvents(),
		false,
	)
	s.False(success)
	s.NoError(err)
}

// ---- applyStartEventsAndSerialize ----

func (s *histImportSuite) TestApplyStartEvents_ApplyError() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	task := s.newTask(s.startEvents())

	s.mockRebuilder.EXPECT().ApplyEvents(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, serviceerror.NewInternal("apply err"))

	_, success, err := s.importer.applyStartEventsAndSerialize(
		context.Background(), wfContext, ms, MutableStateInitializationSpec{}, task,
	)
	s.False(success)
	s.Error(err)
}

func (s *histImportSuite) TestApplyStartEvents_NewMutableStateLogged() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := s.successfulPersistMutableState()
	task := s.newTask(s.startEvents())

	// non-nil newMutableState triggers the warning-log branch.
	newMS := historyi.NewMockMutableState(s.controller)
	s.mockRebuilder.EXPECT().ApplyEvents(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(newMS, nil)

	token, success, err := s.importer.applyStartEventsAndSerialize(
		context.Background(), wfContext, ms, MutableStateInitializationSpec{}, task,
	)
	s.True(success)
	s.NoError(err)
	s.NotNil(token)
}

// ---- applyNonStartEventsAndSerialize ----

func (s *histImportSuite) TestApplyNonStart_PrepareBranchError() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	task := s.newTask(s.nonStartEvents())

	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, int32(0), serviceerror.NewInternal("branch err"))

	_, success, err := s.importer.applyNonStartEventsAndSerialize(
		context.Background(), wfContext, ms, MutableStateInitializationSpec{}, task, false,
	)
	s.False(success)
	s.Error(err)
}

func (s *histImportSuite) TestApplyNonStart_DoContinueFalse() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	task := s.newTask(s.nonStartEvents())

	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(false, int32(1), nil)
	ms.EXPECT().CloseTransactionAsSnapshot(gomock.Any(), historyi.TransactionPolicyPassive).Return(
		&persistence.WorkflowSnapshot{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionState: &persistencespb.WorkflowExecutionState{}},
		[]*persistence.WorkflowEvents{}, nil,
	)
	ms.EXPECT().AddHistorySize(int64(0))

	_, success, err := s.importer.applyNonStartEventsAndSerialize(
		context.Background(), wfContext, ms, MutableStateInitializationSpec{}, task, false,
	)
	s.False(success)
	s.NoError(err)
}

func (s *histImportSuite) TestApplyNonStart_CreateNewBranch_SanityCheck() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	task := s.newTask(s.nonStartEvents())

	// createNewBranch=true uses CreateHistoryBranch (branchMgr.Create); DoContinue=true,
	// BranchIndex==0 -> sanity-check error.
	s.mockBranchMgr.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, int32(0), nil)

	_, success, err := s.importer.applyNonStartEventsAndSerialize(
		context.Background(), wfContext, ms, MutableStateInitializationSpec{}, task, true,
	)
	s.False(success)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
}

func (s *histImportSuite) TestApplyNonStart_GetOrRebuildError() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	task := s.newTask(s.nonStartEvents())

	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, int32(1), nil)
	s.mockConflictResolver.EXPECT().GetOrRebuildMutableState(gomock.Any(), int32(1)).Return(nil, false, serviceerror.NewInternal("rebuild err"))

	_, success, err := s.importer.applyNonStartEventsAndSerialize(
		context.Background(), wfContext, ms, MutableStateInitializationSpec{}, task, false,
	)
	s.False(success)
	s.Error(err)
}

func (s *histImportSuite) TestApplyNonStart_ApplyEventsError() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := historyi.NewMockMutableState(s.controller)
	task := s.newTask(s.nonStartEvents())

	s.mockBranchMgr.EXPECT().GetOrCreate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, int32(1), nil)
	s.mockConflictResolver.EXPECT().GetOrRebuildMutableState(gomock.Any(), int32(1)).Return(ms, false, nil)
	s.mockRebuilder.EXPECT().ApplyEvents(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, serviceerror.NewInternal("apply err"))

	_, success, err := s.importer.applyNonStartEventsAndSerialize(
		context.Background(), wfContext, ms, MutableStateInitializationSpec{}, task, false,
	)
	s.False(success)
	s.Error(err)
}

func (s *histImportSuite) TestApplyNonStart_Success_CreateBranch() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	ms := s.successfulPersistMutableState()
	task := s.newTask(s.nonStartEvents())

	// createNewBranch=true -> Create; DoContinue=true, BranchIndex=1 (not 0) -> proceed.
	s.mockBranchMgr.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, int32(1), nil)
	s.mockConflictResolver.EXPECT().GetOrRebuildMutableState(gomock.Any(), int32(1)).Return(ms, false, nil)
	newMS := historyi.NewMockMutableState(s.controller) // non-nil triggers log branch
	s.mockRebuilder.EXPECT().ApplyEvents(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(newMS, nil)

	token, success, err := s.importer.applyNonStartEventsAndSerialize(
		context.Background(), wfContext, ms, MutableStateInitializationSpec{}, task, true,
	)
	s.True(success)
	s.NoError(err)
	s.NotNil(token)
}

// ---- persistHistoryAndSerializeMutableState ----

func (s *histImportSuite) TestPersist_CloseTransactionError() {
	ms := historyi.NewMockMutableState(s.controller)
	ms.EXPECT().CloseTransactionAsSnapshot(gomock.Any(), historyi.TransactionPolicyPassive).Return(nil, nil, serviceerror.NewInternal("close err"))

	_, err := s.importer.persistHistoryAndSerializeMutableState(context.Background(), ms, MutableStateInitializationSpec{})
	s.Error(err)
}

func (s *histImportSuite) TestPersist_PersistEventsError() {
	ms := historyi.NewMockMutableState(s.controller)
	// non-empty first-event batch with a bad branch token -> PersistWorkflowEvents fails.
	ms.EXPECT().CloseTransactionAsSnapshot(gomock.Any(), historyi.TransactionPolicyPassive).Return(
		&persistence.WorkflowSnapshot{ExecutionInfo: &persistencespb.WorkflowExecutionInfo{}, ExecutionState: &persistencespb.WorkflowExecutionState{}},
		[]*persistence.WorkflowEvents{
			{
				NamespaceID: tests.NamespaceID.String(),
				WorkflowID:  tests.WorkflowID,
				RunID:       tests.RunID,
				BranchToken: []byte("not-a-valid-branch-token"),
				Events:      []*historypb.HistoryEvent{{EventId: 1}},
			},
		},
		nil,
	)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockShard.Resource.ExecutionMgr.EXPECT().AppendHistoryNodes(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewInternal("append err")).AnyTimes()

	_, err := s.importer.persistHistoryAndSerializeMutableState(context.Background(), ms, MutableStateInitializationSpec{})
	s.Error(err)
}

// ---- commit ----

func (s *histImportSuite) TestCommit_BrandNew() {
	wf := s.newMockWorkflow(historyi.NewMockWorkflowContext(s.controller), historyi.NewMockMutableState(s.controller))
	err := s.importer.commit(context.Background(), wf, MutableStateInitializationSpec{IsBrandNew: true})
	var invalidArg *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArg)
}

func (s *histImportSuite) TestCommit_NotExistsInDB_RefreshError() {
	ms := historyi.NewMockMutableState(s.controller)
	wf := s.newMockWorkflow(historyi.NewMockWorkflowContext(s.controller), ms)

	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), ms, false).Return(serviceerror.NewInternal("refresh err"))

	err := s.importer.commit(context.Background(), wf, MutableStateInitializationSpec{ExistsInDB: false})
	s.Error(err)
}

func (s *histImportSuite) TestCommit_NotExistsInDB_CreateError() {
	ms := historyi.NewMockMutableState(s.controller)
	wf := s.newMockWorkflow(historyi.NewMockWorkflowContext(s.controller), ms)

	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), ms, false).Return(nil)
	ms.EXPECT().GetUpdateCondition().Return(int64(3), int64(0))
	ms.EXPECT().SetUpdateCondition(int64(3), int64(7))
	s.mockTxMgr.EXPECT().CreateWorkflow(gomock.Any(), gomock.Any(), wf).Return(serviceerror.NewInternal("create err"))

	err := s.importer.commit(context.Background(), wf, MutableStateInitializationSpec{ExistsInDB: false, DBRecordVersion: 7})
	s.Error(err)
}

func (s *histImportSuite) TestCommit_NotExistsInDB_Success() {
	ms := historyi.NewMockMutableState(s.controller)
	wf := s.newMockWorkflow(historyi.NewMockWorkflowContext(s.controller), ms)

	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), ms, false).Return(nil)
	ms.EXPECT().GetUpdateCondition().Return(int64(3), int64(0))
	ms.EXPECT().SetUpdateCondition(int64(3), int64(7))
	s.mockTxMgr.EXPECT().CreateWorkflow(gomock.Any(), gomock.Any(), wf).Return(nil)

	err := s.importer.commit(context.Background(), wf, MutableStateInitializationSpec{ExistsInDB: false, DBRecordVersion: 7})
	s.NoError(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_LoadWorkflowError() {
	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey)
	wf := s.newMockWorkflow(wfContext, historyi.NewMockMutableState(s.controller))

	s.mockTxMgr.EXPECT().LoadWorkflow(gomock.Any(), tests.NamespaceID, tests.WorkflowID, tests.RunID, gomock.Any()).Return(nil, serviceerror.NewInternal("load err"))

	err := s.importer.commit(context.Background(), wf, MutableStateInitializationSpec{ExistsInDB: true})
	s.Error(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_Dedup() {
	// mem and db version histories equal -> CompareVersionHistory == 0 -> dedup, no write.
	memVH := s.versionHistories(int64(5), int64(100))
	dbVH := s.versionHistories(int64(5), int64(100))

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true})
	s.NoError(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_MemBeforeDB() {
	// mem branch is an ancestor of db branch -> CompareVersionHistory < 0 -> add (no switch).
	memVH := s.versionHistories(int64(5), int64(100))
	dbVH := s.versionHistories(int64(8), int64(100))

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	// the cmpResult < 0 branch reads history size from mem and db mutable states,
	// adds the diff, and writes the DB mutable state.
	wf.ms.EXPECT().GetHistorySize().Return(int64(20))
	dbWf.ms.EXPECT().AddHistorySize(int64(20 - 4))
	dbWf.wfContext.EXPECT().SetWorkflowExecution(gomock.Any(), s.mockShard).Return(nil)

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true, DBHistorySize: 4})
	s.NoError(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_MemAfterDB() {
	// db branch is an ancestor of mem branch -> CompareVersionHistory > 0 -> update DB.
	memVH := s.versionHistories(int64(8), int64(100))
	dbVH := s.versionHistories(int64(5), int64(100))

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	dbWf.wfContext.EXPECT().Clear()
	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), wf.ms, false).Return(nil)
	wf.ms.EXPECT().GetUpdateCondition().Return(int64(9), int64(0))
	wf.ms.EXPECT().SetUpdateCondition(int64(9), int64(11))
	s.mockTxMgr.EXPECT().UpdateWorkflow(gomock.Any(), true, gomock.Any(), wf.wf, nil).Return(nil)

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true, DBRecordVersion: 11})
	s.NoError(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_DBVersionHistoryError() {
	// db version histories empty -> GetCurrentVersionHistory(db) errors.
	memVH := s.versionHistories(int64(5), int64(100))
	dbVH := &historyspb.VersionHistories{}

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true})
	s.Error(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_MemVersionHistoryError() {
	// mem version histories empty -> GetCurrentVersionHistory(mem) errors.
	memVH := &historyspb.VersionHistories{}
	dbVH := s.versionHistories(int64(5), int64(100))

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true})
	s.Error(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_CompareError() {
	// Different branch versions that are not comparable (divergent) -> CompareVersionHistory errors.
	memVH := s.versionHistories(int64(5), int64(100))
	dbVH := s.versionHistories(int64(5), int64(200))

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true})
	s.Error(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_MemBeforeDB_SetWorkflowExecutionError() {
	// cmpResult < 0 with SetWorkflowExecution returning an error (logged, still returns nil).
	memVH := s.versionHistories(int64(5), int64(100))
	dbVH := s.versionHistories(int64(8), int64(100))

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	wf.ms.EXPECT().GetHistorySize().Return(int64(20))
	dbWf.ms.EXPECT().AddHistorySize(int64(20 - 4))
	dbWf.wfContext.EXPECT().SetWorkflowExecution(gomock.Any(), s.mockShard).Return(serviceerror.NewInternal("set err"))

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true, DBHistorySize: 4})
	s.NoError(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_MemAfterDB_RefreshError() {
	memVH := s.versionHistories(int64(8), int64(100))
	dbVH := s.versionHistories(int64(5), int64(100))

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	dbWf.wfContext.EXPECT().Clear()
	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), wf.ms, false).Return(serviceerror.NewInternal("refresh err"))

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true})
	s.Error(err)
}

func (s *histImportSuite) TestCommit_ExistsInDB_MemAfterDB_UpdateError() {
	memVH := s.versionHistories(int64(8), int64(100))
	dbVH := s.versionHistories(int64(5), int64(100))

	wf, dbWf := s.setupCommitExistsInDB(memVH, dbVH)
	dbWf.releaseFn.expect()

	dbWf.wfContext.EXPECT().Clear()
	s.mockTaskRefresher.EXPECT().Refresh(gomock.Any(), wf.ms, false).Return(nil)
	wf.ms.EXPECT().GetUpdateCondition().Return(int64(9), int64(0))
	wf.ms.EXPECT().SetUpdateCondition(int64(9), int64(11))
	s.mockTxMgr.EXPECT().UpdateWorkflow(gomock.Any(), true, gomock.Any(), wf.wf, nil).Return(serviceerror.NewInternal("update err"))

	err := s.importer.commit(context.Background(), wf.wf, MutableStateInitializationSpec{ExistsInDB: true, DBRecordVersion: 11})
	s.Error(err)
}

// ---- commit test helpers ----

type commitWf struct {
	wf        *MockWorkflow
	wfContext *historyi.MockWorkflowContext
	ms        *historyi.MockMutableState
	releaseFn *recordingReleaseFn
}

// setupCommitExistsInDB wires mem workflow + db workflow for the ExistsInDB commit path.
func (s *histImportSuite) setupCommitExistsInDB(
	memVH *historyspb.VersionHistories,
	dbVH *historyspb.VersionHistories,
) (*commitWf, *commitWf) {
	memCtx := historyi.NewMockWorkflowContext(s.controller)
	memCtx.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	memMS := historyi.NewMockMutableState(s.controller)
	memMS.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: memVH}).AnyTimes()
	memWf := s.newMockWorkflow(memCtx, memMS)

	dbCtx := historyi.NewMockWorkflowContext(s.controller)
	dbMS := historyi.NewMockMutableState(s.controller)
	dbMS.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: dbVH}).AnyTimes()
	dbWf := NewMockWorkflow(s.controller)
	dbWf.EXPECT().GetContext().Return(dbCtx).AnyTimes()
	dbWf.EXPECT().GetMutableState().Return(dbMS).AnyTimes()
	rec := &recordingReleaseFn{}
	dbWf.EXPECT().GetReleaseFn().Return(rec.fn).AnyTimes()

	s.mockTxMgr.EXPECT().LoadWorkflow(gomock.Any(), tests.NamespaceID, tests.WorkflowID, tests.RunID, gomock.Any()).Return(dbWf, nil)

	return &commitWf{wf: memWf, wfContext: memCtx, ms: memMS},
		&commitWf{wf: dbWf, wfContext: dbCtx, ms: dbMS, releaseFn: rec}
}

// versionHistories builds a single-branch VersionHistories with a tail item.
func (s *histImportSuite) versionHistories(eventID int64, version int64) *historyspb.VersionHistories {
	return versionhistory.NewVersionHistories(
		versionhistory.NewVersionHistory(
			[]byte("branch-token"),
			[]*historyspb.VersionHistoryItem{
				versionhistory.NewVersionHistoryItem(eventID, version),
			},
		),
	)
}

// ---- recordingReleaseFn ----

type recordingReleaseFn struct {
	called bool
	err    error
}

func (r *recordingReleaseFn) fn(err error) {
	r.called = true
	r.err = err
}

func (r *recordingReleaseFn) expect() {
	// no-op marker for readability; release is asserted by gomock AnyTimes wiring.
}

// ---- Initialize wiring helpers ----

// expectInitializeFromDBNotFound wires the DB-not-found Initialize path which yields
// a brand-new spec (IsBrandNew=true, ExistsInDB=false).
func (s *histImportSuite) expectInitializeFromDBNotFound() (*historyi.MockWorkflowContext, *recordingReleaseFn) {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil)

	wfContext := historyi.NewMockWorkflowContext(s.controller)
	wfContext.EXPECT().Clear().AnyTimes()
	wfContext.EXPECT().GetWorkflowKey().Return(tests.WorkflowKey).AnyTimes()
	rec := &recordingReleaseFn{}
	s.mockWorkflowCache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(), s.mockShard, tests.NamespaceID, gomock.Any(), gomock.Any(),
	).Return(wfContext, historyi.ReleaseWorkflowContextFunc(rec.fn), nil)
	wfContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(nil, serviceerror.NewNotFound("not found"))
	return wfContext, rec
}

// expectInitializeFromTokenSucceeds wires the token-based Initialize path.
func (s *histImportSuite) expectInitializeFromTokenSucceeds(existsInDB bool) {
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil)
}

// buildToken constructs a serialized backfill token via the real initializer serializer.
func (s *histImportSuite) buildToken(_ bool) []byte {
	row := s.minimalMutableStateRow()
	token, err := s.importer.mutableStateInitializer.serializeBackfillToken(row, 1, 0, false)
	s.NoError(err)
	return token
}

// minimalMutableStateRow returns the minimal persistence row needed for
// NewMutableStateFromDB to succeed during Initialize-from-token.
func (s *histImportSuite) minimalMutableStateRow() *persistencespb.WorkflowMutableState {
	return &persistencespb.WorkflowMutableState{
		ActivityInfos:       map[int64]*persistencespb.ActivityInfo{},
		TimerInfos:          map[string]*persistencespb.TimerInfo{},
		ChildExecutionInfos: map[int64]*persistencespb.ChildExecutionInfo{},
		RequestCancelInfos:  map[int64]*persistencespb.RequestCancelInfo{},
		SignalInfos:         map[int64]*persistencespb.SignalInfo{},
		SignalRequestedIds:  []string{},
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:                     tests.NamespaceID.String(),
			WorkflowId:                      tests.WorkflowID,
			VersionHistories:                s.versionHistories(int64(3), int64(100)),
			WorkflowExecutionExpirationTime: nil,
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:           tests.RunID,
			State:           enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
			CreateRequestId: uuid.NewString(),
		},
		NextEventId: 4,
	}
}

var _ = errors.New
var _ = definition.WorkflowKey{}
var _ = commonpb.WorkflowExecution{}
