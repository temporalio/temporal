package ndc

import (
	"context"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/testhooks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

type (
	transactionMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.ContextTest
		mockCreateMgr        *MocktransactionMgrForNewWorkflow
		mockUpdateMgr        *MocktransactionMgrForExistingWorkflow
		mockEventsReapplier  *MockEventsReapplier
		mockWorkflowResetter *MockWorkflowResetter
		mockClusterMetadata  *cluster.MockMetadata

		mockExecutionMgr *persistence.MockExecutionManager

		logger         log.Logger
		namespaceEntry *namespace.Namespace

		transactionMgr *transactionMgrImpl
	}
)

func TestTransactionMgrSuite(t *testing.T) {
	s := new(transactionMgrSuite)
	suite.Run(t, s)
}

func (s *transactionMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockCreateMgr = NewMocktransactionMgrForNewWorkflow(s.controller)
	s.mockUpdateMgr = NewMocktransactionMgrForExistingWorkflow(s.controller)
	s.mockEventsReapplier = NewMockEventsReapplier(s.controller)
	s.mockWorkflowResetter = NewMockWorkflowResetter(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr

	s.logger = s.mockShard.GetLogger()
	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(s.namespaceEntry.Name(), nil).AnyTimes()

	s.transactionMgr = NewTransactionManager(
		s.mockShard,
		wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler, testhooks.TestHooks{}),
		s.mockEventsReapplier,
		s.logger,
		false,
	)
	s.transactionMgr.createMgr = s.mockCreateMgr
	s.transactionMgr.updateMgr = s.mockUpdateMgr
	s.transactionMgr.workflowResetter = s.mockWorkflowResetter
}

func (s *transactionMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *transactionMgrSuite) TestCreateWorkflow() {
	ctx := context.Background()
	targetWorkflow := NewMockWorkflow(s.controller)

	s.mockCreateMgr.EXPECT().dispatchForNewWorkflow(
		ctx, chasm.WorkflowArchetypeID, targetWorkflow,
	).Return(nil)

	err := s.transactionMgr.CreateWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.NoError(err)
}

func (s *transactionMgrSuite) TestUpdateWorkflow() {
	ctx := context.Background()
	isWorkflowRebuilt := true
	targetWorkflow := NewMockWorkflow(s.controller)
	newWorkflow := NewMockWorkflow(s.controller)

	s.mockUpdateMgr.EXPECT().dispatchForExistingWorkflow(
		ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow,
	).Return(nil)

	err := s.transactionMgr.UpdateWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Active_Open() {
	ctx := context.Background()
	releaseCalled := false
	runID := uuid.NewString()

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	mutableState.EXPECT().VisitUpdates(gomock.Any()).Return()
	mutableState.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(mutableState)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 1}},
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	workflowID := "some random workflow ID"
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockEventsReapplier.EXPECT().ReapplyEvents(ctx, mutableState, updateRegistry, workflowEvents.Events, runID).Return(workflowEvents.Events, nil)

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: runID})
	mutableState.EXPECT().AddHistorySize(historySize)
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: s.namespaceEntry.ID().String(),
		WorkflowId:  workflowID,
	})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyActive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)
	weContext.EXPECT().UpdateRegistry(ctx).Return(updateRegistry)
	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Active_Closed() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	LastCompletedWorkflowTaskStartedEventID := int64(9999)
	nextEventID := LastCompletedWorkflowTaskStartedEventID * 2
	lastWorkflowTaskStartedVersion := s.namespaceEntry.FailoverVersion(workflowID)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: LastCompletedWorkflowTaskStartedEventID, Version: lastWorkflowTaskStartedVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)
	histroySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}},
	}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(LastCompletedWorkflowTaskStartedEventID)
	mutableState.EXPECT().AddHistorySize(histroySize)

	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		LastCompletedWorkflowTaskStartedEventID,
		lastWorkflowTaskStartedVersion,
		nextEventID,
		gomock.Any(),
		gomock.Not(""),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false, // allowResetWithPendingChildren
		nil,   // post reset operations

	).Return(nil)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(histroySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_ResetFailed() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	LastCompletedWorkflowTaskStartedEventID := int64(9999)
	nextEventID := LastCompletedWorkflowTaskStartedEventID * 2
	lastWorkflowTaskStartedVersion := s.namespaceEntry.FailoverVersion(workflowID)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: LastCompletedWorkflowTaskStartedEventID, Version: lastWorkflowTaskStartedVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}},
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(LastCompletedWorkflowTaskStartedEventID)
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		LastCompletedWorkflowTaskStartedEventID,
		lastWorkflowTaskStartedVersion,
		nextEventID,
		gomock.Any(),
		gomock.Not(""),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false, // allowResetWithPendingChildren
		nil,   // post reset operations
	).Return(serviceerror.NewInvalidArgument("reset fail"))

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_ResetError() {
	// When ResetWorkflow fails with a non-InvalidArgument error, the reapply is not swallowed:
	// BackfillWorkflow propagates the error so the replication task is retried.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	LastCompletedWorkflowTaskStartedEventID := int64(9999)
	nextEventID := LastCompletedWorkflowTaskStartedEventID * 2
	lastWorkflowTaskStartedVersion := s.namespaceEntry.FailoverVersion(workflowID)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: LastCompletedWorkflowTaskStartedEventID, Version: lastWorkflowTaskStartedVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}},
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(LastCompletedWorkflowTaskStartedEventID)
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		LastCompletedWorkflowTaskStartedEventID,
		lastWorkflowTaskStartedVersion,
		nextEventID,
		gomock.Any(),
		gomock.Not(""),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false, // allowResetWithPendingChildren
		nil,   // post reset operations
	).Return(serviceerror.NewInternal("reset boom"))

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	// UpdateWorkflowExecutionWithNew must not be invoked: the reset error is propagated first.
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.Error(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_FailedWorkflowTask() {
	// No completed or pending workflow task: the reset is anchored at the last failed workflow task in history.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	nextEventID := int64(9)
	version := s.namespaceEntry.FailoverVersion(workflowID)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: nextEventID - 1, Version: version},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)
	historySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}},
	}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(common.EmptyEventID)
	mutableState.EXPECT().GetPendingWorkflowTask().Return(nil).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	// The second workflow task was failed when the workflow was terminated.
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		ShardID:     s.mockShard.GetShardID(),
		BranchToken: versionHistory.GetBranchToken(),
		MinEventID:  common.FirstEventID,
		MaxEventID:  nextEventID,
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
		{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
		{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED},
		{EventId: 5, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
		{EventId: 6, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
		{EventId: 7, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED},
		{EventId: 8, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED},
	}}, nil)

	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		int64(6),
		version,
		nextEventID,
		gomock.Any(),
		gomock.Not(""),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false, // allowResetWithPendingChildren
		nil,   // post reset operations
	).Return(nil)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_NoWorkflowTask() {
	// Closed before any workflow task: there is nothing to reset to, so the events are not reapplied.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	nextEventID := int64(3)
	version := s.namespaceEntry.FailoverVersion(workflowID)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: nextEventID - 1, Version: version},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)
	historySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}},
	}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(common.EmptyEventID)
	mutableState.EXPECT().GetPendingWorkflowTask().Return(nil).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED},
	}}, nil)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_NothingToReapply() {
	// Both clusters close out the in-flight workflow task on failover, so the losing branch can
	// hold only workflow task events. Resetting would reopen the closed workflow for nothing.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	historySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 5, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED}},
	}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_PendingWorkflowTask() {
	// A closed current workflow that never completed a workflow task but still has a real,
	// pending (scheduled, never started) workflow task is anchored at that scheduled task, so
	// the resetter can rebuild to it and reapply the events onto a new run.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	scheduledEventID := int64(2)
	nextEventID := int64(4)
	scheduledEventVersion := s.namespaceEntry.FailoverVersion(workflowID)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: scheduledEventID, Version: scheduledEventVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)
	historySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}},
	}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	// No completed workflow task, but a real scheduled (never started) one survives the close.
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(common.EmptyEventID)
	mutableState.EXPECT().IsTransientWorkflowTask().Return(false).AnyTimes()
	mutableState.EXPECT().GetPendingWorkflowTask().Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: scheduledEventID,
		StartedEventID:   common.EmptyEventID,
		Type:             enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	// Reset must be anchored at the scheduled workflow task's event.
	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		scheduledEventID,
		scheduledEventVersion,
		nextEventID,
		gomock.Any(),
		gomock.Not(""),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false, // allowResetWithPendingChildren
		nil,   // post reset operations
	).Return(nil)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_StartedPendingWorkflowTask() {
	// A closed current workflow that never completed a workflow task but has a real pending
	// workflow task that was already started is anchored at its scheduled event (not the started
	// one): the resetter rebuilds to that workflow task and fails it, so the scheduled event is a
	// sufficient anchor whether or not the task already started.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	scheduledEventID := int64(2)
	startedEventID := int64(3)
	nextEventID := int64(4)
	scheduledEventVersion := s.namespaceEntry.FailoverVersion(workflowID)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: startedEventID, Version: scheduledEventVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)
	historySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}},
	}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	// No completed workflow task, but a real pending task that was already started survives the close.
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(common.EmptyEventID)
	mutableState.EXPECT().IsTransientWorkflowTask().Return(false).AnyTimes()
	mutableState.EXPECT().GetPendingWorkflowTask().Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: scheduledEventID,
		StartedEventID:   startedEventID,
		Type:             enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	// Reset must be anchored at the scheduled workflow task's event even though the task started.
	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		scheduledEventID,
		scheduledEventVersion,
		nextEventID,
		gomock.Any(),
		gomock.Not(""),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false, // allowResetWithPendingChildren
		nil,   // post reset operations
	).Return(nil)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_TransientWorkflowTaskNotUsable() {
	// A transient pending workflow task has no persisted WorkflowTaskScheduled event, so the reset is
	// anchored at the last timed out workflow task in history instead.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	nextEventID := int64(5)
	version := s.namespaceEntry.FailoverVersion(workflowID)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: nextEventID - 1, Version: version},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)
	historySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED}},
	}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(common.EmptyEventID)
	mutableState.EXPECT().IsTransientWorkflowTask().Return(true).AnyTimes()
	mutableState.EXPECT().GetPendingWorkflowTask().Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID: nextEventID,
		StartedEventID:   common.EmptyEventID,
		Type:             enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: []*historypb.HistoryEvent{
		{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
		{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
		{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
		{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT},
	}}, nil)

	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		int64(3),
		version,
		nextEventID,
		gomock.Any(),
		gomock.Not(""),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
		false, // allowResetWithPendingChildren
		nil,   // post reset operations
	).Return(nil)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestGetEventsReapplicationResetEventID_MultiplePages() {
	nextEventID := int64(7)
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: nextEventID - 1, Version: s.namespaceEntry.FailoverVersion("some random workflow ID")},
	})
	mutableState := historyi.NewMockMutableState(s.controller)
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(common.EmptyEventID)
	mutableState.EXPECT().GetPendingWorkflowTask().Return(nil)
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionhistory.NewVersionHistories(versionHistory),
	})
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()

	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		ShardID:     s.mockShard.GetShardID(),
		BranchToken: versionHistory.GetBranchToken(),
		MinEventID:  common.FirstEventID,
		MaxEventID:  nextEventID,
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{EventId: 1, EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED},
			{EventId: 2, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
			{EventId: 3, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
			{EventId: 4, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED},
		},
		NextPageToken: []byte("next page"),
	}, nil)
	// The last workflow task timed out before it started, so the reset is anchored at its scheduled event.
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		ShardID:       s.mockShard.GetShardID(),
		BranchToken:   versionHistory.GetBranchToken(),
		MinEventID:    common.FirstEventID,
		MaxEventID:    nextEventID,
		PageSize:      defaultPageSize,
		NextPageToken: []byte("next page"),
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{EventId: 5, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
			{EventId: 6, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT},
		},
	}, nil)

	resetEventID, err := GetEventsReapplicationResetEventID(context.Background(), s.mockShard, mutableState)
	s.NoError(err)
	s.Equal(int64(5), resetEventID)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Passive_Open() {
	ctx := context.Background()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 1}},
	}
	historySize := rand.Int63()

	workflowID := "some random workflow ID"
	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: s.namespaceEntry.ID().String(),
		WorkflowId:  workflowID,
	})
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)
	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Passive_Closed() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_NotCurrentWorkflow_Active() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}},
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)
	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_NotCurrentWorkflow_Passive() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}},
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion(workflowID)).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, historyi.TransactionPolicyPassive, (*historyi.TransactionPolicy)(nil),
	).Return(nil)
	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestCheckWorkflowExists_DoesNotExists() {
	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(nil, serviceerror.NewNotFound(""))

	exists, err := s.transactionMgr.CheckWorkflowExists(ctx, namespaceID, workflowID, runID, chasm.WorkflowArchetypeID)
	s.NoError(err)
	s.False(exists)
}

func (s *transactionMgrSuite) TestCheckWorkflowExists_DoesExists() {
	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetWorkflowExecutionResponse{}, nil)

	exists, err := s.transactionMgr.CheckWorkflowExists(ctx, namespaceID, workflowID, runID, chasm.WorkflowArchetypeID)
	s.NoError(err)
	s.True(exists)
}

func (s *transactionMgrSuite) TestGetWorkflowCurrentRunID_Missing() {
	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(nil, serviceerror.NewNotFound(""))

	currentRunID, err := s.transactionMgr.GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID)
	s.NoError(err)
	s.Empty(currentRunID)
}

func (s *transactionMgrSuite) TestGetWorkflowCurrentRunID_Exists() {
	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	currentRunID, err := s.transactionMgr.GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID)
	s.NoError(err)
	s.Equal(runID, currentRunID)
}
