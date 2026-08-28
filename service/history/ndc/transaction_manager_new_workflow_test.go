package ndc

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type (
	transactionMgrForNewWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockTransactionMgr *MockTransactionManager
		mockShard          *historyi.MockShardContext

		createMgr *nDCTransactionMgrForNewWorkflowImpl
	}
)

func TestTransactionMgrForNewWorkflowSuite(t *testing.T) {
	s := new(transactionMgrForNewWorkflowSuite)
	suite.Run(t, s)
}

func (s *transactionMgrForNewWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTransactionMgr = NewMockTransactionManager(s.controller)
	s.mockShard = historyi.NewMockShardContext(s.controller)

	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), false).Return(nil).AnyTimes()
	s.createMgr = newTransactionMgrForNewWorkflow(s.mockShard, s.mockTransactionMgr, false, mockTaskRefresher)
}

func (s *transactionMgrForNewWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_Dup() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	newWorkflow := NewMockWorkflow(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	newWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()

	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(runID, nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, newWorkflow)
	s.ErrorIs(err, consts.ErrDuplicate)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_NoCurrentRecord_StateAndSuccessorCombinations() {
	successorCases := []struct {
		name              string
		newExecutionRunID string
		successorRunID    string
	}{
		{name: "without successor"},
		{name: "with new execution run ID", newExecutionRunID: "successor"},
		{name: "with successor run ID", successorRunID: "successor"},
	}

	stateCases := []struct {
		name          string
		state         enumsspb.WorkflowExecutionState
		expectedModes [3]persistence.CreateWorkflowMode
	}{
		{
			name:  "created",
			state: enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
			expectedModes: [3]persistence.CreateWorkflowMode{
				persistence.CreateWorkflowModeBrandNew,
				persistence.CreateWorkflowModeBrandNew,
				persistence.CreateWorkflowModeBrandNew,
			},
		},
		{
			name:  "running",
			state: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			expectedModes: [3]persistence.CreateWorkflowMode{
				persistence.CreateWorkflowModeBrandNew,
				persistence.CreateWorkflowModeBrandNew,
				persistence.CreateWorkflowModeBrandNew,
			},
		},
		{
			name:  "completed",
			state: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			expectedModes: [3]persistence.CreateWorkflowMode{
				persistence.CreateWorkflowModeBrandNew,
				persistence.CreateWorkflowModeBypassCurrent,
				persistence.CreateWorkflowModeBypassCurrent,
			},
		},
		{
			name:  "zombie",
			state: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			expectedModes: [3]persistence.CreateWorkflowMode{
				persistence.CreateWorkflowModeBypassCurrent,
				persistence.CreateWorkflowModeBypassCurrent,
				persistence.CreateWorkflowModeBypassCurrent,
			},
		},
		{
			name:  "corrupted",
			state: enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED,
			expectedModes: [3]persistence.CreateWorkflowMode{
				persistence.CreateWorkflowModeBrandNew,
				persistence.CreateWorkflowModeBypassCurrent,
				persistence.CreateWorkflowModeBypassCurrent,
			},
		},
	}

	for _, stateCase := range stateCases {
		for successorIndex, successorCase := range successorCases {
			expectedMode := stateCase.expectedModes[successorIndex]
			s.Run(stateCase.name+" "+successorCase.name, func() {
				ctx := context.Background()
				namespaceID := namespace.ID("some random namespace ID")
				workflowID := "some random workflow ID"
				runID := "some random run ID"
				releaseCalled := false

				targetWorkflow := NewMockWorkflow(s.controller)
				weContext := historyi.NewMockWorkflowContext(s.controller)
				mutableState := historyi.NewMockMutableState(s.controller)
				var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }
				targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
				targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
				targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

				executionInfo := &persistencespb.WorkflowExecutionInfo{
					NamespaceId:       namespaceID.String(),
					WorkflowId:        workflowID,
					NewExecutionRunId: successorCase.newExecutionRunID,
					SuccessorRunId:    successorCase.successorRunID,
				}
				executionState := &persistencespb.WorkflowExecutionState{
					RunId: runID,
					State: stateCase.state,
				}
				workflowSnapshot := &persistence.WorkflowSnapshot{
					ExecutionState: executionState,
				}
				workflowEventsSeq := []*persistence.WorkflowEvents{}

				mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
				mutableState.EXPECT().GetExecutionState().Return(executionState).AnyTimes()
				if expectedMode == persistence.CreateWorkflowModeBypassCurrent {
					mutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)
				}
				mutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive).Return(
					workflowSnapshot, workflowEventsSeq, nil,
				)

				s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(
					ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID,
				).Return("", nil)

				weContext.EXPECT().CreateWorkflowExecution(
					gomock.Any(),
					s.mockShard,
					gomock.Any(),
					"",
					int64(0),
					mutableState,
					workflowSnapshot,
					workflowEventsSeq,
					historyi.TransactionPolicyPassive,
				).DoAndReturn(func(
					_ context.Context,
					_ historyi.ShardContext,
					createMode persistence.CreateWorkflowMode,
					_ string,
					_ int64,
					_ historyi.MutableState,
					workflowSnapshot *persistence.WorkflowSnapshot,
					_ []*persistence.WorkflowEvents,
					_ historyi.TransactionPolicy,
				) error {
					s.Equal(expectedMode, createMode)
					s.Equal(stateCase.state, workflowSnapshot.ExecutionState.State)
					return persistence.ValidateCreateWorkflowModeState(createMode, *workflowSnapshot)
				})

				err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
				s.Require().NoError(err)
				s.Equal(stateCase.state, executionState.State)
				s.True(releaseCalled)
			})
		}
	}
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_NoCurrentRecord_CompletedWithNewExecutionRunID_CreatesBypassCurrentPreservingState() {
	s.testDispatchForNewWorkflowNoCurrentRecordPreservesState(&persistencespb.WorkflowExecutionInfo{
		NewExecutionRunId: "successor run ID",
	}, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_NoCurrentRecord_CompletedWithSuccessorRunID_CreatesBypassCurrentPreservingState() {
	s.testDispatchForNewWorkflowNoCurrentRecordPreservesState(&persistencespb.WorkflowExecutionInfo{
		SuccessorRunId: "successor run ID",
	}, enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_NoCurrentRecord_CorruptedWithSuccessorRunID_CreatesBypassCurrentPreservingState() {
	s.testDispatchForNewWorkflowNoCurrentRecordPreservesState(&persistencespb.WorkflowExecutionInfo{
		SuccessorRunId: "successor run ID",
	}, enumsspb.WORKFLOW_EXECUTION_STATE_CORRUPTED)
}

func (s *transactionMgrForNewWorkflowSuite) testDispatchForNewWorkflowNoCurrentRecordPreservesState(
	executionInfo *persistencespb.WorkflowExecutionInfo,
	executionStateValue enumsspb.WorkflowExecutionState,
) {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	executionInfo.NamespaceId = namespaceID.String()
	executionInfo.WorkflowId = workflowID

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	executionState := &persistencespb.WorkflowExecutionState{
		RunId: runID,
		State: executionStateValue,
	}
	workflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: executionState,
	}
	// Non-empty event sequence so the reapply branch of createBypassCurrent is exercised (rather than
	// short-circuited by empty lists). On the passive apply path reapply is forwarded to the active
	// cluster; here the mock stands in for a successful reapply.
	workflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	mutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(executionState).AnyTimes()
	mutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)
	mutableState.EXPECT().CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(
		ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID,
	).Return("", nil)

	// A non-current run with a successor and no current record must not resurrect as current: even
	// with events to reapply, it is persisted via bypass-current without touching the (absent)
	// current record. SuppressBy is never called since there is no current workflow to suppress against.
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, workflowEventsSeq).Return(nil)
	weContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		mutableState,
		workflowSnapshot,
		workflowEventsSeq,
		historyi.TransactionPolicyPassive,
	).DoAndReturn(func(
		_ context.Context,
		_ historyi.ShardContext,
		createMode persistence.CreateWorkflowMode,
		_ string,
		_ int64,
		_ historyi.MutableState,
		workflowSnapshot *persistence.WorkflowSnapshot,
		_ []*persistence.WorkflowEvents,
		_ historyi.TransactionPolicy,
	) error {
		s.Equal(persistence.CreateWorkflowModeBypassCurrent, createMode)
		s.Equal(executionStateValue, workflowSnapshot.ExecutionState.State)
		return persistence.ValidateCreateWorkflowModeState(createMode, *workflowSnapshot)
	})

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.NoError(err)
	s.Equal(executionStateValue, executionState.State)
	s.True(releaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_NoCurrentRecord_ZombieWithoutSuccessor_CreatesBypassCurrentPreservingState() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	executionState := &persistencespb.WorkflowExecutionState{
		RunId: runID,
		State: enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
	}
	workflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionState: executionState,
	}
	workflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(executionState).AnyTimes()
	mutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)
	mutableState.EXPECT().CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(
		ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID,
	).Return("", nil)

	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, workflowEventsSeq).Return(nil)
	weContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		gomock.Any(),
		"",
		int64(0),
		mutableState,
		workflowSnapshot,
		workflowEventsSeq,
		gomock.Any(),
	).DoAndReturn(func(
		_ context.Context,
		_ historyi.ShardContext,
		createMode persistence.CreateWorkflowMode,
		_ string,
		_ int64,
		_ historyi.MutableState,
		workflowSnapshot *persistence.WorkflowSnapshot,
		_ []*persistence.WorkflowEvents,
		_ historyi.TransactionPolicy,
	) error {
		s.Equal(persistence.CreateWorkflowModeBypassCurrent, createMode)
		s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, workflowSnapshot.ExecutionState.State)
		return persistence.ValidateCreateWorkflowModeState(createMode, *workflowSnapshot)
	})

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.Require().NoError(err)
	s.Equal(enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, executionState.State)
	s.True(releaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsCurrent() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"
	currentLastWriteVersion := int64(4321)

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: currentRunID,
	}).AnyTimes()
	currentWorkflow.EXPECT().GetVectorClock().Return(currentLastWriteVersion, int64(0), nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentRunID,
		currentLastWriteVersion,
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
		gomock.Any(),
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID.String(),
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
		gomock.Any(),
	).Return(nil)
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, targetWorkflowEventsSeq).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie_ReapplyCandidates() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID.String(),
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{}

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	eventReapplyCandidates := []*historypb.HistoryEvent{{
		EventId: common.FirstEventID + rand.Int63(),
	}}
	eventsToApply := []*persistence.WorkflowEvents{
		{
			NamespaceID: namespaceID.String(),
			WorkflowID:  workflowID,
			RunID:       targetRunID,
			Events:      eventReapplyCandidates,
		},
	}
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(eventReapplyCandidates)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
		gomock.Any(),
	).Return(nil)
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, eventsToApply).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie_Dedup() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID.String(),
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
		gomock.Any(),
	).Return(&persistence.WorkflowConditionFailedError{})
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, targetWorkflowEventsSeq).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_SuppressCurrentAndCreateAsCurrent() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflowPolicy := historyi.TransactionPolicyActive
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil)
	targetWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

	currentContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetContext,
		targetMutableState,
		currentWorkflowPolicy,
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}
