package ndc

import (
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
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

var errTxnNew = errors.New("txnNew test error")

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

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_BrandNew() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	newWorkflow := NewMockWorkflow(s.controller)
	weContext := historyi.NewMockWorkflowContext(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	var releaseFn historyi.ReleaseWorkflowContextFunc = func(error) { releaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	workflowSnapshot := &persistence.WorkflowSnapshot{}
	workflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + rand.Int63(),
		}},
	}}
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().CloseTransactionAsSnapshot(context.Background(), historyi.TransactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(
		ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID,
	).Return("", nil)

	weContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
		mutableState,
		workflowSnapshot,
		workflowEventsSeq,
		gomock.Any(),
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, newWorkflow)
	s.NoError(err)
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

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_GetCurrentRunIDError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	targetWorkflow := NewMockWorkflow(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", errTxnNew)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.Equal(errTxnNew, err)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_LoadWorkflowError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetWorkflow := NewMockWorkflow(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(nil, errTxnNew)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.Equal(errTxnNew, err)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_HappensAfterError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetWorkflow := NewMockWorkflow(s.controller)
	mutableState := historyi.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)
	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, errTxnNew)

	err := s.createMgr.dispatchForNewWorkflow(ctx, chasm.WorkflowArchetypeID, targetWorkflow)
	s.Equal(errTxnNew, err)
}

func (s *transactionMgrForNewWorkflowSuite) TestExecuteTransaction_UnknownPolicy() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicy(-1), nil, targetWorkflow)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestCreateAsCurrent_CloseTransactionError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	targetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive).Return(nil, nil, errTxnNew)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsCurrent, nil, targetWorkflow)
	s.Equal(errTxnNew, err)
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestCreateAsCurrent_GetVectorClockError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: "current run ID",
	}).AnyTimes()

	targetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive).Return(
		&persistence.WorkflowSnapshot{}, []*persistence.WorkflowEvents{}, nil,
	)
	currentWorkflow.EXPECT().GetVectorClock().Return(int64(0), int64(0), errTxnNew)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsCurrent, currentWorkflow, targetWorkflow)
	s.Equal(errTxnNew, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestCreateAsZombie_SuppressByError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyActive, errTxnNew)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsZombie, currentWorkflow, targetWorkflow)
	s.Equal(errTxnNew, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestCreateAsZombie_PolicyNotPassive() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	// bypassVersionSemanticsCheck is false in suite, so active policy triggers internal error
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyActive, nil)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsZombie, currentWorkflow, targetWorkflow)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestCreateAsZombie_CloseTransactionError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive).Return(nil, nil, errTxnNew)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsZombie, currentWorkflow, targetWorkflow)
	s.Equal(errTxnNew, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestCreateAsZombie_CreateError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{EventId: common.FirstEventID + rand.Int63()}},
	}}
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, targetWorkflowEventsSeq).Return(nil)
	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(), s.mockShard, persistence.CreateWorkflowModeBypassCurrent, "", int64(0),
		targetMutableState, targetWorkflowSnapshot, targetWorkflowEventsSeq, gomock.Any(),
	).Return(errTxnNew)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsZombie, currentWorkflow, targetWorkflow)
	s.Equal(errTxnNew, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestExecuteTransaction_PanicRecover() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	targetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive).DoAndReturn(
		func(context.Context, historyi.TransactionPolicy) (*persistence.WorkflowSnapshot, []*persistence.WorkflowEvents, error) {
			panic("txnNew panic")
		},
	)

	s.Panics(func() {
		_ = s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsCurrent, nil, targetWorkflow)
	})
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestCreateAsZombie_ReapplyEventsError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{{
		Events: []*historypb.HistoryEvent{{EventId: common.FirstEventID + rand.Int63()}},
	}}
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(nil)
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive).Return(
		&persistence.WorkflowSnapshot{}, targetWorkflowEventsSeq, nil,
	)

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, targetWorkflowEventsSeq).Return(errTxnNew)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsZombie, currentWorkflow, targetWorkflow)
	s.Equal(errTxnNew, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestCreateAsZombie_ReapplyCandidatesError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(ctx, historyi.TransactionPolicyPassive).Return(
		&persistence.WorkflowSnapshot{}, []*persistence.WorkflowEvents{}, nil,
	)

	eventReapplyCandidates := []*historypb.HistoryEvent{{EventId: common.FirstEventID + rand.Int63()}}
	eventsToApply := []*persistence.WorkflowEvents{
		{
			NamespaceID: namespaceID.String(),
			WorkflowID:  workflowID,
			RunID:       targetRunID,
			Events:      eventReapplyCandidates,
		},
	}
	targetMutableState.EXPECT().GetReapplyCandidateEvents().Return(eventReapplyCandidates)

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, eventsToApply).Return(errTxnNew)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicyCreateAsZombie, currentWorkflow, targetWorkflow)
	s.Equal(errTxnNew, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestSuppressCurrentAndCreateAsCurrent_SuppressByError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, errTxnNew)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicySuppressCurrentAndCreateAsCurrent, currentWorkflow, targetWorkflow)
	s.Equal(errTxnNew, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForNewWorkflowSuite) TestSuppressCurrentAndCreateAsCurrent_ReviveError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()
	targetWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(errTxnNew)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	err := s.createMgr.executeTransaction(ctx, nDCTransactionPolicySuppressCurrentAndCreateAsCurrent, currentWorkflow, targetWorkflow)
	s.Equal(errTxnNew, err)
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
