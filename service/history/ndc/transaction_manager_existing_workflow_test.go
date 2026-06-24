package ndc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

var errTxnExist = errors.New("txnExist test error")

type (
	transactionMgrForExistingWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockTransactionMgr *MockTransactionManager
		mockShard          *historyi.MockShardContext

		updateMgr *nDCTransactionMgrForExistingWorkflowImpl
	}
)

func TestTransactionMgrForExistingWorkflowSuite(t *testing.T) {
	s := new(transactionMgrForExistingWorkflowSuite)
	suite.Run(t, s)
}

func (s *transactionMgrForExistingWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTransactionMgr = NewMockTransactionManager(s.controller)
	s.mockShard = historyi.NewMockShardContext(s.controller)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.EXPECT().StateMachineRegistry().Return(reg).AnyTimes()

	mockTaskRefresher := workflow.NewMockTaskRefresher(s.controller)
	mockTaskRefresher.EXPECT().Refresh(gomock.Any(), gomock.Any(), false).Return(nil).AnyTimes()
	s.updateMgr = newNDCTransactionMgrForExistingWorkflow(s.mockShard, s.mockTransactionMgr, false, mockTaskRefresher)
}

func (s *transactionMgrForExistingWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowGuaranteed() {
	ctx := context.Background()

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()

	targetContext.EXPECT().UpdateWorkflowExecutionWithNewAsPassive(
		gomock.Any(),
		s.mockShard,
		newContext,
		newMutableState,
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentRunning_UpdateAsCurrent() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
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
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	targetWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentComplete_UpdateAsCurrent() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
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
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, nil).Times(0)
	targetWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie_NewRunDoesNotExists() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(false, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie_NewRunDoesExists() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(true, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_IsCurrent() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(targetRunID, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsCurrent() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

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
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyActive, nil)
	targetWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		historyi.TransactionPolicyActive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie_NewRunDoesNotExists() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(false, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_CurrentRunIDEmpty() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	targetWorkflow := NewMockWorkflow(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, true, chasm.WorkflowArchetypeID, targetWorkflow, nil)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_GetCurrentRunIDError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	targetWorkflow := NewMockWorkflow(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", errTxnExist)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, true, chasm.WorkflowArchetypeID, targetWorkflow, nil)
	s.Equal(errTxnExist, err)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_LoadWorkflowError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetWorkflow := NewMockWorkflow(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(nil, errTxnExist)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, true, chasm.WorkflowArchetypeID, targetWorkflow, nil)
	s.Equal(errTxnExist, err)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_HappensAfterError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetWorkflow := NewMockWorkflow(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)
	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, errTxnExist)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, true, chasm.WorkflowArchetypeID, targetWorkflow, nil)
	s.Equal(errTxnExist, err)
}

func (s *transactionMgrForExistingWorkflowSuite) TestExecuteTransaction_UnknownPolicy() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicy(-1), nil, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestExecuteTransaction_PanicRecover() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	targetContext.EXPECT().UpdateWorkflowExecutionAsPassive(gomock.Any(), s.mockShard).DoAndReturn(
		func(context.Context, historyi.ShardContext) error {
			panic("txnExist panic")
		},
	)

	s.Panics(func() {
		_ = s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyUpdateAsCurrent, nil, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	})
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestUpdateAsCurrent_NoNewWorkflow() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	targetContext.EXPECT().UpdateWorkflowExecutionAsPassive(gomock.Any(), s.mockShard).Return(nil)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyUpdateAsCurrent, nil, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.NoError(err)
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestUpdateAsZombie_TargetSuppressByError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, errTxnExist)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyUpdateAsZombie, currentWorkflow, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestUpdateAsZombie_TargetPolicyNotPassive() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyActive, nil)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyUpdateAsZombie, currentWorkflow, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestUpdateAsZombie_NewSuppressByError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newReleaseCalled := false
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, errTxnExist)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyUpdateAsZombie, currentWorkflow, targetWorkflow, newWorkflow, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestUpdateAsZombie_NewPolicyNotPassive() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newReleaseCalled := false
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyActive, nil)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyUpdateAsZombie, currentWorkflow, targetWorkflow, newWorkflow, chasm.WorkflowArchetypeID)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestUpdateAsZombie_CheckWorkflowExistsError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	newRunID := "some random new run ID"

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	newReleaseCalled := false
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(false, errTxnExist)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyUpdateAsZombie, currentWorkflow, targetWorkflow, newWorkflow, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestSuppressCurrentAndUpdateAsCurrent_CurrentNotRunning() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()
	targetWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentContext := historyi.NewMockWorkflowContext(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		currentContext,
		currentMutableState,
		historyi.TransactionPolicyPassive,
		(*historyi.TransactionPolicy)(nil),
		historyi.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent, currentWorkflow, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestSuppressCurrentAndUpdateAsCurrent_SuppressByError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, errTxnExist)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent, currentWorkflow, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestSuppressCurrentAndUpdateAsCurrent_TargetReviveError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()
	targetWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(errTxnExist)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent, currentWorkflow, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestSuppressCurrentAndUpdateAsCurrent_NewReviveError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()
	targetWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(nil)

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	newReleaseCalled := false
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive(gomock.Any(), gomock.Any()).Return(errTxnExist)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := historyi.NewMockMutableState(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent, currentWorkflow, targetWorkflow, newWorkflow, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestConflictResolveAsZombie_TargetSuppressByError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, errTxnExist)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyConflictResolveAsZombie, currentWorkflow, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestConflictResolveAsZombie_TargetPolicyNotPassive() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyActive, nil)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyConflictResolveAsZombie, currentWorkflow, targetWorkflow, nil, chasm.WorkflowArchetypeID)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestConflictResolveAsZombie_NewSuppressByError() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newReleaseCalled := false
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, errTxnExist)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyConflictResolveAsZombie, currentWorkflow, targetWorkflow, newWorkflow, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestConflictResolveAsZombie_NewPolicyNotPassive() {
	ctx := context.Background()

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newReleaseCalled := false
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyActive, nil)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyConflictResolveAsZombie, currentWorkflow, targetWorkflow, newWorkflow, chasm.WorkflowArchetypeID)
	s.Error(err)
	var internalErr *serviceerror.Internal
	s.ErrorAs(err, &internalErr)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestConflictResolveAsZombie_CheckWorkflowExistsError() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	newRunID := "some random new run ID"

	targetWorkflow := NewMockWorkflow(s.controller)
	targetReleaseCalled := false
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	newReleaseCalled := false
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentReleaseCalled := false
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(false, errTxnExist)

	err := s.updateMgr.executeTransaction(ctx, nDCTransactionPolicyConflictResolveAsZombie, currentWorkflow, targetWorkflow, newWorkflow, chasm.WorkflowArchetypeID)
	s.Equal(errTxnExist, err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie_NewRunDoesExists() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := historyi.NewMockWorkflowContext(s.controller)
	newMutableState := historyi.NewMockMutableState(s.controller)
	var newReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().GetReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID, chasm.WorkflowArchetypeID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(true, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(historyi.TransactionPolicyPassive, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		historyi.TransactionPolicyPassive.Ptr(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}
