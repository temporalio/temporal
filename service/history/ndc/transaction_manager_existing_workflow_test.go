package ndc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

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
	s.mockShard.EXPECT().GetThrottledLogger().Return(log.NewNoopLogger()).AnyTimes()

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

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_ClosedTargetRun_AppliesAsZombie() {
	// Closed run with no current execution record (deleted workflow): persist it via bypass-current
	// instead of poison-pilling, so any changes the run carries are replicated without touching the
	// current record (bypass-current allows a missing current record).
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	// no current execution record exists
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	// target run is closed and no new run -> takes the bypass-current path
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

	// with no current workflow to suppress against, the closed orphan is written directly via
	// bypass-current (allows a missing current record).
	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, nil)
	s.NoError(err)
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_ClosedTargetRun_Rebuilt_AppliesAsZombie() {
	// A rebuilt closed run with no current execution record is persisted via conflict-resolve
	// bypass-current (which also allows a missing current record) rather than being dropped.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

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
		gomock.Any(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, nil)
	s.NoError(err)
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_RunningTargetRun_ReconstructsCurrent() {
	// Running run with no current execution record: the current record was deleted, but a running run
	// must be current, so reconstruct it by inserting a brand-new current record (pointing at the
	// target) instead of poison-pilling the apply.
	//
	// Reachability note: this branch is only reached when IsCurrentWorkflowGuaranteed() is false (a run
	// revived from zombie, or a rebuilt mutable state). A run whose stateInDB is RUNNING is guaranteed
	// current and takes the fast path in dispatchForExistingWorkflow, so it never reaches here — hence
	// the mock returns false.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBrandNewCurrent,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, nil)
	s.NoError(err)
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_RunningTargetRun_Rebuilt_ReconstructsCurrent() {
	// Rebuilt running run with no current execution record: reconstruct the current record via
	// conflict-resolve brand-new-current.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetContext := historyi.NewMockWorkflowContext(s.controller)
	targetMutableState := historyi.NewMockMutableState(s.controller)
	var targetReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeBrandNewCurrent,
		targetMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		(*historyi.TransactionPolicy)(nil),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, nil)
	s.NoError(err)
	s.True(targetReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_ClosedTargetRunWithNewRun_ReconstructsCurrent() {
	// Closed run carrying a new run (continue-as-new) with no current execution record: the new run
	// does not yet exist, so reconstruct the current record by inserting a fresh one that points at
	// the new run.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"

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
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(false, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBrandNewCurrent,
		newContext,
		newMutableState,
		historyi.TransactionPolicyPassive,
		gomock.Any(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_ClosedTargetRunWithNewRun_Rebuilt_ReconstructsCurrent() {
	// Rebuilt closed run carrying a new run with no current execution record: the new run does not yet
	// exist, so reconstruct the current record via conflict-resolve brand-new-current, pointing at the
	// new run.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"

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
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(false, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeBrandNewCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		gomock.Any(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_ClosedTargetRunWithNewRun_NewRunAlreadyExists_Running_RepairsCurrentForExistingRun() {
	// Closed target carrying a new run with no current execution record, but the new run already exists
	// (e.g. created by an earlier delivery or by its own state replication) and is running while the
	// current record was deleted out-of-band. Re-creating the new run would fail with "Workflow
	// execution already running", so load the existing (running) new run, insert a fresh current record
	// pointing at it, and persist the target via bypass-current.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	existingNewReleaseCalled := false

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
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()

	// the loaded, already-existing new run (running)
	existingNewWorkflow := NewMockWorkflow(s.controller)
	existingNewContext := historyi.NewMockWorkflowContext(s.controller)
	existingNewMutableState := historyi.NewMockMutableState(s.controller)
	var existingNewReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { existingNewReleaseCalled = true }
	existingNewWorkflow.EXPECT().GetContext().Return(existingNewContext).AnyTimes()
	existingNewWorkflow.EXPECT().GetMutableState().Return(existingNewMutableState).AnyTimes()
	existingNewWorkflow.EXPECT().GetReleaseFn().Return(existingNewReleaseFn).AnyTimes()
	existingNewMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(true, nil)

	// load the existing running new run and insert a fresh current record pointing at it.
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(existingNewWorkflow, nil)
	existingNewContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBrandNewCurrent,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	// the new run already exists, so it is not re-created; the target is written via bypass-current.
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
	s.True(existingNewReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_ClosedTargetRunWithNewRun_NewRunAlreadyExists_Closed_LeavesCurrentMissing() {
	// The already-existing new run is closed: its missing current record means the workflow is being
	// deleted or the run was superseded by a later run, so it must NOT be made current again. We only
	// persist the target via bypass-current and leave the current record missing.
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	existingNewReleaseCalled := false

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
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()

	// the loaded, already-existing new run (closed)
	existingNewWorkflow := NewMockWorkflow(s.controller)
	existingNewMutableState := historyi.NewMockMutableState(s.controller)
	var existingNewReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { existingNewReleaseCalled = true }
	existingNewWorkflow.EXPECT().GetMutableState().Return(existingNewMutableState).AnyTimes()
	existingNewWorkflow.EXPECT().GetReleaseFn().Return(existingNewReleaseFn).AnyTimes()
	existingNewMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(true, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(existingNewWorkflow, nil)

	// the closed new run is not made current; only the target is written via bypass-current.
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
	s.True(existingNewReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoCurrentRecord_ClosedTargetRunWithNewRun_Rebuilt_NewRunAlreadyExists_Running_RepairsCurrentForExistingRun() {
	// Rebuilt counterpart of the running case: the carried new run already exists and is running, so
	// load it and insert a fresh current record pointing at it, then persist the (rebuilt) target via
	// conflict-resolve bypass-current (the loaded run is not rebuilt, so it takes the update-current path).
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	existingNewReleaseCalled := false

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
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: newRunID,
	}).AnyTimes()

	existingNewWorkflow := NewMockWorkflow(s.controller)
	existingNewContext := historyi.NewMockWorkflowContext(s.controller)
	existingNewMutableState := historyi.NewMockMutableState(s.controller)
	var existingNewReleaseFn historyi.ReleaseWorkflowContextFunc = func(error) { existingNewReleaseCalled = true }
	existingNewWorkflow.EXPECT().GetContext().Return(existingNewContext).AnyTimes()
	existingNewWorkflow.EXPECT().GetMutableState().Return(existingNewMutableState).AnyTimes()
	existingNewWorkflow.EXPECT().GetReleaseFn().Return(existingNewReleaseFn).AnyTimes()
	existingNewMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID, chasm.WorkflowArchetypeID).Return("", nil)
	targetMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(true, nil)

	// load the existing running new run and insert a fresh current record pointing at it.
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, newRunID, chasm.WorkflowArchetypeID).Return(existingNewWorkflow, nil)
	existingNewContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBrandNewCurrent,
		(historyi.WorkflowContext)(nil),
		(historyi.MutableState)(nil),
		historyi.TransactionPolicyPassive,
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	// rebuilt target written via conflict-resolve bypass-current (new run not re-created).
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
		gomock.Any(),
		(*historyi.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, chasm.WorkflowArchetypeID, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(existingNewReleaseCalled)
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
