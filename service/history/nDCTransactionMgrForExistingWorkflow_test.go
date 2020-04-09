package history

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/common/persistence"
)

type (
	nDCTransactionMgrForExistingWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockTransactionMgr *MocknDCTransactionMgr

		updateMgr *nDCTransactionMgrForExistingWorkflowImpl
	}
)

func TestNDCTransactionMgrForExistingWorkflowSuite(t *testing.T) {
	s := new(nDCTransactionMgrForExistingWorkflowSuite)
	suite.Run(t, s)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTransactionMgr = NewMocknDCTransactionMgr(s.controller)

	s.updateMgr = newNDCTransactionMgrForExistingWorkflow(s.mockTransactionMgr)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowGuaranteed() {
	ctx := context.Background()
	now := time.Now()

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()

	targetContext.EXPECT().updateWorkflowExecutionWithNewAsPassive(
		now,
		newContext,
		newMutableState,
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_IsCurrent() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(targetRunID, nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.Error(err)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentRunning_UpdateAsCurrent() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil).Times(1)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := NewMockworkflowExecutionContext(s.controller)
	currentMutableState := NewMockmutableState(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := transactionPolicyPassive
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(1)
	targetWorkflow.EXPECT().revive().Return(nil).Times(1)

	targetContext.EXPECT().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.ptr(),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentComplete_UpdateAsCurrent() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil).Times(1)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := NewMockworkflowExecutionContext(s.controller)
	currentMutableState := NewMockmutableState(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := transactionPolicyPassive
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(0)
	targetWorkflow.EXPECT().revive().Return(nil).Times(1)

	targetContext.EXPECT().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.ptr(),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie_NewRunDoesNotExists() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       newRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)
	s.mockTransactionMgr.EXPECT().checkWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(false, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.EXPECT().updateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		transactionPolicyPassive,
		transactionPolicyPassive.ptr(),
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie_NewRunDoesExists() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       newRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)
	s.mockTransactionMgr.EXPECT().checkWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(true, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.EXPECT().updateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeBypassCurrent,
		(workflowExecutionContext)(nil),
		(mutableState)(nil),
		transactionPolicyPassive,
		(*transactionPolicy)(nil),
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_IsCurrent() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(targetRunID, nil).Times(1)

	targetContext.EXPECT().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(workflowExecutionContext)(nil),
		(mutableState)(nil),
		(*transactionPolicy)(nil),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsCurrent() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil).Times(1)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := NewMockworkflowExecutionContext(s.controller)
	currentMutableState := NewMockmutableState(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := transactionPolicyActive
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(1)
	targetWorkflow.EXPECT().revive().Return(nil).Times(1)

	targetContext.EXPECT().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.ptr(),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie_NewRunDoesNotExists() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       newRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)
	s.mockTransactionMgr.EXPECT().checkWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(false, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.EXPECT().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(workflowExecutionContext)(nil),
		(mutableState)(nil),
		(*transactionPolicy)(nil),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie_NewRunDoesExists() {
	ctx := context.Background()
	now := time.Now()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := NewMockworkflowExecutionContext(s.controller)
	newMutableState := NewMockmutableState(s.controller)
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       targetRunID,
	}).AnyTimes()
	newMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       newRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)
	s.mockTransactionMgr.EXPECT().checkWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(true, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.EXPECT().conflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		(workflowExecutionContext)(nil),
		(mutableState)(nil),
		(workflowExecutionContext)(nil),
		(mutableState)(nil),
		(*transactionPolicy)(nil),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}
