// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ndc

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	transactionMgrForExistingWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockTransactionMgr *MockTransactionManager
		mockShard          *shard.MockContext

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
	s.mockShard = shard.NewMockContext(s.controller)

	reg := hsm.NewRegistry()
	err := workflow.RegisterStateMachine(reg)
	s.NoError(err)
	s.mockShard.EXPECT().StateMachineRegistry().Return(reg).AnyTimes()

	s.updateMgr = newNDCTransactionMgrForExistingWorkflow(s.mockShard, s.mockTransactionMgr, false)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
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

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *transactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_IsCurrent() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := false

	targetWorkflow := NewMockWorkflow(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(targetRunID, nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.Error(err)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive().Return(nil)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	targetWorkflow.EXPECT().Revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		workflow.TransactionPolicyPassive,
		workflow.TransactionPolicyPassive.Ptr(),
		workflow.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive().Return(nil)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(workflow.TransactionPolicyPassive, nil).Times(0)
	targetWorkflow.EXPECT().Revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		workflow.TransactionPolicyPassive,
		workflow.TransactionPolicyPassive.Ptr(),
		workflow.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(false, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		workflow.TransactionPolicyPassive,
		workflow.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(true, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		workflow.TransactionPolicyPassive,
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
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
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(targetRunID, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		workflow.TransactionPolicyPassive,
		workflow.TransactionPolicyPassive.Ptr(),
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().Revive().Return(nil)

	currentWorkflow := NewMockWorkflow(s.controller)
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(workflow.TransactionPolicyActive, nil)
	targetWorkflow.EXPECT().Revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		workflow.TransactionPolicyPassive,
		workflow.TransactionPolicyPassive.Ptr(),
		workflow.TransactionPolicyActive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(false, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		workflow.TransactionPolicyPassive,
		workflow.TransactionPolicyPassive.Ptr(),
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMockWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn wcache.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().GetContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().GetMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().GetReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().CheckWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(true, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		workflow.TransactionPolicyPassive,
		workflow.TransactionPolicyPassive.Ptr(),
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}
