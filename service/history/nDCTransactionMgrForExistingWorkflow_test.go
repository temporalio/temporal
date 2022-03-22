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

package history

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/workflow"
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
	now := time.Now().UTC()

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()

	targetContext.EXPECT().UpdateWorkflowExecutionWithNewAsPassive(
		gomock.Any(),
		now,
		newContext,
		newMutableState,
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_IsCurrent() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(targetRunID, nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.Error(err)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentRunning_UpdateAsCurrent() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := workflow.TransactionPolicyPassive
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil)
	targetWorkflow.EXPECT().revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentComplete_UpdateAsCurrent() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := workflow.TransactionPolicyPassive
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(0)
	targetWorkflow.EXPECT().revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie_NewRunDoesNotExists() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

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
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().checkWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(false, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		now,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		workflow.TransactionPolicyPassive,
		workflow.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie_NewRunDoesExists() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

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
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().checkWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(true, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		now,
		persistence.UpdateWorkflowModeBypassCurrent,
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		workflow.TransactionPolicyPassive,
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_IsCurrent() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(targetRunID, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsCurrent() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := workflow.NewMockContext(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := workflow.TransactionPolicyActive
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil)
	targetWorkflow.EXPECT().revive().Return(nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.Ptr(),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie_NewRunDoesNotExists() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

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
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().checkWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(false, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		now,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie_NewRunDoesExists() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	newRunID := "some random new run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn workflow.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := workflow.NewMockContext(s.controller)
	newMutableState := workflow.NewMockMutableState(s.controller)
	var newReleaseFn workflow.ReleaseCacheFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn workflow.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

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
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)
	s.mockTransactionMgr.EXPECT().checkWorkflowExists(ctx, namespaceID, workflowID, newRunID).Return(true, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().ConflictResolveWorkflowExecution(
		gomock.Any(),
		now,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		(workflow.Context)(nil),
		(workflow.MutableState)(nil),
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}
