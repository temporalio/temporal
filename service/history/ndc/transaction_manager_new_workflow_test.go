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
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	historypb "go.temporal.io/api/history/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	transactionMgrForNewWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockTransactionMgr *MockTransactionManager
		mockShard          *shard.MockContext

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
	s.mockShard = shard.NewMockContext(s.controller)

	s.createMgr = newTransactionMgrForNewWorkflow(s.mockShard, s.mockTransactionMgr, false)
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
	mutableState := workflow.NewMockMutableState(s.controller)
	newWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()

	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(runID, nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, newWorkflow)
	s.NoError(err)
}

func (s *transactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_BrandNew() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	newWorkflow := NewMockWorkflow(s.controller)
	weContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	var releaseFn wcache.ReleaseCacheFunc = func(error) { releaseCalled = true }
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
	mutableState.EXPECT().CloseTransactionAsSnapshot(workflow.TransactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(
		ctx, namespaceID, workflowID,
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
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, newWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	currentMutableState := workflow.NewMockMutableState(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(workflow.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

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
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(workflow.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
	).Return(nil)
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, targetWorkflowEventsSeq).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMockWorkflow(s.controller)
	var currentReleaseFn wcache.ReleaseCacheFunc = func(error) { currentReleaseCalled = true }
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
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(workflow.TransactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().GetCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().LoadWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().HappensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().SuppressBy(currentWorkflow).Return(workflow.TransactionPolicyPassive, nil)

	targetContext.EXPECT().CreateWorkflowExecution(
		gomock.Any(),
		s.mockShard,
		persistence.CreateWorkflowModeBypassCurrent,
		"",
		int64(0),
		targetMutableState,
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
	).Return(&persistence.WorkflowConditionFailedError{})
	targetContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, targetWorkflowEventsSeq).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
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
	targetContext := workflow.NewMockContext(s.controller)
	targetMutableState := workflow.NewMockMutableState(s.controller)
	var targetReleaseFn wcache.ReleaseCacheFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().GetContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(targetReleaseFn).AnyTimes()

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
	currentWorkflowPolicy := workflow.TransactionPolicyActive
	currentWorkflow.EXPECT().SuppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil)
	targetWorkflow.EXPECT().Revive().Return(nil)

	currentContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetContext,
		targetMutableState,
		currentWorkflowPolicy,
		workflow.TransactionPolicyPassive.Ptr(),
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}
