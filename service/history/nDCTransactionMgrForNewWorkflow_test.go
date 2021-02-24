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
	historypb "go.temporal.io/api/history/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
)

type (
	nDCTransactionMgrForNewWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		mockTransactionMgr *MocknDCTransactionMgr

		createMgr *nDCTransactionMgrForNewWorkflowImpl
	}
)

func TestNDCTransactionMgrForNewWorkflowSuite(t *testing.T) {
	s := new(nDCTransactionMgrForNewWorkflowSuite)
	suite.Run(t, s)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTransactionMgr = NewMocknDCTransactionMgr(s.controller)

	s.createMgr = newNDCTransactionMgrForNewWorkflow(s.mockTransactionMgr)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_Dup() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	workflow := NewMocknDCWorkflow(s.controller)
	mutableState := NewMockmutableState(s.controller)
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()

	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(runID, nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, workflow)
	s.NoError(err)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_BrandNew_FirstEvents() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	weContext := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }
	workflow.EXPECT().getContext().Return(weContext).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	workflowSnapshot := &persistence.WorkflowSnapshot{}
	workflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID,
		}},
	}}
	workflowHistorySize := int64(12345)
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(
		ctx, namespaceID, workflowID,
	).Return("", nil)

	weContext.EXPECT().persistFirstWorkflowEvents(
		workflowEventsSeq[0],
	).Return(workflowHistorySize, nil)
	weContext.EXPECT().createWorkflowExecution(
		workflowSnapshot,
		workflowHistorySize,
		now,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, workflow)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_BrandNew_NonFirstEvents() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	weContext := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }
	workflow.EXPECT().getContext().Return(weContext).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	workflowSnapshot := &persistence.WorkflowSnapshot{}
	workflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + 1,
		}},
	}}
	workflowHistorySize := int64(12345)
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(
		ctx, namespaceID, workflowID,
	).Return("", nil)

	weContext.EXPECT().persistNonFirstWorkflowEvents(
		workflowEventsSeq[0],
	).Return(workflowHistorySize, nil)
	weContext.EXPECT().createWorkflowExecution(
		workflowSnapshot,
		workflowHistorySize,
		now,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, workflow)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsCurrent_FirstEvents() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"
	currentLastWriteVersion := int64(4321)

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentMutableState := NewMockmutableState(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID,
		}},
	}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: currentRunID,
	}).AnyTimes()
	currentWorkflow.EXPECT().getVectorClock().Return(currentLastWriteVersion, int64(0), nil)

	targetContext.EXPECT().persistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeWorkflowIDReuse,
		currentRunID,
		currentLastWriteVersion,
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsCurrent_NonFirstEvents() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"
	currentLastWriteVersion := int64(4321)

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentMutableState := NewMockmutableState(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + 1,
		}},
	}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	currentMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: currentRunID,
	}).AnyTimes()
	currentWorkflow.EXPECT().getVectorClock().Return(currentLastWriteVersion, int64(0), nil)

	targetContext.EXPECT().persistNonFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeWorkflowIDReuse,
		currentRunID,
		currentLastWriteVersion,
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie_FirstEvents() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID,
		}},
	}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil)

	targetContext.EXPECT().persistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeZombie,
		"",
		int64(0),
	).Return(nil)
	targetContext.EXPECT().reapplyEvents(targetWorkflowEventsSeq).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie_NonFirstEvents() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + 1,
		}},
	}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil)

	targetContext.EXPECT().persistNonFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeZombie,
		"",
		int64(0),
	).Return(nil)
	targetContext.EXPECT().reapplyEvents(targetWorkflowEventsSeq).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie_Dedup_FirstEvents() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID,
		}},
	}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil)

	targetContext.EXPECT().persistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeZombie,
		"",
		int64(0),
	).Return(&persistence.WorkflowExecutionAlreadyStartedError{})
	targetContext.EXPECT().reapplyEvents(targetWorkflowEventsSeq).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie_Dedup_NonFirstEvents() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventId: common.FirstEventID + 1,
		}},
	}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil)

	targetContext.EXPECT().persistNonFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeZombie,
		"",
		int64(0),
	).Return(&persistence.WorkflowExecutionAlreadyStartedError{})
	targetContext.EXPECT().reapplyEvents(targetWorkflowEventsSeq).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_SuppressCurrentAndCreateAsCurrent() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	targetReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := NewMockworkflowExecutionContext(s.controller)
	targetMutableState := NewMockmutableState(s.controller)
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := NewMockworkflowExecutionContext(s.controller)
	currentMutableState := NewMockmutableState(s.controller)
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	targetMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: targetRunID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, namespaceID, workflowID).Return(currentRunID, nil)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, namespaceID, workflowID, currentRunID).Return(currentWorkflow, nil)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflowPolicy := transactionPolicyActive
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil)
	targetWorkflow.EXPECT().revive().Return(nil)

	currentContext.EXPECT().updateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetContext,
		targetMutableState,
		currentWorkflowPolicy,
		transactionPolicyPassive.ptr(),
	).Return(nil)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}
