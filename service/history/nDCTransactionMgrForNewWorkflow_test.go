// Copyright (c) 2019 Uber Technologies, Inc.
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
	ctx "context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/persistence"
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
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	workflow := NewMocknDCWorkflow(s.controller)
	mutableState := NewMockmutableState(s.controller)
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()

	mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(runID, nil).Times(1)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, workflow)
	s.NoError(err)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_BrandNew() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	context := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }
	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	workflowSnapshot := &persistence.WorkflowSnapshot{}
	workflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{}}
	workflowHistorySize := int64(12345)
	mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}).AnyTimes()
	mutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	).Times(1)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(
		ctx, domainID, workflowID,
	).Return("", nil).Times(1)

	context.EXPECT().persistFirstWorkflowEvents(
		workflowEventsSeq[0],
	).Return(workflowHistorySize, nil).Times(1)
	context.EXPECT().createWorkflowExecution(
		workflowSnapshot,
		workflowHistorySize,
		now,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
	).Return(nil).Times(1)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, workflow)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsCurrent() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
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
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	).Times(1)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	currentMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      currentRunID,
	}).AnyTimes()
	currentWorkflow.EXPECT().getVectorClock().Return(currentLastWriteVersion, int64(0), nil)

	targetContext.EXPECT().persistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil).Times(1)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeWorkflowIDReuse,
		currentRunID,
		currentLastWriteVersion,
	).Return(nil).Times(1)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
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
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			DomainID:   domainID,
			WorkflowID: workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	).Times(1)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.EXPECT().persistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil).Times(1)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeZombie,
		"",
		int64(0),
	).Return(nil).Times(1)
	targetContext.EXPECT().reapplyEvents(targetWorkflowEventsSeq).Return(nil).Times(1)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_CreateAsZombie_Dedup() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
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
		ExecutionInfo: &persistence.WorkflowExecutionInfo{
			DomainID:   domainID,
			WorkflowID: workflowID,
		},
	}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	}).AnyTimes()
	targetMutableState.EXPECT().CloseTransactionAsSnapshot(now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	).Times(1)

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.EXPECT().persistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil).Times(1)
	targetContext.EXPECT().createWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeZombie,
		"",
		int64(0),
	).Return(&persistence.WorkflowExecutionAlreadyStartedError{}).Times(1)
	targetContext.EXPECT().reapplyEvents(targetWorkflowEventsSeq).Return(nil).Times(1)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) TestDispatchForNewWorkflow_SuppressCurrentAndCreateAsCurrent() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
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

	targetMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	}).AnyTimes()

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	currentWorkflowPolicy := transactionPolicyActive
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(1)
	targetWorkflow.EXPECT().revive().Return(nil).Times(1)

	currentContext.EXPECT().updateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetContext,
		targetMutableState,
		currentWorkflowPolicy,
		transactionPolicyPassive.ptr(),
	).Return(nil).Times(1)

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}
