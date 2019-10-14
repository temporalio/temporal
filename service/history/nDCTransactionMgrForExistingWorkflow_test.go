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
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	nDCTransactionMgrForExistingWorkflowSuite struct {
		suite.Suite

		controller         *gomock.Controller
		mockTransactionMgr *MocknDCTransactionMgr

		mockService service.Service
		mockShard   *shardContextImpl
		logger      log.Logger

		updateMgr *nDCTransactionMgrForExistingWorkflowImpl
	}
)

func TestNDCTransactionMgrForExistingWorkflowSuite(t *testing.T) {
	s := new(nDCTransactionMgrForExistingWorkflowSuite)
	suite.Run(t, s)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) SetupTest() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockService = service.NewTestService(nil, nil, metricsClient, nil, nil, nil, nil)

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 10, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		metricsClient:             metricsClient,
		timeSource:                clock.NewRealTimeSource(),
	}

	s.controller = gomock.NewController(s.T())
	s.mockTransactionMgr = NewMocknDCTransactionMgr(s.controller)
	s.updateMgr = newNDCTransactionMgrForExistingWorkflow(s.mockTransactionMgr)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowGuaranteed() {
	ctx := ctx.Background()
	now := time.Now()

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := &mockWorkflowExecutionContext{}
	defer newContext.AssertExpectations(s.T())
	newMutableState := &mockMutableState{}
	defer newMutableState.AssertExpectations(s.T())
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.On("IsCurrentWorkflowGuaranteed").Return(true)

	targetContext.On(
		"updateWorkflowExecutionWithNewAsPassive",
		now,
		newContext,
		newMutableState,
	).Return(nil).Once()

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_IsCurrent() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)

	targetMutableState.On("IsCurrentWorkflowGuaranteed").Return(false)
	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(targetRunID, nil).Times(1)

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.Error(err)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentRunning_UpdateAsCurrent() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := &mockWorkflowExecutionContext{}
	defer newContext.AssertExpectations(s.T())
	newMutableState := &mockMutableState{}
	defer newMutableState.AssertExpectations(s.T())
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil).Times(1)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.On("IsCurrentWorkflowGuaranteed").Return(false)
	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := transactionPolicyPassive
	currentMutableState.On("IsWorkflowExecutionRunning").Return(true)
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(1)
	targetWorkflow.EXPECT().revive().Return(nil).Times(1)

	targetContext.On(
		"conflictResolveWorkflowExecution",
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.ptr(),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Once()

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_CurrentComplete_UpdateAsCurrent() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := &mockWorkflowExecutionContext{}
	defer newContext.AssertExpectations(s.T())
	newMutableState := &mockMutableState{}
	defer newMutableState.AssertExpectations(s.T())
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil).Times(1)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.On("IsCurrentWorkflowGuaranteed").Return(false)
	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := transactionPolicyPassive
	currentMutableState.On("IsWorkflowExecutionRunning").Return(false)
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(0)
	targetWorkflow.EXPECT().revive().Return(nil).Times(1)

	targetContext.On(
		"conflictResolveWorkflowExecution",
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.ptr(),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Once()

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_NoRebuild_CurrentWorkflowNotGuaranteed_NotCurrent_UpdateAsZombie() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := false

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := &mockWorkflowExecutionContext{}
	defer newContext.AssertExpectations(s.T())
	newMutableState := &mockMutableState{}
	defer newMutableState.AssertExpectations(s.T())
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.On("IsCurrentWorkflowGuaranteed").Return(false)
	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.On(
		"updateWorkflowExecutionWithNew",
		now,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		transactionPolicyPassive,
		transactionPolicyPassive.ptr(),
	).Return(nil).Once()

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_IsCurrent() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := &mockWorkflowExecutionContext{}
	defer newContext.AssertExpectations(s.T())
	newMutableState := &mockMutableState{}
	defer newMutableState.AssertExpectations(s.T())
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(targetRunID, nil).Times(1)

	targetContext.On(
		"conflictResolveWorkflowExecution",
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(workflowExecutionContext)(nil),
		(mutableState)(nil),
		(*transactionPolicy)(nil),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Once()

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsCurrent() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := &mockWorkflowExecutionContext{}
	defer newContext.AssertExpectations(s.T())
	newMutableState := &mockMutableState{}
	defer newMutableState.AssertExpectations(s.T())
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()
	newWorkflow.EXPECT().revive().Return(nil).Times(1)

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getContext().Return(currentContext).AnyTimes()
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentWorkflowPolicy := transactionPolicyActive
	currentMutableState.On("IsWorkflowExecutionRunning").Return(true)
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(1)
	targetWorkflow.EXPECT().revive().Return(nil).Times(1)

	targetContext.On(
		"conflictResolveWorkflowExecution",
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		currentContext,
		currentMutableState,
		currentWorkflowPolicy.ptr(),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Once()

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}

func (s *nDCTransactionMgrForExistingWorkflowSuite) TestDispatchForExistingWorkflow_Rebuild_NotCurrent_UpdateAsZombie() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	targetRunID := "some random run ID"
	currentRunID := "other random runID"

	isWorkflowRebuilt := true

	targetReleaseCalled := false
	newReleaseCalled := false
	currentReleaseCalled := false

	targetWorkflow := NewMocknDCWorkflow(s.controller)
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	newWorkflow := NewMocknDCWorkflow(s.controller)
	newContext := &mockWorkflowExecutionContext{}
	defer newContext.AssertExpectations(s.T())
	newMutableState := &mockMutableState{}
	defer newMutableState.AssertExpectations(s.T())
	var newReleaseFn releaseWorkflowExecutionFunc = func(error) { newReleaseCalled = true }
	newWorkflow.EXPECT().getContext().Return(newContext).AnyTimes()
	newWorkflow.EXPECT().getMutableState().Return(newMutableState).AnyTimes()
	newWorkflow.EXPECT().getReleaseFn().Return(newReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)
	newWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.On(
		"conflictResolveWorkflowExecution",
		now,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetMutableState,
		newContext,
		newMutableState,
		(workflowExecutionContext)(nil),
		(mutableState)(nil),
		(*transactionPolicy)(nil),
		(*persistence.CurrentWorkflowCAS)(nil),
	).Return(nil).Once()

	err := s.updateMgr.dispatchForExistingWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(newReleaseCalled)
	s.True(currentReleaseCalled)
}
