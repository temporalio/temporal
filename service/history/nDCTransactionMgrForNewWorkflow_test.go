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
	nDCTransactionMgrForNewWorkflowSuite struct {
		suite.Suite

		controller         *gomock.Controller
		mockTransactionMgr *MocknDCTransactionMgr

		mockService service.Service
		mockShard   *shardContextImpl
		logger      log.Logger

		createMgr *nDCTransactionMgrForNewWorkflowImpl
	}
)

func TestNDCTransactionMgrForNewWorkflowSuite(t *testing.T) {
	s := new(nDCTransactionMgrForNewWorkflowSuite)
	suite.Run(t, s)
}

func (s *nDCTransactionMgrForNewWorkflowSuite) SetupTest() {
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
	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()

	mutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})

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
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }
	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	workflowSnapshot := &persistence.WorkflowSnapshot{}
	workflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{}}
	workflowHistorySize := int64(12345)
	mutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})
	mutableState.On("CloseTransactionAsSnapshot", now, transactionPolicyPassive).Return(
		workflowSnapshot, workflowEventsSeq, nil,
	).Once()

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return("", nil).Times(1)

	context.On(
		"persistFirstWorkflowEvents", workflowEventsSeq[0],
	).Return(workflowHistorySize, nil).Once()
	context.On(
		"createWorkflowExecution",
		workflowSnapshot,
		workflowHistorySize,
		now,
		persistence.CreateWorkflowModeBrandNew,
		"",
		int64(0),
	).Return(nil).Once()

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
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
	var currentReleaseFn releaseWorkflowExecutionFunc = func(error) { currentReleaseCalled = true }
	currentWorkflow.EXPECT().getMutableState().Return(currentMutableState).AnyTimes()
	currentWorkflow.EXPECT().getReleaseFn().Return(currentReleaseFn).AnyTimes()

	targetWorkflowSnapshot := &persistence.WorkflowSnapshot{}
	targetWorkflowEventsSeq := []*persistence.WorkflowEvents{&persistence.WorkflowEvents{}}
	targetWorkflowHistorySize := int64(12345)
	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	targetMutableState.On("CloseTransactionAsSnapshot", now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	).Once()

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(true, nil)
	currentMutableState.On("IsWorkflowExecutionRunning").Return(false)
	currentMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      currentRunID,
	})
	currentWorkflow.EXPECT().getVectorClock().Return(currentLastWriteVersion, int64(0), nil)

	targetContext.On(
		"persistFirstWorkflowEvents", targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil).Once()
	targetContext.On(
		"createWorkflowExecution",
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeWorkflowIDReuse,
		currentRunID,
		currentLastWriteVersion,
	).Return(nil).Once()

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
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

	currentWorkflow := NewMocknDCWorkflow(s.controller)
	currentContext := &mockWorkflowExecutionContext{}
	defer currentContext.AssertExpectations(s.T())
	currentMutableState := &mockMutableState{}
	defer currentMutableState.AssertExpectations(s.T())
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
	targetMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      targetRunID,
	})
	targetMutableState.On("CloseTransactionAsSnapshot", now, transactionPolicyPassive).Return(
		targetWorkflowSnapshot, targetWorkflowEventsSeq, nil,
	).Once()

	s.mockTransactionMgr.EXPECT().getCurrentWorkflowRunID(ctx, domainID, workflowID).Return(currentRunID, nil).Times(1)
	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(ctx, domainID, workflowID, currentRunID).Return(currentWorkflow, nil).Times(1)

	targetWorkflow.EXPECT().happensAfter(currentWorkflow).Return(false, nil)
	targetWorkflow.EXPECT().suppressBy(currentWorkflow).Return(transactionPolicyPassive, nil).Times(1)

	targetContext.On(
		"persistFirstWorkflowEvents", targetWorkflowEventsSeq[0],
	).Return(targetWorkflowHistorySize, nil).Once()
	targetContext.On(
		"createWorkflowExecution",
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		persistence.CreateWorkflowModeZombie,
		"",
		int64(0),
	).Return(nil).Once()
	targetContext.On("reapplyEvents", targetWorkflowEventsSeq).Return(nil).Times(1)

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
	targetContext := &mockWorkflowExecutionContext{}
	defer targetContext.AssertExpectations(s.T())
	targetMutableState := &mockMutableState{}
	defer targetMutableState.AssertExpectations(s.T())
	var targetReleaseFn releaseWorkflowExecutionFunc = func(error) { targetReleaseCalled = true }
	targetWorkflow.EXPECT().getContext().Return(targetContext).AnyTimes()
	targetWorkflow.EXPECT().getMutableState().Return(targetMutableState).AnyTimes()
	targetWorkflow.EXPECT().getReleaseFn().Return(targetReleaseFn).AnyTimes()

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
	currentMutableState.On("IsWorkflowExecutionRunning").Return(true)
	currentWorkflowPolicy := transactionPolicyActive
	currentWorkflow.EXPECT().suppressBy(targetWorkflow).Return(currentWorkflowPolicy, nil).Times(1)
	targetWorkflow.EXPECT().revive().Return(nil).Times(1)

	currentContext.On(
		"updateWorkflowExecutionWithNew",
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetContext,
		targetMutableState,
		currentWorkflowPolicy,
		transactionPolicyPassive.ptr(),
	).Return(nil).Once()

	err := s.createMgr.dispatchForNewWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
	s.True(targetReleaseCalled)
	s.True(currentReleaseCalled)
}
