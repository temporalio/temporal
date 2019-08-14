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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	nDCTransactionMgrSuite struct {
		suite.Suite

		logger              log.Logger
		mockExecutionMgr    *mocks.ExecutionManager
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockClusterMetadata *mocks.ClusterMetadata
		mockMetadataMgr     *mocks.MetadataManager
		mockService         service.Service
		mockShard           *shardContextImpl
		mockDomainCache     *cache.DomainCacheMock

		controller    *gomock.Controller
		mockCreateMgr *MocknDCTransactionMgrForNewWorkflow
		mockUpdateMgr *MocknDCTransactionMgrForExistingWorkflow

		transactionMgr *nDCTransactionMgrImpl
	}
)

func TestNDCTransactionMgrSuite(t *testing.T) {
	s := new(nDCTransactionMgrSuite)
	suite.Run(t, s)
}

func (s *nDCTransactionMgrSuite) SetupTest() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockExecutionMgr = &mocks.ExecutionManager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockMetadataMgr = &mocks.MetadataManager{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockService = service.NewTestService(s.mockClusterMetadata, nil, metricsClient, nil, nil, nil)
	s.mockDomainCache = &cache.DomainCacheMock{}

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 10, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               s.mockDomainCache,
		metricsClient:             metricsClient,
		timeSource:                clock.NewRealTimeSource(),
	}
	s.transactionMgr = newNDCTransactionMgr(s.mockShard, newHistoryCache(s.mockShard), s.logger)

	s.controller = gomock.NewController(s.T())
	s.mockCreateMgr = NewMocknDCTransactionMgrForNewWorkflow(s.controller)
	s.mockUpdateMgr = NewMocknDCTransactionMgrForExistingWorkflow(s.controller)
	s.transactionMgr.createMgr = s.mockCreateMgr
	s.transactionMgr.updateMgr = s.mockUpdateMgr

}

func (s *nDCTransactionMgrSuite) TearDownTest() {
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.mockExecutionMgr.AssertExpectations(s.T())
	s.mockMetadataMgr.AssertExpectations(s.T())
	s.mockDomainCache.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *nDCTransactionMgrSuite) TestCreateWorkflow() {
	ctx := ctx.Background()
	now := time.Now()
	targetWorkflow := NewMocknDCWorkflow(s.controller)

	s.mockCreateMgr.EXPECT().dispatchForNewWorkflow(
		ctx, now, targetWorkflow,
	).Return(nil).Times(1)

	err := s.transactionMgr.createWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
}

func (s *nDCTransactionMgrSuite) TestUpdateWorkflow() {
	ctx := ctx.Background()
	now := time.Now()
	isWorkflowRebuilt := true
	targetWorkflow := NewMocknDCWorkflow(s.controller)
	newWorkflow := NewMocknDCWorkflow(s.controller)

	s.mockUpdateMgr.EXPECT().dispatchForExistingWorkflow(
		ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow,
	).Return(nil).Times(1)

	err := s.transactionMgr.updateWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CurrentGuaranteed() {
	ctx := ctx.Background()
	now := time.Now()

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	mutableState.On("IsCurrentWorkflowGuaranteed").Return(true)

	context.On(
		"persistNonFirstWorkflowEvents", workflowEvents,
	).Return(int64(0), nil).Once()
	context.On(
		"updateWorkflowExecutionWithNew", now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Once()

	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CheckDB_NotCurrent() {
	ctx := ctx.Background()
	now := time.Now()

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	context := &mockWorkflowExecutionContext{}
	defer context.AssertExpectations(s.T())
	mutableState := &mockMutableState{}
	defer mutableState.AssertExpectations(s.T())
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	mutableState.On("IsCurrentWorkflowGuaranteed").Return(false)
	mutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil).Once()

	context.On(
		"persistNonFirstWorkflowEvents", workflowEvents,
	).Return(int64(0), nil).Once()
	context.On(
		"updateWorkflowExecutionWithNew", now, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Once()

	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CheckDB_Current() {
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

	workflowEvents := &persistence.WorkflowEvents{}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	mutableState.On("IsCurrentWorkflowGuaranteed").Return(false)
	mutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	})

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil).Once()

	context.On(
		"persistNonFirstWorkflowEvents", workflowEvents,
	).Return(int64(0), nil).Once()
	context.On(
		"updateWorkflowExecutionWithNew", now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Once()

	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestGetWorkflowCurrentRunID_Missing() {
	ctx := ctx.Background()
	domainID := "some random domain ID"
	workflowID := "some random workflow ID"

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(nil, &shared.EntityNotExistsError{}).Once()

	currentRunID, err := s.transactionMgr.getCurrentWorkflowRunID(ctx, domainID, workflowID)
	s.NoError(err)
	s.Equal("", currentRunID)
}

func (s *nDCTransactionMgrSuite) TestGetWorkflowCurrentRunID_Exists() {
	ctx := ctx.Background()
	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil).Once()

	currentRunID, err := s.transactionMgr.getCurrentWorkflowRunID(ctx, domainID, workflowID)
	s.NoError(err)
	s.Equal(runID, currentRunID)
}
