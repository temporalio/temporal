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
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/resource"
)

type (
	nDCTransactionMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockResource        *resource.Test
		mockCreateMgr       *MocknDCTransactionMgrForNewWorkflow
		mockUpdateMgr       *MocknDCTransactionMgrForExistingWorkflow
		mockEventsReapplier *MocknDCEventsReapplier
		mockClusterMetadata *cluster.MockMetadata

		mockShard        *shardContextImpl
		mockExecutionMgr *mocks.ExecutionManager

		logger log.Logger

		transactionMgr *nDCTransactionMgrImpl
	}
)

func TestNDCTransactionMgrSuite(t *testing.T) {
	s := new(nDCTransactionMgrSuite)
	suite.Run(t, s)
}

func (s *nDCTransactionMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.mockCreateMgr = NewMocknDCTransactionMgrForNewWorkflow(s.controller)
	s.mockUpdateMgr = NewMocknDCTransactionMgrForExistingWorkflow(s.controller)
	s.mockEventsReapplier = NewMocknDCEventsReapplier(s.controller)

	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockExecutionMgr = s.mockResource.ExecutionMgr

	s.logger = s.mockResource.Logger
	s.mockShard = &shardContextImpl{
		Resource:                  s.mockResource,
		shardInfo:                 &persistence.ShardInfo{ShardID: 10, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		executionManager:          s.mockExecutionMgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
	}

	s.transactionMgr = newNDCTransactionMgr(s.mockShard, newHistoryCache(s.mockShard), s.mockEventsReapplier, s.logger)
	s.transactionMgr.createMgr = s.mockCreateMgr
	s.transactionMgr.updateMgr = s.mockUpdateMgr
}

func (s *nDCTransactionMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
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

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CurrentGuaranteed_Active_ReapplyEvents() {
	ctx := ctx.Background()
	now := time.Now()
	currentVersion := int64(1234)
	releaseCalled := false
	runID := uuid.New()

	workflow := NewMocknDCWorkflow(s.controller)
	context := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*shared.HistoryEvent{{EventId: common.Int64Ptr(1)}},
	}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockEventsReapplier.EXPECT().reapplyEvents(ctx, mutableState, workflowEvents.Events, runID).Return(workflowEvents.Events, nil).Times(1)

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().GetCurrentVersion().Return(currentVersion).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{RunID: runID}).Times(1)
	context.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	context.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyActive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)
	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CurrentGuaranteed_Passive_NoReapplyEvents() {
	ctx := ctx.Background()
	now := time.Now()
	currentVersion := int64(1234)
	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	context := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*shared.HistoryEvent{{EventId: common.Int64Ptr(1)}},
	}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().GetCurrentVersion().Return(currentVersion).AnyTimes()
	context.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	context.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)
	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CheckDB_NotCurrent_Active() {
	ctx := ctx.Background()
	now := time.Now()
	currentVersion := int64(1234)

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	context := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*shared.HistoryEvent{{
			EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionSignaled),
		}},
		DomainID:   domainID,
		WorkflowID: workflowID,
	}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().GetCurrentVersion().Return(currentVersion).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil).Once()

	context.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	context.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)
	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CheckDB_NotCurrent_Passive() {
	ctx := ctx.Background()
	now := time.Now()
	currentVersion := int64(1234)

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	context := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*shared.HistoryEvent{{
			EventType: common.EventTypePtr(shared.EventTypeWorkflowExecutionSignaled),
		}},
		DomainID:   domainID,
		WorkflowID: workflowID,
	}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().GetCurrentVersion().Return(currentVersion).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil).Once()

	context.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	context.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)
	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CheckDB_Current_Active() {
	ctx := ctx.Background()
	now := time.Now()
	currentVersion := int64(1234)

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	context := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().GetCurrentVersion().Return(currentVersion).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil).Once()

	context.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	context.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyActive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)

	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CheckDB_Current_Passive() {
	ctx := ctx.Background()
	now := time.Now()
	currentVersion := int64(1234)

	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	context := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}

	workflow.EXPECT().getContext().Return(context).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(currentVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().GetCurrentVersion().Return(currentVersion).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   domainID,
		WorkflowID: workflowID,
		RunID:      runID,
	}).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		DomainID:   domainID,
		WorkflowID: workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil).Once()

	context.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	context.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)

	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestCheckWorkflowExists_DoesNotExists() {
	ctx := ctx.Background()
	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}).Return(nil, &shared.EntityNotExistsError{}).Once()

	exists, err := s.transactionMgr.checkWorkflowExists(ctx, domainID, workflowID, runID)
	s.NoError(err)
	s.False(exists)
}

func (s *nDCTransactionMgrSuite) TestCheckWorkflowExists_DoesExists() {
	ctx := ctx.Background()
	domainID := "some random domain ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		DomainID: domainID,
		Execution: shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{}, nil).Once()

	exists, err := s.transactionMgr.checkWorkflowExists(ctx, domainID, workflowID, runID)
	s.NoError(err)
	s.True(exists)
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
