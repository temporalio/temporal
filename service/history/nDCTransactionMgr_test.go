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
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
)

type (
	nDCTransactionMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.ContextTest
		mockCreateMgr        *MocknDCTransactionMgrForNewWorkflow
		mockUpdateMgr        *MocknDCTransactionMgrForExistingWorkflow
		mockEventsReapplier  *MocknDCEventsReapplier
		mockWorkflowResetter *MockworkflowResetter
		mockClusterMetadata  *cluster.MockMetadata

		mockExecutionMgr *mocks.ExecutionManager

		logger         log.Logger
		namespaceEntry *cache.NamespaceCacheEntry

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
	s.mockCreateMgr = NewMocknDCTransactionMgrForNewWorkflow(s.controller)
	s.mockUpdateMgr = NewMocknDCTransactionMgrForExistingWorkflow(s.controller)
	s.mockEventsReapplier = NewMocknDCEventsReapplier(s.controller)
	s.mockWorkflowResetter = NewMockworkflowResetter(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          10,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr

	s.logger = s.mockShard.GetLogger()
	s.namespaceEntry = testGlobalNamespaceEntry

	s.transactionMgr = newNDCTransactionMgr(s.mockShard, newHistoryCache(s.mockShard), s.mockEventsReapplier, s.logger)
	s.transactionMgr.createMgr = s.mockCreateMgr
	s.transactionMgr.updateMgr = s.mockUpdateMgr
	s.transactionMgr.workflowResetter = s.mockWorkflowResetter
}

func (s *nDCTransactionMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *nDCTransactionMgrSuite) TestCreateWorkflow() {
	ctx := context.Background()
	now := time.Now().UTC()
	targetWorkflow := NewMocknDCWorkflow(s.controller)

	s.mockCreateMgr.EXPECT().dispatchForNewWorkflow(
		ctx, now, targetWorkflow,
	).Return(nil).Times(1)

	err := s.transactionMgr.createWorkflow(ctx, now, targetWorkflow)
	s.NoError(err)
}

func (s *nDCTransactionMgrSuite) TestUpdateWorkflow() {
	ctx := context.Background()
	now := time.Now().UTC()
	isWorkflowRebuilt := true
	targetWorkflow := NewMocknDCWorkflow(s.controller)
	newWorkflow := NewMocknDCWorkflow(s.controller)

	s.mockUpdateMgr.EXPECT().dispatchForExistingWorkflow(
		ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow,
	).Return(nil).Times(1)

	err := s.transactionMgr.updateWorkflow(ctx, now, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Active_Open() {
	ctx := context.Background()
	now := time.Now().UTC()
	releaseCalled := false
	runID := uuid.New()

	workflow := NewMocknDCWorkflow(s.controller)
	weContext := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 1}},
	}

	workflow.EXPECT().getContext().Return(weContext).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.GetFailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockEventsReapplier.EXPECT().reapplyEvents(ctx, mutableState, workflowEvents.Events, runID).Return(workflowEvents.Events, nil).Times(1)

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: runID}).Times(1)
	weContext.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	weContext.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyActive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)
	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Active_Closed() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	lastWorkflowTaskStartedEventID := int64(9999)
	nextEventID := lastWorkflowTaskStartedEventID * 2
	lastWorkflowTaskStartedVersion := s.namespaceEntry.GetFailoverVersion()
	versionHistory := versionhistory.New([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: lastWorkflowTaskStartedEventID, Version: lastWorkflowTaskStartedVersion},
	})
	histories := versionhistory.NewVHS(versionHistory)

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	weContext := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}

	workflow.EXPECT().getContext().Return(weContext).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.GetFailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID,
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetPreviousStartedEventID().Return(lastWorkflowTaskStartedEventID).Times(1)

	s.mockWorkflowResetter.EXPECT().resetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		lastWorkflowTaskStartedEventID,
		lastWorkflowTaskStartedVersion,
		nextEventID,
		gomock.Any(),
		gomock.Any(),
		workflow,
		eventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
	).Return(nil).Times(1)

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil).Once()

	weContext.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	weContext.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)

	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Passive_Open() {
	ctx := context.Background()
	now := time.Now().UTC()
	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	weContext := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 1}},
	}

	workflow.EXPECT().getContext().Return(weContext).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.GetFailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	weContext.EXPECT().reapplyEvents([]*persistence.WorkflowEvents{workflowEvents}).Times(1)
	weContext.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	weContext.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)
	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Passive_Closed() {
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

	workflowEvents := &persistence.WorkflowEvents{}

	workflow.EXPECT().getContext().Return(weContext).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.GetFailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil).Once()
	weContext.EXPECT().reapplyEvents([]*persistence.WorkflowEvents{workflowEvents}).Times(1)
	weContext.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	weContext.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)

	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_NotCurrentWorkflow_Active() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	weContext := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}},
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}

	workflow.EXPECT().getContext().Return(weContext).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.GetFailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil).Once()
	weContext.EXPECT().reapplyEvents([]*persistence.WorkflowEvents{workflowEvents}).Times(1)
	weContext.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	weContext.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)
	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestBackfillWorkflow_NotCurrentWorkflow_Passive() {
	ctx := context.Background()
	now := time.Now().UTC()

	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	workflow := NewMocknDCWorkflow(s.controller)
	weContext := NewMockworkflowExecutionContext(s.controller)
	mutableState := NewMockmutableState(s.controller)
	var releaseFn releaseWorkflowExecutionFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}},
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}

	workflow.EXPECT().getContext().Return(weContext).AnyTimes()
	workflow.EXPECT().getMutableState().Return(mutableState).AnyTimes()
	workflow.EXPECT().getReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.GetFailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID,
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil).Once()
	weContext.EXPECT().reapplyEvents([]*persistence.WorkflowEvents{workflowEvents}).Times(1)
	weContext.EXPECT().persistNonFirstWorkflowEvents(workflowEvents).Return(int64(0), nil).Times(1)
	weContext.EXPECT().updateWorkflowExecutionWithNew(
		now, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, transactionPolicyPassive, (*transactionPolicy)(nil),
	).Return(nil).Times(1)
	err := s.transactionMgr.backfillWorkflow(ctx, now, workflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *nDCTransactionMgrSuite) TestCheckWorkflowExists_DoesNotExists() {
	ctx := context.Background()
	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}).Return(nil, serviceerror.NewNotFound("")).Once()

	exists, err := s.transactionMgr.checkWorkflowExists(ctx, namespaceID, workflowID, runID)
	s.NoError(err)
	s.False(exists)
}

func (s *nDCTransactionMgrSuite) TestCheckWorkflowExists_DoesExists() {
	ctx := context.Background()
	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.On("GetWorkflowExecution", &persistence.GetWorkflowExecutionRequest{
		NamespaceID: namespaceID,
		Execution: commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}).Return(&persistence.GetWorkflowExecutionResponse{}, nil).Once()

	exists, err := s.transactionMgr.checkWorkflowExists(ctx, namespaceID, workflowID, runID)
	s.NoError(err)
	s.True(exists)
}

func (s *nDCTransactionMgrSuite) TestGetWorkflowCurrentRunID_Missing() {
	ctx := context.Background()
	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(nil, serviceerror.NewNotFound("")).Once()

	currentRunID, err := s.transactionMgr.getCurrentWorkflowRunID(ctx, namespaceID, workflowID)
	s.NoError(err)
	s.Equal("", currentRunID)
}

func (s *nDCTransactionMgrSuite) TestGetWorkflowCurrentRunID_Exists() {
	ctx := context.Background()
	namespaceID := "some random namespace ID"
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.On("GetCurrentExecution", &persistence.GetCurrentExecutionRequest{
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil).Once()

	currentRunID, err := s.transactionMgr.getCurrentWorkflowRunID(ctx, namespaceID, workflowID)
	s.NoError(err)
	s.Equal(runID, currentRunID)
}
