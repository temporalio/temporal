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

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

type (
	transactionMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.ContextTest
		mockCreateMgr        *MocktransactionMgrForNewWorkflow
		mockUpdateMgr        *MocktransactionMgrForExistingWorkflow
		mockEventsReapplier  *MockEventsReapplier
		mockWorkflowResetter *MockWorkflowResetter
		mockClusterMetadata  *cluster.MockMetadata

		mockExecutionMgr *persistence.MockExecutionManager

		logger         log.Logger
		namespaceEntry *namespace.Namespace

		transactionMgr *transactionMgrImpl
	}
)

func TestTransactionMgrSuite(t *testing.T) {
	s := new(transactionMgrSuite)
	suite.Run(t, s)
}

func (s *transactionMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockCreateMgr = NewMocktransactionMgrForNewWorkflow(s.controller)
	s.mockUpdateMgr = NewMocktransactionMgrForExistingWorkflow(s.controller)
	s.mockEventsReapplier = NewMockEventsReapplier(s.controller)
	s.mockWorkflowResetter = NewMockWorkflowResetter(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr

	s.logger = s.mockShard.GetLogger()
	s.namespaceEntry = tests.GlobalNamespaceEntry

	s.transactionMgr = NewTransactionManager(
		s.mockShard,
		wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler),
		s.mockEventsReapplier,
		s.logger,
		false,
	)
	s.transactionMgr.createMgr = s.mockCreateMgr
	s.transactionMgr.updateMgr = s.mockUpdateMgr
	s.transactionMgr.workflowResetter = s.mockWorkflowResetter
}

func (s *transactionMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *transactionMgrSuite) TestCreateWorkflow() {
	ctx := context.Background()
	targetWorkflow := NewMockWorkflow(s.controller)

	s.mockCreateMgr.EXPECT().dispatchForNewWorkflow(
		ctx, targetWorkflow,
	).Return(nil)

	err := s.transactionMgr.CreateWorkflow(ctx, targetWorkflow)
	s.NoError(err)
}

func (s *transactionMgrSuite) TestUpdateWorkflow() {
	ctx := context.Background()
	isWorkflowRebuilt := true
	targetWorkflow := NewMockWorkflow(s.controller)
	newWorkflow := NewMockWorkflow(s.controller)

	s.mockUpdateMgr.EXPECT().dispatchForExistingWorkflow(
		ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow,
	).Return(nil)

	err := s.transactionMgr.UpdateWorkflow(ctx, isWorkflowRebuilt, targetWorkflow, newWorkflow)
	s.NoError(err)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Active_Open() {
	ctx := context.Background()
	releaseCalled := false
	runID := uuid.New()

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	mutableState.EXPECT().VisitUpdates(gomock.Any()).Return()
	mutableState.EXPECT().GetCurrentVersion().Return(int64(0))
	updateRegistry := update.NewRegistry(mutableState)
	var releaseFn wcache.ReleaseCacheFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 1}},
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockEventsReapplier.EXPECT().ReapplyEvents(ctx, mutableState, updateRegistry, workflowEvents.Events, runID).Return(workflowEvents.Events, nil)

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: runID})
	mutableState.EXPECT().AddHistorySize(historySize)
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, workflow.TransactionPolicyActive, (*workflow.TransactionPolicy)(nil),
	).Return(nil)
	weContext.EXPECT().UpdateRegistry(ctx, nil).Return(updateRegistry)
	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Active_Closed() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	LastCompletedWorkflowTaskStartedEventId := int64(9999)
	nextEventID := LastCompletedWorkflowTaskStartedEventId * 2
	lastWorkflowTaskStartedVersion := s.namespaceEntry.FailoverVersion()
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: LastCompletedWorkflowTaskStartedEventId, Version: lastWorkflowTaskStartedVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)
	histroySize := rand.Int63()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	var releaseFn wcache.ReleaseCacheFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(LastCompletedWorkflowTaskStartedEventId)
	mutableState.EXPECT().AddHistorySize(histroySize)

	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		LastCompletedWorkflowTaskStartedEventId,
		lastWorkflowTaskStartedVersion,
		nextEventID,
		gomock.Any(),
		gomock.Any(),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
	).Return(nil)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(histroySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, workflow.TransactionPolicyPassive, (*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Closed_ResetFailed() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	LastCompletedWorkflowTaskStartedEventId := int64(9999)
	nextEventID := LastCompletedWorkflowTaskStartedEventId * 2
	lastWorkflowTaskStartedVersion := s.namespaceEntry.FailoverVersion()
	versionHistory := versionhistory.NewVersionHistory([]byte("branch token"), []*historyspb.VersionHistoryItem{
		{EventId: LastCompletedWorkflowTaskStartedEventId, Version: lastWorkflowTaskStartedVersion},
	})
	histories := versionhistory.NewVersionHistories(versionHistory)

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	var releaseFn wcache.ReleaseCacheFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		VersionHistories: histories,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().GetNextEventID().Return(nextEventID).AnyTimes()
	mutableState.EXPECT().GetLastCompletedWorkflowTaskStartedEventId().Return(LastCompletedWorkflowTaskStartedEventId)
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockWorkflowResetter.EXPECT().ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		runID,
		versionHistory.GetBranchToken(),
		LastCompletedWorkflowTaskStartedEventId,
		lastWorkflowTaskStartedVersion,
		nextEventID,
		gomock.Any(),
		gomock.Any(),
		targetWorkflow,
		targetWorkflow,
		EventsReapplicationResetWorkflowReason,
		workflowEvents.Events,
		nil,
	).Return(serviceerror.NewInvalidArgument("reset fail"))

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, workflow.TransactionPolicyPassive, (*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Passive_Open() {
	ctx := context.Background()

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	var releaseFn wcache.ReleaseCacheFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{EventId: 1}},
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(true).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, workflow.TransactionPolicyPassive, (*workflow.TransactionPolicy)(nil),
	).Return(nil)
	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_CurrentWorkflow_Passive_Closed() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	var releaseFn wcache.ReleaseCacheFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeUpdateCurrent, nil, nil, workflow.TransactionPolicyPassive, (*workflow.TransactionPolicy)(nil),
	).Return(nil)

	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_NotCurrentWorkflow_Active() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	var releaseFn wcache.ReleaseCacheFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}},
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, workflow.TransactionPolicyPassive, (*workflow.TransactionPolicy)(nil),
	).Return(nil)
	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestBackfillWorkflow_NotCurrentWorkflow_Passive() {
	ctx := context.Background()

	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"
	currentRunID := "other random run ID"

	releaseCalled := false

	targetWorkflow := NewMockWorkflow(s.controller)
	weContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	var releaseFn wcache.ReleaseCacheFunc = func(error) { releaseCalled = true }

	workflowEvents := &persistence.WorkflowEvents{
		Events: []*historypb.HistoryEvent{{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		}},
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}
	historySize := rand.Int63()

	targetWorkflow.EXPECT().GetContext().Return(weContext).AnyTimes()
	targetWorkflow.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	targetWorkflow.EXPECT().GetReleaseFn().Return(releaseFn).AnyTimes()

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(s.namespaceEntry.IsGlobalNamespace(), s.namespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName).AnyTimes()

	mutableState.EXPECT().IsCurrentWorkflowGuaranteed().Return(false).AnyTimes()
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).AnyTimes()
	mutableState.EXPECT().GetNamespaceEntry().Return(s.namespaceEntry).AnyTimes()
	mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
	}).AnyTimes()
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: runID,
	}).AnyTimes()
	mutableState.EXPECT().AddHistorySize(historySize)

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: currentRunID}, nil)
	weContext.EXPECT().ReapplyEvents(gomock.Any(), s.mockShard, []*persistence.WorkflowEvents{workflowEvents})
	weContext.EXPECT().PersistWorkflowEvents(gomock.Any(), s.mockShard, workflowEvents).Return(historySize, nil)
	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(), s.mockShard, persistence.UpdateWorkflowModeBypassCurrent, nil, nil, workflow.TransactionPolicyPassive, (*workflow.TransactionPolicy)(nil),
	).Return(nil)
	err := s.transactionMgr.BackfillWorkflow(ctx, targetWorkflow, workflowEvents)
	s.NoError(err)
	s.True(releaseCalled)
}

func (s *transactionMgrSuite) TestCheckWorkflowExists_DoesNotExists() {
	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(nil, serviceerror.NewNotFound(""))

	exists, err := s.transactionMgr.CheckWorkflowExists(ctx, namespaceID, workflowID, runID)
	s.NoError(err)
	s.False(exists)
}

func (s *transactionMgrSuite) TestCheckWorkflowExists_DoesExists() {
	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(&persistence.GetWorkflowExecutionResponse{}, nil)

	exists, err := s.transactionMgr.CheckWorkflowExists(ctx, namespaceID, workflowID, runID)
	s.NoError(err)
	s.True(exists)
}

func (s *transactionMgrSuite) TestGetWorkflowCurrentRunID_Missing() {
	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(nil, serviceerror.NewNotFound(""))

	currentRunID, err := s.transactionMgr.GetCurrentWorkflowRunID(ctx, namespaceID, workflowID)
	s.NoError(err)
	s.Equal("", currentRunID)
}

func (s *transactionMgrSuite) TestGetWorkflowCurrentRunID_Exists() {
	ctx := context.Background()
	namespaceID := namespace.ID("some random namespace ID")
	workflowID := "some random workflow ID"
	runID := "some random run ID"

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), &persistence.GetCurrentExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
	}).Return(&persistence.GetCurrentExecutionResponse{RunID: runID}, nil)

	currentRunID, err := s.transactionMgr.GetCurrentWorkflowRunID(ctx, namespaceID, workflowID)
	s.NoError(err)
	s.Equal(runID, currentRunID)
}
