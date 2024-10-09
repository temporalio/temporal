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

package replication

import (
	"context"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	syncWorkflowStateSuite struct {
		suite.Suite
		*require.Assertions
		workflowCache              *wcache.MockCache
		logger                     log.Logger
		mockShard                  *shard.ContextTest
		controller                 *gomock.Controller
		releaseFunc                func(err error)
		workflowContext            *workflow.MockContext
		newRunWorkflowContext      *workflow.MockContext
		namespaceID                string
		execution                  *common.WorkflowExecution
		newRunId                   string
		syncStateRetriever         *SyncStateRetrieverImpl
		workflowConsistencyChecker *api.MockWorkflowConsistencyChecker
	}
)

func TestSyncWorkflowState(t *testing.T) {
	s := new(syncWorkflowStateSuite)
	suite.Run(t, s)
}

func (s *syncWorkflowStateSuite) SetupSuite() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.workflowContext = workflow.NewMockContext(s.controller)
	s.newRunWorkflowContext = workflow.NewMockContext(s.controller)
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	s.releaseFunc = func(err error) {}
	s.workflowCache = wcache.NewMockCache(s.controller)
	s.logger = s.mockShard.GetLogger()
	s.namespaceID = tests.NamespaceID.String()
	s.execution = &common.WorkflowExecution{
		WorkflowId: uuid.New(),
		RunId:      uuid.New(),
	}
	s.newRunId = uuid.New()
	s.workflowConsistencyChecker = api.NewMockWorkflowConsistencyChecker(s.controller)
	s.syncStateRetriever = NewSyncStateRetriever(s.mockShard, s.workflowCache, s.workflowConsistencyChecker, s.logger)
}

func (s *syncWorkflowStateSuite) TearDownSuite() {
}

func (s *syncWorkflowStateSuite) SetupTest() {

}

func (s *syncWorkflowStateSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_TransitionHistoryDisabled() {
	mu := workflow.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)

	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: nil, // transition history is disabled
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		nil,
		nil,
	)
	s.Nil(result)
	s.Error(err)
	s.ErrorIs(err, consts.ErrTransitionHistoryDisabled)
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_ReturnMutation() {
	mu := workflow.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
	versionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories: versionHistories,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
		ExecutionInfo: executionInfo,
	})
	mu.EXPECT().HSM().Return(nil)
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          12,
		},
		versionHistories)
	s.NoError(err)
	s.NotNil(result)
	s.Nil(result.Snapshot)
	s.NotNil(result.Mutation)
	s.Equal(result.Type, Mutation)
	s.Nil(result.EventBlobs)
	s.Nil(result.NewRunInfo)
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_ReturnSnapshot() {
	mu := workflow.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
	versionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories: versionHistories,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
		ExecutionInfo: executionInfo,
	})
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		&persistencespb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          13,
		},
		versionHistories)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Snapshot)
	s.Nil(result.Mutation)
	s.Equal(result.Type, Snapshot)
	s.Nil(result.EventBlobs)
	s.Nil(result.NewRunInfo)
}

func (s *syncWorkflowStateSuite) TestSyncWorkflowState_NoVersionTransitionProvided_ReturnSnapshot() {
	mu := workflow.NewMockMutableState(s.controller)
	s.workflowConsistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(gomock.Any(), nil, gomock.Any(), definition.WorkflowKey{
		NamespaceID: s.namespaceID,
		WorkflowID:  s.execution.WorkflowId,
		RunID:       s.execution.RunId,
	}, locks.PriorityLow).Return(
		api.NewWorkflowLease(nil, func(err error) {}, mu), nil)
	versionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories: versionHistories,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	mu.EXPECT().CloneToProto().Return(&persistencespb.WorkflowMutableState{
		ExecutionInfo: executionInfo,
	})
	result, err := s.syncStateRetriever.GetSyncWorkflowStateArtifact(
		context.Background(),
		s.namespaceID,
		s.execution,
		nil,
		versionHistories)
	s.NoError(err)
	s.NotNil(result)
	s.NotNil(result.Snapshot)
	s.Nil(result.Mutation)
	s.Equal(result.Type, Snapshot)
	s.Nil(result.EventBlobs)
	s.Nil(result.NewRunInfo)
}

func (s *syncWorkflowStateSuite) TestGetNewRunInfo() {
	mu := workflow.NewMockMutableState(s.controller)
	versionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:  versionHistories,
		NewExecutionRunId: s.newRunId,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.mockShard, namespace.ID(s.namespaceID), &common.WorkflowExecution{
		WorkflowId: s.execution.WorkflowId,
		RunId:      s.newRunId,
	}, locks.PriorityLow).Return(s.newRunWorkflowContext, s.releaseFunc, nil)
	s.newRunWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).
		Return(mu, nil).Times(1)

	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: versionHistories.Histories[0].GetBranchToken(),
		MinEventID:  1,
		MaxEventID:  2,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*common.DataBlob{
			{Data: []byte("event1")}},
	}, nil)
	newRunInfo, err := s.syncStateRetriever.getNewRunInfo(context.Background(), namespace.ID(s.namespaceID), s.execution, s.newRunId)
	s.NoError(err)
	s.NotNil(newRunInfo)
}

func (s *syncWorkflowStateSuite) TestGetNewRunInfo_NotFound() {
	mu := workflow.NewMockMutableState(s.controller)
	versionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("branchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 2, Version: 13},
				},
			},
		},
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencespb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 12},
			{NamespaceFailoverVersion: 2, TransitionCount: 15},
		},
		SubStateMachineTombstoneBatches: []*persistencespb.StateMachineTombstoneBatch{
			{
				VersionedTransition: &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 12},
			},
		},
		VersionHistories:  versionHistories,
		NewExecutionRunId: s.newRunId,
	}
	mu.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), s.mockShard, namespace.ID(s.namespaceID), &common.WorkflowExecution{
		WorkflowId: s.execution.WorkflowId,
		RunId:      s.newRunId,
	}, locks.PriorityLow).Return(s.newRunWorkflowContext, s.releaseFunc, nil)
	s.newRunWorkflowContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).
		Return(nil, serviceerror.NewNotFound("not found")).Times(1)

	newRunInfo, err := s.syncStateRetriever.getNewRunInfo(context.Background(), namespace.ID(s.namespaceID), s.execution, s.newRunId)
	s.NoError(err)
	s.Nil(newRunInfo)
}

func (s *syncWorkflowStateSuite) TestGetSyncStateEvents() {
	targetVersionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 1,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("target ranchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 18, Version: 13},
				},
			},
			{
				BranchToken: []byte("target ranchToken2"),
				Items: []*history.VersionHistoryItem{
					{EventId: 10, Version: 10},
				},
			},
		},
	}
	sourceVersionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("source branchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 20, Version: 13},
				},
			},
		},
	}
	s.mockShard.Resource.ExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken: sourceVersionHistories.Histories[0].GetBranchToken(),
		MinEventID:  19,
		MaxEventID:  21,
		ShardID:     s.mockShard.GetShardID(),
		PageSize:    defaultPageSize,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*common.DataBlob{
			{Data: []byte("event1")}},
	}, nil)

	events, err := s.syncStateRetriever.getSyncStateEvents(context.Background(), targetVersionHistories, sourceVersionHistories)

	s.NoError(err)
	s.NotNil(events)
}

func (s *syncWorkflowStateSuite) TestGetSyncStateEvents_EventsUpToDate_ReturnNothing() {
	targetVersionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("target ranchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 18, Version: 13},
				},
			},
		},
	}
	sourceVersionHistories := &history.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*history.VersionHistory{
			{
				BranchToken: []byte("source branchToken1"),
				Items: []*history.VersionHistoryItem{
					{EventId: 1, Version: 10},
					{EventId: 18, Version: 13},
				},
			},
		},
	}

	events, err := s.syncStateRetriever.getSyncStateEvents(context.Background(), targetVersionHistories, sourceVersionHistories)

	s.NoError(err)
	s.Nil(events)
}

func (s *syncWorkflowStateSuite) TestGetUpdatedSubStateMachine() {
	reg := hsm.NewRegistry()
	var def1 = hsmtest.NewDefinition("type1")
	err := reg.RegisterMachine(def1)
	s.NoError(err)

	root, err := hsm.NewRoot(reg, def1.Type(), hsmtest.NewData(hsmtest.State1), make(map[string]*persistencespb.StateMachineMap), &hsmtest.NodeBackend{})
	s.NoError(err)
	root.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 10}
	child1, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "child1"}, hsmtest.NewData(hsmtest.State1))
	s.Nil(err)
	child1.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 8}
	child2, err := root.AddChild(hsm.Key{Type: def1.Type(), ID: "child2"}, hsmtest.NewData(hsmtest.State1))
	s.Nil(err)
	child2.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 10}

	result, err := s.syncStateRetriever.getUpdatedSubStateMachine(root, &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 9})
	s.NoError(err)
	s.Equal(1, len(result))
	s.Equal(len(child2.Path()), len(result[0].Path.Path))
	s.Equal(child2.Path()[0].ID, result[0].Path.Path[0].Id)
}
