// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"fmt"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	hsmStateReplicatorSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockNamespaceCache  *namespace.MockRegistry
		mockClusterMetadata *cluster.MockMetadata
		mockMutableState    *workflow.MockMutableState

		mockExecutionMgr *persistence.MockExecutionManager

		workflowCache wcache.Cache
		logger        log.Logger

		workflowKey     definition.WorkflowKey
		namespaceEntry  *namespace.Namespace
		stateMachineDef hsm.StateMachineDefinition

		nDCHSMStateReplicator *HSMStateReplicatorImpl
	}
)

type testBackend struct {
	nextTransitionCount int64
	currentVersion      int64
}

func (b *testBackend) AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
	return nil // Not needed for deletion tests
}

func (b *testBackend) LoadHistoryEvent(ctx context.Context, token []byte) (*historypb.HistoryEvent, error) {
	return nil, nil // Not needed for deletion tests
}

func (b *testBackend) GetCurrentVersion() int64 {
	return b.currentVersion
}

func (b *testBackend) NextTransitionCount() int64 {
	return b.nextTransitionCount
}

func TestHSMStateReplicatorSuite(t *testing.T) {
	s := new(hsmStateReplicatorSuite)
	suite.Run(t, s)
}

func (s *hsmStateReplicatorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockMutableState = workflow.NewMockMutableState(s.controller)
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	mockEngine := shard.NewMockEngine(s.controller)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().NotifyNewHistoryEvent(gomock.Any()).AnyTimes()
	mockEngine.EXPECT().Stop().MaxTimes(1)
	s.mockShard.SetEngineForTesting(mockEngine)

	stateMachineRegistry := s.mockShard.StateMachineRegistry()
	err := workflow.RegisterStateMachine(stateMachineRegistry)
	s.NoError(err)
	s.stateMachineDef = hsmtest.NewDefinition("test")
	err = stateMachineRegistry.RegisterMachine(s.stateMachineDef)
	s.NoError(err)
	err = stateMachineRegistry.RegisterTaskSerializer(hsmtest.TaskType, hsmtest.TaskSerializer{})
	s.NoError(err)

	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), s.mockShard.GetLogger(), metrics.NoopMetricsHandler)

	s.namespaceEntry = tests.GlobalNamespaceEntry
	s.workflowKey = definition.NewWorkflowKey(s.namespaceEntry.ID().String(), tests.WorkflowID, tests.RunID)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceEntry.ID()).Return(s.namespaceEntry, nil).AnyTimes()

	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(cluster.TestCurrentClusterInitialFailoverVersion, s.namespaceEntry.FailoverVersion()).Return(true).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.nDCHSMStateReplicator = NewHSMStateReplicator(
		s.mockShard,
		s.workflowCache,
		s.logger,
	)
}

func (s *hsmStateReplicatorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_WorkflowNotFound() {
	nonExistKey := definition.NewWorkflowKey(
		s.namespaceEntry.ID().String(),
		"non-exist workflowID",
		uuid.New(),
	)

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: nonExistKey.NamespaceID,
		WorkflowID:  nonExistKey.WorkflowID,
		RunID:       nonExistKey.RunID,
	}).Return(nil, serviceerror.NewNotFound("")).Times(1)

	lastEventID := int64(10)
	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey: nonExistKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				{EventId: lastEventID, Version: s.namespaceEntry.FailoverVersion()},
			},
		},
	})
	s.Error(err)
	retryReplicationErr, ok := err.(*serviceerrors.RetryReplication)
	s.True(ok)
	s.Equal(nonExistKey.NamespaceID, retryReplicationErr.NamespaceId)
	s.Equal(nonExistKey.WorkflowID, retryReplicationErr.WorkflowId)
	s.Equal(nonExistKey.RunID, retryReplicationErr.RunId)
	s.Equal(common.EmptyEventID, retryReplicationErr.StartEventId)
	s.Equal(common.EmptyVersion, retryReplicationErr.StartEventVersion)
	s.Equal(lastEventID+1, retryReplicationErr.EndEventId)
	s.Equal(s.namespaceEntry.FailoverVersion(), retryReplicationErr.EndEventVersion)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_Diverge_LocalEventVersionLarger() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey: s.workflowKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				// incoming version smaller, should not sync
				{EventId: 102, Version: s.namespaceEntry.FailoverVersion() - 100},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_Diverge_IncomingEventVersionLarger() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey: s.workflowKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				// incoming version large, should resend history
				{EventId: 80, Version: s.namespaceEntry.FailoverVersion() - 100},
				{EventId: 202, Version: s.namespaceEntry.FailoverVersion() + 100},
			},
		},
	})
	s.Error(err)
	retryReplicationErr, ok := err.(*serviceerrors.RetryReplication)
	s.True(ok)
	s.Equal(s.workflowKey.NamespaceID, retryReplicationErr.NamespaceId)
	s.Equal(s.workflowKey.WorkflowID, retryReplicationErr.WorkflowId)
	s.Equal(s.workflowKey.RunID, retryReplicationErr.RunId)
	s.Equal(int64(50), retryReplicationErr.StartEventId) // LCA
	s.Equal(s.namespaceEntry.FailoverVersion()-100, retryReplicationErr.StartEventVersion)
	s.Equal(int64(203), retryReplicationErr.EndEventId)
	s.Equal(s.namespaceEntry.FailoverVersion()+100, retryReplicationErr.EndEventVersion)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_LocalEventVersionSuperSet() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	// Only asserting state sync happens here
	// There are other tests asserting the actual state sync result
	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.UpdateWorkflowExecutionResponse{
		UpdateMutableStateStats: persistence.MutableStateStatistics{
			HistoryStatistics: &persistence.HistoryStatistics{},
		},
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey: s.workflowKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				// incoming is a subset of local version history, should sync
				{EventId: 50, Version: s.namespaceEntry.FailoverVersion() - 100},
			},
		},
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							// despite local has more events, incoming state could still be newer for a certain node
							// and state should be synced
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingEventVersionSuperSet() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey: s.workflowKey,
		EventVersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{
				// incoming version large, should resend history
				{EventId: 50, Version: s.namespaceEntry.FailoverVersion() - 100},
				{EventId: 202, Version: s.namespaceEntry.FailoverVersion()},
				{EventId: 302, Version: s.namespaceEntry.FailoverVersion() + 100},
			},
		},
	})
	s.Error(err)
	retryReplicationErr, ok := err.(*serviceerrors.RetryReplication)
	s.True(ok)
	s.Equal(s.workflowKey.NamespaceID, retryReplicationErr.NamespaceId)
	s.Equal(s.workflowKey.WorkflowID, retryReplicationErr.WorkflowId)
	s.Equal(s.workflowKey.RunID, retryReplicationErr.RunId)
	s.Equal(int64(102), retryReplicationErr.StartEventId)
	s.Equal(s.namespaceEntry.FailoverVersion(), retryReplicationErr.StartEventVersion)
	s.Equal(int64(303), retryReplicationErr.EndEventId)
	s.Equal(s.namespaceEntry.FailoverVersion()+100, retryReplicationErr.EndEventVersion)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingStateStale() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State1), // stale state
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingLastUpdateVersionStale() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3), // newer state
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								// smaller than current node last updated version
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 50,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingLastUpdateVersionedTransitionStale() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3), // newer state
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
								// smaller than current node last update transition count
								TransitionCount: 49,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingLastUpdateVersionNewer() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.UpdateWorkflowExecutionResponse{
		UpdateMutableStateStats: persistence.MutableStateStatistics{
			HistoryStatistics: &persistence.HistoryStatistics{},
		},
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State1), // state stale
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								// newer than current node last update version
								// should sync despite state is older than the current node
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 200,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingLastUpdateVersionedTransitionNewer() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.UpdateWorkflowExecutionResponse{
		UpdateMutableStateStats: persistence.MutableStateStatistics{
			HistoryStatistics: &persistence.HistoryStatistics{},
		},
	}, nil).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
								// higher transition count
								TransitionCount: 51,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingStateNewer_WorkflowOpen() {
	persistedState := s.buildWorkflowMutableState()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			s.Equal(persistence.UpdateWorkflowModeUpdateCurrent, request.Mode)

			subStateMachineByType := request.UpdateWorkflowMutation.ExecutionInfo.SubStateMachinesByType
			s.Len(subStateMachineByType, 1)
			machines := subStateMachineByType[s.stateMachineDef.Type()]
			s.Len(machines.MachinesById, 1)
			machine := machines.MachinesById["child1"]
			s.Equal([]byte(hsmtest.State3), machine.Data)
			s.Equal(int64(24), machine.TransitionCount) // transition count is cluster local and should only be increamented by 1
			s.Len(request.UpdateWorkflowMutation.Tasks[tasks.CategoryTimer], 1)
			s.Len(request.UpdateWorkflowMutation.Tasks[tasks.CategoryOutbound], 1)
			s.Empty(request.UpdateWorkflowEvents)
			s.Empty(request.NewWorkflowEvents)
			s.Empty(request.NewWorkflowSnapshot)
			return &persistence.UpdateWorkflowExecutionResponse{
				UpdateMutableStateStats: persistence.MutableStateStatistics{
					HistoryStatistics: &persistence.HistoryStatistics{},
				},
			}, nil
		},
	).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingStateNewer_WorkflowZombie() {
	persistedState := s.buildWorkflowMutableState()
	persistedState.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	persistedState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			s.Equal(persistence.UpdateWorkflowModeBypassCurrent, request.Mode)
			// other fields are tested in TestSyncHSM_IncomingStateNewer_WorkflowOpen
			return &persistence.UpdateWorkflowExecutionResponse{
				UpdateMutableStateStats: persistence.MutableStateStatistics{
					HistoryStatistics: &persistence.HistoryStatistics{},
				},
			}, nil
		},
	).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_IncomingStateNewer_WorkflowClosed() {
	persistedState := s.buildWorkflowMutableState()
	persistedState.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	persistedState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: s.workflowKey.NamespaceID,
		WorkflowID:  s.workflowKey.WorkflowID,
		RunID:       s.workflowKey.RunID,
	}).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	s.mockExecutionMgr.EXPECT().SetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.SetWorkflowExecutionRequest) (*persistence.SetWorkflowExecutionResponse, error) {

			subStateMachineByType := request.SetWorkflowSnapshot.ExecutionInfo.SubStateMachinesByType
			s.Len(subStateMachineByType, 1)
			machines := subStateMachineByType[s.stateMachineDef.Type()]
			s.Len(machines.MachinesById, 1)
			machine := machines.MachinesById["child1"]
			s.Equal([]byte(hsmtest.State3), machine.Data)
			s.Equal(int64(24), machine.TransitionCount) // transition count is cluster local and should only be increamented by 1
			s.Len(request.SetWorkflowSnapshot.Tasks[tasks.CategoryTimer], 1)
			s.Len(request.SetWorkflowSnapshot.Tasks[tasks.CategoryOutbound], 1)
			return &persistence.SetWorkflowExecutionResponse{}, nil
		},
	).Times(1)

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode: &persistencespb.StateMachineNode{
			Children: map[string]*persistencespb.StateMachineMap{
				s.stateMachineDef.Type(): {
					MachinesById: map[string]*persistencespb.StateMachineNode{
						"child1": {
							Data: []byte(hsmtest.State3),
							InitialVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							},
							LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
								NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
							},
							TransitionCount: 50,
						},
					},
				},
			},
		},
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_DeleteNode_Success() {
	persistedState := s.buildWorkflowMutableState()

	targetFailoverVersion := s.namespaceEntry.FailoverVersion()
	sourceFailoverVersion := targetFailoverVersion + 500

	// Set up target cluster state
	targetNode := persistedState.ExecutionInfo.SubStateMachinesByType[s.stateMachineDef.Type()].MachinesById["child1"]
	targetNode.InitialVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: targetFailoverVersion,
		TransitionCount:          10,
	}
	targetNode.LastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: targetFailoverVersion,
		TransitionCount:          10,
	}
	targetNode.Data = []byte(hsmtest.State2)

	// Set up mocks
	var replicationLogs []string
	logReplication := func(msg string) {
		replicationLogs = append(replicationLogs, msg)
		s.T().Logf("REPLICATION: %s", msg)
	}

	s.mockMutableState.EXPECT().GetWorkflowStateStatus().DoAndReturn(func() (enumsspb.WorkflowExecutionState, enumspb.WorkflowExecutionStatus) {
		logReplication("GetWorkflowStateStatus called")
		return enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	}).AnyTimes()

	s.mockMutableState.EXPECT().GetExecutionInfo().DoAndReturn(func() *persistencespb.WorkflowExecutionInfo {
		logReplication("GetExecutionInfo called")
		return persistedState.ExecutionInfo
	}).AnyTimes()

	s.mockMutableState.EXPECT().GetWorkflowKey().DoAndReturn(func() definition.WorkflowKey {
		logReplication("GetWorkflowKey called")
		return s.workflowKey
	}).AnyTimes()

	s.mockMutableState.EXPECT().HSM().DoAndReturn(func() *hsm.Node {
		logReplication("HSM called")
		targetRoot, err := hsm.NewRoot(
			s.mockShard.StateMachineRegistry(),
			workflow.StateMachineType,
			persistedState.ExecutionInfo,
			persistedState.ExecutionInfo.SubStateMachinesByType,
			s.mockMutableState,
		)
		s.NoError(err)
		return targetRoot
	}).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.GetWorkflowExecutionRequest) (*persistence.GetWorkflowExecutionResponse, error) {
			logReplication("GetWorkflowExecution called")
			return &persistence.GetWorkflowExecutionResponse{
				State:           persistedState,
				DBRecordVersion: 777,
			}, nil
		}).Times(1)

	s.mockExecutionMgr.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, request *persistence.UpdateWorkflowExecutionRequest) (*persistence.UpdateWorkflowExecutionResponse, error) {
			info := request.UpdateWorkflowMutation.ExecutionInfo
			// Check that the entire entry for this machine type is removed
			_, ok := info.SubStateMachinesByType[s.stateMachineDef.Type()]
			s.False(ok, "Entire state machine type entry should be removed since no machines remain")

			return &persistence.UpdateWorkflowExecutionResponse{
				UpdateMutableStateStats: persistence.MutableStateStatistics{
					HistoryStatistics: &persistence.HistoryStatistics{},
				},
			}, nil
		},
	).Times(1)

	fmt.Printf("TEST: Target state=%+v\n", persistedState.ExecutionInfo.SubStateMachinesByType)

	// Create source state representing deletion
	sourceNode := &persistencespb.StateMachineNode{
		InitialVersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: targetFailoverVersion, // Same initial version
			TransitionCount:          10,
		},
		LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
			NamespaceFailoverVersion: sourceFailoverVersion, // Higher version
			TransitionCount:          50,
		},
		Children: map[string]*persistencespb.StateMachineMap{}, // Empty children indicating deletion
	}

	fmt.Printf("TEST: Source state=%+v\n", sourceNode)

	logReplication("=== Version Info ===")
	logReplication(fmt.Sprintf("Source initial version: %+v", sourceNode.InitialVersionedTransition))
	logReplication(fmt.Sprintf("Source last update: %+v", sourceNode.LastUpdateVersionedTransition))
	logReplication(fmt.Sprintf("Target initial version: %+v", targetNode.InitialVersionedTransition))
	logReplication(fmt.Sprintf("Target last update: %+v", targetNode.LastUpdateVersionedTransition))

	err := s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode:    sourceNode,
	})
	s.NoError(err)

	s.T().Logf("Replication flow:")
	for _, log := range replicationLogs {
		s.T().Logf("  %s", log)
	}
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_DeleteNode_TargetNewer() {
	persistedState := s.buildWorkflowMutableState()
	// Make target node have newer version
	targetNode := persistedState.ExecutionInfo.SubStateMachinesByType[s.stateMachineDef.Type()].MachinesById["child1"]
	targetNode.LastUpdateVersionedTransition.NamespaceFailoverVersion += 200

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	// No update expected since target is newer
	// UpdateWorkflowExecution should not be called

	backend := &testBackend{
		nextTransitionCount: 100,
		currentVersion:      s.namespaceEntry.FailoverVersion(),
	}

	incomingRoot, err := hsm.NewRoot(
		s.mockShard.StateMachineRegistry(),
		workflow.StateMachineType,
		hsmtest.NewData(hsmtest.State1),
		map[string]*persistencespb.StateMachineMap{},
		backend,
	)
	s.NoError(err)

	child1, err := incomingRoot.AddChild(hsm.Key{Type: s.stateMachineDef.Type(), ID: "child1"}, hsmtest.NewData(hsmtest.State2))
	s.NoError(err)

	// Set version info before delete
	child1.InternalRepr().InitialVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
		TransitionCount:          10,
	}
	child1.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
		TransitionCount:          50,
	}

	err = incomingRoot.DeleteChild(child1.Key)
	s.NoError(err)

	err = s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode:    incomingRoot.InternalRepr(),
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_DeleteNode_InitialTransitionMismatch() {
	persistedState := s.buildWorkflowMutableState()
	// Make target node have different initial version
	targetNode := persistedState.ExecutionInfo.SubStateMachinesByType[s.stateMachineDef.Type()].MachinesById["child1"]
	targetNode.InitialVersionedTransition.NamespaceFailoverVersion += 100

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	// No update expected since initial versions mismatch
	// UpdateWorkflowExecution should not be called

	backend := &testBackend{
		nextTransitionCount: 100,
		currentVersion:      s.namespaceEntry.FailoverVersion(),
	}

	incomingRoot, err := hsm.NewRoot(
		s.mockShard.StateMachineRegistry(),
		workflow.StateMachineType,
		hsmtest.NewData(hsmtest.State1),
		map[string]*persistencespb.StateMachineMap{},
		backend,
	)
	s.NoError(err)

	child1, err := incomingRoot.AddChild(hsm.Key{Type: s.stateMachineDef.Type(), ID: "child1"}, hsmtest.NewData(hsmtest.State2))
	s.NoError(err)

	// Set version info before delete, with mismatched initial version
	child1.InternalRepr().InitialVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(), // Different from target's version
		TransitionCount:          10,
	}
	child1.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
		TransitionCount:          50,
	}

	err = incomingRoot.DeleteChild(child1.Key)
	s.NoError(err)

	err = s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode:    incomingRoot.InternalRepr(),
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) TestSyncHSM_DeleteNode_ConcurrentDeletion() {
	persistedState := s.buildWorkflowMutableState()
	// Remove the node from persisted state to simulate it was already deleted
	delete(persistedState.ExecutionInfo.SubStateMachinesByType[s.stateMachineDef.Type()].MachinesById, "child1")

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{
		State:           persistedState,
		DBRecordVersion: 777,
	}, nil).Times(1)

	// No update expected since node is already deleted
	// UpdateWorkflowExecution should not be called

	incomingRoot, err := hsm.NewRoot(
		s.mockShard.StateMachineRegistry(),
		workflow.StateMachineType,
		hsmtest.NewData(hsmtest.State1),
		map[string]*persistencespb.StateMachineMap{},
		&testBackend{
			nextTransitionCount: 100,
			currentVersion:      s.namespaceEntry.FailoverVersion(),
		},
	)
	s.NoError(err)

	child1, err := incomingRoot.AddChild(hsm.Key{Type: s.stateMachineDef.Type(), ID: "child1"},
		hsmtest.NewData(hsmtest.State2))
	s.NoError(err)

	// Set version info before delete
	child1.InternalRepr().InitialVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
		TransitionCount:          10,
	}
	child1.InternalRepr().LastUpdateVersionedTransition = &persistencespb.VersionedTransition{
		NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
		TransitionCount:          50,
	}

	err = incomingRoot.DeleteChild(child1.Key)
	s.NoError(err)

	err = s.nDCHSMStateReplicator.SyncHSMState(context.Background(), &shard.SyncHSMRequest{
		WorkflowKey:         s.workflowKey,
		EventVersionHistory: persistedState.ExecutionInfo.VersionHistories.Histories[0],
		StateMachineNode:    incomingRoot.InternalRepr(),
	})
	s.NoError(err)
}

func (s *hsmStateReplicatorSuite) buildWorkflowMutableState() *persistencespb.WorkflowMutableState {

	info := &persistencespb.WorkflowExecutionInfo{
		NamespaceId: s.workflowKey.NamespaceID,
		WorkflowId:  s.workflowKey.WorkflowID,
		ExecutionStats: &persistencespb.ExecutionStats{
			HistorySize: 1234,
		},
		VersionHistories: &historyspb.VersionHistories{
			CurrentVersionHistoryIndex: 0,
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte("token#1"),
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 50, Version: s.namespaceEntry.FailoverVersion() - 100},
						{EventId: 102, Version: s.namespaceEntry.FailoverVersion()},
					},
				},
			},
		},
		SubStateMachinesByType: map[string]*persistencespb.StateMachineMap{
			s.stateMachineDef.Type(): {
				MachinesById: map[string]*persistencespb.StateMachineNode{
					"child1": {
						Data: []byte(hsmtest.State2),
						InitialVersionedTransition: &persistencespb.VersionedTransition{
							NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion(),
							TransitionCount:          10,
						},
						LastUpdateVersionedTransition: &persistencespb.VersionedTransition{
							NamespaceFailoverVersion: s.namespaceEntry.FailoverVersion() + 100,
							TransitionCount:          50,
						},
						TransitionCount: 23,
					},
				},
			},
		},
	}

	state := &persistencespb.WorkflowExecutionState{
		RunId:  s.workflowKey.RunID,
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	return &persistencespb.WorkflowMutableState{
		ExecutionInfo:  info,
		ExecutionState: state,
		NextEventId:    int64(103),
	}
}
