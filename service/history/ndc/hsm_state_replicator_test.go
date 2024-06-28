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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
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

		workflowCache *wcache.CacheImpl
		logger        log.Logger

		workflowKey     definition.WorkflowKey
		namespaceEntry  *namespace.Namespace
		stateMachineDef hsm.StateMachineDefinition

		nDCHSMStateReplicator *HSMStateReplicatorImpl
	}
)

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

	var ok bool
	s.workflowCache, ok = wcache.NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler).(*wcache.CacheImpl)
	s.True(ok)

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
