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

package shard

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/enums/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	contextSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *ContextTest
		mockClusterMetadata  *cluster.MockMetadata
		mockShardManager     *persistence.MockShardManager
		mockExecutionManager *persistence.MockExecutionManager
		mockNamespaceCache   *namespace.MockRegistry
		mockHistoryEngine    *MockEngine

		timeSource *clock.EventTimeSource
	}
)

func TestShardContextSuite(t *testing.T) {
	s := &contextSuite{}
	suite.Run(t, s)
}

func (s *contextSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	s.timeSource = clock.NewEventTimeSource()
	shardContext := NewTestContextWithTimeSource(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
		s.timeSource,
	)
	s.mockShard = shardContext

	shardContext.MockHostInfoProvider.EXPECT().HostInfo().Return(shardContext.Resource.GetHostInfo()).AnyTimes()

	s.mockNamespaceCache = shardContext.Resource.NamespaceCache
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	s.mockClusterMetadata = shardContext.Resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	s.mockExecutionManager = shardContext.Resource.ExecutionMgr
	s.mockShardManager = shardContext.Resource.ShardMgr
	s.mockHistoryEngine = NewMockEngine(s.controller)
	shardContext.engineFuture.Set(s.mockHistoryEngine, nil)
}

func (s *contextSuite) TestAddTasks_Success() {
	tasks := map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer:    {&tasks.ActivityTask{}},           // Just for testing purpose. In the real code ActivityTask can't be passed to shardContext.AddTasks.
		tasks.CategoryTimer:       {&tasks.ActivityRetryTimerTask{}}, // Just for testing purpose. In the real code ActivityRetryTimerTask can't be passed to shardContext.AddTasks.
		tasks.CategoryReplication: {&tasks.HistoryReplicationTask{}}, // Just for testing purpose. In the real code HistoryReplicationTask can't be passed to shardContext.AddTasks.
		tasks.CategoryVisibility:  {&tasks.DeleteExecutionVisibilityTask{}},
	}

	addTasksRequest := &persistence.AddHistoryTasksRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,

		Tasks: tasks,
	}

	s.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), addTasksRequest).Return(nil)
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(tasks)

	err := s.mockShard.AddTasks(context.Background(), addTasksRequest)
	s.NoError(err)
}

func (s *contextSuite) TestTimerMaxReadLevelInitialization() {

	now := time.Now().Truncate(time.Millisecond)
	persistenceShardInfo := &persistencespb.ShardInfo{
		ShardId: 0,
		QueueAckLevels: map[int32]*persistencespb.QueueAckLevel{
			tasks.CategoryTimer.ID(): {
				AckLevel: now.Add(-time.Minute).UnixNano(),
				ClusterConsumerState: map[string]*persistencespb.QueueConsumerState{
					cluster.TestCurrentClusterName: {
						ShardWatermarks: map[int32]int64{
							s.mockShard.shardID: now.UnixNano(),
						},
					},
				},
			},
		},
		QueueStates: map[int32]*persistencespb.QueueState{
			tasks.CategoryTimer.ID(): {
				ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
					FireTime: timestamp.TimePtr(now.Add(time.Duration(rand.Intn(3)-2) * time.Minute)),
					TaskId:   rand.Int63(),
				},
			},
		},
	}
	s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), gomock.Any()).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: persistenceShardInfo,
		},
		nil,
	)

	// clear shardInfo and load from persistence
	shardContextImpl := s.mockShard
	shardContextImpl.shardInfo = nil
	err := shardContextImpl.loadShardMetadata(convert.BoolPtr(false))
	s.NoError(err)

	for clusterName, info := range s.mockShard.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		timerQueueAckLevels := persistenceShardInfo.QueueAckLevels[tasks.CategoryTimer.ID()]
		timerQueueStates := persistenceShardInfo.QueueStates[tasks.CategoryTimer.ID()]

		maxReadLevel := shardContextImpl.getScheduledTaskMaxReadLevel(clusterName).FireTime
		s.False(maxReadLevel.Before(timestamp.UnixOrZeroTime(timerQueueAckLevels.AckLevel)))

		if consumerState, ok := timerQueueAckLevels.ClusterConsumerState[clusterName]; ok {
			for _, watermark := range consumerState.ShardWatermarks {
				s.False(maxReadLevel.Before(timestamp.UnixOrZeroTime(watermark)))
			}
		}

		s.False(maxReadLevel.Before(timestamp.TimeValue(timerQueueStates.ExclusiveReaderHighWatermark.FireTime)))
	}
}

func (s *contextSuite) TestTimerMaxReadLevelUpdate() {
	now := time.Now().Add(time.Minute)
	s.timeSource.Update(now)

	_, err := s.mockShard.UpdateScheduledQueueExclusiveHighReadWatermark()
	s.NoError(err)

	s.True(s.mockShard.scheduledTaskMaxReadLevel.After(now))
}

func (s *contextSuite) TestDeleteWorkflowExecution_Success() {
	workflowKey := definition.WorkflowKey{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}
	branchToken := []byte("branchToken")
	stage := tasks.DeleteWorkflowExecutionStageNone

	s.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil)
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any())
	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)

	err := s.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		nil,
		nil,
		0,
		&stage,
	)

	s.NoError(err)
	s.Equal(tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageHistory|tasks.DeleteWorkflowExecutionStageVisibility, stage)
}

func (s *contextSuite) TestDeleteWorkflowExecution_Continue_Success() {
	workflowKey := definition.WorkflowKey{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}
	branchToken := []byte("branchToken")

	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)
	stage := tasks.DeleteWorkflowExecutionStageVisibility
	err := s.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		nil,
		nil,
		0,
		&stage,
	)
	s.NoError(err)
	s.Equal(tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageHistory|tasks.DeleteWorkflowExecutionStageVisibility, stage)

	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)
	stage = tasks.DeleteWorkflowExecutionStageVisibility | tasks.DeleteWorkflowExecutionStageCurrent
	err = s.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		nil,
		nil,
		0,
		&stage,
	)
	s.NoError(err)
	s.Equal(tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageHistory|tasks.DeleteWorkflowExecutionStageVisibility, stage)

	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)
	stage = tasks.DeleteWorkflowExecutionStageVisibility | tasks.DeleteWorkflowExecutionStageCurrent | tasks.DeleteWorkflowExecutionStageMutableState
	err = s.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		nil,
		nil,
		0,
		&stage,
	)
	s.NoError(err)
	s.Equal(tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageHistory|tasks.DeleteWorkflowExecutionStageVisibility, stage)
}

func (s *contextSuite) TestDeleteWorkflowExecution_ErrorAndContinue_Success() {
	workflowKey := definition.WorkflowKey{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}
	branchToken := []byte("branchToken")

	s.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil)
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any())
	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
	stage := tasks.DeleteWorkflowExecutionStageNone
	err := s.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		nil,
		nil,
		0,
		&stage,
	)
	s.Error(err)
	s.Equal(tasks.DeleteWorkflowExecutionStageVisibility, stage)

	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
	err = s.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		nil,
		nil,
		0,
		&stage,
	)
	s.Error(err)
	s.Equal(tasks.DeleteWorkflowExecutionStageVisibility|tasks.DeleteWorkflowExecutionStageCurrent, stage)

	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(errors.New("some error"))
	err = s.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		nil,
		nil,
		0,
		&stage,
	)
	s.Error(err)
	s.Equal(tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageVisibility, stage)

	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Return(nil)
	err = s.mockShard.DeleteWorkflowExecution(
		context.Background(),
		workflowKey,
		branchToken,
		nil,
		nil,
		0,
		&stage,
	)
	s.NoError(err)
	s.Equal(tasks.DeleteWorkflowExecutionStageCurrent|tasks.DeleteWorkflowExecutionStageMutableState|tasks.DeleteWorkflowExecutionStageVisibility|tasks.DeleteWorkflowExecutionStageHistory, stage)
}

func (s *contextSuite) TestAcquireShardOwnershipLostErrorIsNotRetried() {
	s.mockShard.state = contextStateAcquiring
	s.mockShard.acquireShardRetryPolicy = backoff.NewExponentialRetryPolicy(time.Nanosecond).
		WithMaximumAttempts(5)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(&persistence.ShardOwnershipLostError{}).Times(1)

	s.mockShard.acquireShard()

	s.Assert().Equal(contextStateStopping, s.mockShard.state)
}

func (s *contextSuite) TestAcquireShardNonOwnershipLostErrorIsRetried() {
	s.mockShard.state = contextStateAcquiring
	s.mockShard.acquireShardRetryPolicy = backoff.NewExponentialRetryPolicy(time.Nanosecond).
		WithMaximumAttempts(5)
	// TODO: make this 5 times instead of 6 when retry policy is fixed
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("temp error")).Times(6)

	s.mockShard.acquireShard()

	s.Assert().Equal(contextStateStopping, s.mockShard.state)
}

func (s *contextSuite) TestAcquireShardEventuallySucceeds() {
	s.mockShard.state = contextStateAcquiring
	s.mockShard.acquireShardRetryPolicy = backoff.NewExponentialRetryPolicy(time.Nanosecond).
		WithMaximumAttempts(5)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(fmt.Errorf("temp error")).Times(3)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any()).MinTimes(1)

	s.mockShard.acquireShard()

	s.Assert().Equal(contextStateAcquired, s.mockShard.state)
}

func (s *contextSuite) TestAcquireShardNoError() {
	s.mockShard.state = contextStateAcquiring
	s.mockShard.acquireShardRetryPolicy = backoff.NewExponentialRetryPolicy(time.Nanosecond).
		WithMaximumAttempts(5)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(nil).Times(1)
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any()).MinTimes(1)

	s.mockShard.acquireShard()

	s.Assert().Equal(contextStateAcquired, s.mockShard.state)
}

func (s *contextSuite) TestHandoverNamespace() {
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any()).Times(1)

	namespaceEntry := namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{Id: tests.NamespaceID.String(), Name: tests.Namespace.String()},
		&persistencespb.NamespaceConfig{
			Retention: timestamp.DurationFromDays(1),
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: cluster.TestCurrentClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
			State: enums.REPLICATION_STATE_HANDOVER,
		},
		tests.Version,
	)
	s.mockShard.UpdateHandoverNamespace(namespaceEntry, false)
	_, handoverNS, err := s.mockShard.GetReplicationStatus([]string{})
	s.NoError(err)

	handoverInfo, ok := handoverNS[namespaceEntry.Name().String()]
	s.True(ok)
	s.Equal(s.mockShard.immediateTaskExclusiveMaxReadLevel-1, handoverInfo.HandoverReplicationTaskId)

	// make shard status invalid
	// ideally we should use s.mockShard.transition() method
	// but that will cause shard trying to re-acquire the shard in the background
	s.mockShard.stateLock.Lock()
	s.mockShard.state = contextStateAcquiring
	s.mockShard.stateLock.Unlock()

	// note: no mock for NotifyNewTasks

	s.mockShard.UpdateHandoverNamespace(namespaceEntry, false)
	_, handoverNS, err = s.mockShard.GetReplicationStatus([]string{})
	s.NoError(err)

	handoverInfo, ok = handoverNS[namespaceEntry.Name().String()]
	s.True(ok)
	s.Equal(s.mockShard.immediateTaskExclusiveMaxReadLevel-1, handoverInfo.HandoverReplicationTaskId)

	// delete namespace
	s.mockShard.UpdateHandoverNamespace(namespaceEntry, true)
	_, handoverNS, err = s.mockShard.GetReplicationStatus([]string{})
	s.NoError(err)

	_, ok = handoverNS[namespaceEntry.Name().String()]
	s.False(ok)
}

func (s *contextSuite) TestAckCompatibility_OldToNew() {
	var shardInfo persistencespb.ShardInfo

	shardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{
		21: {
			AckLevel: rand.Int63(),
			ClusterAckLevel: map[string]int64{
				"random cluster name": 42,
			},
			ClusterConsumerState: map[string]*persistencespb.QueueConsumerState{},
		},
	}

	loadShardInfoCompatibilityCheck(&shardInfo)
	s.Nil(shardInfo.QueueAckLevels[21].ClusterAckLevel)
	s.Equal(map[string]*persistencespb.QueueConsumerState{
		"random cluster name": {
			ShardWatermarks: map[int32]int64{
				shardInfo.ShardId: 42,
			},
		},
	}, shardInfo.QueueAckLevels[21].ClusterConsumerState)
}

func (s *contextSuite) TestAckCompatibility_NewToOld() {
	var shardInfo persistencespb.ShardInfo

	shardInfo.QueueAckLevels = map[int32]*persistencespb.QueueAckLevel{
		21: {
			AckLevel:        rand.Int63(),
			ClusterAckLevel: map[string]int64{},
			ClusterConsumerState: map[string]*persistencespb.QueueConsumerState{
				"random cluster name": {
					ShardWatermarks: map[int32]int64{
						shardInfo.ShardId: 42,
					},
				},
			},
		},
	}

	storeShardInfoCompatibilityCheck(&shardInfo)
	s.Equal(map[string]int64{
		"random cluster name": 42,
	}, shardInfo.QueueAckLevels[21].ClusterAckLevel)
}
