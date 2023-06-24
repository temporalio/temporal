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

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
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
		shardID              int32
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

	s.shardID = 1
	s.timeSource = clock.NewEventTimeSource()
	shardContext := NewTestContextWithTimeSource(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: s.shardID,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
		s.timeSource,
	)
	s.mockShard = shardContext

	shardContext.Resource.HostInfoProvider.EXPECT().HostInfo().Return(shardContext.Resource.GetHostInfo()).AnyTimes()

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

func (s *contextSuite) TestOverwriteScheduledTaskTimestamp() {
	now := s.timeSource.Now()
	s.timeSource.Update(now)
	maxReadLevel, err := s.mockShard.UpdateScheduledQueueExclusiveHighReadWatermark()
	s.NoError(err)

	now = now.Add(time.Minute)
	s.timeSource.Update(now)

	workflowKey := definition.NewWorkflowKey(
		tests.NamespaceID.String(),
		tests.WorkflowID,
		tests.RunID,
	)
	fakeTask := tasks.NewFakeTask(
		workflowKey,
		tasks.CategoryTimer,
		time.Time{},
	)
	testTasks := map[tasks.Category][]tasks.Task{
		tasks.CategoryTimer: {fakeTask},
	}

	s.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(testTasks).AnyTimes()

	testCases := []struct {
		taskTimestamp     time.Time
		expectedTimestamp time.Time
	}{
		{
			// task timestamp is lower than both scheduled queue max read level and now
			// should be overwritten to be later than both
			taskTimestamp:     maxReadLevel.FireTime.Add(-time.Minute),
			expectedTimestamp: now.Add(persistence.ScheduledTaskMinPrecision),
		},
		{
			// task timestamp is lower than now but higher than scheduled queue max read level
			// should still be overwritten to be later than both
			taskTimestamp:     now.Add(-time.Minute),
			expectedTimestamp: now.Add(persistence.ScheduledTaskMinPrecision),
		},
		{
			// task timestamp is later than both now and scheduled queue max read level
			// should not be overwritten
			taskTimestamp:     now.Add(time.Minute),
			expectedTimestamp: now.Add(time.Minute),
		},
	}

	for _, tc := range testCases {
		fakeTask.SetVisibilityTime(tc.taskTimestamp)
		err = s.mockShard.AddTasks(
			context.Background(),
			&persistence.AddHistoryTasksRequest{
				ShardID:     s.mockShard.GetShardID(),
				NamespaceID: workflowKey.NamespaceID,
				WorkflowID:  workflowKey.WorkflowID,
				RunID:       workflowKey.RunID,
				Tasks:       testTasks,
			},
		)
		s.NoError(err)
		s.True(fakeTask.GetVisibilityTime().After(now))
		s.True(fakeTask.GetVisibilityTime().After(maxReadLevel.FireTime))
		s.True(fakeTask.GetVisibilityTime().Equal(tc.expectedTimestamp))
	}
}

func (s *contextSuite) TestAddTasks_Success() {
	testTasks := map[tasks.Category][]tasks.Task{
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

		Tasks: testTasks,
	}

	s.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), addTasksRequest).Return(nil)
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(testTasks)

	err := s.mockShard.AddTasks(context.Background(), addTasksRequest)
	s.NoError(err)
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

func (s *contextSuite) TestDeleteWorkflowExecution_DeleteVisibilityTaskNotifiction() {
	workflowKey := definition.WorkflowKey{
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  tests.WorkflowID,
		RunID:       tests.RunID,
	}
	branchToken := []byte("branchToken")
	stage := tasks.DeleteWorkflowExecutionStageNone

	// add task fails with error that suggests operation can't possibly succeed, no task notification
	s.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(persistence.ErrPersistenceLimitExceeded).Times(1)
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
	s.Equal(tasks.DeleteWorkflowExecutionStageNone, stage)

	// add task succeeds but second operation fails, send task notification
	s.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), gomock.Any()).Return(nil).Times(1)
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any()).Times(1)
	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(persistence.ErrPersistenceLimitExceeded).Times(1)
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
	s.Equal(tasks.DeleteWorkflowExecutionStageVisibility, stage)
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

func (s *contextSuite) TestUpdateGetRemoteClusterInfo_Legacy_8_4() {
	clusterMetadata := cluster.NewMockMetadata(s.controller)
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             8,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	s.mockShard.clusterMetadata = clusterMetadata

	ackTaskID := rand.Int63()
	ackTimestamp := time.Unix(0, rand.Int63())
	s.mockShard.UpdateRemoteClusterInfo(
		cluster.TestAlternativeClusterName,
		ackTaskID,
		ackTimestamp,
	)
	remoteAckStatus, _, err := s.mockShard.GetReplicationStatus([]string{cluster.TestAlternativeClusterName})
	s.NoError(err)
	s.Equal(map[string]*historyservice.ShardReplicationStatusPerCluster{
		cluster.TestAlternativeClusterName: {
			AckedTaskId:             ackTaskID,
			AckedTaskVisibilityTime: timestamp.TimePtr(ackTimestamp),
		},
	}, remoteAckStatus)
}

func (s *contextSuite) TestUpdateGetRemoteClusterInfo_Legacy_4_8() {
	clusterMetadata := cluster.NewMockMetadata(s.controller)
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             8,
		},
	}).AnyTimes()
	s.mockShard.clusterMetadata = clusterMetadata

	ackTaskID := rand.Int63()
	ackTimestamp := time.Unix(0, rand.Int63())
	s.mockShard.UpdateRemoteClusterInfo(
		cluster.TestAlternativeClusterName,
		ackTaskID,
		ackTimestamp,
	)
	remoteAckStatus, _, err := s.mockShard.GetReplicationStatus([]string{cluster.TestAlternativeClusterName})
	s.NoError(err)
	s.Equal(map[string]*historyservice.ShardReplicationStatusPerCluster{
		cluster.TestAlternativeClusterName: {
			AckedTaskId:             ackTaskID,
			AckedTaskVisibilityTime: timestamp.TimePtr(ackTimestamp),
		},
	}, remoteAckStatus)
}

func (s *contextSuite) TestUpdateGetRemoteReaderInfo_8_4() {
	clusterMetadata := cluster.NewMockMetadata(s.controller)
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             8,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             4,
		},
	}).AnyTimes()
	s.mockShard.clusterMetadata = clusterMetadata

	ackTaskID := rand.Int63()
	ackTimestamp := time.Unix(0, rand.Int63())
	err := s.mockShard.UpdateRemoteReaderInfo(
		ReplicationReaderIDFromClusterShardID(
			cluster.TestAlternativeClusterInitialFailoverVersion,
			1,
		),
		ackTaskID,
		ackTimestamp,
	)
	s.NoError(err)
	remoteAckStatus, _, err := s.mockShard.GetReplicationStatus([]string{cluster.TestAlternativeClusterName})
	s.NoError(err)
	s.Equal(map[string]*historyservice.ShardReplicationStatusPerCluster{
		cluster.TestAlternativeClusterName: {
			AckedTaskId:             ackTaskID,
			AckedTaskVisibilityTime: timestamp.TimePtr(ackTimestamp),
		},
	}, remoteAckStatus)
}

func (s *contextSuite) TestUpdateGetRemoteReaderInfo_4_8() {
	clusterMetadata := cluster.NewMockMetadata(s.controller)
	clusterMetadata.EXPECT().GetClusterID().Return(cluster.TestCurrentClusterInitialFailoverVersion).AnyTimes()
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	clusterMetadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestCurrentClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestCurrentClusterFrontendAddress,
			ShardCount:             4,
		},
		cluster.TestAlternativeClusterName: {
			Enabled:                true,
			InitialFailoverVersion: cluster.TestAlternativeClusterInitialFailoverVersion,
			RPCAddress:             cluster.TestAlternativeClusterFrontendAddress,
			ShardCount:             8,
		},
	}).AnyTimes()
	s.mockShard.clusterMetadata = clusterMetadata

	ack1TaskID := rand.Int63()
	ack1Timestamp := time.Unix(0, rand.Int63())
	err := s.mockShard.UpdateRemoteReaderInfo(
		ReplicationReaderIDFromClusterShardID(
			cluster.TestAlternativeClusterInitialFailoverVersion,
			1, // maps to local shard 1
		),
		ack1TaskID,
		ack1Timestamp,
	)
	s.NoError(err)
	ack5TaskID := rand.Int63()
	ack5Timestamp := time.Unix(0, rand.Int63())
	err = s.mockShard.UpdateRemoteReaderInfo(
		ReplicationReaderIDFromClusterShardID(
			cluster.TestAlternativeClusterInitialFailoverVersion,
			5, // maps to local shard 1
		),
		ack5TaskID,
		ack5Timestamp,
	)
	s.NoError(err)

	ackTaskID := ack1TaskID
	ackTimestamp := ack1Timestamp
	if ackTaskID > ack5TaskID {
		ackTaskID = ack5TaskID
		ackTimestamp = ack5Timestamp
	}

	remoteAckStatus, _, err := s.mockShard.GetReplicationStatus([]string{cluster.TestAlternativeClusterName})
	s.NoError(err)
	s.Equal(map[string]*historyservice.ShardReplicationStatusPerCluster{
		cluster.TestAlternativeClusterName: {
			AckedTaskId:             ackTaskID,
			AckedTaskVisibilityTime: timestamp.TimePtr(ackTimestamp),
		},
	}, remoteAckStatus)
}

func (s *contextSuite) TestShardStopReasonAssertOwnership() {
	s.mockShard.state = contextStateAcquired
	s.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), gomock.Any()).
		Return(&persistence.ShardOwnershipLostError{}).Times(1)

	err := s.mockShard.AssertOwnership(context.Background())
	s.Error(err)

	s.False(s.mockShard.IsValid())
	s.True(s.mockShard.stoppedForOwnershipLost())
}

func (s *contextSuite) TestShardStopReasonShardRead() {
	s.mockShard.state = contextStateAcquired
	s.mockExecutionManager.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).
		Return(nil, &persistence.ShardOwnershipLostError{}).Times(1)

	_, err := s.mockShard.GetCurrentExecution(context.Background(), nil)
	s.Error(err)

	s.False(s.mockShard.IsValid())
	s.True(s.mockShard.stoppedForOwnershipLost())
}

func (s *contextSuite) TestShardStopReasonAcquireShard() {
	s.mockShard.state = contextStateAcquiring
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).
		Return(&persistence.ShardOwnershipLostError{}).Times(1)

	s.mockShard.acquireShard()

	s.Assert().Equal(contextStateStopping, s.mockShard.state)
	s.False(s.mockShard.IsValid())
	s.True(s.mockShard.stoppedForOwnershipLost())
}

func (s *contextSuite) TestShardStopReasonUnload() {
	s.mockShard.state = contextStateAcquired

	s.mockShard.UnloadForOwnershipLost()

	s.Assert().Equal(contextStateStopping, s.mockShard.state)
	s.False(s.mockShard.IsValid())
	s.True(s.mockShard.stoppedForOwnershipLost())
}

func (s *contextSuite) TestShardStopReasonCloseShard() {
	s.mockShard.state = contextStateAcquired
	s.mockHistoryEngine.EXPECT().Stop().Times(1)

	s.mockShard.FinishStop()

	s.False(s.mockShard.IsValid())
	s.False(s.mockShard.stoppedForOwnershipLost())
}
