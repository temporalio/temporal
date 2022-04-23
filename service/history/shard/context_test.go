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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	contextSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		shardContext Context

		mockResource *resource.Test

		namespaceID        namespace.ID
		mockNamespaceCache *namespace.MockRegistry
		namespaceEntry     *namespace.Namespace
		timeSource         *clock.EventTimeSource

		mockClusterMetadata  *cluster.MockMetadata
		mockExecutionManager *persistence.MockExecutionManager
		mockHistoryEngine    *MockEngine
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
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		tests.NewDynamicConfig(),
		s.timeSource,
	)
	s.shardContext = shardContext

	s.mockResource = shardContext.Resource
	shardContext.MockHostInfoProvider.EXPECT().HostInfo().Return(s.mockResource.GetHostInfo()).AnyTimes()

	s.namespaceID = "namespace-Id"
	s.namespaceEntry = namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: s.namespaceID.String()}, &persistencespb.NamespaceConfig{}, "")
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	shardContext.namespaceRegistry = s.mockResource.NamespaceCache

	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	shardContext.clusterMetadata = s.mockClusterMetadata

	s.mockExecutionManager = s.mockResource.ExecutionMgr
	s.mockHistoryEngine = NewMockEngine(s.controller)
	shardContext.engineFuture.Set(s.mockHistoryEngine, nil)
}

func (s *contextSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *contextSuite) TestAddTasks_Success() {
	task := &persistencespb.TimerTaskInfo{
		NamespaceId:     s.namespaceID.String(),
		WorkflowId:      "workflow-id",
		RunId:           "run-id",
		TaskType:        enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		Version:         1,
		EventId:         2,
		ScheduleAttempt: 1,
		TaskId:          12345,
		VisibilityTime:  timestamp.TimeNowPtrUtc(),
	}

	tasks := map[tasks.Category][]tasks.Task{
		tasks.CategoryTransfer:    {&tasks.ActivityTask{}},           // Just for testing purpose. In the real code ActivityTask can't be passed to shardContext.AddTasks.
		tasks.CategoryTimer:       {&tasks.ActivityRetryTimerTask{}}, // Just for testing purpose. In the real code ActivityRetryTimerTask can't be passed to shardContext.AddTasks.
		tasks.CategoryReplication: {&tasks.HistoryReplicationTask{}}, // Just for testing purpose. In the real code HistoryReplicationTask can't be passed to shardContext.AddTasks.
		tasks.CategoryVisibility:  {&tasks.DeleteExecutionVisibilityTask{}},
	}

	addTasksRequest := &persistence.AddHistoryTasksRequest{
		ShardID:     s.shardContext.GetShardID(),
		NamespaceID: task.GetNamespaceId(),
		WorkflowID:  task.GetWorkflowId(),
		RunID:       task.GetRunId(),

		Tasks: tasks,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	s.mockExecutionManager.EXPECT().AddHistoryTasks(gomock.Any(), addTasksRequest).Return(nil)
	s.mockHistoryEngine.EXPECT().NotifyNewTasks(gomock.Any(), tasks)

	err := s.shardContext.AddTasks(context.Background(), addTasksRequest)
	s.NoError(err)
}

func (s *contextSuite) TestTimerMaxReadLevelInitialization() {

	now := time.Now().Truncate(time.Millisecond)
	persistenceShardInfo := &persistencespb.ShardInfo{
		ShardId:           0,
		TimerAckLevelTime: timestamp.TimePtr(now.Add(-time.Minute)),
		ClusterTimerAckLevel: map[string]*time.Time{
			cluster.TestCurrentClusterName: timestamp.TimePtr(now),
		},
	}
	s.mockResource.ShardMgr.EXPECT().GetOrCreateShard(gomock.Any(), gomock.Any()).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: persistenceShardInfo,
		},
		nil,
	)
	s.mockResource.ClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)

	// clear shardInfo and load from persistence
	shardContextImpl := s.shardContext.(*ContextTest)
	shardContextImpl.shardInfo = nil
	err := shardContextImpl.loadShardMetadata(convert.BoolPtr(false))
	s.NoError(err)

	for clusterName, info := range s.shardContext.GetClusterMetadata().GetAllClusterInfo() {
		if !info.Enabled {
			continue
		}

		maxReadLevel := shardContextImpl.getScheduledTaskMaxReadLevel(clusterName).FireTime
		s.False(maxReadLevel.Before(*persistenceShardInfo.TimerAckLevelTime))

		if clusterAckLevel, ok := persistenceShardInfo.ClusterTimerAckLevel[clusterName]; ok {
			s.False(maxReadLevel.Before(*clusterAckLevel))
		}
	}
}

func (s *contextSuite) TestTimerMaxReadLevelUpdate() {
	clusterName := cluster.TestCurrentClusterName
	s.mockResource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(clusterName).AnyTimes()

	now := time.Now()
	s.timeSource.Update(now)
	maxReadLevel := s.shardContext.GetQueueMaxReadLevel(tasks.CategoryTimer, clusterName)

	s.timeSource.Update(now.Add(-time.Minute))
	newMaxReadLevel := s.shardContext.GetQueueMaxReadLevel(tasks.CategoryTimer, clusterName)
	s.Equal(maxReadLevel, newMaxReadLevel)

	s.timeSource.Update(now.Add(time.Minute))
	newMaxReadLevel = s.shardContext.GetQueueMaxReadLevel(tasks.CategoryTimer, clusterName)
	s.True(newMaxReadLevel.FireTime.After(maxReadLevel.FireTime))
}
