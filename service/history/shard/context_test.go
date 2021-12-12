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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
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

	shardContext := NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		tests.NewDynamicConfig(),
	)
	s.shardContext = shardContext

	s.mockResource = shardContext.Resource

	s.namespaceID = "namespace-Id"
	s.namespaceEntry = namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: s.namespaceID.String()}, &persistencespb.NamespaceConfig{}, "")
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	shardContext.namespaceRegistry = s.mockResource.NamespaceCache

	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	shardContext.clusterMetadata = s.mockClusterMetadata

	s.mockExecutionManager = s.mockResource.ExecutionMgr
	s.mockHistoryEngine = NewMockEngine(s.controller)
	shardContext.engine = s.mockHistoryEngine
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

	transferTasks := []tasks.Task{&tasks.ActivityTask{}}              // Just for testing purpose. In the real code ActivityTask can't be passed to shardContext.AddTasks.
	timerTasks := []tasks.Task{&tasks.ActivityRetryTimerTask{}}       // Just for testing purpose. In the real code ActivityRetryTimerTask can't be passed to shardContext.AddTasks.
	replicationTasks := []tasks.Task{&tasks.HistoryReplicationTask{}} // Just for testing purpose. In the real code HistoryReplicationTask can't be passed to shardContext.AddTasks.
	visibilityTasks := []tasks.Task{&tasks.DeleteExecutionVisibilityTask{}}

	addTasksRequest := &persistence.AddTasksRequest{
		ShardID:     s.shardContext.GetShardID(),
		NamespaceID: task.GetNamespaceId(),
		WorkflowID:  task.GetWorkflowId(),
		RunID:       task.GetRunId(),

		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
		ReplicationTasks: replicationTasks,
		VisibilityTasks:  visibilityTasks,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	s.mockExecutionManager.EXPECT().AddTasks(addTasksRequest).Return(nil)
	s.mockHistoryEngine.EXPECT().NotifyNewTransferTasks(gomock.Any(), transferTasks)
	s.mockHistoryEngine.EXPECT().NotifyNewTimerTasks(gomock.Any(), timerTasks)
	s.mockHistoryEngine.EXPECT().NotifyNewVisibilityTasks(visibilityTasks)
	s.mockHistoryEngine.EXPECT().NotifyNewReplicationTasks(replicationTasks)

	err := s.shardContext.AddTasks(addTasksRequest)
	s.NoError(err)
}
