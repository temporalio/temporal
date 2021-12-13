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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	timerQueueTaskExecutorBaseSuite struct {
		suite.Suite
		*require.Assertions

		controller                   *gomock.Controller
		mockShard                    *shard.ContextTest
		mockEngine                   *shard.MockEngine
		mockWorkflowExecutionContext *workflow.MockContext
		mockMutableState             *workflow.MockMutableState

		mockExecutionManager         *persistence.MockExecutionManager
		mockArchivalClient           *archiver.MockClient
		mockNamespaceCache           *namespace.MockRegistry
		mockClusterMetadata          *cluster.MockMetadata
		mockSearchAttributesProvider *searchattribute.MockProvider

		timerQueueTaskExecutorBase *timerQueueTaskExecutorBase
	}
)

func TestTimerQueueTaskExecutorBaseSuite(t *testing.T) {
	s := new(timerQueueTaskExecutorBaseSuite)
	suite.Run(t, s)
}

func (s *timerQueueTaskExecutorBaseSuite) SetupSuite() {

}

func (s *timerQueueTaskExecutorBaseSuite) TearDownSuite() {

}

func (s *timerQueueTaskExecutorBaseSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockWorkflowExecutionContext = workflow.NewMockContext(s.controller)
	s.mockMutableState = workflow.NewMockMutableState(s.controller)

	config := tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		config,
	)
	s.mockEngine = shard.NewMockEngine(s.controller)
	s.mockShard.SetEngineForTesting(s.mockEngine)

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockArchivalClient = archiver.NewMockClient(s.controller)
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockSearchAttributesProvider = s.mockShard.Resource.SearchAttributesProvider

	logger := s.mockShard.GetLogger()

	h := &historyEngineImpl{
		shard:           s.mockShard,
		logger:          logger,
		metricsClient:   s.mockShard.GetMetricsClient(),
		archivalClient:  s.mockArchivalClient,
		clusterMetadata: s.mockClusterMetadata,
	}

	s.timerQueueTaskExecutorBase = newTimerQueueTaskExecutorBase(
		s.mockShard,
		h,
		logger,
		s.mockShard.GetMetricsClient(),
		config,
	)
}

func (s *timerQueueTaskExecutorBaseSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *timerQueueTaskExecutorBaseSuite) TestDeleteWorkflow_NoErr() {
	task := &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(),
			tests.WorkflowID,
			tests.RunID,
		),
		Version:             123,
		TaskID:              12345,
		VisibilityTimestamp: time.Now().UTC(),
	}

	s.mockWorkflowExecutionContext.EXPECT().Clear()

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionManager.EXPECT().AddTasks(gomock.Any()).Return(nil)
	s.mockEngine.EXPECT().NotifyNewTransferTasks(gomock.Any(), gomock.Any())
	s.mockEngine.EXPECT().NotifyNewTimerTasks(gomock.Any(), gomock.Any())
	s.mockEngine.EXPECT().NotifyNewVisibilityTasks(gomock.Any())
	s.mockEngine.EXPECT().NotifyNewReplicationTasks(gomock.Any())

	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteHistoryBranch(gomock.Any()).Return(nil)
	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil)

	err := s.timerQueueTaskExecutorBase.deleteWorkflow(task, s.mockWorkflowExecutionContext, s.mockMutableState)
	s.NoError(err)
}

func (s *timerQueueTaskExecutorBaseSuite) TestArchiveHistory_NoErr_InlineArchivalFailed() {
	task := &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(),
			tests.WorkflowID,
			tests.RunID,
		),
		Version:             123,
		TaskID:              12345,
		VisibilityTimestamp: time.Now().UTC(),
	}

	s.mockWorkflowExecutionContext.EXPECT().LoadExecutionStats().Return(&persistencespb.ExecutionStats{
		HistorySize: 1024,
	}, nil)
	s.mockWorkflowExecutionContext.EXPECT().Clear()
	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101))

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionManager.EXPECT().AddTasks(gomock.Any()).Return(nil)
	s.mockEngine.EXPECT().NotifyNewTransferTasks(gomock.Any(), gomock.Any())
	s.mockEngine.EXPECT().NotifyNewTimerTasks(gomock.Any(), gomock.Any())
	s.mockEngine.EXPECT().NotifyNewVisibilityTasks(gomock.Any())
	s.mockEngine.EXPECT().NotifyNewReplicationTasks(gomock.Any())

	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any()).Return(nil)

	s.mockArchivalClient.EXPECT().Archive(gomock.Any(), archiverClientRequestMatcher{inline: true}).
		Return(&archiver.ClientResponse{
			HistoryArchivedInline: false,
		}, nil)

	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false)

	namespaceRegistryEntry := namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{}, &persistencespb.NamespaceConfig{}, false, nil, 0)
	err := s.timerQueueTaskExecutorBase.archiveWorkflow(task, s.mockWorkflowExecutionContext, s.mockMutableState, namespaceRegistryEntry)
	s.NoError(err)
}

func (s *timerQueueTaskExecutorBaseSuite) TestArchiveHistory_SendSignalErr() {
	task := &tasks.DeleteHistoryEventTask{
		WorkflowKey: definition.NewWorkflowKey(
			tests.NamespaceID.String(),
			tests.WorkflowID,
			tests.RunID,
		),
		Version:             123,
		TaskID:              12345,
		VisibilityTimestamp: time.Now().UTC(),
	}

	s.mockWorkflowExecutionContext.EXPECT().LoadExecutionStats().Return(&persistencespb.ExecutionStats{
		HistorySize: 1024 * 1024 * 1024,
	}, nil)

	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101))

	s.mockArchivalClient.EXPECT().Archive(gomock.Any(), archiverClientRequestMatcher{inline: false}).
		Return(nil, errors.New("failed to send signal"))
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false)

	namespaceRegistryEntry := namespace.NewNamespaceForTest(&persistencespb.NamespaceInfo{}, &persistencespb.NamespaceConfig{}, false, nil, 0)
	err := s.timerQueueTaskExecutorBase.archiveWorkflow(task, s.mockWorkflowExecutionContext, s.mockMutableState, namespaceRegistryEntry)
	s.Error(err)
}

type (
	archiverClientRequestMatcher struct {
		inline bool
	}
)

func (m archiverClientRequestMatcher) Matches(x interface{}) bool {
	req := x.(*archiver.ClientRequest)
	return req.CallerService == common.HistoryServiceName &&
		req.AttemptArchiveInline == m.inline &&
		req.ArchiveRequest.Targets[0] == archiver.ArchiveTargetHistory
}

func (m archiverClientRequestMatcher) String() string {
	return "archiverClientRequestMatcher"
}
