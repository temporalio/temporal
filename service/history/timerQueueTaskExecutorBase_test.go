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

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	timerQueueTaskExecutorBaseSuite struct {
		suite.Suite
		*require.Assertions

		controller                   *gomock.Controller
		mockShard                    *shard.ContextTest
		mockEngine                   *shard.MockEngine
		mockWorkflowExecutionContext *MockworkflowExecutionContext
		mockMutableState             *MockmutableState

		mockExecutionManager  *persistence.MockExecutionManager
		mockVisibilityManager *mocks.VisibilityManager
		mockHistoryMgr        *persistence.MockHistoryManager
		mockArchivalClient    *archiver.MockClient
		mockNamespaceCache    *cache.MockNamespaceCache
		mockClusterMetadata   *cluster.MockMetadata

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
	s.mockWorkflowExecutionContext = NewMockworkflowExecutionContext(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)

	config := NewDynamicConfigForTest()
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
	s.mockShard.SetEngine(s.mockEngine)

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockVisibilityManager = s.mockShard.Resource.VisibilityMgr
	s.mockHistoryMgr = s.mockShard.Resource.HistoryMgr
	s.mockArchivalClient = archiver.NewMockClient(s.controller)
	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata

	logger := s.mockShard.GetLogger()

	h := &historyEngineImpl{
		shard:          s.mockShard,
		logger:         logger,
		metricsClient:  s.mockShard.GetMetricsClient(),
		visibilityMgr:  s.mockVisibilityManager,
		historyV2Mgr:   s.mockHistoryMgr,
		archivalClient: s.mockArchivalClient,
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
	s.mockShard.Finish(s.T())
}

func (s *timerQueueTaskExecutorBaseSuite) TestDeleteWorkflow_NoErr() {
	task := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		TaskId:          12345,
		VisibilityTime:  timestamp.TimeNowPtrUtc(),
	}

	s.mockWorkflowExecutionContext.EXPECT().clear()

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testGlobalNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionManager.EXPECT().AddTasks(gomock.Any()).Return(nil)
	s.mockEngine.EXPECT().NotifyNewTransferTasks(gomock.Any())
	s.mockEngine.EXPECT().NotifyNewTimerTasks(gomock.Any())
	s.mockEngine.EXPECT().NotifyNewVisibilityTasks(gomock.Any())

	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any()).Return(nil)
	s.mockHistoryMgr.EXPECT().DeleteHistoryBranch(gomock.Any()).Return(nil)
	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil)

	err := s.timerQueueTaskExecutorBase.deleteWorkflow(task, s.mockWorkflowExecutionContext, s.mockMutableState)
	s.NoError(err)
}

func (s *timerQueueTaskExecutorBaseSuite) TestArchiveHistory_NoErr_InlineArchivalFailed() {
	s.mockWorkflowExecutionContext.EXPECT().loadExecutionStats().Return(&persistencespb.ExecutionStats{
		HistorySize: 1024,
	}, nil)
	s.mockWorkflowExecutionContext.EXPECT().clear()
	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101))

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(testGlobalNamespaceEntry, nil).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockExecutionManager.EXPECT().AddTasks(gomock.Any()).Return(nil)
	s.mockEngine.EXPECT().NotifyNewTransferTasks(gomock.Any())
	s.mockEngine.EXPECT().NotifyNewTimerTasks(gomock.Any())
	s.mockEngine.EXPECT().NotifyNewVisibilityTasks(gomock.Any())

	s.mockExecutionManager.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any()).Return(nil)
	s.mockExecutionManager.EXPECT().DeleteWorkflowExecution(gomock.Any()).Return(nil)

	s.mockArchivalClient.EXPECT().Archive(gomock.Any(), archiverClientRequestMatcher{inline: true}).
		Return(&archiver.ClientResponse{
			HistoryArchivedInline: false,
		}, nil)

	namespaceCacheEntry := cache.NewNamespaceCacheEntryForTest(&persistencespb.NamespaceInfo{}, &persistencespb.NamespaceConfig{}, false, nil, 0, nil)
	err := s.timerQueueTaskExecutorBase.archiveWorkflow(&persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1}, s.mockWorkflowExecutionContext, s.mockMutableState, namespaceCacheEntry)
	s.NoError(err)
}

func (s *timerQueueTaskExecutorBaseSuite) TestArchiveHistory_SendSignalErr() {
	s.mockWorkflowExecutionContext.EXPECT().loadExecutionStats().Return(&persistencespb.ExecutionStats{
		HistorySize: 1024 * 1024 * 1024,
	}, nil)

	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101))

	s.mockArchivalClient.EXPECT().Archive(gomock.Any(), archiverClientRequestMatcher{inline: false}).
		Return(nil, errors.New("failed to send signal"))

	namespaceCacheEntry := cache.NewNamespaceCacheEntryForTest(&persistencespb.NamespaceInfo{}, &persistencespb.NamespaceConfig{}, false, nil, 0, nil)
	err := s.timerQueueTaskExecutorBase.archiveWorkflow(&persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1}, s.mockWorkflowExecutionContext, s.mockMutableState, namespaceCacheEntry)
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
