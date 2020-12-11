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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	dc "go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	timerQueueTaskExecutorBaseSuiteV2 struct {
		suite.Suite
		*require.Assertions

		controller                   *gomock.Controller
		mockShard                    *shard.ContextTest
		mockWorkflowExecutionContext *MockworkflowExecutionContext
		mockMutableState             *MockmutableState

		mockExecutionManager  *mocks.ExecutionManager
		mockVisibilityManager *mocks.VisibilityManager
		mockHistoryV2Manager  *mocks.HistoryV2Manager
		mockArchivalClient    *archiver.ClientMock

		timerQueueTaskExecutorBase *timerQueueTaskExecutorBase
	}
)

func TestTimerQueueTaskExecutorBaseSuiteV2(t *testing.T) {
	s := new(timerQueueTaskExecutorBaseSuiteV2)
	suite.Run(t, s)
}

func (s *timerQueueTaskExecutorBaseSuiteV2) SetupSuite() {

}

func (s *timerQueueTaskExecutorBaseSuiteV2) TearDownSuite() {

}

func (s *timerQueueTaskExecutorBaseSuiteV2) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockWorkflowExecutionContext = NewMockworkflowExecutionContext(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)

	config := NewDynamicConfigForTest()
	config.DisableKafkaForVisibility = dc.GetBoolPropertyFn(true)
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

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockVisibilityManager = s.mockShard.Resource.VisibilityMgr
	s.mockHistoryV2Manager = s.mockShard.Resource.HistoryMgr
	s.mockArchivalClient = &archiver.ClientMock{}

	logger := s.mockShard.GetLogger()

	h := &historyEngineImpl{
		shard:          s.mockShard,
		logger:         logger,
		metricsClient:  s.mockShard.GetMetricsClient(),
		visibilityMgr:  s.mockVisibilityManager,
		historyV2Mgr:   s.mockHistoryV2Manager,
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

func (s *timerQueueTaskExecutorBaseSuiteV2) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockArchivalClient.AssertExpectations(s.T())
}

func (s *timerQueueTaskExecutorBaseSuiteV2) TestDeleteWorkflow_NoErr() {
	task := &persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1,
		TaskId:          12345,
		VisibilityTime:  timestamp.TimeNowPtrUtc(),
	}

	s.mockWorkflowExecutionContext.EXPECT().clear().Times(1)
	s.mockWorkflowExecutionContext.EXPECT().updateWorkflowExecutionWithNew(
		gomock.Any(),
		persistence.UpdateWorkflowModeBypassCurrent,
		nil, // no new workflow
		nil, // no new workflow
		transactionPolicyPassive,
		nil,
	).Times(1)

	s.mockExecutionManager.On("DeleteCurrentWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockExecutionManager.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockHistoryV2Manager.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil).Times(1)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil).AnyTimes()
	s.mockMutableState.EXPECT().AddVisibilityTasks(gomock.Any()).Times(1)

	err := s.timerQueueTaskExecutorBase.deleteWorkflow(task, s.mockWorkflowExecutionContext, s.mockMutableState)
	s.NoError(err)
}

func (s *timerQueueTaskExecutorBaseSuiteV2) TestArchiveHistory_NoErr_InlineArchivalFailed() {
	s.mockWorkflowExecutionContext.EXPECT().loadExecutionStats().Return(&persistencespb.ExecutionStats{
		HistorySize: 1024,
	}, nil).Times(1)
	s.mockWorkflowExecutionContext.EXPECT().clear().Times(1)
	s.mockWorkflowExecutionContext.EXPECT().updateWorkflowExecutionWithNew(
		gomock.Any(),
		persistence.UpdateWorkflowModeBypassCurrent,
		nil, // no new workflow
		nil, // no new workflow
		transactionPolicyPassive,
		nil,
	).Times(1)

	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil).Times(1)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil).Times(1)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101)).Times(1)
	s.mockMutableState.EXPECT().AddVisibilityTasks(gomock.Any()).Times(1)

	s.mockExecutionManager.On("DeleteCurrentWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockExecutionManager.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()

	s.mockArchivalClient.On("Archive", mock.Anything, mock.MatchedBy(func(req *archiver.ClientRequest) bool {
		return req.CallerService == common.HistoryServiceName && req.AttemptArchiveInline && req.ArchiveRequest.Targets[0] == archiver.ArchiveTargetHistory
	})).Return(&archiver.ClientResponse{
		HistoryArchivedInline: false,
	}, nil)

	namespaceCacheEntry := cache.NewNamespaceCacheEntryForTest(&persistencespb.NamespaceInfo{}, &persistencespb.NamespaceConfig{}, false, nil, 0, nil)
	err := s.timerQueueTaskExecutorBase.archiveWorkflow(&persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1}, s.mockWorkflowExecutionContext, s.mockMutableState, namespaceCacheEntry)
	s.NoError(err)
}

func (s *timerQueueTaskExecutorBaseSuiteV2) TestArchiveHistory_SendSignalErr() {
	s.mockWorkflowExecutionContext.EXPECT().loadExecutionStats().Return(&persistencespb.ExecutionStats{
		HistorySize: 1024 * 1024 * 1024,
	}, nil).Times(1)

	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil).Times(1)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil).Times(1)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101)).Times(1)

	s.mockArchivalClient.On("Archive", mock.Anything, mock.MatchedBy(func(req *archiver.ClientRequest) bool {
		return req.CallerService == common.HistoryServiceName && !req.AttemptArchiveInline && req.ArchiveRequest.Targets[0] == archiver.ArchiveTargetHistory
	})).Return(nil, errors.New("failed to send signal"))

	namespaceCacheEntry := cache.NewNamespaceCacheEntryForTest(&persistencespb.NamespaceInfo{}, &persistencespb.NamespaceConfig{}, false, nil, 0, nil)
	err := s.timerQueueTaskExecutorBase.archiveWorkflow(&persistencespb.TimerTaskInfo{
		ScheduleAttempt: 1}, s.mockWorkflowExecutionContext, s.mockMutableState, namespaceCacheEntry)
	s.Error(err)
}
