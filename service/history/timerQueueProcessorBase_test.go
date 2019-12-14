// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/worker/archiver"
)

type (
	timerQueueProcessorBaseSuite struct {
		suite.Suite
		*require.Assertions

		clusterName string
		logger      log.Logger

		controller                   *gomock.Controller
		mockShard                    *shardContextTest
		mockWorkflowExecutionContext *MockworkflowExecutionContext
		mockMutableState             *MockmutableState

		mockQueueAckMgr       *MockTimerQueueAckMgr
		mockExecutionManager  *mocks.ExecutionManager
		mockVisibilityManager *mocks.VisibilityManager
		mockHistoryV2Manager  *mocks.HistoryV2Manager
		mockArchivalClient    *archiver.ClientMock

		scope            int
		notificationChan chan struct{}

		timerQueueProcessor *timerQueueProcessorBase
	}
)

func TestTimerQueueProcessorBaseSuite(t *testing.T) {
	s := new(timerQueueProcessorBaseSuite)
	suite.Run(t, s)
}

func (s *timerQueueProcessorBaseSuite) SetupSuite() {

}

func (s *timerQueueProcessorBaseSuite) TearDownSuite() {

}

func (s *timerQueueProcessorBaseSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	shardID := 0
	s.clusterName = cluster.TestAlternativeClusterName
	s.scope = 0

	s.controller = gomock.NewController(s.T())
	s.mockWorkflowExecutionContext = NewMockworkflowExecutionContext(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:          shardID,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		NewDynamicConfigForTest(),
	)

	s.mockExecutionManager = s.mockShard.resource.ExecutionMgr
	s.mockVisibilityManager = s.mockShard.resource.VisibilityMgr
	s.mockHistoryV2Manager = s.mockShard.resource.HistoryMgr
	s.mockArchivalClient = &archiver.ClientMock{}
	s.mockQueueAckMgr = &MockTimerQueueAckMgr{}

	s.logger = s.mockShard.GetLogger()

	s.notificationChan = make(chan struct{})
	h := &historyEngineImpl{
		shard:          s.mockShard,
		logger:         s.logger,
		metricsClient:  s.mockShard.GetMetricsClient(),
		visibilityMgr:  s.mockVisibilityManager,
		historyV2Mgr:   s.mockHistoryV2Manager,
		archivalClient: s.mockArchivalClient,
	}

	s.timerQueueProcessor = newTimerQueueProcessorBase(
		s.scope,
		s.mockShard,
		h,
		s.mockQueueAckMgr,
		NewLocalTimerGate(clock.NewRealTimeSource()),
		dynamicconfig.GetIntPropertyFn(10),
		s.logger,
	)
}

func (s *timerQueueProcessorBaseSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
	s.mockQueueAckMgr.AssertExpectations(s.T())
	s.mockArchivalClient.AssertExpectations(s.T())
}

func (s *timerQueueProcessorBaseSuite) TestDeleteWorkflow_NoErr() {
	task := &persistence.TimerTaskInfo{
		TaskID:              12345,
		VisibilityTimestamp: time.Now(),
	}
	executionInfo := workflow.WorkflowExecution{
		WorkflowId: &task.WorkflowID,
		RunId:      &task.RunID,
	}
	ctx := newWorkflowExecutionContext(task.DomainID, executionInfo, s.mockShard, s.mockExecutionManager, log.NewNoop())

	s.mockExecutionManager.On("DeleteCurrentWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockExecutionManager.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockHistoryV2Manager.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
	s.mockVisibilityManager.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil).Times(1)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil).AnyTimes()

	err := s.timerQueueProcessor.deleteWorkflow(task, ctx, s.mockMutableState)
	s.NoError(err)
}

func (s *timerQueueProcessorBaseSuite) TestArchiveHistory_NoErr_InlineArchivalFailed() {
	s.mockWorkflowExecutionContext.EXPECT().loadExecutionStats().Return(&persistence.ExecutionStats{
		HistorySize: 1024,
	}, nil).Times(1)
	s.mockWorkflowExecutionContext.EXPECT().clear().Times(1)

	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil).Times(1)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil).Times(1)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101)).Times(1)

	s.mockExecutionManager.On("DeleteCurrentWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockExecutionManager.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockVisibilityManager.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()

	s.mockArchivalClient.On("Archive", mock.Anything, mock.MatchedBy(func(req *archiver.ClientRequest) bool {
		return req.CallerService == common.HistoryServiceName && req.AttemptArchiveInline && req.ArchiveRequest.Targets[0] == archiver.ArchiveTargetHistory
	})).Return(&archiver.ClientResponse{
		HistoryArchivedInline: false,
	}, nil)

	domainCacheEntry := cache.NewDomainCacheEntryForTest(&persistence.DomainInfo{}, &persistence.DomainConfig{}, false, nil, 0, nil)
	err := s.timerQueueProcessor.archiveWorkflow(&persistence.TimerTaskInfo{}, s.mockWorkflowExecutionContext, s.mockMutableState, domainCacheEntry)
	s.NoError(err)
}

func (s *timerQueueProcessorBaseSuite) TestArchiveHistory_SendSignalErr() {
	s.mockWorkflowExecutionContext.EXPECT().loadExecutionStats().Return(&persistence.ExecutionStats{
		HistorySize: 1024 * 1024 * 1024,
	}, nil).Times(1)

	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil).Times(1)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil).Times(1)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101)).Times(1)

	s.mockArchivalClient.On("Archive", mock.Anything, mock.MatchedBy(func(req *archiver.ClientRequest) bool {
		return req.CallerService == common.HistoryServiceName && !req.AttemptArchiveInline && req.ArchiveRequest.Targets[0] == archiver.ArchiveTargetHistory
	})).Return(nil, errors.New("failed to send signal"))

	domainCacheEntry := cache.NewDomainCacheEntryForTest(&persistence.DomainInfo{}, &persistence.DomainConfig{}, false, nil, 0, nil)
	err := s.timerQueueProcessor.archiveWorkflow(&persistence.TimerTaskInfo{}, s.mockWorkflowExecutionContext, s.mockMutableState, domainCacheEntry)
	s.Error(err)
}
