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
	"github.com/uber-go/tally"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/messaging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
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
		mockWorkflowExecutionContext *MockworkflowExecutionContext
		mockMutableState             *MockmutableState

		mockService           service.Service
		mockShard             ShardContext
		mockClusterMetadata   *mocks.ClusterMetadata
		mockMessagingClient   messaging.Client
		mockQueueAckMgr       *MockTimerQueueAckMgr
		mockClientBean        *client.MockClientBean
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

	s.controller = gomock.NewController(s.T())
	s.mockWorkflowExecutionContext = NewMockworkflowExecutionContext(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)

	shardID := 0
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.clusterName = cluster.TestAlternativeClusterName
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockQueueAckMgr = &MockTimerQueueAckMgr{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	s.mockClientBean = &client.MockClientBean{}
	s.mockService = service.NewTestService(s.mockClusterMetadata, nil, metricsClient, s.mockClientBean, nil, nil, nil)
	s.mockExecutionManager = &mocks.ExecutionManager{}
	s.mockVisibilityManager = &mocks.VisibilityManager{}
	s.mockHistoryV2Manager = &mocks.HistoryV2Manager{}
	s.mockArchivalClient = &archiver.ClientMock{}
	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: shardID, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		clusterMetadata:           s.mockClusterMetadata,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		metricsClient:             metricsClient,
		standbyClusterCurrentTime: make(map[string]time.Time),
		timeSource:                clock.NewRealTimeSource(),
		executionManager:          s.mockExecutionManager,
	}

	s.scope = 0
	s.notificationChan = make(chan struct{})
	h := &historyEngineImpl{
		shard:          s.mockShard,
		logger:         s.logger,
		metricsClient:  metricsClient,
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
	s.mockQueueAckMgr.AssertExpectations(s.T())
	s.mockClientBean.AssertExpectations(s.T())
	s.mockExecutionManager.AssertExpectations(s.T())
	s.mockHistoryV2Manager.AssertExpectations(s.T())
	s.mockVisibilityManager.AssertExpectations(s.T())
	s.mockArchivalClient.AssertExpectations(s.T())
	s.controller.Finish()
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
	err := s.timerQueueProcessor.archiveWorkflow(&persistence.TimerTaskInfo{}, s.mockWorkflowExecutionContext, s.mockMutableState, domainCacheEntry, true, false)
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
	err := s.timerQueueProcessor.archiveWorkflow(&persistence.TimerTaskInfo{}, s.mockWorkflowExecutionContext, s.mockMutableState, domainCacheEntry, true, false)
	s.Error(err)
}

func (s *timerQueueProcessorBaseSuite) TestArchiveVisibility_SendSignalNoErr() {
	s.mockWorkflowExecutionContext.EXPECT().clear().Times(1)

	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil).Times(1)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101)).AnyTimes()
	s.mockMutableState.EXPECT().GetStartEvent().Return(&workflow.HistoryEvent{
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
	}, nil).Times(1)
	s.mockMutableState.EXPECT().GetCompletionEvent().Return(&workflow.HistoryEvent{
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
	}, nil).Times(1)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		WorkflowTypeName: "some random workflow type name",
		StartTimestamp:   time.Now().Add(-time.Hour),
		CloseStatus:      1,
	}).Times(1)

	s.mockExecutionManager.On("DeleteCurrentWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockExecutionManager.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()
	s.mockHistoryV2Manager.On("DeleteHistoryBranch", mock.Anything).Return(nil).Once()
	s.mockVisibilityManager.On("DeleteWorkflowExecution", mock.Anything).Return(nil).Once()

	s.mockArchivalClient.On("Archive", mock.Anything, mock.MatchedBy(func(req *archiver.ClientRequest) bool {
		return req.CallerService == common.HistoryServiceName && req.AttemptArchiveInline && req.ArchiveRequest.Targets[0] == archiver.ArchiveTargetVisibility
	})).Return(&archiver.ClientResponse{
		HistoryArchivedInline: false,
	}, nil)

	domainCacheEntry := cache.NewDomainCacheEntryForTest(&persistence.DomainInfo{}, &persistence.DomainConfig{}, false, nil, 0, nil)
	err := s.timerQueueProcessor.archiveWorkflow(&persistence.TimerTaskInfo{}, s.mockWorkflowExecutionContext, s.mockMutableState, domainCacheEntry, false, true)
	s.NoError(err)
}

func (s *timerQueueProcessorBaseSuite) TestArchiveBoth_SendSignalErr() {
	s.mockWorkflowExecutionContext.EXPECT().loadExecutionStats().Return(&persistence.ExecutionStats{
		HistorySize: 1024 * 1024 * 1024,
	}, nil).Times(1)

	s.mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{1, 2, 3}, nil).Times(1)
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1234), nil).Times(1)
	s.mockMutableState.EXPECT().GetNextEventID().Return(int64(101)).Times(2)
	s.mockMutableState.EXPECT().GetStartEvent().Return(&workflow.HistoryEvent{
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
	}, nil).Times(1)
	s.mockMutableState.EXPECT().GetCompletionEvent().Return(&workflow.HistoryEvent{
		Timestamp: common.Int64Ptr(time.Now().UnixNano()),
	}, nil).Times(1)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		WorkflowTypeName: "some random workflow type name",
		StartTimestamp:   time.Now().Add(-time.Hour),
		CloseStatus:      1,
	}).Times(1)

	s.mockArchivalClient.On("Archive", mock.Anything, mock.MatchedBy(func(req *archiver.ClientRequest) bool {
		return req.CallerService == common.HistoryServiceName && !req.AttemptArchiveInline && len(req.ArchiveRequest.Targets) == 2
	})).Return(nil, errors.New("failed to send signal"))

	domainCacheEntry := cache.NewDomainCacheEntryForTest(&persistence.DomainInfo{}, &persistence.DomainConfig{}, false, nil, 0, nil)
	err := s.timerQueueProcessor.archiveWorkflow(&persistence.TimerTaskInfo{}, s.mockWorkflowExecutionContext, s.mockMutableState, domainCacheEntry, true, true)
	s.Error(err)
}
