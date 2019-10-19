// Copyright (c) 2019 Uber Technologies, Inc.
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
	ctx "context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	nDCBranchMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller       *gomock.Controller
		mockContext      *MockworkflowExecutionContext
		mockMutableState *MockmutableState

		mockService         service.Service
		mockShard           *shardContextImpl
		mockDomainCache     *cache.DomainCacheMock
		mockHistoryV2Mgr    *mocks.HistoryV2Manager
		mockClusterMetadata *mocks.ClusterMetadata

		logger log.Logger

		branchIndex int
		domainID    string
		workflowID  string
		runID       string

		nDCBranchMgr *nDCBranchMgrImpl
	}
)

func TestNDCBranchMgrSuite(t *testing.T) {
	s := new(nDCBranchMgrSuite)
	suite.Run(t, s)
}

func (s *nDCBranchMgrSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockContext = NewMockworkflowExecutionContext(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockDomainCache = &cache.DomainCacheMock{}
	s.mockHistoryV2Mgr = &mocks.HistoryV2Manager{}
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockService = service.NewTestService(
		s.mockClusterMetadata,
		nil,
		metricsClient,
		nil,
		nil,
		nil,
		nil)
	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		domainCache:               s.mockDomainCache,
		shardInfo:                 &persistence.ShardInfo{ShardID: 10, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		historyV2Mgr:              s.mockHistoryV2Mgr,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		timeSource:                clock.NewRealTimeSource(),
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	s.domainID = uuid.New()
	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
	s.branchIndex = 0
	s.nDCBranchMgr = newNDCBranchMgr(
		s.mockShard, s.mockContext, s.mockMutableState, s.logger,
	)
}

func (s *nDCBranchMgrSuite) TearDownTest() {
	s.mockHistoryV2Mgr.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *nDCBranchMgrSuite) TestCreateNewBranch() {
	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventVersion := int64(200)
	baseBranchLCAEventID := int64(1394)
	baseBranchLastEventVersion := int64(400)
	baseBranchLastEventID := int64(2333)
	versionHistory := persistence.NewVersionHistory(baseBranchToken, []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(10, 0),
		persistence.NewVersionHistoryItem(50, 100),
		persistence.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		persistence.NewVersionHistoryItem(baseBranchLastEventID, baseBranchLastEventVersion),
	})
	versionHistories := persistence.NewVersionHistories(versionHistory)

	newBranchToken := []byte("some random new branch token")
	newVersionHistory, err := versionHistory.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	}).AnyTimes()

	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.MatchedBy(func(input *persistence.ForkHistoryBranchRequest) bool {
		input.Info = ""
		s.Equal(&persistence.ForkHistoryBranchRequest{
			ForkBranchToken: baseBranchToken,
			ForkNodeID:      baseBranchLCAEventID + 1,
			Info:            "",
			ShardID:         common.IntPtr(s.mockShard.GetShardID()),
		}, input)
		return true
	})).Return(&persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}, nil).Once()

	newIndex, err := s.nDCBranchMgr.createNewBranch(ctx.Background(), baseBranchToken, baseBranchLCAEventID, newVersionHistory)
	s.Nil(err)
	s.Equal(1, newIndex)

	compareVersionHistory, err := versionHistory.DuplicateUntilLCAItem(
		persistence.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	)
	s.NoError(err)
	s.NoError(compareVersionHistory.SetBranchToken(newBranchToken))
	newVersionHistory, err = versionHistories.GetVersionHistory(newIndex)
	s.NoError(err)
	s.True(compareVersionHistory.Equals(newVersionHistory))
}

func (s *nDCBranchMgrSuite) TestFlushBufferedEvents() {

	lastWriteVersion := int64(300)
	versionHistory := persistence.NewVersionHistory([]byte("some random base branch token"), []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(10, 0),
		persistence.NewVersionHistoryItem(50, 100),
		persistence.NewVersionHistoryItem(100, 200),
		persistence.NewVersionHistoryItem(150, 300),
	})
	versionHistories := persistence.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionHistory.Duplicate()
	err := incomingVersionHistory.AddOrUpdateItem(
		persistence.NewVersionHistoryItem(200, 300),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil).Times(1)
	decisionInfo := &decisionInfo{
		ScheduleID: 1234,
		StartedID:  2345,
	}
	s.mockMutableState.EXPECT().GetInFlightDecision().Return(decisionInfo, true).Times(1)
	// GetExecutionInfo's return value is not used by this test
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{}).Times(1)
	s.mockMutableState.EXPECT().AddDecisionTaskFailedEvent(
		decisionInfo.ScheduleID,
		decisionInfo.StartedID,
		shared.DecisionTaskFailedCauseFailoverCloseDecision,
		[]byte(nil),
		identityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&shared.HistoryEvent{}, nil).Times(1)
	s.mockMutableState.EXPECT().FlushBufferedEvents().Return(nil).Times(1)

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", lastWriteVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	s.mockContext.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	ctx := ctx.Background()

	_, _, err = s.nDCBranchMgr.flushBufferedEvents(ctx, incomingVersionHistory)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchAppendable_NoMissingEventInBetween() {

	versionHistory := persistence.NewVersionHistory([]byte("some random base branch token"), []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(10, 0),
		persistence.NewVersionHistoryItem(50, 100),
		persistence.NewVersionHistoryItem(100, 200),
		persistence.NewVersionHistoryItem(150, 300),
	})
	versionHistories := persistence.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionHistory.Duplicate()
	err := incomingVersionHistory.AddOrUpdateItem(
		persistence.NewVersionHistoryItem(200, 300),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()

	doContinue, index, err := s.nDCBranchMgr.prepareVersionHistory(
		ctx.Background(),
		incomingVersionHistory,
		150+1,
		300)
	s.NoError(err)
	s.True(doContinue)
	s.Equal(0, index)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchAppendable_MissingEventInBetween() {

	versionHistory := persistence.NewVersionHistory([]byte("some random base branch token"), []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(10, 0),
		persistence.NewVersionHistoryItem(50, 100),
		persistence.NewVersionHistoryItem(100, 200),
		persistence.NewVersionHistoryItem(150, 300),
	})
	versionHistories := persistence.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionHistory.Duplicate()
	incomingFirstEventVersionHistoryItem := persistence.NewVersionHistoryItem(200, 300)
	err := incomingVersionHistory.AddOrUpdateItem(
		incomingFirstEventVersionHistoryItem,
	)
	s.NoError(err)

	execution := &persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()

	_, _, err = s.nDCBranchMgr.prepareVersionHistory(
		ctx.Background(),
		incomingVersionHistory,
		150+2,
		300)
	s.IsType(&shared.RetryTaskV2Error{}, err)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchNotAppendable_NoMissingEventInBetween() {
	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventID := int64(85)
	baseBranchLCAEventVersion := int64(200)

	versionHistory := persistence.NewVersionHistory(baseBranchToken, []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(10, 0),
		persistence.NewVersionHistoryItem(50, 100),
		persistence.NewVersionHistoryItem(baseBranchLCAEventID+10, baseBranchLCAEventVersion),
		persistence.NewVersionHistoryItem(150, 300),
	})
	versionHistories := persistence.NewVersionHistories(versionHistory)

	incomingVersionHistory := persistence.NewVersionHistory(nil, []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(10, 0),
		persistence.NewVersionHistoryItem(50, 100),
		persistence.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		persistence.NewVersionHistoryItem(200, 400),
	})

	newBranchToken := []byte("some random new branch token")

	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	}).AnyTimes()

	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.MatchedBy(func(input *persistence.ForkHistoryBranchRequest) bool {
		input.Info = ""
		s.Equal(&persistence.ForkHistoryBranchRequest{
			ForkBranchToken: baseBranchToken,
			ForkNodeID:      baseBranchLCAEventID + 1,
			Info:            "",
			ShardID:         common.IntPtr(s.mockShard.GetShardID()),
		}, input)
		return true
	})).Return(&persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}, nil).Once()

	doContinue, index, err := s.nDCBranchMgr.prepareVersionHistory(
		ctx.Background(),
		incomingVersionHistory,
		baseBranchLCAEventID+1,
		baseBranchLCAEventVersion,
	)
	s.NoError(err)
	s.True(doContinue)
	s.Equal(1, index)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchNotAppendable_MissingEventInBetween() {
	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventID := int64(85)
	baseBranchLCAEventVersion := int64(200)
	baseBranchLastEventID := int64(150)
	baseBranchLastEventVersion := int64(300)

	versionHistory := persistence.NewVersionHistory(baseBranchToken, []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(10, 0),
		persistence.NewVersionHistoryItem(50, 100),
		persistence.NewVersionHistoryItem(baseBranchLCAEventID+10, baseBranchLCAEventVersion),
		persistence.NewVersionHistoryItem(baseBranchLastEventID, baseBranchLastEventVersion),
	})
	versionHistories := persistence.NewVersionHistories(versionHistory)

	incomingVersionHistory := persistence.NewVersionHistory(nil, []*persistence.VersionHistoryItem{
		persistence.NewVersionHistoryItem(10, 0),
		persistence.NewVersionHistoryItem(50, 100),
		persistence.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		persistence.NewVersionHistoryItem(200, 400),
	})

	execution := &persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	}
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(execution).AnyTimes()

	_, _, err := s.nDCBranchMgr.prepareVersionHistory(
		ctx.Background(),
		incomingVersionHistory,
		baseBranchLCAEventID+2,
		baseBranchLCAEventVersion,
	)
	s.IsType(&shared.RetryTaskV2Error{}, err)
}
