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
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/clock"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service"
)

type (
	nDCConflictResolverSuite struct {
		suite.Suite

		controller       *gomock.Controller
		mockStateBuilder *MocknDCStateRebuilder

		mockService      service.Service
		mockShard        *shardContextImpl
		mockContext      *mockWorkflowExecutionContext
		mockMutableState *mockMutableState
		logger           log.Logger

		domainID   string
		domainName string
		workflowID string
		runID      string

		nDCConflictResolver *nDCConflictResolverImpl
	}
)

func TestNDCConflictResolverSuite(t *testing.T) {
	s := new(nDCConflictResolverSuite)
	suite.Run(t, s)
}

func (s *nDCConflictResolverSuite) SetupTest() {
	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockService = service.NewTestService(
		nil,
		nil,
		metricsClient,
		nil,
		nil,
		nil,
		nil)
	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 10, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		timeSource:                clock.NewRealTimeSource(),
	}

	s.domainID = uuid.New()
	s.domainName = "some random domain name"
	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
	s.mockContext = &mockWorkflowExecutionContext{}
	s.mockMutableState = &mockMutableState{}

	s.controller = gomock.NewController(s.T())
	s.mockStateBuilder = NewMocknDCStateRebuilder(s.controller)
	s.nDCConflictResolver = newNDCConflictResolver(
		s.mockShard, s.mockContext, s.mockMutableState, s.logger,
	)
	s.nDCConflictResolver.stateRebuilder = s.mockStateBuilder
}

func (s *nDCConflictResolverSuite) TearDownTest() {
	s.mockContext.AssertExpectations(s.T())
	s.mockMutableState.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *nDCConflictResolverSuite) TestRebuild() {
	ctx := ctx.Background()
	updateCondition := int64(59)
	requestID := uuid.New()
	version := int64(12)
	historySize := int64(12345)

	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(5)
	versionHistory0 := persistence.NewVersionHistory(
		branchToken0,
		[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID0, version)},
	)
	branchToken1 := []byte("other random branch token")
	lastEventID1 := int64(2)
	versionHistory1 := persistence.NewVersionHistory(
		branchToken1,
		[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID1, version)},
	)
	versionHistories := persistence.NewVersionHistories(versionHistory0)
	_, _, err := versionHistories.AddVersionHistory(versionHistory1)
	s.NoError(err)

	s.mockMutableState.On("GetUpdateCondition").Return(updateCondition)
	s.mockMutableState.On("GetVersionHistories").Return(versionHistories)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	})

	workflowIdentifier := definition.NewWorkflowIdentifier(
		s.domainID,
		s.workflowID,
		s.runID,
	)
	mockRebuildMutableState := &mockMutableState{}
	defer mockRebuildMutableState.AssertExpectations(s.T())
	mockRebuildMutableState.On("GetVersionHistories").Return(
		persistence.NewVersionHistories(
			persistence.NewVersionHistory(
				branchToken1,
				[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID1, version)},
			),
		),
	).Once()
	mockRebuildMutableState.On("SetVersionHistories", versionHistories).Return(nil).Once()
	mockRebuildMutableState.On("SetUpdateCondition", updateCondition).Once()

	s.mockStateBuilder.EXPECT().rebuild(
		ctx,
		gomock.Any(),
		workflowIdentifier,
		branchToken1,
		lastEventID1,
		version,
		workflowIdentifier,
		branchToken1,
		requestID,
	).Return(mockRebuildMutableState, historySize, nil).Times(1)

	s.mockContext.On("clear").Once()
	s.mockContext.On("setHistorySize", historySize).Once()
	rebuiltMutableState, err := s.nDCConflictResolver.rebuild(ctx, 1, requestID)
	s.NoError(err)
	s.NotNil(rebuiltMutableState)
	s.Equal(1, versionHistories.GetCurrentVersionHistoryIndex())
}

func (s *nDCConflictResolverSuite) TestPrepareMutableState_NoRebuild() {
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	version := int64(12)
	versionHistoryItem := persistence.NewVersionHistoryItem(lastEventID, version)
	versionHistory := persistence.NewVersionHistory(
		branchToken,
		[]*persistence.VersionHistoryItem{versionHistoryItem},
	)
	versionHistories := persistence.NewVersionHistories(versionHistory)
	s.mockMutableState.On("GetVersionHistories").Return(versionHistories)

	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.prepareMutableState(ctx.Background(), 0, version)
	s.NoError(err)
	s.False(isRebuilt)
	s.Equal(s.mockMutableState, rebuiltMutableState)
}

func (s *nDCConflictResolverSuite) TestPrepareMutableState_Rebuild() {
	ctx := ctx.Background()
	updateCondition := int64(59)
	version := int64(12)
	incomingVersion := version + 1
	historySize := int64(12345)

	// current branch
	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)

	versionHistoryItem0 := persistence.NewVersionHistoryItem(lastEventID0, version)
	versionHistory0 := persistence.NewVersionHistory(
		branchToken0,
		[]*persistence.VersionHistoryItem{versionHistoryItem0},
	)

	// stale branch, used for rebuild
	branchToken1 := []byte("other random branch token")
	lastEventID1 := lastEventID0 - 1
	versionHistoryItem1 := persistence.NewVersionHistoryItem(lastEventID1, version)
	versionHistory1 := persistence.NewVersionHistory(
		branchToken1,
		[]*persistence.VersionHistoryItem{versionHistoryItem1},
	)

	versionHistories := persistence.NewVersionHistories(versionHistory0)
	_, _, err := versionHistories.AddVersionHistory(versionHistory1)
	s.Nil(err)

	s.mockMutableState.On("GetUpdateCondition").Return(updateCondition)
	s.mockMutableState.On("GetVersionHistories").Return(versionHistories)
	s.mockMutableState.On("GetExecutionInfo").Return(&persistence.WorkflowExecutionInfo{
		DomainID:   s.domainID,
		WorkflowID: s.workflowID,
		RunID:      s.runID,
	})

	workflowIdentifier := definition.NewWorkflowIdentifier(
		s.domainID,
		s.workflowID,
		s.runID,
	)
	mockRebuildMutableState := &mockMutableState{}
	defer mockRebuildMutableState.AssertExpectations(s.T())
	mockRebuildMutableState.On("GetVersionHistories").Return(
		persistence.NewVersionHistories(
			persistence.NewVersionHistory(
				branchToken1,
				[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID1, version)},
			),
		),
	).Once()
	mockRebuildMutableState.On("SetVersionHistories", versionHistories).Return(nil).Once()
	mockRebuildMutableState.On("SetUpdateCondition", updateCondition).Once()

	s.mockStateBuilder.EXPECT().rebuild(
		ctx,
		gomock.Any(),
		workflowIdentifier,
		branchToken1,
		lastEventID1,
		version,
		workflowIdentifier,
		branchToken1,
		gomock.Any(),
	).Return(mockRebuildMutableState, historySize, nil).Times(1)

	s.mockContext.On("clear").Once()
	s.mockContext.On("setHistorySize", int64(historySize)).Once()
	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.prepareMutableState(ctx, 1, incomingVersion)
	s.NoError(err)
	s.NotNil(rebuiltMutableState)
	s.True(isRebuilt)
}
