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
	"context"
	"reflect"
	"runtime"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	"github.com/uber/cadence/.gen/go/shared"
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
	nDCWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller       *gomock.Controller
		mockContext      *MockworkflowExecutionContext
		mockMutableState *MockmutableState

		mockService         service.Service
		mockShard           *shardContextImpl
		mockClusterMetadata *mocks.ClusterMetadata
		mockDomainCache     *cache.DomainCacheMock
		logger              log.Logger

		domainID   string
		workflowID string
		runID      string
	}
)

func TestNDCWorkflowSuite(t *testing.T) {
	s := new(nDCWorkflowSuite)
	suite.Run(t, s)
}

func (s *nDCWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockContext = NewMockworkflowExecutionContext(s.controller)
	s.mockMutableState = NewMockmutableState(s.controller)

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.mockClusterMetadata = &mocks.ClusterMetadata{}
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.mockService = service.NewTestService(s.mockClusterMetadata, nil, metricsClient, nil, nil, nil, nil)
	s.mockDomainCache = &cache.DomainCacheMock{}

	s.mockShard = &shardContextImpl{
		service:                   s.mockService,
		shardInfo:                 &persistence.ShardInfo{ShardID: 10, RangeID: 1, TransferAckLevel: 0},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    s.logger,
		domainCache:               s.mockDomainCache,
		metricsClient:             metrics.NewClient(tally.NoopScope, metrics.History),
		timeSource:                clock.NewRealTimeSource(),
	}
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	s.domainID = uuid.New()
	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
}

func (s *nDCWorkflowSuite) TearDownTest() {
	s.mockDomainCache.AssertExpectations(s.T())
	s.controller.Finish()
}

func (s *nDCWorkflowSuite) TestGetMethods() {
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	versionHistoryItem := persistence.NewVersionHistoryItem(lastEventID, lastEventVersion)
	versionHistory := persistence.NewVersionHistory(
		branchToken,
		[]*persistence.VersionHistoryItem{versionHistoryItem},
	)
	versionHistories := persistence.NewVersionHistories(versionHistory)
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:        s.domainID,
		WorkflowID:      s.workflowID,
		RunID:           s.runID,
		LastEventTaskID: lastEventTaskID,
	}).AnyTimes()

	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockDomainCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		noopReleaseFn,
	)

	s.Equal(s.mockContext, nDCWorkflow.getContext())
	s.Equal(s.mockMutableState, nDCWorkflow.getMutableState())
	// NOTE golang does not seem to let people compare functions, easily
	//  link: https://github.com/stretchr/testify/issues/182
	// this is a hack to compare 2 functions, being the same
	expectedReleaseFn := runtime.FuncForPC(reflect.ValueOf(noopReleaseFn).Pointer()).Name()
	actualReleaseFn := runtime.FuncForPC(reflect.ValueOf(nDCWorkflow.getReleaseFn()).Pointer()).Name()
	s.Equal(expectedReleaseFn, actualReleaseFn)
	version, taskID, err := nDCWorkflow.getVectorClock()
	s.NoError(err)
	s.Equal(lastEventVersion, version)
	s.Equal(lastEventTaskID, taskID)
}

func (s *nDCWorkflowSuite) TestHappensAfter_LargerVersion() {
	thisLastWriteVersion := int64(0)
	thisLastEventTaskID := int64(100)
	thatLastWriteVersion := thisLastWriteVersion - 1
	thatLastEventTaskID := int64(123)

	s.True(workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	))
}

func (s *nDCWorkflowSuite) TestHappensAfter_SmallerVersion() {
	thisLastWriteVersion := int64(0)
	thisLastEventTaskID := int64(100)
	thatLastWriteVersion := thisLastWriteVersion + 1
	thatLastEventTaskID := int64(23)

	s.False(workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	))
}

func (s *nDCWorkflowSuite) TestHappensAfter_SameVersion_SmallerTaskID() {
	thisLastWriteVersion := int64(0)
	thisLastEventTaskID := int64(100)
	thatLastWriteVersion := thisLastWriteVersion
	thatLastEventTaskID := thisLastEventTaskID + 1

	s.False(workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	))
}

func (s *nDCWorkflowSuite) TestHappensAfter_SameVersion_LatrgerTaskID() {
	thisLastWriteVersion := int64(0)
	thisLastEventTaskID := int64(100)
	thatLastWriteVersion := thisLastWriteVersion
	thatLastEventTaskID := thisLastEventTaskID - 1

	s.True(workflowHappensAfter(
		thisLastWriteVersion,
		thisLastEventTaskID,
		thatLastWriteVersion,
		thatLastEventTaskID,
	))
}

func (s *nDCWorkflowSuite) TestSuppressWorkflowBy_Error() {
	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockDomainCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		noopReleaseFn,
	)

	incomingMockContext := NewMockworkflowExecutionContext(s.controller)
	incomingMockMutableState := NewMockmutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockDomainCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		noopReleaseFn,
	)

	// cannot suppress by older workflow
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	versionHistories := persistence.NewVersionHistories(persistence.NewVersionHistory(
		branchToken,
		[]*persistence.VersionHistoryItem{
			persistence.NewVersionHistoryItem(lastEventID, lastEventVersion),
		},
	))
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:        s.domainID,
		WorkflowID:      s.workflowID,
		RunID:           s.runID,
		LastEventTaskID: lastEventTaskID,
	}).AnyTimes()

	incomingRunID := uuid.New()
	incomingBranchToken := []byte("other random branch token")
	incomingLastEventID := int64(2)
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion - 1
	incomingVersionHistories := persistence.NewVersionHistories(persistence.NewVersionHistory(
		incomingBranchToken,
		[]*persistence.VersionHistoryItem{
			persistence.NewVersionHistoryItem(incomingLastEventID, incomingLastEventVersion),
		},
	))
	incomingMockMutableState.EXPECT().GetVersionHistories().Return(incomingVersionHistories).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:        s.domainID,
		WorkflowID:      s.workflowID,
		RunID:           incomingRunID,
		LastEventTaskID: incomingLastEventTaskID,
	}).AnyTimes()

	_, err := nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.Error(err)
}

func (s *nDCWorkflowSuite) TestSuppressWorkflowBy_Terminate() {
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	versionHistories := persistence.NewVersionHistories(persistence.NewVersionHistory(
		branchToken,
		[]*persistence.VersionHistoryItem{
			persistence.NewVersionHistoryItem(lastEventID, lastEventVersion),
		},
	))
	s.mockMutableState.EXPECT().GetNextEventID().Return(lastEventID + 1).AnyTimes()
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:        s.domainID,
		WorkflowID:      s.workflowID,
		RunID:           s.runID,
		LastEventTaskID: lastEventTaskID,
	}).AnyTimes()
	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockDomainCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		noopReleaseFn,
	)

	incomingRunID := uuid.New()
	incomingBranchToken := []byte("other random branch token")
	incomingLastEventID := int64(2)
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingVersionHistories := persistence.NewVersionHistories(persistence.NewVersionHistory(
		incomingBranchToken,
		[]*persistence.VersionHistoryItem{
			persistence.NewVersionHistoryItem(incomingLastEventID, incomingLastEventVersion),
		},
	))
	incomingMockContext := NewMockworkflowExecutionContext(s.controller)
	incomingMockMutableState := NewMockmutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockDomainCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		noopReleaseFn,
	)
	incomingMockMutableState.EXPECT().GetVersionHistories().Return(incomingVersionHistories).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:        s.domainID,
		WorkflowID:      s.workflowID,
		RunID:           incomingRunID,
		LastEventTaskID: incomingLastEventTaskID,
	}).AnyTimes()

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", lastEventVersion).Return(cluster.TestCurrentClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastEventVersion, true).Return(nil).AnyTimes()
	inFlightDecision := &decisionInfo{
		Version:    1234,
		ScheduleID: 5678,
		StartedID:  9012,
	}
	s.mockMutableState.EXPECT().GetInFlightDecision().Return(inFlightDecision, true).Times(1)
	s.mockMutableState.EXPECT().AddDecisionTaskFailedEvent(
		inFlightDecision.ScheduleID,
		inFlightDecision.StartedID,
		shared.DecisionTaskFailedCauseFailoverCloseDecision,
		[]byte(nil),
		identityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&shared.HistoryEvent{}, nil).Times(1)
	s.mockMutableState.EXPECT().FlushBufferedEvents().Return(nil).Times(1)

	s.mockMutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		lastEventID+1, workflowTerminationReason, gomock.Any(), workflowTerminationIdentity,
	).Return(&shared.HistoryEvent{}, nil).Times(1)

	// if workflow is in zombie or finished state, keep as is
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(1)
	policy, err := nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(transactionPolicyPassive, policy)

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(1)
	policy, err = nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(transactionPolicyActive, policy)
}

func (s *nDCWorkflowSuite) TestSuppressWorkflowBy_Zombiefy() {
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	lastEventTaskID := int64(144)
	lastEventVersion := int64(12)
	versionHistories := persistence.NewVersionHistories(persistence.NewVersionHistory(
		branchToken,
		[]*persistence.VersionHistoryItem{
			persistence.NewVersionHistoryItem(lastEventID, lastEventVersion),
		},
	))
	s.mockMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()
	executionInfo := &persistence.WorkflowExecutionInfo{
		DomainID:        s.domainID,
		WorkflowID:      s.workflowID,
		RunID:           s.runID,
		LastEventTaskID: lastEventTaskID,
		State:           persistence.WorkflowStateRunning,
		CloseStatus:     persistence.WorkflowCloseStatusNone,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(executionInfo).AnyTimes()
	nDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockDomainCache,
		s.mockClusterMetadata,
		s.mockContext,
		s.mockMutableState,
		noopReleaseFn,
	)

	incomingRunID := uuid.New()
	incomingBranchToken := []byte("other random branch token")
	incomingLastEventID := int64(2)
	incomingLastEventTaskID := int64(144)
	incomingLastEventVersion := lastEventVersion + 1
	incomingVersionHistories := persistence.NewVersionHistories(persistence.NewVersionHistory(
		incomingBranchToken,
		[]*persistence.VersionHistoryItem{
			persistence.NewVersionHistoryItem(incomingLastEventID, incomingLastEventVersion),
		},
	))
	incomingMockContext := NewMockworkflowExecutionContext(s.controller)
	incomingMockMutableState := NewMockmutableState(s.controller)
	incomingNDCWorkflow := newNDCWorkflow(
		context.Background(),
		s.mockDomainCache,
		s.mockClusterMetadata,
		incomingMockContext,
		incomingMockMutableState,
		noopReleaseFn,
	)
	incomingMockMutableState.EXPECT().GetVersionHistories().Return(incomingVersionHistories).AnyTimes()
	incomingMockMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		DomainID:        s.domainID,
		WorkflowID:      s.workflowID,
		RunID:           incomingRunID,
		LastEventTaskID: incomingLastEventTaskID,
	}).AnyTimes()

	s.mockClusterMetadata.On("ClusterNameForFailoverVersion", lastEventVersion).Return(cluster.TestAlternativeClusterName)
	s.mockClusterMetadata.On("GetCurrentClusterName").Return(cluster.TestCurrentClusterName)

	// if workflow is in zombie or finished state, keep as is
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(1)
	policy, err := nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(transactionPolicyPassive, policy)

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).Times(1)
	policy, err = nDCWorkflow.suppressBy(incomingNDCWorkflow)
	s.NoError(err)
	s.Equal(transactionPolicyPassive, policy)
	s.Equal(persistence.WorkflowStateZombie, executionInfo.State)
	s.Equal(persistence.WorkflowCloseStatusNone, executionInfo.CloseStatus)
}
