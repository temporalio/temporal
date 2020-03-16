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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/definition"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
)

type (
	nDCWorkflowResetterSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		mockShard               *shardContextTest
		mockBaseMutableState    *MockmutableState
		mockRebuiltMutableState *MockmutableState
		mockTransactionMgr      *MocknDCTransactionMgr
		mockStateBuilder        *MocknDCStateRebuilder

		logger           log.Logger
		mockHistoryV2Mgr *mocks.HistoryV2Manager

		domainID   string
		domainName string
		workflowID string
		baseRunID  string
		newContext workflowExecutionContext
		newRunID   string

		nDCWorkflowResetter *nDCWorkflowResetterImpl
	}
)

func TestNDCWorkflowResetterSuite(t *testing.T) {
	s := new(nDCWorkflowResetterSuite)
	suite.Run(t, s)
}

func (s *nDCWorkflowResetterSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockBaseMutableState = NewMockmutableState(s.controller)
	s.mockRebuiltMutableState = NewMockmutableState(s.controller)
	s.mockTransactionMgr = NewMocknDCTransactionMgr(s.controller)
	s.mockStateBuilder = NewMocknDCStateRebuilder(s.controller)

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardID:          10,
				RangeID:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr

	s.logger = s.mockShard.GetLogger()

	s.domainID = uuid.New()
	s.domainName = "some random domain name"
	s.workflowID = "some random workflow ID"
	s.baseRunID = uuid.New()
	s.newContext = newWorkflowExecutionContext(
		s.domainID,
		commonproto.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.newRunID,
		},
		s.mockShard,
		nil,
		s.logger,
	)
	s.newRunID = uuid.New()

	s.nDCWorkflowResetter = newNDCWorkflowResetter(
		s.mockShard, s.mockTransactionMgr, s.domainID, s.workflowID, s.baseRunID, s.newContext, s.newRunID, s.logger,
	)
	s.nDCWorkflowResetter.stateRebuilder = s.mockStateBuilder
}

func (s *nDCWorkflowResetterSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *nDCWorkflowResetterSuite) TestResetWorkflow_NoError() {
	ctx := context.Background()
	now := time.Now()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := persistence.NewVersionHistory(
		branchToken,
		[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := persistence.NewVersionHistories(versionHistory)

	baseEventID := lastEventID - 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingVersion := baseVersion + 3

	rebuiltHistorySize := int64(9999)
	newBranchToken := []byte("other random branch token")

	s.mockBaseMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()

	mockBaseWorkflowReleaseFnCalled := false
	mockBaseWorkflowReleaseFn := func(err error) {
		mockBaseWorkflowReleaseFnCalled = true
	}
	mockBaseWorkflow := NewMocknDCWorkflow(s.controller)
	mockBaseWorkflow.EXPECT().getMutableState().Return(s.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().getReleaseFn().Return(mockBaseWorkflowReleaseFn).Times(1)

	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(
		ctx,
		s.domainID,
		s.workflowID,
		s.baseRunID,
	).Return(mockBaseWorkflow, nil).Times(1)

	s.mockStateBuilder.EXPECT().rebuild(
		ctx,
		now,
		definition.NewWorkflowIdentifier(
			s.domainID,
			s.workflowID,
			s.baseRunID,
		),
		branchToken,
		baseEventID,
		baseVersion,
		definition.NewWorkflowIdentifier(
			s.domainID,
			s.workflowID,
			s.newRunID,
		),
		newBranchToken,
		gomock.Any(),
	).Return(s.mockRebuiltMutableState, rebuiltHistorySize, nil).Times(1)

	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: branchToken,
		ForkNodeID:      baseEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.domainID, s.workflowID, s.newRunID),
		ShardID:         &shardId,
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: newBranchToken}, nil).Times(1)

	rebuiltMutableState, err := s.nDCWorkflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingVersion,
	)
	s.NoError(err)
	s.Equal(s.mockRebuiltMutableState, rebuiltMutableState)
	s.Equal(s.newContext.getHistorySize(), rebuiltHistorySize)
	s.True(mockBaseWorkflowReleaseFnCalled)
}

func (s *nDCWorkflowResetterSuite) TestResetWorkflow_Error() {
	ctx := context.Background()
	now := time.Now()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := persistence.NewVersionHistory(
		branchToken,
		[]*persistence.VersionHistoryItem{persistence.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := persistence.NewVersionHistories(versionHistory)
	baseEventID := lastEventID + 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingFirstEventVersion := baseVersion + 3

	s.mockBaseMutableState.EXPECT().GetVersionHistories().Return(versionHistories).AnyTimes()

	mockBaseWorkflowReleaseFn := func(err error) {
	}
	mockBaseWorkflow := NewMocknDCWorkflow(s.controller)
	mockBaseWorkflow.EXPECT().getMutableState().Return(s.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().getReleaseFn().Return(mockBaseWorkflowReleaseFn).Times(1)

	s.mockTransactionMgr.EXPECT().loadNDCWorkflow(
		ctx,
		s.domainID,
		s.workflowID,
		s.baseRunID,
	).Return(mockBaseWorkflow, nil).Times(1)

	rebuiltMutableState, err := s.nDCWorkflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	s.Error(err)
	s.IsType(&serviceerror.RetryTaskV2{}, err)
	s.Nil(rebuiltMutableState)

	retryErr, isRetryError := err.(*serviceerror.RetryTaskV2)
	s.True(isRetryError)
	expectedErr := serviceerror.NewRetryTaskV2(
		resendOnResetWorkflowMessage,
		s.domainID,
		s.workflowID,
		s.newRunID,
		common.EmptyEventID,
		common.EmptyVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	s.Equal(retryErr, expectedErr)
}
