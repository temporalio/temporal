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

package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	resetterSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		mockShard               *shard.ContextTest
		mockBaseMutableState    *workflow.MockMutableState
		mockRebuiltMutableState *workflow.MockMutableState
		mockTransactionMgr      *MockTransactionManager
		mockStateBuilder        *MockStateRebuilder

		logger          log.Logger
		mockExecManager *persistence.MockExecutionManager

		namespaceID namespace.ID
		namespace   namespace.Name
		workflowID  string
		baseRunID   string
		newContext  workflow.Context
		newRunID    string

		workflowResetter *resetterImpl
	}
)

func TestResetterSuite(t *testing.T) {
	s := new(resetterSuite)
	suite.Run(t, s)
}

func (s *resetterSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockBaseMutableState = workflow.NewMockMutableState(s.controller)
	s.mockRebuiltMutableState = workflow.NewMockMutableState(s.controller)
	s.mockTransactionMgr = NewMockTransactionManager(s.controller)
	s.mockStateBuilder = NewMockStateRebuilder(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockExecManager = s.mockShard.Resource.ExecutionMgr

	s.logger = s.mockShard.GetLogger()

	s.namespaceID = namespace.ID(uuid.New())
	s.namespace = "some random namespace name"
	s.workflowID = "some random workflow ID"
	s.baseRunID = uuid.New()
	s.newContext = workflow.NewContext(
		s.mockShard.GetConfig(),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.newRunID,
		),
		s.logger,
		s.mockShard.GetThrottledLogger(),
		s.mockShard.GetMetricsHandler(),
	)
	s.newRunID = uuid.New()

	s.workflowResetter = NewResetter(
		s.mockShard, s.mockTransactionMgr, s.namespaceID, s.workflowID, s.baseRunID, s.newContext, s.newRunID, s.logger,
	)
	s.workflowResetter.stateRebuilder = s.mockStateBuilder
}

func (s *resetterSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *resetterSuite) TestResetWorkflow_NoError() {
	ctx := context.Background()
	now := time.Now().UTC()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	baseEventID := lastEventID - 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingVersion := baseVersion + 3

	rebuiltHistorySize := int64(9999)
	newBranchToken := []byte("other random branch token")

	s.mockBaseMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	mockBaseWorkflowReleaseFnCalled := false
	mockBaseWorkflowReleaseFn := func(err error) {
		mockBaseWorkflowReleaseFnCalled = true
	}
	mockBaseWorkflow := NewMockWorkflow(s.controller)
	mockBaseWorkflow.EXPECT().GetMutableState().Return(s.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().GetReleaseFn().Return(mockBaseWorkflowReleaseFn)

	s.mockTransactionMgr.EXPECT().LoadWorkflow(
		ctx,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
	).Return(mockBaseWorkflow, nil)

	s.mockStateBuilder.EXPECT().Rebuild(
		ctx,
		now,
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.baseRunID,
		),
		branchToken,
		baseEventID,
		util.Ptr(baseVersion),
		definition.NewWorkflowKey(
			s.namespaceID.String(),
			s.workflowID,
			s.newRunID,
		),
		newBranchToken,
		gomock.Any(),
	).Return(s.mockRebuiltMutableState, rebuiltHistorySize, nil)
	s.mockRebuiltMutableState.EXPECT().AddHistorySize(rebuiltHistorySize)

	shardID := s.mockShard.GetShardID()
	s.mockExecManager.EXPECT().ForkHistoryBranch(gomock.Any(), &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: branchToken,
		ForkNodeID:      baseEventID + 1,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(s.namespaceID.String(), s.workflowID, s.newRunID),
		ShardID:         shardID,
		NamespaceID:     s.namespaceID.String(),
	}).Return(&persistence.ForkHistoryBranchResponse{NewBranchToken: newBranchToken}, nil)

	rebuiltMutableState, err := s.workflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingVersion,
	)
	s.NoError(err)
	s.Equal(s.mockRebuiltMutableState, rebuiltMutableState)
	s.True(mockBaseWorkflowReleaseFnCalled)
}

func (s *resetterSuite) TestResetWorkflow_Error() {
	ctx := context.Background()
	now := time.Now().UTC()

	branchToken := []byte("some random branch token")
	lastEventID := int64(500)
	version := int64(123)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	baseEventID := lastEventID + 100
	baseVersion := version
	incomingFirstEventID := baseEventID + 12
	incomingFirstEventVersion := baseVersion + 3

	s.mockBaseMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	mockBaseWorkflowReleaseFn := func(err error) {
	}
	mockBaseWorkflow := NewMockWorkflow(s.controller)
	mockBaseWorkflow.EXPECT().GetMutableState().Return(s.mockBaseMutableState).AnyTimes()
	mockBaseWorkflow.EXPECT().GetReleaseFn().Return(mockBaseWorkflowReleaseFn)

	s.mockTransactionMgr.EXPECT().LoadWorkflow(
		ctx,
		s.namespaceID,
		s.workflowID,
		s.baseRunID,
	).Return(mockBaseWorkflow, nil)

	rebuiltMutableState, err := s.workflowResetter.resetWorkflow(
		ctx,
		now,
		baseEventID,
		baseVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	s.Error(err)
	s.IsType(&serviceerrors.RetryReplication{}, err)
	s.Nil(rebuiltMutableState)

	retryErr, isRetryError := err.(*serviceerrors.RetryReplication)
	s.True(isRetryError)
	expectedErr := serviceerrors.NewRetryReplication(
		resendOnResetWorkflowMessage,
		s.namespaceID.String(),
		s.workflowID,
		s.newRunID,
		common.EmptyEventID,
		common.EmptyVersion,
		incomingFirstEventID,
		incomingFirstEventVersion,
	)
	s.Equal(retryErr, expectedErr)
}
