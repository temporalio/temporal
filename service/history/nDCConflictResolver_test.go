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
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	nDCConflictResolverSuite struct {
		suite.Suite
		*require.Assertions

		controller       *gomock.Controller
		mockShard        *shard.ContextTest
		mockContext      *workflow.MockContext
		mockMutableState *workflow.MockMutableState
		mockStateBuilder *MocknDCStateRebuilder

		logger log.Logger

		namespaceID string
		namespace   string
		workflowID  string
		runID       string

		nDCConflictResolver *nDCConflictResolverImpl
	}
)

func TestNDCConflictResolverSuite(t *testing.T) {
	s := new(nDCConflictResolverSuite)
	suite.Run(t, s)
}

func (s *nDCConflictResolverSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockContext = workflow.NewMockContext(s.controller)
	s.mockMutableState = workflow.NewMockMutableState(s.controller)
	s.mockStateBuilder = NewMocknDCStateRebuilder(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 10,
				RangeId: 1,
			}},
		tests.NewDynamicConfig(),
	)

	s.logger = s.mockShard.GetLogger()

	s.namespaceID = uuid.New()
	s.namespace = "some random namespace name"
	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()

	s.nDCConflictResolver = newNDCConflictResolver(
		s.mockShard, s.mockContext, s.mockMutableState, s.logger,
	)
	s.nDCConflictResolver.stateRebuilder = s.mockStateBuilder
}

func (s *nDCConflictResolverSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *nDCConflictResolverSuite) TestRebuild() {
	ctx := context.Background()
	updateCondition := int64(59)
	dbVersion := int64(1444)
	requestID := uuid.New()
	version := int64(12)
	historySize := int64(12345)

	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(5)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID0, version)},
	)
	branchToken1 := []byte("other random branch token")
	lastEventID1 := int64(2)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddVersionHistory(versionHistories, versionHistory1)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetUpdateCondition().Return(updateCondition, dbVersion).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	workflowKey := definition.NewWorkflowKey(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
	mockRebuildMutableState := workflow.NewMockMutableState(s.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					branchToken1,
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
				),
			),
		},
	).Times(2)
	mockRebuildMutableState.EXPECT().SetUpdateCondition(updateCondition, dbVersion)

	s.mockStateBuilder.EXPECT().rebuild(
		ctx,
		gomock.Any(),
		workflowKey,
		branchToken1,
		lastEventID1,
		convert.Int64Ptr(version),
		workflowKey,
		branchToken1,
		requestID,
	).Return(mockRebuildMutableState, historySize, nil)

	s.mockContext.EXPECT().Clear()
	s.mockContext.EXPECT().SetHistorySize(historySize)
	rebuiltMutableState, err := s.nDCConflictResolver.rebuild(ctx, 1, requestID)
	s.NoError(err)
	s.NotNil(rebuiltMutableState)
	s.Equal(int32(1), versionHistories.GetCurrentVersionHistoryIndex())
}

func (s *nDCConflictResolverSuite) TestPrepareMutableState_NoRebuild() {
	branchToken := []byte("some random branch token")
	lastEventID := int64(2)
	version := int64(12)
	versionHistoryItem := versionhistory.NewVersionHistoryItem(lastEventID, version)
	versionHistory := versionhistory.NewVersionHistory(
		branchToken,
		[]*historyspb.VersionHistoryItem{versionHistoryItem},
	)
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()

	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.prepareMutableState(context.Background(), 0, version)
	s.NoError(err)
	s.False(isRebuilt)
	s.Equal(s.mockMutableState, rebuiltMutableState)
}

func (s *nDCConflictResolverSuite) TestPrepareMutableState_Rebuild() {
	ctx := context.Background()
	updateCondition := int64(59)
	dbVersion := int64(1444)
	version := int64(12)
	incomingVersion := version + 1
	historySize := int64(12345)

	// current branch
	branchToken0 := []byte("some random branch token")
	lastEventID0 := int64(2)

	versionHistoryItem0 := versionhistory.NewVersionHistoryItem(lastEventID0, version)
	versionHistory0 := versionhistory.NewVersionHistory(
		branchToken0,
		[]*historyspb.VersionHistoryItem{versionHistoryItem0},
	)

	// stale branch, used for rebuild
	branchToken1 := []byte("other random branch token")
	lastEventID1 := lastEventID0 - 1
	versionHistoryItem1 := versionhistory.NewVersionHistoryItem(lastEventID1, version)
	versionHistory1 := versionhistory.NewVersionHistory(
		branchToken1,
		[]*historyspb.VersionHistoryItem{versionHistoryItem1},
	)

	versionHistories := versionhistory.NewVersionHistories(versionHistory0)
	_, _, err := versionhistory.AddVersionHistory(versionHistories, versionHistory1)
	s.Nil(err)

	s.mockMutableState.EXPECT().GetUpdateCondition().Return(updateCondition, dbVersion).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	workflowKey := definition.NewWorkflowKey(
		s.namespaceID,
		s.workflowID,
		s.runID,
	)
	mockRebuildMutableState := workflow.NewMockMutableState(s.controller)
	mockRebuildMutableState.EXPECT().GetExecutionInfo().Return(
		&persistencespb.WorkflowExecutionInfo{
			VersionHistories: versionhistory.NewVersionHistories(
				versionhistory.NewVersionHistory(
					branchToken1,
					[]*historyspb.VersionHistoryItem{versionhistory.NewVersionHistoryItem(lastEventID1, version)},
				),
			),
		},
	).Times(2)
	mockRebuildMutableState.EXPECT().SetUpdateCondition(updateCondition, dbVersion)

	s.mockStateBuilder.EXPECT().rebuild(
		ctx,
		gomock.Any(),
		workflowKey,
		branchToken1,
		lastEventID1,
		convert.Int64Ptr(version),
		workflowKey,
		branchToken1,
		gomock.Any(),
	).Return(mockRebuildMutableState, historySize, nil)

	s.mockContext.EXPECT().Clear()
	s.mockContext.EXPECT().SetHistorySize(historySize)
	rebuiltMutableState, isRebuilt, err := s.nDCConflictResolver.prepareMutableState(ctx, 1, incomingVersion)
	s.NoError(err)
	s.NotNil(rebuiltMutableState)
	s.True(isRebuilt)
}
