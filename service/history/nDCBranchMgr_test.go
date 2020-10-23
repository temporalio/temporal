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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	nDCBranchMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shardContextTest
		mockContext         *MockworkflowExecutionContext
		mockMutableState    *MockmutableState
		mockClusterMetadata *cluster.MockMetadata

		mockHistoryV2Mgr *mocks.HistoryV2Manager

		logger log.Logger

		branchIndex int
		namespaceID string
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

	s.mockShard = newTestShardContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistenceblobs.ShardInfo{
				ShardId:          10,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		NewDynamicConfigForTest(),
	)

	s.mockHistoryV2Mgr = s.mockShard.resource.HistoryMgr
	s.mockClusterMetadata = s.mockShard.resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.namespaceID = uuid.New()
	s.workflowID = "some random workflow ID"
	s.runID = uuid.New()
	s.branchIndex = 0
	s.nDCBranchMgr = newNDCBranchMgr(
		s.mockShard, s.mockContext, s.mockMutableState, s.logger,
	)
}

func (s *nDCBranchMgrSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *nDCBranchMgrSuite) TestCreateNewBranch() {
	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventVersion := int64(200)
	baseBranchLCAEventID := int64(1394)
	baseBranchLastEventVersion := int64(400)
	baseBranchLastEventID := int64(2333)
	versionHistory := versionhistory.New(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewItem(10, 0),
		versionhistory.NewItem(50, 100),
		versionhistory.NewItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewItem(baseBranchLastEventID, baseBranchLastEventVersion),
	})
	versionHistories := versionhistory.NewVHS(versionHistory)

	newBranchToken := []byte("some random new branch token")
	newVersionHistory, err := versionhistory.DuplicateUntilLCAItem(versionHistory,
		versionhistory.NewItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistenceblobs.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.MatchedBy(func(input *persistence.ForkHistoryBranchRequest) bool {
		input.Info = ""
		s.Equal(&persistence.ForkHistoryBranchRequest{
			ForkBranchToken: baseBranchToken,
			ForkNodeID:      baseBranchLCAEventID + 1,
			Info:            "",
			ShardID:         &shardId,
		}, input)
		return true
	})).Return(&persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}, nil).Once()

	newIndex, err := s.nDCBranchMgr.createNewBranch(context.Background(), baseBranchToken, baseBranchLCAEventID, newVersionHistory)
	s.Nil(err)
	s.Equal(int32(1), newIndex)

	compareVersionHistory, err := versionhistory.DuplicateUntilLCAItem(
		versionHistory,
		versionhistory.NewItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	)
	s.NoError(err)
	s.NoError(versionhistory.SetBranchToken(compareVersionHistory, newBranchToken))
	newVersionHistory, err = versionhistory.GetVersionHistory(versionHistories, newIndex)
	s.NoError(err)
	s.True(compareVersionHistory.Equal(newVersionHistory))
}

func (s *nDCBranchMgrSuite) TestFlushBufferedEvents() {

	lastWriteVersion := int64(300)
	versionHistory := versionhistory.New([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewItem(10, 0),
		versionhistory.NewItem(50, 100),
		versionhistory.NewItem(100, 200),
		versionhistory.NewItem(150, 300),
	})
	versionHistories := versionhistory.NewVHS(versionHistory)

	incomingVersionHistory := versionhistory.Duplicate(versionHistory)
	err := versionhistory.AddOrUpdateItem(
		incomingVersionHistory,
		versionhistory.NewItem(200, 300),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil).Times(1)
	workflowTask := &workflowTaskInfo{
		ScheduleID: 1234,
		StartedID:  2345,
	}
	s.mockMutableState.EXPECT().GetInFlightWorkflowTask().Return(workflowTask, true).Times(1)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask.ScheduleID,
		workflowTask.StartedID,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil,
		identityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{}, nil).Times(1)
	s.mockMutableState.EXPECT().FlushBufferedEvents().Return(nil).Times(1)

	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(lastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockContext.EXPECT().updateWorkflowExecutionAsActive(gomock.Any()).Return(nil).Times(1)

	ctx := context.Background()

	_, _, err = s.nDCBranchMgr.flushBufferedEvents(ctx, incomingVersionHistory)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchAppendable_NoMissingEventInBetween() {

	versionHistory := versionhistory.New([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewItem(10, 0),
		versionhistory.NewItem(50, 100),
		versionhistory.NewItem(100, 200),
		versionhistory.NewItem(150, 300),
	})
	versionHistories := versionhistory.NewVHS(versionHistory)

	incomingVersionHistory := versionhistory.Duplicate(versionHistory)
	err := versionhistory.AddOrUpdateItem(
		incomingVersionHistory,
		versionhistory.NewItem(200, 300),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()

	doContinue, index, err := s.nDCBranchMgr.prepareVersionHistory(
		context.Background(),
		incomingVersionHistory,
		150+1,
		300)
	s.NoError(err)
	s.True(doContinue)
	s.Equal(int32(0), index)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchAppendable_MissingEventInBetween() {

	versionHistory := versionhistory.New([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewItem(10, 0),
		versionhistory.NewItem(50, 100),
		versionhistory.NewItem(100, 200),
		versionhistory.NewItem(150, 300),
	})
	versionHistories := versionhistory.NewVHS(versionHistory)

	incomingVersionHistory := versionhistory.Duplicate(versionHistory)
	incomingFirstEventVersionHistoryItem := versionhistory.NewItem(200, 300)
	err := versionhistory.AddOrUpdateItem(
		incomingVersionHistory,
		incomingFirstEventVersionHistoryItem,
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistenceblobs.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	_, _, err = s.nDCBranchMgr.prepareVersionHistory(
		context.Background(),
		incomingVersionHistory,
		150+2,
		300)
	s.IsType(&serviceerrors.RetryTaskV2{}, err)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchNotAppendable_NoMissingEventInBetween() {
	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventID := int64(85)
	baseBranchLCAEventVersion := int64(200)

	versionHistory := versionhistory.New(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewItem(10, 0),
		versionhistory.NewItem(50, 100),
		versionhistory.NewItem(baseBranchLCAEventID+10, baseBranchLCAEventVersion),
		versionhistory.NewItem(150, 300),
	})
	versionHistories := versionhistory.NewVHS(versionHistory)

	incomingVersionHistory := versionhistory.New(nil, []*historyspb.VersionHistoryItem{
		versionhistory.NewItem(10, 0),
		versionhistory.NewItem(50, 100),
		versionhistory.NewItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewItem(200, 400),
	})

	newBranchToken := []byte("some random new branch token")

	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistenceblobs.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	shardId := s.mockShard.GetShardID()
	s.mockHistoryV2Mgr.On("ForkHistoryBranch", mock.MatchedBy(func(input *persistence.ForkHistoryBranchRequest) bool {
		input.Info = ""
		s.Equal(&persistence.ForkHistoryBranchRequest{
			ForkBranchToken: baseBranchToken,
			ForkNodeID:      baseBranchLCAEventID + 1,
			Info:            "",
			ShardID:         &shardId,
		}, input)
		return true
	})).Return(&persistence.ForkHistoryBranchResponse{
		NewBranchToken: newBranchToken,
	}, nil).Once()

	doContinue, index, err := s.nDCBranchMgr.prepareVersionHistory(
		context.Background(),
		incomingVersionHistory,
		baseBranchLCAEventID+1,
		baseBranchLCAEventVersion,
	)
	s.NoError(err)
	s.True(doContinue)
	s.Equal(int32(1), index)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchNotAppendable_MissingEventInBetween() {
	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventID := int64(85)
	baseBranchLCAEventVersion := int64(200)
	baseBranchLastEventID := int64(150)
	baseBranchLastEventVersion := int64(300)

	versionHistory := versionhistory.New(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewItem(10, 0),
		versionhistory.NewItem(50, 100),
		versionhistory.NewItem(baseBranchLCAEventID+10, baseBranchLCAEventVersion),
		versionhistory.NewItem(baseBranchLastEventID, baseBranchLastEventVersion),
	})
	versionHistories := versionhistory.NewVHS(versionHistory)

	incomingVersionHistory := versionhistory.New(nil, []*historyspb.VersionHistoryItem{
		versionhistory.NewItem(10, 0),
		versionhistory.NewItem(50, 100),
		versionhistory.NewItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewItem(200, 400),
	})

	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistenceblobs.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistenceblobs.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	_, _, err := s.nDCBranchMgr.prepareVersionHistory(
		context.Background(),
		incomingVersionHistory,
		baseBranchLCAEventID+2,
		baseBranchLCAEventVersion,
	)
	s.IsType(&serviceerrors.RetryTaskV2{}, err)
}
