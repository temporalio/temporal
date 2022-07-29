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
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	nDCBranchMgrSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockContext         *workflow.MockContext
		mockMutableState    *workflow.MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		mockExecutionManager *persistence.MockExecutionManager

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
	s.mockContext = workflow.NewMockContext(s.controller)
	s.mockMutableState = workflow.NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId: 10,
				RangeId: 1,
			}},
		tests.NewDynamicConfig(),
	)

	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
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
	s.mockShard.StopForTest()
}

func (s *nDCBranchMgrSuite) TestCreateNewBranch() {
	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventVersion := int64(200)
	baseBranchLCAEventID := int64(1394)
	baseBranchLastEventVersion := int64(400)
	baseBranchLastEventID := int64(2333)
	versionHistory := versionhistory.NewVersionHistory(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(baseBranchLastEventID, baseBranchLastEventVersion),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	newBranchToken := []byte("some random new branch token")
	newVersionHistory, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(versionHistory,
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	shardID := s.mockShard.GetShardID()
	s.mockExecutionManager.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *persistence.ForkHistoryBranchRequest) (*persistence.ForkHistoryBranchResponse, error) {
			input.Info = ""
			s.Equal(&persistence.ForkHistoryBranchRequest{
				ForkBranchToken: baseBranchToken,
				ForkNodeID:      baseBranchLCAEventID + 1,
				Info:            "",
				ShardID:         shardID,
			}, input)
			return &persistence.ForkHistoryBranchResponse{
				NewBranchToken: newBranchToken,
			}, nil
		})

	newIndex, err := s.nDCBranchMgr.createNewBranch(context.Background(), baseBranchToken, baseBranchLCAEventID, newVersionHistory)
	s.Nil(err)
	s.Equal(int32(1), newIndex)

	compareVersionHistory, err := versionhistory.CopyVersionHistoryUntilLCAVersionHistoryItem(
		versionHistory,
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
	)
	s.NoError(err)
	versionhistory.SetVersionHistoryBranchToken(compareVersionHistory, newBranchToken)
	newVersionHistory, err = versionhistory.GetVersionHistory(versionHistories, newIndex)
	s.NoError(err)
	s.True(compareVersionHistory.Equal(newVersionHistory))
}

func (s *nDCBranchMgrSuite) TestClearTransientWorkflowTask() {

	lastWriteVersion := int64(300)
	versionHistory := versionhistory.NewVersionHistory([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(100, 200),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	err := versionhistory.AddOrUpdateVersionHistoryItem(
		incomingVersionHistory,
		versionhistory.NewVersionHistoryItem(200, 300),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().HasTransientWorkflowTask().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().ClearTransientWorkflowTask().Return(nil).AnyTimes()

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	_, _, err = s.nDCBranchMgr.prepareVersionHistory(
		context.Background(),
		incomingVersionHistory,
		150+2,
		300)
	s.IsType(&serviceerrors.RetryReplication{}, err)
}

func (s *nDCBranchMgrSuite) TestFlushBufferedEvents() {

	lastWriteVersion := int64(300)
	versionHistory := versionhistory.NewVersionHistory([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(100, 200),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	err := versionhistory.AddOrUpdateVersionHistoryItem(
		incomingVersionHistory,
		versionhistory.NewVersionHistoryItem(200, 300),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)
	workflowTask := &workflow.WorkflowTaskInfo{
		ScheduledEventID: 1234,
		StartedEventID:   2345,
	}
	s.mockMutableState.EXPECT().GetInFlightWorkflowTask().Return(workflowTask, true)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask.ScheduledEventID,
		workflowTask.StartedEventID,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil,
		consts.IdentityHistoryService,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{}, nil)
	s.mockMutableState.EXPECT().FlushBufferedEvents()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any()).Return(nil)

	ctx := context.Background()

	_, _, err = s.nDCBranchMgr.flushBufferedEvents(ctx, incomingVersionHistory)
	s.NoError(err)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchAppendable_NoMissingEventInBetween() {

	versionHistory := versionhistory.NewVersionHistory([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(100, 200),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	err := versionhistory.AddOrUpdateVersionHistoryItem(
		incomingVersionHistory,
		versionhistory.NewVersionHistoryItem(200, 300),
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: versionHistories}).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().HasTransientWorkflowTask().Return(false).AnyTimes()

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

	versionHistory := versionhistory.NewVersionHistory([]byte("some random base branch token"), []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(100, 200),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.CopyVersionHistory(versionHistory)
	incomingFirstEventVersionHistoryItem := versionhistory.NewVersionHistoryItem(200, 300)
	err := versionhistory.AddOrUpdateVersionHistoryItem(
		incomingVersionHistory,
		incomingFirstEventVersionHistoryItem,
	)
	s.NoError(err)

	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().HasTransientWorkflowTask().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	_, _, err = s.nDCBranchMgr.prepareVersionHistory(
		context.Background(),
		incomingVersionHistory,
		150+2,
		300)
	s.IsType(&serviceerrors.RetryReplication{}, err)
}

func (s *nDCBranchMgrSuite) TestPrepareVersionHistory_BranchNotAppendable_NoMissingEventInBetween() {
	baseBranchToken := []byte("some random base branch token")
	baseBranchLCAEventID := int64(85)
	baseBranchLCAEventVersion := int64(200)

	versionHistory := versionhistory.NewVersionHistory(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID+10, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(150, 300),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(200, 400),
	})

	newBranchToken := []byte("some random new branch token")

	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().HasTransientWorkflowTask().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	shardID := s.mockShard.GetShardID()
	s.mockExecutionManager.EXPECT().ForkHistoryBranch(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, input *persistence.ForkHistoryBranchRequest) (*persistence.ForkHistoryBranchResponse, error) {
			input.Info = ""
			s.Equal(&persistence.ForkHistoryBranchRequest{
				ForkBranchToken: baseBranchToken,
				ForkNodeID:      baseBranchLCAEventID + 1,
				Info:            "",
				ShardID:         shardID,
			}, input)
			return &persistence.ForkHistoryBranchResponse{
				NewBranchToken: newBranchToken,
			}, nil
		})

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

	versionHistory := versionhistory.NewVersionHistory(baseBranchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID+10, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(baseBranchLastEventID, baseBranchLastEventVersion),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)

	incomingVersionHistory := versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(10, 0),
		versionhistory.NewVersionHistoryItem(50, 100),
		versionhistory.NewVersionHistoryItem(baseBranchLCAEventID, baseBranchLCAEventVersion),
		versionhistory.NewVersionHistoryItem(200, 400),
	})

	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().HasTransientWorkflowTask().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	_, _, err := s.nDCBranchMgr.prepareVersionHistory(
		context.Background(),
		incomingVersionHistory,
		baseBranchLCAEventID+2,
		baseBranchLCAEventVersion,
	)
	s.IsType(&serviceerrors.RetryReplication{}, err)
}
