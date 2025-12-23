package ndc

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	bufferEventFlusherSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockContext         *historyi.MockWorkflowContext
		mockMutableState    *historyi.MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		logger log.Logger

		namespaceID string
		workflowID  string
		runID       string

		nDCBufferEventFlusher *BufferEventFlusherImpl
	}
)

func TestBufferEventFlusherSuite(t *testing.T) {
	s := new(bufferEventFlusherSuite)
	suite.Run(t, s)
}

func (s *bufferEventFlusherSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockContext = historyi.NewMockWorkflowContext(s.controller)
	s.mockMutableState = historyi.NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 10,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.namespaceID = uuid.NewString()
	s.workflowID = "some random workflow ID"
	s.runID = uuid.NewString()
	s.nDCBufferEventFlusher = NewBufferEventFlusher(
		s.mockShard, s.mockContext, s.mockMutableState, s.logger,
	)
}

func (s *bufferEventFlusherSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *bufferEventFlusherSuite) TestClearTransientWorkflowTask() {

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

	s.mockMutableState.EXPECT().HasBufferedEvents().Return(false).AnyTimes()
	s.mockMutableState.EXPECT().HasStartedWorkflowTask().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().IsTransientWorkflowTask().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().ClearTransientWorkflowTask().Return(nil).AnyTimes()

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		NamespaceId:      s.namespaceID,
		WorkflowId:       s.workflowID,
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: s.runID,
	}).AnyTimes()

	_, _, err = s.nDCBufferEventFlusher.flush(context.Background())
	s.NoError(err)
}

func (s *bufferEventFlusherSuite) TestFlushBufferedEvents() {

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

	s.mockMutableState.EXPECT().IsWorkflow().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().GetLastWriteVersion().Return(lastWriteVersion, nil).AnyTimes()
	s.mockMutableState.EXPECT().HasBufferedEvents().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true).AnyTimes()
	s.mockMutableState.EXPECT().UpdateCurrentVersion(lastWriteVersion, true).Return(nil)
	workflowTask := &historyi.WorkflowTaskInfo{
		ScheduledEventID: 1234,
		StartedEventID:   2345,
	}
	s.mockMutableState.EXPECT().GetStartedWorkflowTask().Return(workflowTask)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_FAILOVER_CLOSE_COMMAND,
		nil,
		consts.IdentityHistoryService,
		nil,
		"",
		"",
		"",
		int64(0),
	).Return(&historypb.HistoryEvent{}, nil)
	s.mockMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	s.mockMutableState.EXPECT().AddWorkflowTaskScheduledEvent(
		false,
		enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
	).Return(&historyi.WorkflowTaskInfo{}, nil)
	s.mockMutableState.EXPECT().FlushBufferedEvents()
	s.mockClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, lastWriteVersion).Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.mockContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), s.mockShard).Return(nil)

	_, _, err = s.nDCBufferEventFlusher.flush(context.Background())
	s.NoError(err)
}
