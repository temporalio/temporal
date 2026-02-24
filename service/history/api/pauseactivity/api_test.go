package pauseactivity

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type (
	pauseActivitySuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockEventsCache     *events.MockCache
		mockNamespaceCache  *namespace.MockRegistry
		mockTaskGenerator   *workflow.MockTaskGenerator
		mockMutableState    *historyi.MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		executionInfo *persistencespb.WorkflowExecutionInfo

		validator *api.CommandAttrValidator

		logger log.Logger
	}
)

func TestActivityOptionsSuite(t *testing.T) {
	s := new(pauseActivitySuite)
	suite.Run(t, s)
}

func (s *pauseActivitySuite) SetupSuite() {
}

func (s *pauseActivitySuite) TearDownSuite() {
}

func (s *pauseActivitySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTaskGenerator = workflow.NewMockTaskGenerator(s.controller)
	s.mockMutableState = historyi.NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	s.executionInfo = &persistencespb.WorkflowExecutionInfo{
		VersionHistories:                 versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		FirstExecutionRunId:              uuid.NewString(),
		WorkflowExecutionTimerTaskStatus: workflow.TimerTaskStatusCreated,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(s.executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()

	s.validator = api.NewCommandAttrValidator(
		s.mockShard.GetNamespaceRegistry(),
		s.mockShard.GetConfig(),
		nil,
	)
}

func (s *pauseActivitySuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *pauseActivitySuite) TestPauseActivityAcceptance() {
	activityId := "activity_id"
	activityInfo := &persistencespb.ActivityInfo{
		TaskQueue:  "task_queue_name",
		ActivityId: activityId,
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
	}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(activityInfo, true)
	s.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any())

	err := workflow.PauseActivity(s.mockMutableState, activityId, nil)
	s.NoError(err)
}
