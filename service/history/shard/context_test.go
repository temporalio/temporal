package shard

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
)

type (
	contextSuite struct {
		suite.Suite
		*require.Assertions

		controller   *gomock.Controller
		shardContext Context

		mockResource *resource.Test

		namespaceID        string
		mockNamespaceCache *cache.MockNamespaceCache
		namespaceEntry     *cache.NamespaceCacheEntry

		mockClusterMetadata  *cluster.MockMetadata
		mockExecutionManager *mocks.ExecutionManager
		mockHistoryEngine    *MockEngine
	}
)

func TestShardContextSuite(t *testing.T) {
	s := &contextSuite{}
	suite.Run(t, s)
}

func (s *contextSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())

	shardContext := NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
			}},
		configs.NewDynamicConfigForTest())
	s.shardContext = shardContext

	s.mockResource = shardContext.Resource

	s.namespaceID = "namespace-Id"
	s.namespaceEntry = cache.NewLocalNamespaceCacheEntryForTest(&persistencespb.NamespaceInfo{Id: s.namespaceID}, &persistencespb.NamespaceConfig{}, "", nil)
	s.mockNamespaceCache = s.mockResource.NamespaceCache

	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockExecutionManager = s.mockResource.ExecutionMgr
	s.mockHistoryEngine = NewMockEngine(s.controller)
	shardContext.engine = s.mockHistoryEngine
}

func (s *contextSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
	s.mockExecutionManager.AssertExpectations(s.T())
}

func (s *contextSuite) TestAddTasks_Success() {
	task := &persistencespb.TimerTaskInfo{
		NamespaceId:     s.namespaceID,
		WorkflowId:      "workflow-id",
		RunId:           "run-id",
		TaskType:        enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION,
		Version:         1,
		EventId:         2,
		ScheduleAttempt: 1,
		TaskId:          12345,
		VisibilityTime:  timestamp.TimeNowPtrUtc(),
	}

	transferTasks := []persistence.Task{&persistence.ActivityTask{}}              // Just for testing purpose. In the real code ActivityTask can't be passed to shardContext.AddTasks.
	timerTasks := []persistence.Task{&persistence.ActivityRetryTimerTask{}}       // Just for testing purpose. In the real code ActivityRetryTimerTask can't be passed to shardContext.AddTasks.
	replicationTasks := []persistence.Task{&persistence.HistoryReplicationTask{}} // Just for testing purpose. In the real code HistoryReplicationTask can't be passed to shardContext.AddTasks.
	visibilityTasks := []persistence.Task{&persistence.DeleteExecutionVisibilityTask{}}

	addTasksRequest := &persistence.AddTasksRequest{
		NamespaceID: task.GetNamespaceId(),
		WorkflowID:  task.GetWorkflowId(),
		RunID:       task.GetRunId(),

		TransferTasks:    transferTasks,
		TimerTasks:       timerTasks,
		ReplicationTasks: replicationTasks,
		VisibilityTasks:  visibilityTasks,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil)
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	s.mockExecutionManager.On("AddTasks", addTasksRequest).Return(nil).Once()
	s.mockHistoryEngine.EXPECT().NotifyNewTransferTasks(transferTasks).Times(1)
	s.mockHistoryEngine.EXPECT().NotifyNewTimerTasks(timerTasks).Times(1)
	s.mockHistoryEngine.EXPECT().NotifyNewVisibilityTasks(visibilityTasks).Times(1)

	err := s.shardContext.AddTasks(addTasksRequest)
	s.NoError(err)
}
