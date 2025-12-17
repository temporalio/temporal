package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	executableHistoryTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller              *gomock.Controller
		clusterMetadata         *cluster.MockMetadata
		clientBean              *client.MockBean
		shardController         *shard.MockController
		namespaceCache          *namespace.MockRegistry
		metricsHandler          metrics.Handler
		logger                  log.Logger
		executableTask          *MockExecutableTask
		eagerNamespaceRefresher *MockEagerNamespaceRefresher
		eventSerializer         serialization.Serializer
		mockExecutionManager    *persistence.MockExecutionManager
		mockEventHandler        *eventhandler.MockHistoryEventsHandler

		replicationTask   *replicationspb.HistoryTaskAttributes
		sourceClusterName string
		sourceShardKey    ClusterShardKey

		taskID                     int64
		task                       *ExecutableHistoryTask
		events                     []*historypb.HistoryEvent
		eventsBatches              [][]*historypb.HistoryEvent
		eventsBlob                 *commonpb.DataBlob
		eventsBlobs                []*commonpb.DataBlob
		newRunEvents               []*historypb.HistoryEvent
		newRunID                   string
		processToolBox             ProcessToolBox
		replicationMultipleBatches bool
	}
)

func TestExecutableHistoryTaskSuite(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		replicationMultipleBatches bool
	}{
		{
			name:                       "ReplicationMultipleBatchesEnabled",
			replicationMultipleBatches: true,
		},
		{
			name:                       "ReplicationMultipleBatchesDisabled",
			replicationMultipleBatches: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &executableHistoryTaskSuite{
				replicationMultipleBatches: tc.replicationMultipleBatches,
			}
			suite.Run(t, s)
		})
	}
}

func (s *executableHistoryTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableHistoryTaskSuite) TearDownSuite() {

}

func (s *executableHistoryTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.eventSerializer = serialization.NewSerializer()
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.mockEventHandler = eventhandler.NewMockHistoryEventsHandler(s.controller)
	s.taskID = rand.Int63()
	s.processToolBox = ProcessToolBox{
		ClusterMetadata:         s.clusterMetadata,
		ClientBean:              s.clientBean,
		ShardController:         s.shardController,
		NamespaceCache:          s.namespaceCache,
		MetricsHandler:          s.metricsHandler,
		Logger:                  s.logger,
		EagerNamespaceRefresher: s.eagerNamespaceRefresher,
		EventSerializer:         s.eventSerializer,
		DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
		Config:                  tests.NewDynamicConfig(),
		HistoryEventsHandler:    s.mockEventHandler,
	}
	s.processToolBox.Config.ReplicationMultipleBatches = dynamicconfig.GetBoolPropertyFn(s.replicationMultipleBatches)

	firstEventID := rand.Int63()
	nextEventID := firstEventID + 1
	version := rand.Int63()
	eventsBlob, _ := s.eventSerializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId: firstEventID,
		Version: version,
	}})
	s.events, _ = s.eventSerializer.DeserializeEvents(eventsBlob)
	s.eventsBatches = [][]*historypb.HistoryEvent{s.events}
	newEventsBlob, _ := s.eventSerializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId: 1,
		Version: version,
	}})
	s.newRunEvents, _ = s.eventSerializer.DeserializeEvents(newEventsBlob)
	s.newRunID = uuid.NewString()

	if s.processToolBox.Config.ReplicationMultipleBatches() {
		s.eventsBlobs = []*commonpb.DataBlob{eventsBlob}
	} else {
		s.eventsBlob = eventsBlob
	}

	s.replicationTask = &replicationspb.HistoryTaskAttributes{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		BaseExecutionInfo: &workflowspb.BaseExecutionInfo{},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{
			EventId: nextEventID - 1,
			Version: version,
		}},
		Events:        s.eventsBlob,
		NewRunEvents:  newEventsBlob,
		NewRunId:      s.newRunID,
		EventsBatches: s.eventsBlobs,
	}
	s.sourceClusterName = cluster.TestCurrentClusterName
	s.sourceShardKey = ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}

	s.task = NewExecutableHistoryTask(
		s.processToolBox,
		s.taskID,
		time.Unix(0, rand.Int63()),
		s.replicationTask,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	s.executableTask.EXPECT().GetPriority().Return(enumsspb.TASK_PRIORITY_HIGH).AnyTimes()
}

func (s *executableHistoryTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableHistoryTaskSuite) TestExecute_Process() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	s.mockEventHandler.EXPECT().HandleHistoryEvents(
		gomock.Any(),
		s.sourceClusterName,
		definition.NewWorkflowKey(s.task.NamespaceID, s.task.WorkflowID, s.task.RunID),
		s.task.baseExecutionInfo,
		s.task.versionHistoryItems,
		s.eventsBatches,
		s.newRunEvents,
		s.newRunID,
	).Return(nil).Times(1)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableHistoryTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableHistoryTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableHistoryTaskSuite) TestExecute_Err() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableHistoryTaskSuite) TestHandleErr_Resend_Success() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	shardContext := historyi.NewMockShardContext(s.controller)
	engine := historyi.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	s.mockEventHandler.EXPECT().HandleHistoryEvents(
		gomock.Any(),
		s.sourceClusterName,
		definition.NewWorkflowKey(s.task.NamespaceID, s.task.WorkflowID, s.task.RunID),
		s.task.baseExecutionInfo,
		s.task.versionHistoryItems,
		s.eventsBatches,
		s.newRunEvents,
		s.newRunID,
	).Return(nil)

	err := serviceerrors.NewRetryReplication(
		"",
		s.task.NamespaceID,
		s.task.WorkflowID,
		s.task.RunID,
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
	)
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, err, ResendAttempt).Return(true, nil)

	s.NoError(s.task.HandleErr(err))
}

func (s *executableHistoryTaskSuite) TestHandleErr_Resend_Error() {
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID, gomock.Any()).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	err := serviceerrors.NewRetryReplication(
		"",
		s.task.NamespaceID,
		s.task.WorkflowID,
		s.task.RunID,
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
		rand.Int63(),
	)
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, err, ResendAttempt).Return(false, errors.New("OwO"))

	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableHistoryTaskSuite) TestHandleErr_Other() {
	err := errors.New("OwO")
	s.Equal(err, s.task.HandleErr(err))

	err = serviceerror.NewNotFound("")
	s.Equal(nil, s.task.HandleErr(err))

	err = consts.ErrDuplicate
	s.executableTask.EXPECT().MarkTaskDuplicated().Times(1)
	s.Equal(nil, s.task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableHistoryTaskSuite) TestMarkPoisonPill() {
	shardID := rand.Int31()
	shardContext := historyi.NewMockShardContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()

	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: s.replicationTask,
		},
		RawTaskInfo: nil,
	}
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().MarkPoisonPill().Times(1)

	err := s.task.MarkPoisonPill()
	s.NoError(err)
}

func (s *executableHistoryTaskSuite) TestBatchWith_Success() {
	s.generateTwoBatchableTasks()
}

func (s *executableHistoryTaskSuite) TestBatchWith_EventNotConsecutive_BatchFailed() {
	currentTask, incomingTask := s.generateTwoBatchableTasks()
	currentTask.eventsDesResponse.events = [][]*historypb.HistoryEvent{
		{
			{
				EventId: 101,
				Version: 3,
			},
			{
				EventId: 102,
				Version: 3,
			},
			{
				EventId: 103,
				Version: 3,
			},
		},
	}
	incomingTask.eventsDesResponse.events = [][]*historypb.HistoryEvent{
		{
			{
				EventId: 105,
				Version: 3,
			},
		},
	}
	_, success := currentTask.BatchWith(incomingTask)
	s.False(success)
}

func (s *executableHistoryTaskSuite) TestBatchWith_EventVersionNotMatch_BatchFailed() {
	currentTask, incomingTask := s.generateTwoBatchableTasks()
	currentTask.eventsDesResponse.events = [][]*historypb.HistoryEvent{
		{
			{
				EventId: 101,
				Version: 3,
			},
			{
				EventId: 102,
				Version: 3,
			},
			{
				EventId: 103,
				Version: 3,
			},
		},
	}
	incomingTask.eventsDesResponse.events = [][]*historypb.HistoryEvent{
		{
			{
				EventId: 104,
				Version: 4,
			},
		},
	}
	_, success := currentTask.BatchWith(incomingTask)
	s.False(success)
}

func (s *executableHistoryTaskSuite) TestBatchWith_VersionHistoryDoesNotMatch_BatchFailed() {
	currentTask, incomingTask := s.generateTwoBatchableTasks()
	currentTask.versionHistoryItems = []*historyspb.VersionHistoryItem{
		{
			EventId: 108,
			Version: 3,
		},
	}
	incomingTask.versionHistoryItems = []*historyspb.VersionHistoryItem{
		{
			EventId: 108,
			Version: 4,
		},
	}
	_, success := currentTask.BatchWith(incomingTask)
	s.False(success)
}

func (s *executableHistoryTaskSuite) TestBatchWith_WorkflowKeyDoesNotMatch_BatchFailed() {
	currentTask, incomingTask := s.generateTwoBatchableTasks()
	currentTask.WorkflowID = "1"
	incomingTask.WorkflowID = "2"
	_, success := currentTask.BatchWith(incomingTask)
	s.False(success)
}

func (s *executableHistoryTaskSuite) TestBatchWith_CurrentTaskHasNewRunEvents_BatchFailed() {
	currentTask, incomingTask := s.generateTwoBatchableTasks()
	currentTask.eventsDesResponse.newRunEvents = []*historypb.HistoryEvent{
		{
			EventId: 104,
			Version: 3,
		},
	}
	_, success := currentTask.BatchWith(incomingTask)
	s.False(success)
}

func (s *executableHistoryTaskSuite) TestBatchWith_IncomingTaskHasNewRunEvents_BatchSuccess() {
	currentTask, incomingTask := s.generateTwoBatchableTasks()
	incomingTask.newRunID = uuid.NewString()
	incomingTask.eventsDesResponse.newRunEvents = []*historypb.HistoryEvent{
		{
			EventId: 104,
			Version: 3,
		},
	}
	batchedTask, success := currentTask.BatchWith(incomingTask)
	s.True(success)
	s.Equal(incomingTask.newRunID, batchedTask.(*ExecutableHistoryTask).newRunID)
}

func (s *executableHistoryTaskSuite) TestNewExecutableHistoryTask() {
	if s.processToolBox.Config.ReplicationMultipleBatches() {
		s.Equal(s.eventsBlobs, s.task.eventsBlobs)
	} else {
		s.Equal(len(s.task.eventsBlobs), 1)
		s.Equal(s.eventsBlob, s.task.eventsBlobs[0])
	}
}

func (s *executableHistoryTaskSuite) generateTwoBatchableTasks() (*ExecutableHistoryTask, *ExecutableHistoryTask) {
	currentEvent := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 101,
				Version: 3,
			},
			{
				EventId: 102,
				Version: 3,
			},
			{
				EventId: 103,
				Version: 3,
			},
		},
	}
	incomingEvent := [][]*historypb.HistoryEvent{
		{
			{
				EventId: 104,
				Version: 3,
			},
			{
				EventId: 105,
				Version: 3,
			},
			{
				EventId: 106,
				Version: 3,
			},
		},
	}
	currentVersionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: 102,
			Version: 3,
		},
	}
	incomingVersionHistoryItems := []*historyspb.VersionHistoryItem{
		{
			EventId: 108,
			Version: 3,
		},
	}
	namespaceId := uuid.NewString()
	workflowId := uuid.NewString()
	runId := uuid.NewString()
	workflowKeyCurrent := definition.NewWorkflowKey(namespaceId, workflowId, runId)
	workflowKeyIncoming := definition.NewWorkflowKey(namespaceId, workflowId, runId)
	sourceTaskId := int64(111)
	incomingTaskId := int64(120)
	currentTask := s.buildExecutableHistoryTask(currentEvent, nil, "", sourceTaskId, currentVersionHistoryItems, workflowKeyCurrent)
	incomingTask := s.buildExecutableHistoryTask(incomingEvent, nil, "", incomingTaskId, incomingVersionHistoryItems, workflowKeyIncoming)

	resultTask, batched := currentTask.BatchWith(incomingTask)

	// following assert are used for testing happy case, do not delete
	s.True(batched)

	resultHistoryTask, _ := resultTask.(*ExecutableHistoryTask)
	s.NotNil(resultHistoryTask)

	s.Equal(sourceTaskId, resultHistoryTask.TaskID())
	s.Equal(incomingVersionHistoryItems, resultHistoryTask.versionHistoryItems)
	expectedBatchedEvents := append(currentEvent, incomingEvent...)

	s.Equal(len(resultHistoryTask.eventsDesResponse.events), len(expectedBatchedEvents))
	for i := range expectedBatchedEvents {
		protorequire.ProtoSliceEqual(s.T(), expectedBatchedEvents[i], resultHistoryTask.eventsDesResponse.events[i])
	}
	s.Nil(resultHistoryTask.eventsDesResponse.newRunEvents)
	return currentTask, incomingTask
}

func (s *executableHistoryTaskSuite) buildExecutableHistoryTask(
	events [][]*historypb.HistoryEvent,
	newRunEvents []*historypb.HistoryEvent,
	newRunID string,
	taskId int64,
	versionHistoryItems []*historyspb.VersionHistoryItem,
	workflowKey definition.WorkflowKey,
) *ExecutableHistoryTask {
	eventsBlob, _ := s.eventSerializer.SerializeEvents(events[0])
	newRunEventsBlob, _ := s.eventSerializer.SerializeEvents(newRunEvents)
	replicationTaskAttribute := &replicationspb.HistoryTaskAttributes{
		WorkflowId:          workflowKey.WorkflowID,
		NamespaceId:         workflowKey.NamespaceID,
		RunId:               workflowKey.RunID,
		BaseExecutionInfo:   &workflowspb.BaseExecutionInfo{},
		VersionHistoryItems: versionHistoryItems,
		Events:              eventsBlob,
		NewRunEvents:        newRunEventsBlob,
		NewRunId:            newRunID,
	}
	executableTask := NewMockExecutableTask(s.controller)
	executableTask.EXPECT().TaskID().Return(taskId).AnyTimes()
	executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	executableHistoryTask := NewExecutableHistoryTask(
		s.processToolBox,
		taskId,
		time.Unix(0, rand.Int63()),
		replicationTaskAttribute,
		s.sourceClusterName,
		s.sourceShardKey,
		&replicationspb.ReplicationTask{
			Priority: enumsspb.TASK_PRIORITY_HIGH,
		},
	)
	executableHistoryTask.ExecutableTask = executableTask
	return executableHistoryTask
}
