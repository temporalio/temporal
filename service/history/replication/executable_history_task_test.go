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

package replication

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/tests"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/testing/protorequire"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
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
		ndcHistoryResender      *xdc.MockNDCHistoryResender
		metricsHandler          metrics.Handler
		logger                  log.Logger
		executableTask          *MockExecutableTask
		eagerNamespaceRefresher *MockEagerNamespaceRefresher
		eventSerializer         serialization.Serializer
		mockExecutionManager    *persistence.MockExecutionManager

		replicationTask   *replicationspb.HistoryTaskAttributes
		sourceClusterName string

		taskID         int64
		task           *ExecutableHistoryTask
		events         []*historypb.HistoryEvent
		newRunEvents   []*historypb.HistoryEvent
		newRunID       string
		processToolBox ProcessToolBox
	}
)

func TestExecutableHistoryTaskSuite(t *testing.T) {
	s := new(executableHistoryTaskSuite)
	suite.Run(t, s)
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
	s.ndcHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.eventSerializer = serialization.NewSerializer()
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)

	firstEventID := rand.Int63()
	nextEventID := firstEventID + 1
	version := rand.Int63()
	eventsBlob, _ := s.eventSerializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId: firstEventID,
		Version: version,
	}}, enumspb.ENCODING_TYPE_PROTO3)
	s.events, _ = s.eventSerializer.DeserializeEvents(eventsBlob)

	newEventsBlob, _ := s.eventSerializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId: 1,
		Version: version,
	}}, enumspb.ENCODING_TYPE_PROTO3)
	s.newRunEvents, _ = s.eventSerializer.DeserializeEvents(newEventsBlob)
	s.newRunID = uuid.NewString()

	s.replicationTask = &replicationspb.HistoryTaskAttributes{
		NamespaceId:       uuid.NewString(),
		WorkflowId:        uuid.NewString(),
		RunId:             uuid.NewString(),
		BaseExecutionInfo: &workflowspb.BaseExecutionInfo{},
		VersionHistoryItems: []*history.VersionHistoryItem{{
			EventId: nextEventID - 1,
			Version: version,
		}},
		Events:       eventsBlob,
		NewRunEvents: newEventsBlob,
		NewRunId:     s.newRunID,
	}
	s.sourceClusterName = cluster.TestCurrentClusterName

	s.taskID = rand.Int63()
	s.processToolBox = ProcessToolBox{
		ClusterMetadata:         s.clusterMetadata,
		ClientBean:              s.clientBean,
		ShardController:         s.shardController,
		NamespaceCache:          s.namespaceCache,
		NDCHistoryResender:      s.ndcHistoryResender,
		MetricsHandler:          s.metricsHandler,
		Logger:                  s.logger,
		EagerNamespaceRefresher: s.eagerNamespaceRefresher,
		EventSerializer:         s.eventSerializer,
		DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
		Config:                  tests.NewDynamicConfig(),
	}
	s.task = NewExecutableHistoryTask(
		s.processToolBox,
		s.taskID,
		time.Unix(0, rand.Int63()),
		s.replicationTask,
		s.sourceClusterName,
	)
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
}

func (s *executableHistoryTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableHistoryTaskSuite) TestExecute_Process() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		definition.NewWorkflowKey(s.task.NamespaceID, s.task.WorkflowID, s.task.RunID),
		s.task.baseExecutionInfo,
		s.task.versionHistoryItems,
		[][]*historypb.HistoryEvent{s.events},
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
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableHistoryTaskSuite) TestExecute_Err() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableHistoryTaskSuite) TestHandleErr_Resend_Success() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().ReplicateHistoryEvents(
		gomock.Any(),
		definition.NewWorkflowKey(s.task.NamespaceID, s.task.WorkflowID, s.task.RunID),
		s.task.baseExecutionInfo,
		s.task.versionHistoryItems,
		[][]*historypb.HistoryEvent{s.events},
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
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
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

	err = serviceerror.NewUnavailable("")
	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableHistoryTaskSuite) TestMarkPoisonPill() {
	events := s.events

	shardID := rand.Int31()
	shardContext := shard.NewMockContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()
	s.mockExecutionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           shardID,
		SourceClusterName: s.sourceClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId:  s.task.NamespaceID,
			WorkflowId:   s.task.WorkflowID,
			RunId:        s.task.RunID,
			TaskId:       s.task.ExecutableTask.TaskID(),
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: events[0].GetEventId(),
			NextEventId:  events[len(events)-1].GetEventId() + 1,
			Version:      events[0].GetVersion(),
		},
	}).Return(nil)

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
	currentTask.versionHistoryItems = []*history.VersionHistoryItem{
		{
			EventId: 108,
			Version: 3,
		},
	}
	incomingTask.versionHistoryItems = []*history.VersionHistoryItem{
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
	currentVersionHistoryItems := []*history.VersionHistoryItem{
		{
			EventId: 102,
			Version: 3,
		},
	}
	incomingVersionHistoryItems := []*history.VersionHistoryItem{
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
	sourceCluster := uuid.NewString()
	sourceTaskId := int64(111)
	incomingTaskId := int64(120)
	currentTask := s.buildExecutableHistoryTask(currentEvent, nil, "", sourceTaskId, currentVersionHistoryItems, workflowKeyCurrent, sourceCluster)
	incomingTask := s.buildExecutableHistoryTask(incomingEvent, nil, "", incomingTaskId, incomingVersionHistoryItems, workflowKeyIncoming, sourceCluster)

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
	versionHistoryItems []*history.VersionHistoryItem,
	workflowKey definition.WorkflowKey,
	sourceCluster string,
) *ExecutableHistoryTask {
	eventsBlob, _ := s.eventSerializer.SerializeEvents(events[0], enumspb.ENCODING_TYPE_PROTO3)
	newRunEventsBlob, _ := s.eventSerializer.SerializeEvents(newRunEvents, enumspb.ENCODING_TYPE_PROTO3)
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
	executableTask.EXPECT().SourceClusterName().Return(sourceCluster).AnyTimes()
	executableHistoryTask := NewExecutableHistoryTask(
		s.processToolBox,
		taskId,
		time.Unix(0, rand.Int63()),
		replicationTaskAttribute,
		sourceCluster,
	)
	executableHistoryTask.ExecutableTask = executableTask
	return executableHistoryTask
}
