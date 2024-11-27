// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	executableBackfillHistoryEventsTaskSuite struct {
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
		config                  *configs.Config

		replicationTask   *replicationspb.ReplicationTask
		sourceClusterName string
		sourceShardKey    ClusterShardKey

		taskID        int64
		task          *ExecutableBackfillHistoryEventsTask
		events        []*historypb.HistoryEvent
		eventsBatches [][]*historypb.HistoryEvent
		eventsBlobs   []*commonpb.DataBlob
		newRunEvents  []*historypb.HistoryEvent
		newRunID      string
		firstEventID  int64
		nextEventID   int64
		version       int64
	}
)

func TestExecutableBackfillHistoryEventsTaskSuite(t *testing.T) {
	s := new(executableBackfillHistoryEventsTaskSuite)
	suite.Run(t, s)
}

func (s *executableBackfillHistoryEventsTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableBackfillHistoryEventsTaskSuite) TearDownSuite() {

}

func (s *executableBackfillHistoryEventsTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.eventSerializer = serialization.NewSerializer()
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)

	s.firstEventID = int64(10)
	s.nextEventID = int64(21)
	s.version = rand.Int63()
	eventsBlob, _ := s.eventSerializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId: s.firstEventID,
		Version: s.version,
	}}, enumspb.ENCODING_TYPE_PROTO3)
	s.events, _ = s.eventSerializer.DeserializeEvents(eventsBlob)
	s.eventsBatches = [][]*historypb.HistoryEvent{s.events}
	newEventsBlob, _ := s.eventSerializer.SerializeEvents([]*historypb.HistoryEvent{{
		EventId: 1,
		Version: s.version,
	}}, enumspb.ENCODING_TYPE_PROTO3)
	s.newRunEvents, _ = s.eventSerializer.DeserializeEvents(newEventsBlob)
	s.newRunID = uuid.NewString()

	s.eventsBlobs = []*commonpb.DataBlob{eventsBlob}
	s.taskID = rand.Int63()

	s.replicationTask = &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_BACKFILL_HISTORY_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_BackfillHistoryTaskAttributes{
			BackfillHistoryTaskAttributes: &replicationspb.BackfillHistoryTaskAttributes{
				NamespaceId: uuid.NewString(),
				WorkflowId:  uuid.NewString(),
				RunId:       uuid.NewString(),
				EventVersionHistory: []*history.VersionHistoryItem{{
					EventId: s.nextEventID - 1,
					Version: s.version,
				}},
				EventBatches: s.eventsBlobs,
				NewRunInfo: &replicationspb.NewRunInfo{
					RunId:      s.newRunID,
					EventBatch: newEventsBlob,
				},
			},
		},
		VersionedTransition: &persistencepb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.sourceClusterName = cluster.TestCurrentClusterName
	s.sourceShardKey = ClusterShardKey{
		ClusterID: int32(cluster.TestCurrentClusterInitialFailoverVersion),
		ShardID:   rand.Int31(),
	}
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.config = tests.NewDynamicConfig()

	taskCreationTime := time.Unix(0, rand.Int63())
	s.task = NewExecutableBackfillHistoryEventsTask(
		ProcessToolBox{
			ClusterMetadata:         s.clusterMetadata,
			ClientBean:              s.clientBean,
			ShardController:         s.shardController,
			NamespaceCache:          s.namespaceCache,
			MetricsHandler:          s.metricsHandler,
			Logger:                  s.logger,
			EventSerializer:         s.eventSerializer,
			EagerNamespaceRefresher: s.eagerNamespaceRefresher,
			DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
			Config:                  s.config,
		},
		s.taskID,
		taskCreationTime,
		s.sourceClusterName,
		s.sourceShardKey,
		s.replicationTask,
	)
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	s.executableTask.EXPECT().TaskCreationTime().Return(taskCreationTime).AnyTimes()
}

func (s *executableBackfillHistoryEventsTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableBackfillHistoryEventsTaskSuite) TestExecute_Process() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().ReplicationTask().Times(1).Return(s.replicationTask)
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

	engine.EXPECT().BackfillHistoryEvents(gomock.Any(), &shard.BackfillHistoryEventsRequest{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: s.task.NamespaceID,
			WorkflowID:  s.task.WorkflowID,
			RunID:       s.task.RunID,
		},
		SourceClusterName:   s.sourceClusterName,
		VersionedHistory:    s.replicationTask.VersionedTransition,
		VersionHistoryItems: s.replicationTask.GetBackfillHistoryTaskAttributes().EventVersionHistory,
		Events:              s.eventsBatches,
		NewEvents:           s.newRunEvents,
		NewRunID:            s.newRunID,
	}).Return(nil)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableBackfillHistoryEventsTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableBackfillHistoryEventsTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableBackfillHistoryEventsTaskSuite) TestExecute_Err() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableBackfillHistoryEventsTaskSuite) TestHandleErr_Resend_Success() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().ReplicationTask().Times(1).Return(s.replicationTask)
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
	err := serviceerrors.NewRetryReplication(
		"",
		s.task.NamespaceID,
		s.task.WorkflowID,
		s.task.RunID,
		s.firstEventID,
		s.version,
		s.nextEventID-1,
		s.version,
	)
	s.executableTask.EXPECT().BackFillEvents(
		gomock.Any(),
		s.sourceClusterName,
		definition.NewWorkflowKey(s.task.NamespaceID, s.task.WorkflowID, s.task.RunID),
		s.firstEventID+1,
		s.version,
		s.nextEventID-2,
		s.version,
		"").Return(nil)
	engine.EXPECT().BackfillHistoryEvents(gomock.Any(), gomock.Any()).Return(nil)
	s.NoError(s.task.HandleErr(err))
}

func (s *executableBackfillHistoryEventsTaskSuite) TestHandleErr_Resend_Error() {
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	err := serviceerrors.NewRetryReplication(
		"",
		s.task.NamespaceID,
		s.task.WorkflowID,
		s.task.RunID,
		s.firstEventID,
		s.version,
		s.nextEventID-1,
		s.version,
	)
	backFillErr := errors.New("OwO")
	s.executableTask.EXPECT().BackFillEvents(
		gomock.Any(),
		s.sourceClusterName,
		definition.NewWorkflowKey(s.task.NamespaceID, s.task.WorkflowID, s.task.RunID),
		s.firstEventID+1,
		s.version,
		s.nextEventID-2,
		s.version,
		"").Return(backFillErr)
	actualErr := s.task.HandleErr(err)

	s.Equal(backFillErr, actualErr)
}
