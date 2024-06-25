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

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	persistencepb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

type (
	executableSyncHSMTaskSuite struct {
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
		mockExecutionManager    *persistence.MockExecutionManager
		config                  *configs.Config

		replicationTask   *replicationspb.SyncHSMAttributes
		sourceClusterName string

		taskID int64
		task   *ExecutableSyncHSMTask
	}
)

func TestExecutableSyncHSMTaskSuite(t *testing.T) {
	s := new(executableSyncHSMTaskSuite)
	suite.Run(t, s)
}

func (s *executableSyncHSMTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableSyncHSMTaskSuite) TearDownSuite() {

}

func (s *executableSyncHSMTaskSuite) SetupTest() {
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

	s.replicationTask = &replicationspb.SyncHSMAttributes{
		NamespaceId: uuid.NewString(),
		WorkflowId:  uuid.NewString(),
		RunId:       uuid.NewString(),
		VersionHistory: &historyspb.VersionHistory{
			BranchToken: []byte("branch token 2"),
			Items: []*historyspb.VersionHistoryItem{
				{EventId: 5, Version: 10},
				{EventId: 10, Version: 20},
			},
		},
		StateMachineNode: &persistencepb.StateMachineNode{
			Children: map[string]*persistencepb.StateMachineMap{
				"test": {
					MachinesById: map[string]*persistencepb.StateMachineNode{
						"machine1": {
							Data: []byte("machine1 data"),
						},
						"machine2": {
							Data: []byte("machine1 data"),
						},
					},
				},
			},
		},
	}
	s.sourceClusterName = cluster.TestCurrentClusterName
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.config = tests.NewDynamicConfig()

	s.taskID = rand.Int63()
	taskCreationTime := time.Unix(0, rand.Int63())
	s.task = NewExecutableSyncHSMTask(
		ProcessToolBox{
			ClusterMetadata:         s.clusterMetadata,
			ClientBean:              s.clientBean,
			ShardController:         s.shardController,
			NamespaceCache:          s.namespaceCache,
			NDCHistoryResender:      s.ndcHistoryResender,
			MetricsHandler:          s.metricsHandler,
			Logger:                  s.logger,
			EagerNamespaceRefresher: s.eagerNamespaceRefresher,
			DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
			Config:                  s.config,
		},
		s.taskID,
		taskCreationTime,
		s.replicationTask,
		s.sourceClusterName,
		enumsspb.TASK_PRIORITY_HIGH,
	)
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	s.executableTask.EXPECT().TaskCreationTime().Return(taskCreationTime).AnyTimes()
}

func (s *executableSyncHSMTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableSyncHSMTaskSuite) TestExecute_Process() {
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
	engine.EXPECT().SyncHSM(gomock.Any(), &shard.SyncHSMRequest{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: s.task.NamespaceID,
			WorkflowID:  s.task.WorkflowID,
			RunID:       s.task.RunID,
		},
		StateMachineNode:    s.replicationTask.GetStateMachineNode(),
		EventVersionHistory: s.replicationTask.GetVersionHistory(),
	}).Return(nil)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableSyncHSMTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableSyncHSMTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableSyncHSMTaskSuite) TestExecute_Err() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableSyncHSMTaskSuite) TestHandleErr_Resend_Success() {
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
	engine.EXPECT().SyncHSM(gomock.Any(), gomock.Any()).Return(nil)
	s.NoError(s.task.HandleErr(err))
}

func (s *executableSyncHSMTaskSuite) TestHandleErr_Resend_Error() {
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

func (s *executableSyncHSMTaskSuite) TestMarkPoisonPill() {
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
		TaskInfo: &persistencepb.ReplicationTaskInfo{
			NamespaceId:    s.task.NamespaceID,
			WorkflowId:     s.task.WorkflowID,
			RunId:          s.task.RunID,
			TaskId:         s.task.ExecutableTask.TaskID(),
			TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM,
			VisibilityTime: timestamppb.New(s.task.TaskCreationTime()),
		},
	}).Return(nil)

	err := s.task.MarkPoisonPill()
	s.NoError(err)
}

func (s *executableSyncHSMTaskSuite) TestMarkPoisonPill_MaxAttemptsReached() {
	s.task.markPoisonPillAttempts = MarkPoisonPillMaxAttempts - 1
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
		TaskInfo: &persistencepb.ReplicationTaskInfo{
			NamespaceId:    s.task.NamespaceID,
			WorkflowId:     s.task.WorkflowID,
			RunId:          s.task.RunID,
			TaskId:         s.task.ExecutableTask.TaskID(),
			TaskType:       enumsspb.TASK_TYPE_REPLICATION_SYNC_HSM,
			VisibilityTime: timestamppb.New(s.task.TaskCreationTime()),
		},
	}).Return(serviceerror.NewInternal("failed"))

	err := s.task.MarkPoisonPill()
	s.Error(err)
	err = s.task.MarkPoisonPill()
	s.NoError(err)
}
