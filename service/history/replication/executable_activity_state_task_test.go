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
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/shard"
)

type (
	executableActivityStateTaskSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		clusterMetadata    *cluster.MockMetadata
		clientBean         *client.MockBean
		shardController    *shard.MockController
		namespaceCache     *namespace.MockRegistry
		ndcHistoryResender *xdc.MockNDCHistoryResender
		metricsHandler     metrics.Handler
		logger             log.Logger
		executableTask     *MockExecutableTask

		replicationTask   *replicationspb.SyncActivityTaskAttributes
		sourceClusterName string

		taskID int64
		task   *ExecutableActivityStateTask
	}
)

func TestExecutableActivityStateTaskSuite(t *testing.T) {
	s := new(executableActivityStateTaskSuite)
	suite.Run(t, s)
}

func (s *executableActivityStateTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableActivityStateTaskSuite) TearDownSuite() {

}

func (s *executableActivityStateTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.ndcHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.replicationTask = &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:        uuid.NewString(),
		WorkflowId:         uuid.NewString(),
		RunId:              uuid.NewString(),
		Version:            rand.Int63(),
		ScheduledEventId:   rand.Int63(),
		ScheduledTime:      timestamp.TimePtr(time.Unix(0, rand.Int63())),
		StartedEventId:     rand.Int63(),
		StartedTime:        timestamp.TimePtr(time.Unix(0, rand.Int63())),
		LastHeartbeatTime:  timestamp.TimePtr(time.Unix(0, rand.Int63())),
		Details:            &commonpb.Payloads{},
		Attempt:            rand.Int31(),
		LastFailure:        &failurepb.Failure{},
		LastWorkerIdentity: uuid.NewString(),
		BaseExecutionInfo:  &workflowspb.BaseExecutionInfo{},
		VersionHistory:     &history.VersionHistory{},
	}
	s.sourceClusterName = cluster.TestCurrentClusterName
	s.taskID = rand.Int63()
	s.task = NewExecutableActivityStateTask(
		ProcessToolBox{
			ClusterMetadata:    s.clusterMetadata,
			ClientBean:         s.clientBean,
			ShardController:    s.shardController,
			NamespaceCache:     s.namespaceCache,
			NDCHistoryResender: s.ndcHistoryResender,
			MetricsHandler:     s.metricsHandler,
			Logger:             s.logger,
		},
		s.taskID,
		time.Unix(0, rand.Int63()),
		s.replicationTask,
		s.sourceClusterName,
	)
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
}

func (s *executableActivityStateTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableActivityStateTaskSuite) TestExecute_Process() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().SyncActivity(gomock.Any(), &historyservice.SyncActivityRequest{
		NamespaceId:        s.replicationTask.NamespaceId,
		WorkflowId:         s.replicationTask.WorkflowId,
		RunId:              s.replicationTask.RunId,
		Version:            s.replicationTask.Version,
		ScheduledEventId:   s.replicationTask.ScheduledEventId,
		ScheduledTime:      s.replicationTask.ScheduledTime,
		StartedEventId:     s.replicationTask.StartedEventId,
		StartedTime:        s.replicationTask.StartedTime,
		LastHeartbeatTime:  s.replicationTask.LastHeartbeatTime,
		Details:            s.replicationTask.Details,
		Attempt:            s.replicationTask.Attempt,
		LastFailure:        s.replicationTask.LastFailure,
		LastWorkerIdentity: s.replicationTask.LastWorkerIdentity,
		BaseExecutionInfo:  s.replicationTask.BaseExecutionInfo,
		VersionHistory:     s.replicationTask.GetVersionHistory(),
	}).Return(nil)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableActivityStateTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableActivityStateTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableActivityStateTaskSuite) TestExecute_Err() {
	err := errors.New("OwO")
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}

func (s *executableActivityStateTaskSuite) TestHandleErr_Resend_Success() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()
	shardContext := shard.NewMockContext(s.controller)
	engine := shard.NewMockEngine(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetEngine(gomock.Any()).Return(engine, nil).AnyTimes()
	engine.EXPECT().SyncActivity(gomock.Any(), &historyservice.SyncActivityRequest{
		NamespaceId:        s.replicationTask.NamespaceId,
		WorkflowId:         s.replicationTask.WorkflowId,
		RunId:              s.replicationTask.RunId,
		Version:            s.replicationTask.Version,
		ScheduledEventId:   s.replicationTask.ScheduledEventId,
		ScheduledTime:      s.replicationTask.ScheduledTime,
		StartedEventId:     s.replicationTask.StartedEventId,
		StartedTime:        s.replicationTask.StartedTime,
		LastHeartbeatTime:  s.replicationTask.LastHeartbeatTime,
		Details:            s.replicationTask.Details,
		Attempt:            s.replicationTask.Attempt,
		LastFailure:        s.replicationTask.LastFailure,
		LastWorkerIdentity: s.replicationTask.LastWorkerIdentity,
		BaseExecutionInfo:  s.replicationTask.BaseExecutionInfo,
		VersionHistory:     s.replicationTask.GetVersionHistory(),
	}).Return(nil)

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
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, err).Return(nil)

	s.NoError(s.task.HandleErr(err))
}

func (s *executableActivityStateTaskSuite) TestHandleErr_Resend_Error() {
	s.executableTask.EXPECT().GetNamespaceInfo(s.task.NamespaceID).Return(
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
	s.executableTask.EXPECT().Resend(gomock.Any(), s.sourceClusterName, err).Return(errors.New("OwO"))

	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableActivityStateTaskSuite) TestHandleErr_Other() {
	err := errors.New("OwO")
	s.Equal(err, s.task.HandleErr(err))

	err = serviceerror.NewNotFound("")
	s.Equal(nil, s.task.HandleErr(err))

	err = serviceerror.NewUnavailable("")
	s.Equal(err, s.task.HandleErr(err))
}

func (s *executableActivityStateTaskSuite) TestMarkPoisonPill() {
	shardID := rand.Int31()
	shardContext := shard.NewMockContext(s.controller)
	executionManager := persistence.NewMockExecutionManager(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil).AnyTimes()
	shardContext.EXPECT().GetShardID().Return(shardID).AnyTimes()
	shardContext.EXPECT().GetExecutionManager().Return(executionManager).AnyTimes()
	executionManager.EXPECT().PutReplicationTaskToDLQ(gomock.Any(), &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           shardID,
		SourceClusterName: s.sourceClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId:      s.task.NamespaceID,
			WorkflowId:       s.task.WorkflowID,
			RunId:            s.task.RunID,
			TaskId:           s.task.ExecutableTask.TaskID(),
			TaskType:         enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
			ScheduledEventId: s.task.req.ScheduledEventId,
			Version:          s.task.req.Version,
		},
	}).Return(nil)

	err := s.task.MarkPoisonPill()
	s.NoError(err)
}
