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
	"go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

type (
	executableVerifyVersionedTransitionTaskSuite struct {
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
		wfcache                 *cache.MockCache
		eventSerializer         serialization.Serializer
		mockExecutionManager    *persistence.MockExecutionManager
		config                  *configs.Config
		sourceClusterName       string

		taskID      int64
		namespaceID string
		workflowID  string
		runID       string
		task        *ExecutableVerifyVersionedTransitionTask
		newRunID    string
		toolBox     ProcessToolBox
	}
)

func TestExecutableVerifyVersionedTransitionTaskSuite(t *testing.T) {
	s := new(executableVerifyVersionedTransitionTaskSuite)
	suite.Run(t, s)
}

func (s *executableVerifyVersionedTransitionTaskSuite) SetupSuite() {
	s.Assertions = require.New(s.T())
}

func (s *executableVerifyVersionedTransitionTaskSuite) TearDownSuite() {

}

func (s *executableVerifyVersionedTransitionTaskSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())
	s.clusterMetadata = cluster.NewMockMetadata(s.controller)
	s.clientBean = client.NewMockBean(s.controller)
	s.shardController = shard.NewMockController(s.controller)
	s.namespaceCache = namespace.NewMockRegistry(s.controller)
	s.ndcHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.metricsHandler = metrics.NoopMetricsHandler
	s.logger = log.NewNoopLogger()
	s.executableTask = NewMockExecutableTask(s.controller)
	s.eventSerializer = serialization.NewSerializer()
	s.eagerNamespaceRefresher = NewMockEagerNamespaceRefresher(s.controller)
	s.wfcache = cache.NewMockCache(s.controller)
	s.namespaceID = uuid.NewString()
	s.workflowID = uuid.NewString()
	s.runID = "old_run"
	s.newRunID = "new_run"

	s.taskID = rand.Int63()

	s.sourceClusterName = cluster.TestCurrentClusterName
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.config = tests.NewDynamicConfig()

	taskCreationTime := time.Unix(0, rand.Int63())
	s.toolBox = ProcessToolBox{
		ClusterMetadata:         s.clusterMetadata,
		ClientBean:              s.clientBean,
		ShardController:         s.shardController,
		NamespaceCache:          s.namespaceCache,
		NDCHistoryResender:      s.ndcHistoryResender,
		MetricsHandler:          s.metricsHandler,
		Logger:                  s.logger,
		EventSerializer:         s.eventSerializer,
		EagerNamespaceRefresher: s.eagerNamespaceRefresher,
		DLQWriter:               NewExecutionManagerDLQWriter(s.mockExecutionManager),
		Config:                  s.config,
		WorkflowCache:           s.wfcache,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
			},
		},
		VersionedTransition: &persistencepb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.task = NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		taskCreationTime,
		s.sourceClusterName,
		replicationTask,
	)
	s.task.ExecutableTask = s.executableTask
	s.executableTask.EXPECT().TaskID().Return(s.taskID).AnyTimes()
	s.executableTask.EXPECT().SourceClusterName().Return(s.sourceClusterName).AnyTimes()
	s.executableTask.EXPECT().TaskCreationTime().Return(taskCreationTime).AnyTimes()
}

func (s *executableVerifyVersionedTransitionTaskSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_CurrentBranch_VerifySuccess() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
			},
		},
		VersionedTransition: &persistencepb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().ReplicationTask().Times(1).Return(replicationTask)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := workflow.NewMockMutableState(s.controller)
	mu.EXPECT().GetNextEventID().Return(taskNextEvent).AnyTimes()
	mu.EXPECT().GetExecutionInfo().Return(&persistencepb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencepb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 3},
			{NamespaceFailoverVersion: 3, TransitionCount: 6},
		},
	}).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.newRunID, workflow.NewMockMutableState(s.controller), nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_CurrentBranch_NewRunNotFound() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
			},
		},
		VersionedTransition: &persistencepb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          5,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().ReplicationTask().Times(1).Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := workflow.NewMockMutableState(s.controller)
	mu.EXPECT().GetNextEventID().Return(taskNextEvent).AnyTimes()
	mu.EXPECT().GetExecutionInfo().Return(&persistencepb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencepb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 3},
			{NamespaceFailoverVersion: 3, TransitionCount: 6},
		},
	}).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.newRunID, nil, serviceerror.NewNotFound("workflow not found"))
	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.IsType(&serviceerror.DataLoss{}, err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) mockGetMutableState(
	namespaceId string,
	workflowId string,
	runId string,
	mutableState workflow.MutableState,
	err error,
) {
	shardContext := shard.NewMockContext(s.controller)
	s.shardController.EXPECT().GetShardByNamespaceWorkflow(
		namespace.ID(s.task.NamespaceID),
		s.task.WorkflowID,
	).Return(shardContext, nil)
	wfCtx := workflow.NewMockContext(s.controller)
	if err == nil {
		wfCtx.EXPECT().LoadMutableState(gomock.Any(), shardContext).Return(mutableState, err)
	}
	s.wfcache.EXPECT().GetOrCreateWorkflowExecution(
		gomock.Any(),
		shardContext,
		namespace.ID(namespaceId),
		&common.WorkflowExecution{
			WorkflowId: workflowId,
			RunId:      runId,
		},
		locks.PriorityLow,
	).Return(wfCtx, func(err error) {}, err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_CurrentBranch_NotUpToDate() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
			},
		},
		VersionedTransition: &persistencepb.VersionedTransition{
			NamespaceFailoverVersion: 3,
			TransitionCount:          7,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := workflow.NewMockMutableState(s.controller)
	mu.EXPECT().GetNextEventID().Return(taskNextEvent).AnyTimes()
	mu.EXPECT().GetExecutionInfo().Return(&persistencepb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencepb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 3},
			{NamespaceFailoverVersion: 3, TransitionCount: 6},
		},
	}).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.IsType(&serviceerrors.SyncState{}, err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_NonCurrentBranch_VerifySuccess() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
			},
		},
		VersionedTransition: &persistencepb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := workflow.NewMockMutableState(s.controller)
	mu.EXPECT().GetNextEventID().Return(taskNextEvent).AnyTimes()
	mu.EXPECT().GetExecutionInfo().Return(&persistencepb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencepb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 3},
			{NamespaceFailoverVersion: 3, TransitionCount: 6},
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte{1, 2, 3},
					Items: []*historyspb.VersionHistoryItem{
						{
							EventId: 11,
							Version: 1,
						},
					},
				},
			},
		},
	}).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)
	s.mockGetMutableState(s.namespaceID, s.workflowID, s.newRunID, workflow.NewMockMutableState(s.controller), nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_NonCurrentBranch_NotUpToDate() {
	taskNextEvent := int64(10)
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_VERIFY_VERSIONED_TRANSITION_TASK,
		SourceTaskId: s.taskID,
		Attributes: &replicationspb.ReplicationTask_VerifyVersionedTransitionTaskAttributes{
			VerifyVersionedTransitionTaskAttributes: &replicationspb.VerifyVersionedTransitionTaskAttributes{
				NamespaceId: s.namespaceID,
				WorkflowId:  s.workflowID,
				RunId:       s.runID,
				NextEventId: taskNextEvent,
				NewRunId:    s.newRunID,
				EventVersionHistory: []*historyspb.VersionHistoryItem{
					{
						EventId: 9,
						Version: 1,
					},
				},
			},
		},
		VersionedTransition: &persistencepb.VersionedTransition{
			NamespaceFailoverVersion: 1,
			TransitionCount:          4,
		},
	}
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().ReplicationTask().Return(replicationTask).AnyTimes()
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), true, nil,
	).AnyTimes()

	mu := workflow.NewMockMutableState(s.controller)
	mu.EXPECT().GetNextEventID().Return(taskNextEvent).AnyTimes()
	mu.EXPECT().GetExecutionInfo().Return(&persistencepb.WorkflowExecutionInfo{
		TransitionHistory: []*persistencepb.VersionedTransition{
			{NamespaceFailoverVersion: 1, TransitionCount: 3},
			{NamespaceFailoverVersion: 3, TransitionCount: 6},
		},
		VersionHistories: &historyspb.VersionHistories{
			Histories: []*historyspb.VersionHistory{
				{
					BranchToken: []byte{1, 2, 3},
					Items: []*historyspb.VersionHistoryItem{
						{
							EventId: 8,
							Version: 1,
						},
					},
				},
			},
		},
	}).AnyTimes()

	s.mockGetMutableState(s.namespaceID, s.workflowID, s.runID, mu, nil)

	task := NewExecutableVerifyVersionedTransitionTask(
		s.toolBox,
		s.taskID,
		time.Now(),
		s.sourceClusterName,
		replicationTask,
	)
	task.ExecutableTask = s.executableTask

	err := task.Execute()
	s.IsType(&serviceerrors.RetryReplication{}, err)
	re, ok := err.(*serviceerrors.RetryReplication)
	s.True(ok)

	s.Equal(int64(8), re.StartEventId)
	s.Equal(int64(1), re.StartEventVersion)
	s.Equal(taskNextEvent, re.EndEventId)
	s.Equal(int64(1), re.EndEventVersion)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_Skip_TerminalState() {
	s.executableTask.EXPECT().TerminalState().Return(true)

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_Skip_Namespace() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		uuid.NewString(), false, nil,
	).AnyTimes()

	err := s.task.Execute()
	s.NoError(err)
}

func (s *executableVerifyVersionedTransitionTaskSuite) TestExecute_Err() {
	s.executableTask.EXPECT().TerminalState().Return(false)
	err := errors.New("OwO")
	s.executableTask.EXPECT().GetNamespaceInfo(gomock.Any(), s.task.NamespaceID).Return(
		"", false, err,
	).AnyTimes()

	s.Equal(err, s.task.Execute())
}
