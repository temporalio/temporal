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
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resourcetest"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	deletemanager "go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	taskExecutorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		remoteCluster      string
		mockResource       *resourcetest.Test
		mockShard          *shard.ContextTest
		config             *configs.Config
		historyClient      *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache *namespace.MockRegistry
		clusterMetadata    *cluster.MockMetadata
		workflowCache      *wcache.MockCache
		nDCHistoryResender *xdc.MockNDCHistoryResender

		replicationTaskExecutor *taskExecutorImpl
	}
)

func TestTaskExecutorSuite(t *testing.T) {
	s := new(taskExecutorSuite)
	suite.Run(t, s)
}

func (s *taskExecutorSuite) SetupSuite() {

}

func (s *taskExecutorSuite) TearDownSuite() {

}

func (s *taskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.remoteCluster = cluster.TestAlternativeClusterName

	s.config = tests.NewDynamicConfig()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			ReplicationDlqAckLevel: map[string]int64{
				cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID,
			},
		},
		s.config,
	)
	s.mockResource = s.mockShard.Resource
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.clusterMetadata = s.mockResource.ClusterMetadata
	s.nDCHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.historyClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.workflowCache = wcache.NewMockCache(s.controller)

	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()
	s.mockShard.SetHistoryClientForTesting(s.historyClient)
	s.replicationTaskExecutor = NewTaskExecutor(
		s.remoteCluster,
		s.mockShard,
		s.nDCHistoryResender,
		deletemanager.NewMockDeleteManager(s.controller),
		s.workflowCache,
	).(*taskExecutorImpl)
}

func (s *taskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *taskExecutorSuite) TestFilterTask_Apply() {
	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(namespace.NewGlobalNamespaceForTest(
			nil,
			nil,
			&persistencespb.NamespaceReplicationConfig{Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			}},
			0,
		), nil)
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, false)
	s.NoError(err)
	s.True(ok)
}

func (s *taskExecutorSuite) TestFilterTask_NotApply() {
	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(namespace.NewGlobalNamespaceForTest(
			nil,
			nil,
			&persistencespb.NamespaceReplicationConfig{Clusters: []string{cluster.TestAlternativeClusterName}},
			0,
		), nil)
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, false)
	s.NoError(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestFilterTask_Error() {
	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(nil, fmt.Errorf("random error"))
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, false)
	s.Error(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestFilterTask_EnforceApply() {
	namespaceID := namespace.ID(uuid.New())
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, true)
	s.NoError(err)
	s.True(ok)
}

func (s *taskExecutorSuite) TestFilterTask_NamespaceNotFound() {
	namespaceID := namespace.ID(uuid.New())
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(nil, &serviceerror.NamespaceNotFound{})
	ok, err := s.replicationTaskExecutor.filterTask(namespaceID, false)
	s.NoError(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask() {
	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:                namespaceID.String(),
				WorkflowId:                 workflowID,
				RunId:                      runID,
				Version:                    1234,
				ScheduledEventId:           2345,
				ScheduledTime:              nil,
				StartedEventId:             2346,
				StartedTime:                nil,
				LastHeartbeatTime:          nil,
				Attempt:                    10,
				LastFailure:                nil,
				LastWorkerIdentity:         "",
				LastStartedBuildId:         "ABC",
				LastStartedRedirectCounter: 8,
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
		NamespaceId:                namespaceID.String(),
		WorkflowId:                 workflowID,
		RunId:                      runID,
		Version:                    1234,
		ScheduledEventId:           2345,
		ScheduledTime:              nil,
		StartedEventId:             2346,
		StartedTime:                nil,
		LastHeartbeatTime:          nil,
		Attempt:                    10,
		LastFailure:                nil,
		LastWorkerIdentity:         "",
		LastStartedBuildId:         "ABC",
		LastStartedRedirectCounter: 8,
	}

	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(&historyservice.SyncActivityResponse{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask_Resend() {
	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:        namespaceID.String(),
				WorkflowId:         workflowID,
				RunId:              runID,
				Version:            1234,
				ScheduledEventId:   2345,
				ScheduledTime:      nil,
				StartedEventId:     2346,
				StartedTime:        nil,
				LastHeartbeatTime:  nil,
				Attempt:            10,
				LastFailure:        nil,
				LastWorkerIdentity: "",
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
		NamespaceId:        namespaceID.String(),
		WorkflowId:         workflowID,
		RunId:              runID,
		Version:            1234,
		ScheduledEventId:   2345,
		ScheduledTime:      nil,
		StartedEventId:     2346,
		StartedTime:        nil,
		LastHeartbeatTime:  nil,
		Attempt:            10,
		LastFailure:        nil,
		LastWorkerIdentity: "",
	}

	resendErr := serviceerrors.NewRetryReplication(
		"some random error message",
		namespaceID.String(),
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(nil, resendErr)
	s.nDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		gomock.Any(),
		s.remoteCluster,
		namespaceID,
		workflowID,
		runID,
		int64(123),
		int64(234),
		int64(345),
		int64(456),
	)

	s.historyClient.EXPECT().SyncActivity(gomock.Any(), request).Return(&historyservice.SyncActivityResponse{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask() {
	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId:         namespaceID.String(),
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
				Events:              nil,
				NewRunEvents:        nil,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
		Events:              nil,
		NewRunEvents:        nil,
	}
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(&historyservice.ReplicateEventsV2Response{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcess_HistoryReplicationTask_Resend() {
	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
				NamespaceId:         namespaceID.String(),
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
				Events:              nil,
				NewRunEvents:        nil,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{EventId: 233, Version: 2333}},
		Events:              nil,
		NewRunEvents:        nil,
	}

	resendErr := serviceerrors.NewRetryReplication(
		"some random error message",
		namespaceID.String(),
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil, resendErr)
	s.nDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		gomock.Any(),
		s.remoteCluster,
		namespaceID,
		workflowID,
		runID,
		int64(123),
		int64(234),
		int64(345),
		int64(456),
	)

	s.historyClient.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(&historyservice.ReplicateEventsV2Response{}, nil)
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncWorkflowStateTask() {
	namespaceID := namespace.ID(uuid.New())
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_WORKFLOW_STATE_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncWorkflowStateTaskAttributes{
			SyncWorkflowStateTaskAttributes: &replicationspb.SyncWorkflowStateTaskAttributes{
				WorkflowState: &persistencespb.WorkflowMutableState{
					ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
						NamespaceId: namespaceID.String(),
					},
				},
			},
		},
	}
	s.historyClient.EXPECT().ReplicateWorkflowState(gomock.Any(), gomock.Any()).Return(&historyservice.ReplicateWorkflowStateResponse{}, nil)

	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncHSMTask() {
	namespaceID := namespace.ID(uuid.New())
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		SourceTaskId: rand.Int63(),
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_SYNC_HSM_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncHsmAttributes{SyncHsmAttributes: &replicationspb.SyncHSMAttributes{
			NamespaceId:      namespaceID.String(),
			WorkflowId:       workflowID,
			RunId:            runID,
			VersionHistory:   &historyspb.VersionHistory{},
			StateMachineNode: &persistencespb.StateMachineNode{},
		}},
		VisibilityTime: timestamppb.New(time.Now()),
	}

	// not handling SyncHSMTask in deprecated replication task processing code path
	err := s.replicationTaskExecutor.Execute(context.Background(), task, true)
	s.ErrorIs(err, ErrUnknownReplicationTask)
}
