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

package history

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
)

type (
	replicationDLQHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		mockResource     *resource.Test
		mockShard        *shard.ContextTest
		config           *configs.Config
		mockClientBean   *client.MockBean
		adminClient      *adminservicemock.MockAdminServiceClient
		clusterMetadata  *cluster.MockMetadata
		executionManager *persistence.MockExecutionManager
		shardManager     *persistence.MockShardManager
		taskExecutor     *MockreplicationTaskExecutor
		taskExecutors    map[string]replicationTaskExecutor
		sourceCluster    string

		replicationMessageHandler *replicationDLQHandlerImpl
	}
)

func TestReplicationMessageHandlerSuite(t *testing.T) {
	s := new(replicationDLQHandlerSuite)
	suite.Run(t, s)
}

func (s *replicationDLQHandlerSuite) SetupSuite() {

}

func (s *replicationDLQHandlerSuite) TearDownSuite() {

}

func (s *replicationDLQHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                0,
				RangeId:                1,
				ReplicationAckLevel:    0,
				ReplicationDlqAckLevel: map[string]int64{cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID},
			}},
		tests.NewDynamicConfig(),
	)
	s.mockResource = s.mockShard.Resource
	s.mockClientBean = s.mockResource.ClientBean
	s.adminClient = s.mockResource.RemoteAdminClient
	s.clusterMetadata = s.mockResource.ClusterMetadata
	s.executionManager = s.mockResource.ExecutionMgr
	s.shardManager = s.mockResource.ShardMgr
	s.config = tests.NewDynamicConfig()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.taskExecutors = make(map[string]replicationTaskExecutor)
	s.taskExecutor = NewMockreplicationTaskExecutor(s.controller)
	s.sourceCluster = cluster.TestAlternativeClusterName
	s.taskExecutors[s.sourceCluster] = s.taskExecutor

	s.replicationMessageHandler = newReplicationDLQHandler(
		s.mockShard,
		s.taskExecutors,
	).(*replicationDLQHandlerImpl)
}

func (s *replicationDLQHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *replicationDLQHandlerSuite) TestReadMessages_OK() {
	ctx := context.Background()

	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	taskID := int64(12345)
	version := int64(2333)
	firstEventID := int64(144)
	nextEventID := int64(233)

	lastMessageID := int64(1394)
	pageSize := 1
	pageToken := []byte("some random token")
	dbResp := &persistence.GetReplicationTasksFromDLQResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				namespaceID,
				workflowID,
				runID,
			),
			Version:      version,
			FirstEventID: firstEventID,
			NextEventID:  nextEventID,
			TaskID:       taskID,
		}},
		NextPageToken: pageToken,
	}

	remoteTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{
			HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{
					Version: version,
					EventId: nextEventID - 1,
				}},
				Events: &commonpb.DataBlob{},
			},
		},
	}

	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(&persistence.GetReplicationTasksFromDLQRequest{
		ShardID:           s.mockShard.GetShardID(),
		SourceClusterName: s.sourceCluster,
		GetReplicationTasksRequest: persistence.GetReplicationTasksRequest{
			MinTaskID:     persistence.EmptyQueueMessageID,
			MaxTaskID:     lastMessageID,
			BatchSize:     pageSize,
			NextPageToken: pageToken,
		},
	}).Return(dbResp, nil)

	s.mockClientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(s.adminClient).AnyTimes()
	s.adminClient.EXPECT().GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{
			ReplicationTasks: []*replicationspb.ReplicationTask{remoteTask},
		}, nil)
	tasks, token, err := s.replicationMessageHandler.getMessages(ctx, s.sourceCluster, lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Equal(pageToken, token)
	s.Equal([]*replicationspb.ReplicationTask{remoteTask}, tasks)
}

func (s *replicationDLQHandlerSuite) TestPurgeMessages() {
	lastMessageID := int64(1)

	s.executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			ShardID:              s.mockShard.GetShardID(),
			SourceClusterName:    s.sourceCluster,
			ExclusiveBeginTaskID: persistence.EmptyQueueMessageID,
			InclusiveEndTaskID:   lastMessageID,
		}).Return(nil)

	s.shardManager.EXPECT().UpdateShard(gomock.Any()).Return(nil)
	err := s.replicationMessageHandler.purgeMessages(s.sourceCluster, lastMessageID)
	s.NoError(err)
}
func (s *replicationDLQHandlerSuite) TestMergeMessages() {
	ctx := context.Background()

	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	taskID := int64(12345)
	version := int64(2333)
	firstEventID := int64(144)
	nextEventID := int64(233)

	lastMessageID := int64(1394)
	pageSize := 1
	pageToken := []byte("some random token")

	dbResp := &persistence.GetReplicationTasksFromDLQResponse{
		Tasks: []tasks.Task{&tasks.HistoryReplicationTask{
			WorkflowKey: definition.NewWorkflowKey(
				namespaceID,
				workflowID,
				runID,
			),
			Version:      version,
			FirstEventID: firstEventID,
			NextEventID:  nextEventID,
			TaskID:       taskID,
		}},
		NextPageToken: pageToken,
	}

	remoteTask := &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		SourceTaskId: taskID,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{
			HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{
					Version: version,
					EventId: nextEventID - 1,
				}},
				Events: &commonpb.DataBlob{},
			},
		},
	}

	s.executionManager.EXPECT().GetReplicationTasksFromDLQ(&persistence.GetReplicationTasksFromDLQRequest{
		ShardID:           s.mockShard.GetShardID(),
		SourceClusterName: s.sourceCluster,
		GetReplicationTasksRequest: persistence.GetReplicationTasksRequest{
			MinTaskID:     persistence.EmptyQueueMessageID,
			MaxTaskID:     lastMessageID,
			BatchSize:     pageSize,
			NextPageToken: pageToken,
		},
	}).Return(dbResp, nil)

	s.mockClientBean.EXPECT().GetRemoteAdminClient(s.sourceCluster).Return(s.adminClient).AnyTimes()
	s.adminClient.EXPECT().GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{
			ReplicationTasks: []*replicationspb.ReplicationTask{remoteTask},
		}, nil)
	s.taskExecutor.EXPECT().execute(remoteTask, true).Return(0, nil)
	s.executionManager.EXPECT().RangeDeleteReplicationTaskFromDLQ(&persistence.RangeDeleteReplicationTaskFromDLQRequest{
		ShardID:              s.mockShard.GetShardID(),
		SourceClusterName:    s.sourceCluster,
		ExclusiveBeginTaskID: persistence.EmptyQueueMessageID,
		InclusiveEndTaskID:   lastMessageID,
	}).Return(nil)

	s.shardManager.EXPECT().UpdateShard(gomock.Any()).Return(nil)

	token, err := s.replicationMessageHandler.mergeMessages(ctx, s.sourceCluster, lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Equal(pageToken, token)
}
