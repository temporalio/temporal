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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/admin/adminservicetest"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/shard"
)

type (
	replicationDLQHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		mockShard              *shard.TestContext
		config                 *config.Config
		mockClientBean         *client.MockBean
		adminClient            *adminservicetest.MockClient
		clusterMetadata        *cluster.MockMetadata
		executionManager       *mocks.ExecutionManager
		shardManager           *mocks.ShardManager
		replicatorTaskExecutor *MockreplicationTaskExecutor

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

	s.config = config.NewForTest()

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:                0,
			RangeID:                1,
			ReplicationDLQAckLevel: map[string]int64{"test": -1},
		},
		s.config,
	)

	s.mockClientBean = s.mockShard.Resource.ClientBean
	s.adminClient = s.mockShard.Resource.RemoteAdminClient
	s.clusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.executionManager = s.mockShard.Resource.ExecutionMgr
	s.shardManager = s.mockShard.Resource.ShardMgr

	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()
	s.replicatorTaskExecutor = NewMockreplicationTaskExecutor(s.controller)

	s.replicationMessageHandler = newReplicationDLQHandler(
		s.mockShard,
		s.replicatorTaskExecutor,
	).(*replicationDLQHandlerImpl)
}

func (s *replicationDLQHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *replicationDLQHandlerSuite) TestReadMessages_OK() {
	ctx := context.Background()
	sourceCluster := "test"
	lastMessageID := int64(1)
	pageSize := 1
	pageToken := []byte{}

	resp := &persistence.GetReplicationTasksFromDLQResponse{
		Tasks: []*persistence.ReplicationTaskInfo{
			{
				DomainID:   uuid.New(),
				WorkflowID: uuid.New(),
				RunID:      uuid.New(),
				TaskType:   0,
				TaskID:     1,
			},
		},
	}
	s.executionManager.On("GetReplicationTasksFromDLQ", &persistence.GetReplicationTasksFromDLQRequest{
		SourceClusterName: sourceCluster,
		GetReplicationTasksRequest: persistence.GetReplicationTasksRequest{
			ReadLevel:     -1,
			MaxReadLevel:  lastMessageID,
			BatchSize:     pageSize,
			NextPageToken: pageToken,
		},
	}).Return(resp, nil).Times(1)

	s.mockClientBean.EXPECT().GetRemoteAdminClient(sourceCluster).Return(s.adminClient).AnyTimes()
	s.adminClient.EXPECT().
		GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&replicator.GetDLQReplicationMessagesResponse{}, nil)
	tasks, token, err := s.replicationMessageHandler.readMessages(ctx, sourceCluster, lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Nil(token)
	s.Nil(tasks)
}

func (s *replicationDLQHandlerSuite) TestPurgeMessages_OK() {
	sourceCluster := "test"
	lastMessageID := int64(1)

	s.executionManager.On("RangeDeleteReplicationTaskFromDLQ",
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			SourceClusterName:    sourceCluster,
			ExclusiveBeginTaskID: -1,
			InclusiveEndTaskID:   lastMessageID,
		}).Return(nil).Times(1)

	s.shardManager.On("UpdateShard", mock.Anything).Return(nil)
	err := s.replicationMessageHandler.purgeMessages(sourceCluster, lastMessageID)
	s.NoError(err)
}

func (s *replicationDLQHandlerSuite) TestMergeMessages_OK() {
	ctx := context.Background()
	sourceCluster := "test"
	lastMessageID := int64(1)
	pageSize := 1
	pageToken := []byte{}

	resp := &persistence.GetReplicationTasksFromDLQResponse{
		Tasks: []*persistence.ReplicationTaskInfo{
			{
				DomainID:   uuid.New(),
				WorkflowID: uuid.New(),
				RunID:      uuid.New(),
				TaskType:   0,
				TaskID:     1,
			},
		},
	}
	s.executionManager.On("GetReplicationTasksFromDLQ", &persistence.GetReplicationTasksFromDLQRequest{
		SourceClusterName: sourceCluster,
		GetReplicationTasksRequest: persistence.GetReplicationTasksRequest{
			ReadLevel:     -1,
			MaxReadLevel:  lastMessageID,
			BatchSize:     pageSize,
			NextPageToken: pageToken,
		},
	}).Return(resp, nil).Times(1)

	s.mockClientBean.EXPECT().GetRemoteAdminClient(sourceCluster).Return(s.adminClient).AnyTimes()
	replicationTask := &replicator.ReplicationTask{
		TaskType:              replicator.ReplicationTaskTypeHistory.Ptr(),
		SourceTaskId:          common.Int64Ptr(lastMessageID),
		HistoryTaskAttributes: &replicator.HistoryTaskAttributes{},
	}
	s.adminClient.EXPECT().
		GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&replicator.GetDLQReplicationMessagesResponse{
			ReplicationTasks: []*replicator.ReplicationTask{
				replicationTask,
			},
		}, nil)
	s.replicatorTaskExecutor.EXPECT().execute(sourceCluster, replicationTask, true).Return(0, nil).Times(1)
	s.executionManager.On("RangeDeleteReplicationTaskFromDLQ",
		&persistence.RangeDeleteReplicationTaskFromDLQRequest{
			SourceClusterName:    sourceCluster,
			ExclusiveBeginTaskID: -1,
			InclusiveEndTaskID:   lastMessageID,
		}).Return(nil).Times(1)

	s.shardManager.On("UpdateShard", mock.Anything).Return(nil)

	token, err := s.replicationMessageHandler.mergeMessages(ctx, sourceCluster, lastMessageID, pageSize, pageToken)
	s.NoError(err)
	s.Nil(token)
}
