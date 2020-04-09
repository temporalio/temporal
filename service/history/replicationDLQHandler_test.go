package history

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/temporalio/temporal/.gen/proto/adminservice"
	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/primitives"
	"github.com/temporalio/temporal/common/resource"
)

type (
	replicationDLQHandlerSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		mockResource           *resource.Test
		mockShard              ShardContext
		config                 *Config
		mockClientBean         *client.MockBean
		adminClient            *adminservicemock.MockAdminServiceClient
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
	s.controller = gomock.NewController(s.T())

	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.mockClientBean = s.mockResource.ClientBean
	s.adminClient = s.mockResource.RemoteAdminClient
	s.clusterMetadata = s.mockResource.ClusterMetadata
	s.executionManager = s.mockResource.ExecutionMgr
	s.shardManager = s.mockResource.ShardMgr
	logger := log.NewNoop()
	s.mockShard = &shardContextImpl{
		shardID:  0,
		Resource: s.mockResource,
		shardInfo: &persistence.ShardInfoWithFailover{ShardInfo: &persistenceblobs.ShardInfo{
			ShardId:                0,
			RangeId:                1,
			ReplicationDLQAckLevel: map[string]int64{"test": -1},
		}},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
		closeCh:                   make(chan int, 100),
		config:                    NewDynamicConfigForTest(),
		logger:                    logger,
		remoteClusterCurrentTime:  make(map[string]time.Time),
		executionManager:          s.executionManager,
	}
	s.config = NewDynamicConfigForTest()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()
	s.replicatorTaskExecutor = NewMockreplicationTaskExecutor(s.controller)

	s.replicationMessageHandler = newReplicationDLQHandler(
		s.mockShard,
		s.replicatorTaskExecutor,
	).(*replicationDLQHandlerImpl)
}

func (s *replicationDLQHandlerSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
}

func (s *replicationDLQHandlerSuite) TestReadMessages_OK() {
	ctx := context.Background()
	sourceCluster := "test"
	lastMessageID := int64(1)
	pageSize := 1
	pageToken := []byte{}

	resp := &persistence.GetReplicationTasksFromDLQResponse{
		Tasks: []*persistenceblobs.ReplicationTaskInfo{
			&persistenceblobs.ReplicationTaskInfo{
				NamespaceId: primitives.MustParseUUID(uuid.New()),
				WorkflowId:  uuid.New(),
				RunId:       primitives.MustParseUUID(uuid.New()),
				TaskId:      0,
				TaskType:    1,
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
		Return(&adminservice.GetDLQReplicationMessagesResponse{}, nil)
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
		Tasks: []*persistenceblobs.ReplicationTaskInfo{
			&persistenceblobs.ReplicationTaskInfo{
				NamespaceId: primitives.MustParseUUID(uuid.New()),
				WorkflowId:  uuid.New(),
				RunId:       primitives.MustParseUUID(uuid.New()),
				TaskId:      0,
				TaskType:    1,
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
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:     replicationgenpb.ReplicationTaskType_HistoryTask,
		SourceTaskId: lastMessageID,
		Attributes: &replicationgenpb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationgenpb.HistoryTaskAttributes{},
		},
	}
	s.adminClient.EXPECT().
		GetDLQReplicationMessages(ctx, gomock.Any()).
		Return(&adminservice.GetDLQReplicationMessagesResponse{
			ReplicationTasks: []*replicationgenpb.ReplicationTask{
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
