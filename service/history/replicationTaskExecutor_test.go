package history

import (
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	executionpb "go.temporal.io/temporal-proto/execution"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/resource"
	"github.com/temporalio/temporal/common/xdc"
)

type (
	replicationTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		currentCluster      string
		mockResource        *resource.Test
		mockShard           ShardContext
		mockEngine          *MockEngine
		config              *Config
		historyClient       *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache  *cache.MockNamespaceCache
		mockClientBean      *client.MockBean
		adminClient         *adminservicemock.MockAdminServiceClient
		clusterMetadata     *cluster.MockMetadata
		executionManager    *mocks.ExecutionManager
		nDCHistoryResender  *xdc.MockNDCHistoryResender
		historyRereplicator *xdc.MockHistoryRereplicator

		replicationTaskHandler *replicationTaskExecutorImpl
	}
)

func TestReplicationTaskExecutorSuite(t *testing.T) {
	s := new(replicationTaskExecutorSuite)
	suite.Run(t, s)
}

func (s *replicationTaskExecutorSuite) SetupSuite() {

}

func (s *replicationTaskExecutorSuite) TearDownSuite() {

}

func (s *replicationTaskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.currentCluster = "test"

	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockClientBean = s.mockResource.ClientBean
	s.adminClient = s.mockResource.RemoteAdminClient
	s.clusterMetadata = s.mockResource.ClusterMetadata
	s.executionManager = s.mockResource.ExecutionMgr
	s.nDCHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.historyRereplicator = &xdc.MockHistoryRereplicator{}
	logger := log.NewNoop()
	s.mockShard = &shardContextImpl{
		shardID:  0,
		Resource: s.mockResource,
		shardInfo: &persistence.ShardInfoWithFailover{ShardInfo: &persistenceblobs.ShardInfo{
			ShardId:                0,
			RangeId:                1,
			ReplicationAckLevel:    0,
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
	s.mockEngine = NewMockEngine(s.controller)
	s.config = NewDynamicConfigForTest()
	s.historyClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()

	s.replicationTaskHandler = newReplicationTaskExecutor(
		s.currentCluster,
		s.mockNamespaceCache,
		s.nDCHistoryResender,
		s.historyRereplicator,
		s.mockEngine,
		metricsClient,
		s.mockShard.GetLogger(),
	).(*replicationTaskExecutorImpl)
}

func (s *replicationTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
}

func (s *replicationTaskExecutorSuite) TestConvertRetryTaskError_OK() {
	err := serviceerror.NewRetryTask("", "", "", "", common.EmptyEventID)
	_, ok := s.replicationTaskHandler.convertRetryTaskError(err)
	s.True(ok)
}

func (s *replicationTaskExecutorSuite) TestConvertRetryTaskError_NotOK() {
	err := serviceerror.NewRetryTaskV2("", "", "", "", common.EmptyEventID, common.EmptyVersion, common.EmptyEventID, common.EmptyVersion)
	_, ok := s.replicationTaskHandler.convertRetryTaskError(err)
	s.False(ok)
}

func (s *replicationTaskExecutorSuite) TestConvertRetryTaskV2Error_OK() {
	err := serviceerror.NewRetryTaskV2("", "", "", "", common.EmptyEventID, common.EmptyVersion, common.EmptyEventID, common.EmptyVersion)
	_, ok := s.replicationTaskHandler.convertRetryTaskV2Error(err)
	s.True(ok)
}

func (s *replicationTaskExecutorSuite) TestConvertRetryTaskV2Error_NotOK() {
	err := serviceerror.NewRetryTask("", "", "", "", common.EmptyEventID)
	_, ok := s.replicationTaskHandler.convertRetryTaskV2Error(err)
	s.False(ok)
}

func (s *replicationTaskExecutorSuite) TestFilterTask() {
	namespaceID := uuid.New()
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(cache.NewGlobalNamespaceCacheEntryForTest(
			nil,
			nil,
			&persistence.NamespaceReplicationConfig{
				Clusters: []*persistence.ClusterReplicationConfig{
					{
						ClusterName: "test",
					},
				}},
			0,
			s.clusterMetadata,
		), nil)
	ok, err := s.replicationTaskHandler.filterTask(namespaceID, false)
	s.NoError(err)
	s.True(ok)
}

func (s *replicationTaskExecutorSuite) TestFilterTask_Error() {
	namespaceID := uuid.New()
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(nil, fmt.Errorf("test"))
	ok, err := s.replicationTaskHandler.filterTask(namespaceID, false)
	s.Error(err)
	s.False(ok)
}

func (s *replicationTaskExecutorSuite) TestFilterTask_EnforceApply() {
	namespaceID := uuid.New()
	ok, err := s.replicationTaskHandler.filterTask(namespaceID, true)
	s.NoError(err)
	s.True(ok)
}

func (s *replicationTaskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask() {
	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationgenpb.ReplicationTask{
		TaskType: replicationgenpb.ReplicationTaskType_SyncActivityTask,
		Attributes: &replicationgenpb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationgenpb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
		NamespaceId:        namespaceID,
		WorkflowId:         workflowID,
		RunId:              runID,
		Version:            0,
		ScheduledId:        0,
		ScheduledTime:      0,
		StartedId:          0,
		StartedTime:        0,
		LastHeartbeatTime:  0,
		Attempt:            0,
		LastFailureReason:  "",
		LastWorkerIdentity: "",
	}

	s.mockEngine.EXPECT().SyncActivity(gomock.Any(), request).Return(nil).Times(1)
	_, err := s.replicationTaskHandler.execute(s.currentCluster, task, true)
	s.NoError(err)
}

func (s *replicationTaskExecutorSuite) TestProcessTaskOnce_HistoryReplicationTask() {
	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationgenpb.ReplicationTask{
		TaskType: replicationgenpb.ReplicationTaskType_HistoryTask,
		Attributes: &replicationgenpb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationgenpb.HistoryTaskAttributes{
				NamespaceId:  namespaceID,
				WorkflowId:   workflowID,
				RunId:        runID,
				FirstEventId: 1,
				NextEventId:  5,
				Version:      common.EmptyVersion,
			},
		},
	}
	request := &historyservice.ReplicateEventsRequest{
		NamespaceId: namespaceID,
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		SourceCluster:     "test",
		ForceBufferEvents: false,
		FirstEventId:      1,
		NextEventId:       5,
		Version:           common.EmptyVersion,
		ResetWorkflow:     false,
		NewRunNDC:         false,
	}

	s.mockEngine.EXPECT().ReplicateEvents(gomock.Any(), request).Return(nil).Times(1)
	_, err := s.replicationTaskHandler.execute(s.currentCluster, task, true)
	s.NoError(err)
}

func (s *replicationTaskExecutorSuite) TestProcess_HistoryV2ReplicationTask() {
	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationgenpb.ReplicationTask{
		TaskType: replicationgenpb.ReplicationTaskType_HistoryV2Task,
		Attributes: &replicationgenpb.ReplicationTask_HistoryTaskV2Attributes{
			HistoryTaskV2Attributes: &replicationgenpb.HistoryTaskV2Attributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID,
		WorkflowExecution: &executionpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}

	s.mockEngine.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil).Times(1)
	_, err := s.replicationTaskHandler.execute(s.currentCluster, task, true)
	s.NoError(err)
}
