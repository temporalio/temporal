// Copyright (c) 2019 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/adminservicemock"
	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/mocks"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/resource"
)

type (
	replicationTaskProcessorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		mockResource            *resource.Test
		mockShard               ShardContext
		mockEngine              *MockEngine
		config                  *Config
		historyClient           *historyservicemock.MockHistoryServiceClient
		replicationTaskFetcher  *MockReplicationTaskFetcher
		mockDomainCache         *cache.MockDomainCache
		mockClientBean          *client.MockBean
		adminClient             *adminservicemock.MockAdminServiceClient
		clusterMetadata         *cluster.MockMetadata
		executionManager        *mocks.ExecutionManager
		requestChan             chan *request
		replicationTaskExecutor *MockreplicationTaskExecutor

		replicationTaskProcessor *ReplicationTaskProcessorImpl
	}
)

func TestReplicationTaskProcessorSuite(t *testing.T) {
	s := new(replicationTaskProcessorSuite)
	suite.Run(t, s)
}

func (s *replicationTaskProcessorSuite) SetupSuite() {

}

func (s *replicationTaskProcessorSuite) TearDownSuite() {

}

func (s *replicationTaskProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.mockDomainCache = s.mockResource.DomainCache
	s.mockClientBean = s.mockResource.ClientBean
	s.adminClient = s.mockResource.RemoteAdminClient
	s.clusterMetadata = s.mockResource.ClusterMetadata
	s.executionManager = s.mockResource.ExecutionMgr
	s.replicationTaskExecutor = NewMockreplicationTaskExecutor(s.controller)
	logger := log.NewNoop()
	s.mockShard = &shardContextImpl{
		shardID:                   0,
		Resource:                  s.mockResource,
		shardInfo:                 &persistence.ShardInfoWithFailover{ShardInfo: &persistenceblobs.ShardInfo{RangeID: 1, TransferAckLevel: 0}},
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
	s.requestChan = make(chan *request, 10)

	s.replicationTaskFetcher = NewMockReplicationTaskFetcher(s.controller)

	s.replicationTaskFetcher.EXPECT().GetSourceCluster().Return("standby").AnyTimes()
	s.replicationTaskFetcher.EXPECT().GetRequestChan().Return(s.requestChan).AnyTimes()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()

	s.replicationTaskProcessor = NewReplicationTaskProcessor(
		s.mockShard,
		s.mockEngine,
		s.config,
		metricsClient,
		s.replicationTaskFetcher,
		s.replicationTaskExecutor,
	)
}

func (s *replicationTaskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
}

func (s *replicationTaskProcessorSuite) TestSendFetchMessageRequest() {
	s.replicationTaskProcessor.sendFetchMessageRequest()
	requestMessage := <-s.requestChan

	s.Equal(int32(0), requestMessage.token.GetShardID())
	s.Equal(int64(-1), requestMessage.token.GetLastProcessedMessageId())
	s.Equal(int64(-1), requestMessage.token.GetLastRetrievedMessageId())
}

func (s *replicationTaskProcessorSuite) TestHandleSyncShardStatus() {
	now := time.Now()
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), &historyservice.SyncShardStatusRequest{
		SourceCluster: "standby",
		ShardId:       0,
		Timestamp:     now.UnixNano(),
	}).Return(nil).Times(1)

	err := s.replicationTaskProcessor.handleSyncShardStatus(&replication.SyncShardStatus{
		Timestamp: now.UnixNano(),
	})
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestPutReplicationTaskToDLQ_SyncActivityReplicationTask() {
	domainID := uuid.NewRandom()
	workflowID := uuid.New()
	runID := uuid.NewRandom()
	task := &replication.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeSyncActivity,
		Attributes: &replication.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: &replication.SyncActivityTaskAttributes{
			DomainId:   domainID.String(),
			WorkflowId: workflowID,
			RunId:      runID.String(),
		}},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: "standby",
		TaskInfo: &persistenceblobs.ReplicationTaskInfo{
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			TaskType:   persistence.ReplicationTaskTypeSyncActivity,
		},
	}
	s.executionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err := s.replicationTaskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestPutReplicationTaskToDLQ_HistoryReplicationTask() {
	domainID := uuid.NewRandom()
	workflowID := uuid.New()
	runID := uuid.NewRandom()
	task := &replication.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeHistory,
		Attributes: &replication.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: &replication.HistoryTaskAttributes{
			DomainId:   domainID.String(),
			WorkflowId: workflowID,
			RunId:      runID.String(),
		}},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: "standby",
		TaskInfo: &persistenceblobs.ReplicationTaskInfo{
			DomainID:   domainID,
			WorkflowID: workflowID,
			RunID:      runID,
			TaskType:   persistence.ReplicationTaskTypeHistory,
		},
	}
	s.executionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err := s.replicationTaskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestPutReplicationTaskToDLQ_HistoryV2ReplicationTask() {
	domainID := uuid.NewRandom()
	workflowID := uuid.New()
	runID := uuid.NewRandom()
	events := []*commonproto.HistoryEvent{
		{
			EventId: 1,
			Version: 1,
		},
	}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeBatchEvents(events, common.EncodingTypeProto3)
	s.NoError(err)
	task := &replication.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeHistoryV2,
		Attributes: &replication.ReplicationTask_HistoryTaskV2Attributes{HistoryTaskV2Attributes: &replication.HistoryTaskV2Attributes{
			DomainId:   domainID.String(),
			WorkflowId: workflowID,
			RunId:      runID.String(),
			Events: &commonproto.DataBlob{
				EncodingType: enums.EncodingTypeThriftRW,
				Data:         data.Data,
			},
		}},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: "standby",
		TaskInfo: &persistenceblobs.ReplicationTaskInfo{
			DomainID:     domainID,
			WorkflowID:   workflowID,
			RunID:        runID,
			TaskType:     persistence.ReplicationTaskTypeHistory,
			FirstEventID: 1,
			NextEventID:  1,
			Version:      1,
		},
	}
	s.executionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err = s.replicationTaskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}
