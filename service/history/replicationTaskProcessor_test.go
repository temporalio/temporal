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

	"github.com/uber/cadence/.gen/go/admin/adminservicetest"
	"github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/history/historyservicetest"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/resource"
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
		historyClient           *historyservicetest.MockClient
		replicationTaskFetcher  *MockReplicationTaskFetcher
		mockDomainCache         *cache.MockDomainCache
		mockClientBean          *client.MockBean
		adminClient             *adminservicetest.MockClient
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
		shardInfo:                 &persistence.ShardInfo{ShardID: 0, RangeID: 1, TransferAckLevel: 0},
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
	s.historyClient = historyservicetest.NewMockClient(s.controller)
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
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), &history.SyncShardStatusRequest{
		SourceCluster: common.StringPtr("standby"),
		ShardId:       common.Int64Ptr(0),
		Timestamp:     common.Int64Ptr(now.UnixNano()),
	}).Return(nil).Times(1)

	err := s.replicationTaskProcessor.handleSyncShardStatus(&replicator.SyncShardStatus{
		Timestamp: common.Int64Ptr(now.UnixNano()),
	})
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestPutReplicationTaskToDLQ_SyncActivityReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeSyncActivity.Ptr(),
		SyncActivityTaskAttributes: &replicator.SyncActivityTaskAttributes{
			DomainId:   common.StringPtr(domainID),
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: "standby",
		TaskInfo: &persistence.ReplicationTaskInfo{
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
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeHistory.Ptr(),
		HistoryTaskAttributes: &replicator.HistoryTaskAttributes{
			DomainId:   common.StringPtr(domainID),
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: "standby",
		TaskInfo: &persistence.ReplicationTaskInfo{
			DomainID:            domainID,
			WorkflowID:          workflowID,
			RunID:               runID,
			TaskType:            persistence.ReplicationTaskTypeHistory,
			LastReplicationInfo: make(map[string]*persistence.ReplicationInfo),
		},
	}
	s.executionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err := s.replicationTaskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestPutReplicationTaskToDLQ_HistoryV2ReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	events := []*shared.HistoryEvent{
		{
			EventId: common.Int64Ptr(1),
			Version: common.Int64Ptr(1),
		},
	}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeBatchEvents(events, common.EncodingTypeThriftRW)
	s.NoError(err)
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeHistoryV2.Ptr(),
		HistoryTaskV2Attributes: &replicator.HistoryTaskV2Attributes{
			DomainId:   common.StringPtr(domainID),
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
			Events: &shared.DataBlob{
				EncodingType: shared.EncodingTypeThriftRW.Ptr(),
				Data:         data.Data,
			},
		},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: "standby",
		TaskInfo: &persistence.ReplicationTaskInfo{
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
