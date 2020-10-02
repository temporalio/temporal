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
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
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
		mockNamespaceCache      *cache.MockNamespaceCache
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
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockClientBean = s.mockResource.ClientBean
	s.adminClient = s.mockResource.RemoteAdminClient
	s.clusterMetadata = s.mockResource.ClusterMetadata
	s.executionManager = s.mockResource.ExecutionMgr
	s.replicationTaskExecutor = NewMockreplicationTaskExecutor(s.controller)
	logger := log.NewNoop()
	s.mockShard = &shardContextImpl{
		shardID:                   0,
		Resource:                  s.mockResource,
		shardInfo:                 &persistence.ShardInfoWithFailover{ShardInfo: &persistenceblobs.ShardInfo{RangeId: 1, TransferAckLevel: 0}},
		transferSequenceNumber:    1,
		maxTransferSequenceNumber: 100000,
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
	rateLimiter := quotas.NewDynamicRateLimiter(func() float64 {
		return 100
	})
	s.replicationTaskFetcher.EXPECT().GetSourceCluster().Return("standby").AnyTimes()
	s.replicationTaskFetcher.EXPECT().GetRequestChan().Return(s.requestChan).AnyTimes()
	s.replicationTaskFetcher.EXPECT().GetRateLimiter().Return(rateLimiter).AnyTimes()
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

	s.Equal(int32(0), requestMessage.token.GetShardId())
	s.Equal(int64(-1), requestMessage.token.GetLastProcessedMessageId())
	s.Equal(int64(-1), requestMessage.token.GetLastRetrievedMessageId())
}

func (s *replicationTaskProcessorSuite) TestHandleSyncShardStatus() {
	now := timestamp.TimeNowPtrUtc()
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), &historyservice.SyncShardStatusRequest{
		SourceCluster: "standby",
		ShardId:       0,
		StatusTime:    now,
	}).Return(nil).Times(1)

	err := s.replicationTaskProcessor.handleSyncShardStatus(&replicationspb.SyncShardStatus{
		StatusTime: now,
	})
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestPutReplicationTaskToDLQ_SyncActivityReplicationTask() {
	namespaceID := uuid.NewRandom().String()
	workflowID := uuid.New()
	runID := uuid.NewRandom().String()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Attempt:     1,
		}},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: "standby",
		TaskInfo: &persistenceblobs.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		},
	}
	s.executionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err := s.replicationTaskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestPutReplicationTaskToDLQ_HistoryV2ReplicationTask() {
	namespaceID := uuid.NewRandom().String()
	workflowID := uuid.New()
	runID := uuid.NewRandom().String()
	events := []*historypb.HistoryEvent{
		{
			EventId: 1,
			Version: 1,
		},
	}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeBatchEvents(events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			Events: &commonpb.DataBlob{
				EncodingType: enumspb.ENCODING_TYPE_PROTO3,
				Data:         data.Data,
			},
		}},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: "standby",
		TaskInfo: &persistenceblobs.ReplicationTaskInfo{
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: 1,
			NextEventId:  1,
			Version:      1,
		},
	}
	s.executionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err = s.replicationTaskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}
