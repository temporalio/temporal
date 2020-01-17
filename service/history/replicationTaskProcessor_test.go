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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/go/admin/adminservicetest"
	"github.com/temporalio/temporal/.gen/go/history"
	"github.com/temporalio/temporal/.gen/go/history/historyservicetest"
	"github.com/temporalio/temporal/.gen/go/shared"
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

		mockResource           *resource.Test
		mockShard              ShardContext
		mockEngine             *MockEngine
		config                 *Config
		historyClient          *historyservicetest.MockClient
		replicationTaskFetcher *MockReplicationTaskFetcher
		mockDomainCache        *cache.MockDomainCache
		mockClientBean         *client.MockBean
		adminClient            *adminservicetest.MockClient
		clusterMetadata        *cluster.MockMetadata
		executionManager       *mocks.ExecutionManager
		requestChan            chan *request

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
		s.historyClient,
		metricsClient,
		s.replicationTaskFetcher,
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

func (s *replicationTaskProcessorSuite) TestConvertRetryTaskError_OK() {
	err := &shared.RetryTaskError{}
	_, ok := s.replicationTaskProcessor.convertRetryTaskError(err)
	s.True(ok)
}

func (s *replicationTaskProcessorSuite) TestConvertRetryTaskError_NotOK() {
	err := &shared.RetryTaskV2Error{}
	_, ok := s.replicationTaskProcessor.convertRetryTaskError(err)
	s.False(ok)
}

func (s *replicationTaskProcessorSuite) TestConvertRetryTaskV2Error_OK() {
	err := &shared.RetryTaskV2Error{}
	_, ok := s.replicationTaskProcessor.convertRetryTaskV2Error(err)
	s.True(ok)
}

func (s *replicationTaskProcessorSuite) TestConvertRetryTaskV2Error_NotOK() {
	err := &shared.RetryTaskError{}
	_, ok := s.replicationTaskProcessor.convertRetryTaskV2Error(err)
	s.False(ok)
}

func (s *replicationTaskProcessorSuite) TestFilterTask() {
	domainID := uuid.New()
	s.mockDomainCache.EXPECT().
		GetDomainByID(domainID).
		Return(cache.NewGlobalDomainCacheEntryForTest(
			nil,
			nil,
			&persistence.DomainReplicationConfig{
				Clusters: []*persistence.ClusterReplicationConfig{
					{
						ClusterName: "active",
					},
				}},
			0,
			s.clusterMetadata,
		), nil)
	ok, err := s.replicationTaskProcessor.filterTask(domainID)
	s.NoError(err)
	s.True(ok)
}

func (s *replicationTaskProcessorSuite) TestFilterTask_Error() {
	domainID := uuid.New()
	s.mockDomainCache.EXPECT().
		GetDomainByID(domainID).
		Return(nil, fmt.Errorf("test"))
	ok, err := s.replicationTaskProcessor.filterTask(domainID)
	s.Error(err)
	s.False(ok)
}

func (s *replicationTaskProcessorSuite) TestHandleSyncShardStatus() {
	now := time.Now()
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), &history.SyncShardStatusRequest{
		SourceCluster: common.StringPtr("standby"),
		ShardId:       common.Int64Ptr(0),
		Timestamp:     common.Int64Ptr(now.UnixNano()),
	}).Return(nil).Times(1)

	err := s.replicationTaskProcessor.handleSyncShardStatus(&commonproto.SyncShardStatus{
		Timestamp: now.UnixNano(),
	})
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestProcessTaskOnce_DomainReplicationTask() {
	defer func() {
		if r := recover(); r == nil {
			s.Fail("The Domain replication task should panic")
		}
	}()

	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeDomain,
	}
	err := s.replicationTaskProcessor.processTaskOnce(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestProcessTaskOnce_SyncShardReplicationTask() {
	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeSyncShardStatus,
	}
	err := s.replicationTaskProcessor.processTaskOnce(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestProcessTaskOnce_HistoryMetadataReplicationTask() {
	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeHistoryMetadata,
	}
	err := s.replicationTaskProcessor.processTaskOnce(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestProcessTaskOnce_SyncActivityReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeSyncActivity,
		Attributes: &commonproto.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: &commonproto.SyncActivityTaskAttributes{
			DomainId:   domainID,
			WorkflowId: workflowID,
			RunId:      runID,
		}},
	}
	request := &history.SyncActivityRequest{
		DomainId:   common.StringPtr(domainID),
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}

	s.mockDomainCache.EXPECT().
		GetDomainByID(domainID).
		Return(cache.NewGlobalDomainCacheEntryForTest(
			nil,
			nil,
			&persistence.DomainReplicationConfig{
				Clusters: []*persistence.ClusterReplicationConfig{
					{
						ClusterName: "active",
					},
				}},
			0,
			s.clusterMetadata,
		), nil).Times(1)
	s.mockEngine.EXPECT().SyncActivity(gomock.Any(), request).Return(nil).Times(1)
	err := s.replicationTaskProcessor.processTaskOnce(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestProcessTaskOnce_HistoryReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeHistory,
		Attributes: &commonproto.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: &commonproto.HistoryTaskAttributes{
			DomainId:   domainID,
			WorkflowId: workflowID,
			RunId:      runID,
		}},
	}
	request := &history.ReplicateEventsRequest{
		DomainUUID: common.StringPtr(domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		SourceCluster:     common.StringPtr("standby"),
		ForceBufferEvents: common.BoolPtr(false),
	}

	s.mockDomainCache.EXPECT().
		GetDomainByID(domainID).
		Return(cache.NewGlobalDomainCacheEntryForTest(
			nil,
			nil,
			&persistence.DomainReplicationConfig{
				Clusters: []*persistence.ClusterReplicationConfig{
					{
						ClusterName: "active",
					},
				}},
			0,
			s.clusterMetadata,
		), nil).Times(1)
	s.mockEngine.EXPECT().ReplicateEvents(gomock.Any(), request).Return(nil).Times(1)
	err := s.replicationTaskProcessor.processTaskOnce(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestProcessTaskOnce_HistoryV2ReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeHistoryV2,
		Attributes: &commonproto.ReplicationTask_HistoryTaskV2Attributes{HistoryTaskV2Attributes: &commonproto.HistoryTaskV2Attributes{
			DomainId:   domainID,
			WorkflowId: workflowID,
			RunId:      runID,
		}},
	}
	request := &history.ReplicateEventsV2Request{
		DomainUUID: common.StringPtr(domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}

	s.mockDomainCache.EXPECT().
		GetDomainByID(domainID).
		Return(cache.NewGlobalDomainCacheEntryForTest(
			nil,
			nil,
			&persistence.DomainReplicationConfig{
				Clusters: []*persistence.ClusterReplicationConfig{
					{
						ClusterName: "active",
					},
				}},
			0,
			s.clusterMetadata,
		), nil).Times(1)
	s.mockEngine.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil).Times(1)
	err := s.replicationTaskProcessor.processTaskOnce(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestPutReplicationTaskToDLQ_SyncActivityReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeSyncActivity,
		Attributes: &commonproto.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: &commonproto.SyncActivityTaskAttributes{
			DomainId:   domainID,
			WorkflowId: workflowID,
			RunId:      runID,
		}},
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
	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeHistory,
		Attributes: &commonproto.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: &commonproto.HistoryTaskAttributes{
			DomainId:   domainID,
			WorkflowId: workflowID,
			RunId:      runID,
		}},
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
	task := &commonproto.ReplicationTask{
		TaskType: enums.ReplicationTaskTypeHistoryV2,
		Attributes: &commonproto.ReplicationTask_HistoryTaskV2Attributes{HistoryTaskV2Attributes: &commonproto.HistoryTaskV2Attributes{
			DomainId:   domainID,
			WorkflowId: workflowID,
			RunId:      runID,
			Events: &commonproto.DataBlob{
				EncodingType: enums.EncodingTypeThriftRW,
				Data:         data.Data,
			},
		}},
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
