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
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/quotas"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/shard"
)

type (
	taskProcessorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		mockShard        *shard.TestContext
		mockEngine       *engine.MockEngine
		config           *config.Config
		historyClient    *historyservicetest.MockClient
		taskFetcher      *MockTaskFetcher
		mockDomainCache  *cache.MockDomainCache
		mockClientBean   *client.MockBean
		adminClient      *adminservicetest.MockClient
		clusterMetadata  *cluster.MockMetadata
		executionManager *mocks.ExecutionManager
		requestChan      chan *request
		taskExecutor     *MockTaskExecutor

		taskProcessor *taskProcessorImpl
	}
)

func TestTaskProcessorSuite(t *testing.T) {
	s := new(taskProcessorSuite)
	suite.Run(t, s)
}

func (s *taskProcessorSuite) SetupSuite() {

}

func (s *taskProcessorSuite) TearDownSuite() {

}

func (s *taskProcessorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:          0,
			RangeID:          1,
			TransferAckLevel: 0,
		},
		s.config,
	)

	s.mockDomainCache = s.mockShard.Resource.DomainCache
	s.mockClientBean = s.mockShard.Resource.ClientBean
	s.adminClient = s.mockShard.Resource.RemoteAdminClient
	s.clusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.executionManager = s.mockShard.Resource.ExecutionMgr
	s.taskExecutor = NewMockTaskExecutor(s.controller)

	s.mockEngine = engine.NewMockEngine(s.controller)
	s.config = config.NewForTest()
	s.config.ReplicationTaskProcessorNoTaskRetryWait = dynamicconfig.GetDurationPropertyFnFilteredByTShardID(1 * time.Millisecond)
	s.historyClient = historyservicetest.NewMockClient(s.controller)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.requestChan = make(chan *request, 10)

	s.taskFetcher = NewMockTaskFetcher(s.controller)
	rateLimiter := quotas.NewDynamicRateLimiter(func() float64 {
		return 100
	})
	s.taskFetcher.EXPECT().GetSourceCluster().Return("standby").AnyTimes()
	s.taskFetcher.EXPECT().GetRequestChan().Return(s.requestChan).AnyTimes()
	s.taskFetcher.EXPECT().GetRateLimiter().Return(rateLimiter).AnyTimes()
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()

	s.taskProcessor = NewTaskProcessor(
		s.mockShard,
		s.mockEngine,
		s.config,
		metricsClient,
		s.taskFetcher,
		s.taskExecutor,
	).(*taskProcessorImpl)
}

func (s *taskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *taskProcessorSuite) TestProcessResponse_NoTask() {
	response := &replicator.ReplicationMessages{
		LastRetrievedMessageId: common.Int64Ptr(100),
	}

	s.taskProcessor.processResponse(response)
	s.Equal(int64(100), s.taskProcessor.lastProcessedMessageID)
	s.Equal(int64(100), s.taskProcessor.lastRetrievedMessageID)
}

func (s *taskProcessorSuite) TestSendFetchMessageRequest() {
	s.taskProcessor.sendFetchMessageRequest()
	requestMessage := <-s.requestChan

	s.Equal(int32(0), requestMessage.token.GetShardID())
	s.Equal(int64(-1), requestMessage.token.GetLastProcessedMessageId())
	s.Equal(int64(-1), requestMessage.token.GetLastRetrievedMessageId())
}

func (s *taskProcessorSuite) TestHandleSyncShardStatus() {
	now := time.Now()
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), &history.SyncShardStatusRequest{
		SourceCluster: common.StringPtr("standby"),
		ShardId:       common.Int64Ptr(0),
		Timestamp:     common.Int64Ptr(now.UnixNano()),
	}).Return(nil).Times(1)

	err := s.taskProcessor.handleSyncShardStatus(&replicator.SyncShardStatus{
		Timestamp: common.Int64Ptr(now.UnixNano()),
	})
	s.NoError(err)
}

func (s *taskProcessorSuite) TestPutReplicationTaskToDLQ_SyncActivityReplicationTask() {
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
	err := s.taskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestPutReplicationTaskToDLQ_HistoryReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeHistory.Ptr(),
		HistoryTaskAttributes: &replicator.HistoryTaskAttributes{
			DomainId:     common.StringPtr(domainID),
			WorkflowId:   common.StringPtr(workflowID),
			RunId:        common.StringPtr(runID),
			FirstEventId: common.Int64Ptr(1),
			NextEventId:  common.Int64Ptr(1),
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
			FirstEventID:        int64(1),
			NextEventID:         int64(2),
		},
	}
	s.executionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err := s.taskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestPutReplicationTaskToDLQ_HistoryV2ReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	events := []*shared.HistoryEvent{
		{
			EventId: common.Int64Ptr(1),
			Version: common.Int64Ptr(1),
		},
	}
	serializer := s.mockShard.GetPayloadSerializer()
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
			NextEventID:  2,
			Version:      1,
		},
	}
	s.executionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err = s.taskProcessor.putReplicationTaskToDLQ(task)
	s.NoError(err)
}

func (s *taskProcessorSuite) TestGenerateDLQRequest_ReplicationTaskTypeHistoryV2() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	events := []*shared.HistoryEvent{
		{
			EventId: common.Int64Ptr(1),
			Version: common.Int64Ptr(1),
		},
	}
	serializer := s.mockShard.GetPayloadSerializer()
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
	request, err := s.taskProcessor.generateDLQRequest(task)
	s.NoError(err)
	s.Equal("standby", request.SourceClusterName)
	s.Equal(int64(1), request.TaskInfo.FirstEventID)
	s.Equal(int64(2), request.TaskInfo.NextEventID)
	s.Equal(int64(1), request.TaskInfo.GetVersion())
	s.Equal(domainID, request.TaskInfo.GetDomainID())
	s.Equal(workflowID, request.TaskInfo.GetWorkflowID())
	s.Equal(runID, request.TaskInfo.GetRunID())
	s.Equal(persistence.ReplicationTaskTypeHistory, request.TaskInfo.GetTaskType())
}

func (s *taskProcessorSuite) TestGenerateDLQRequest_ReplicationTaskTypeHistory() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeHistory.Ptr(),
		HistoryTaskAttributes: &replicator.HistoryTaskAttributes{
			DomainId:     common.StringPtr(domainID),
			WorkflowId:   common.StringPtr(workflowID),
			RunId:        common.StringPtr(runID),
			FirstEventId: common.Int64Ptr(1),
			NextEventId:  common.Int64Ptr(1),
			Version:      common.Int64Ptr(1),
		},
	}
	request, err := s.taskProcessor.generateDLQRequest(task)
	s.NoError(err)
	s.Equal("standby", request.SourceClusterName)
	s.Equal(int64(1), request.TaskInfo.FirstEventID)
	s.Equal(int64(2), request.TaskInfo.NextEventID)
	s.Equal(int64(1), request.TaskInfo.GetVersion())
	s.Equal(domainID, request.TaskInfo.GetDomainID())
	s.Equal(workflowID, request.TaskInfo.GetWorkflowID())
	s.Equal(runID, request.TaskInfo.GetRunID())
	s.Equal(persistence.ReplicationTaskTypeHistory, request.TaskInfo.GetTaskType())
}

func (s *taskProcessorSuite) TestGenerateDLQRequest_ReplicationTaskTypeSyncActivity() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeSyncActivity.Ptr(),
		SyncActivityTaskAttributes: &replicator.SyncActivityTaskAttributes{
			DomainId:    common.StringPtr(domainID),
			WorkflowId:  common.StringPtr(workflowID),
			RunId:       common.StringPtr(runID),
			ScheduledId: common.Int64Ptr(1),
		},
	}
	request, err := s.taskProcessor.generateDLQRequest(task)
	s.NoError(err)
	s.Equal("standby", request.SourceClusterName)
	s.Equal(int64(1), request.TaskInfo.ScheduledID)
	s.Equal(domainID, request.TaskInfo.GetDomainID())
	s.Equal(workflowID, request.TaskInfo.GetWorkflowID())
	s.Equal(runID, request.TaskInfo.GetRunID())
	s.Equal(persistence.ReplicationTaskTypeSyncActivity, request.TaskInfo.GetTaskType())
}
