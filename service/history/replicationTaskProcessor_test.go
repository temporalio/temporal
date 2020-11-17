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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"

	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
)

type (
	replicationTaskProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller                  *gomock.Controller
		mockResource                *resource.Test
		mockShard                   *shard.ContextTest
		mockEngine                  *MockEngine
		mockNamespaceCache          *cache.MockNamespaceCache
		mockClientBean              *client.MockBean
		mockAdminClient             *adminservicemock.MockAdminServiceClient
		mockClusterMetadata         *cluster.MockMetadata
		mockHistoryClient           *historyservicemock.MockHistoryServiceClient
		mockReplicationTaskExecutor *MockreplicationTaskExecutor
		mockReplicationTaskFetcher  *MockReplicationTaskFetcher

		mockExecutionManager *mocks.ExecutionManager

		config      *configs.Config
		requestChan chan *request

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

	s.config = NewDynamicConfigForTest()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:          0,
				RangeId:          1,
				TransferAckLevel: 0,
				ClusterReplicationLevel: map[string]int64{
					cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID,
				},
			},
		},
		s.config,
	)
	s.mockEngine = NewMockEngine(s.controller)
	s.mockResource = s.mockShard.Resource
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockClientBean = s.mockResource.ClientBean
	s.mockAdminClient = s.mockResource.RemoteAdminClient
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.mockExecutionManager = s.mockResource.ExecutionMgr
	s.mockReplicationTaskExecutor = NewMockreplicationTaskExecutor(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockReplicationTaskFetcher = NewMockReplicationTaskFetcher(s.controller)
	rateLimiter := quotas.NewDynamicRateLimiter(func() float64 {
		return 100
	})
	s.mockReplicationTaskFetcher.EXPECT().GetSourceCluster().Return(cluster.TestAlternativeClusterName).AnyTimes()
	s.mockReplicationTaskFetcher.EXPECT().GetRequestChan().Return(s.requestChan).AnyTimes()
	s.mockReplicationTaskFetcher.EXPECT().GetRateLimiter().Return(rateLimiter).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.requestChan = make(chan *request, 10)

	s.replicationTaskProcessor = NewReplicationTaskProcessor(
		s.mockShard,
		s.mockEngine,
		s.config,
		metricsClient,
		s.mockReplicationTaskFetcher,
		s.mockReplicationTaskExecutor,
	)
}

func (s *replicationTaskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockResource.Finish(s.T())
}

func (s *replicationTaskProcessorSuite) TestHandleSyncShardStatus_Stale() {
	now := timestamp.TimePtr(time.Now().Add(-2 * dropSyncShardTaskTimeThreshold))
	err := s.replicationTaskProcessor.handleSyncShardStatus(&replicationspb.SyncShardStatus{
		StatusTime: now,
	})
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestHandleSyncShardStatus_Success() {
	now := timestamp.TimeNowPtrUtc()
	s.mockEngine.EXPECT().SyncShardStatus(gomock.Any(), &historyservice.SyncShardStatusRequest{
		SourceCluster: cluster.TestAlternativeClusterName,
		ShardId:       0,
		StatusTime:    now,
	}).Return(nil).Times(1)

	err := s.replicationTaskProcessor.handleSyncShardStatus(&replicationspb.SyncShardStatus{
		StatusTime: now,
	})
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestHandleReplicationTask_SyncActivity() {
	namespaceID := uuid.NewRandom().String()
	workflowID := uuid.New()
	runID := uuid.NewRandom().String()
	attempt := int32(2)
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Attempt:     attempt,
			},
		},
	}

	s.mockReplicationTaskExecutor.EXPECT().execute(task, false).Return(0, nil).Times(1)
	err := s.replicationTaskProcessor.handleReplicationTask(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestHandleReplicationTask_History() {
	namespaceID := uuid.NewRandom().String()
	workflowID := uuid.New()
	runID := uuid.NewRandom().String()
	events := []*historypb.HistoryEvent{{
		EventId: 1,
		Version: 1,
	}}
	versionHistory := []*historyspb.VersionHistoryItem{{
		EventId: 1,
		Version: 1,
	}}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)

	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{
			HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         data.Data,
				},
				VersionHistoryItems: versionHistory,
			},
		},
	}

	s.mockReplicationTaskExecutor.EXPECT().execute(task, false).Return(0, nil).Times(1)
	err = s.replicationTaskProcessor.handleReplicationTask(task)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestHandleReplicationDLQTask_SyncActivity() {
	namespaceID := uuid.NewRandom().String()
	workflowID := uuid.New()
	runID := uuid.NewRandom().String()
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		},
	}

	s.mockExecutionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationDLQTask(request)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestHandleReplicationDLQTask_History() {
	namespaceID := uuid.NewRandom().String()
	workflowID := uuid.New()
	runID := uuid.NewRandom().String()

	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: 1,
			NextEventId:  1,
			Version:      1,
		},
	}

	s.mockExecutionManager.On("PutReplicationTaskToDLQ", request).Return(nil)
	err := s.replicationTaskProcessor.handleReplicationDLQTask(request)
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestConvertTaskToDLQTask_SyncActivity() {
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
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId: namespaceID,
			WorkflowId:  workflowID,
			RunId:       runID,
			TaskType:    enumsspb.TASK_TYPE_REPLICATION_SYNC_ACTIVITY,
		},
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *replicationTaskProcessorSuite) TestConvertTaskToDLQTask_History() {
	namespaceID := uuid.NewRandom().String()
	workflowID := uuid.New()
	runID := uuid.NewRandom().String()
	events := []*historypb.HistoryEvent{{
		EventId: 1,
		Version: 1,
	}}
	versionHistory := []*historyspb.VersionHistoryItem{{
		EventId: 1,
		Version: 1,
	}}
	serializer := s.mockResource.GetPayloadSerializer()
	data, err := serializer.SerializeEvents(events, enumspb.ENCODING_TYPE_PROTO3)
	s.NoError(err)

	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{
			HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Events: &commonpb.DataBlob{
					EncodingType: enumspb.ENCODING_TYPE_PROTO3,
					Data:         data.Data,
				},
				VersionHistoryItems: versionHistory,
			},
		},
	}
	request := &persistence.PutReplicationTaskToDLQRequest{
		SourceClusterName: cluster.TestAlternativeClusterName,
		TaskInfo: &persistencespb.ReplicationTaskInfo{
			NamespaceId:  namespaceID,
			WorkflowId:   workflowID,
			RunId:        runID,
			TaskType:     enumsspb.TASK_TYPE_REPLICATION_HISTORY,
			FirstEventId: 1,
			NextEventId:  1,
			Version:      1,
		},
	}

	dlqTask, err := s.replicationTaskProcessor.convertTaskToDLQTask(task)
	s.NoError(err)
	s.Equal(request, dlqTask)
}

func (s *replicationTaskProcessorSuite) TestCleanupReplicationTask_Noop() {
	ackedTaskID := int64(12345)
	s.mockResource.ShardMgr.On("UpdateShard", mock.Anything).Return(nil).Times(1)
	err := s.mockShard.UpdateClusterReplicationLevel(cluster.TestAlternativeClusterName, ackedTaskID)
	s.NoError(err)

	s.replicationTaskProcessor.minTxAckedTaskID = ackedTaskID
	err = s.replicationTaskProcessor.cleanupReplicationTasks()
	s.NoError(err)
}

func (s *replicationTaskProcessorSuite) TestCleanupReplicationTask_Cleanup() {
	ackedTaskID := int64(12345)
	s.mockResource.ShardMgr.On("UpdateShard", mock.Anything).Return(nil).Times(1)
	err := s.mockShard.UpdateClusterReplicationLevel(cluster.TestAlternativeClusterName, ackedTaskID)
	s.NoError(err)

	s.replicationTaskProcessor.minTxAckedTaskID = ackedTaskID - 1
	s.mockExecutionManager.On("RangeCompleteReplicationTask", &persistence.RangeCompleteReplicationTaskRequest{
		InclusiveEndTaskID: ackedTaskID,
	}).Return(nil).Times(1)
	err = s.replicationTaskProcessor.cleanupReplicationTasks()
	s.NoError(err)
}
