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
	"fmt"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	"go.temporal.io/server/api/persistenceblobs/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/mocks"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/xdc"
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
			ReplicationDlqAckLevel: map[string]int64{"test": -1},
		}},
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

func (s *replicationTaskExecutorSuite) TestFilterTask() {
	namespaceID := uuid.New()
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(cache.NewGlobalNamespaceCacheEntryForTest(
			nil,
			nil,
			&persistenceblobs.NamespaceReplicationConfig{
				Clusters: []string{
					"test",
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
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
				Attempt:     1,
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
		NamespaceId:        namespaceID,
		WorkflowId:         workflowID,
		RunId:              runID,
		Version:            0,
		ScheduledId:        0,
		ScheduledTime:      nil,
		StartedId:          0,
		StartedTime:        nil,
		LastHeartbeatTime:  nil,
		Attempt:            1,
		LastFailure:        nil,
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
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
			HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{
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
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		SourceCluster:     "test",
		ForceBufferEvents: false,
		FirstEventId:      1,
		NextEventId:       5,
		Version:           common.EmptyVersion,
		ResetWorkflow:     false,
		NewRunNdc:         false,
	}

	s.mockEngine.EXPECT().ReplicateEvents(gomock.Any(), request).Return(nil).Times(1)
	_, err := s.replicationTaskHandler.execute(s.currentCluster, task, true)
	s.NoError(err)
}

func (s *replicationTaskExecutorSuite) TestProcess_HistoryV2ReplicationTask() {
	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{
			HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
				RunId:       runID,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	}

	s.mockEngine.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil).Times(1)
	_, err := s.replicationTaskHandler.execute(s.currentCluster, task, true)
	s.NoError(err)
}
