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
	"fmt"
	"testing"

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
	"github.com/uber/cadence/common/xdc"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/shard"
)

type (
	taskExecutorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		currentCluster      string
		mockShard           *shard.TestContext
		mockEngine          *engine.MockEngine
		config              *config.Config
		historyClient       *historyservicetest.MockClient
		mockDomainCache     *cache.MockDomainCache
		mockClientBean      *client.MockBean
		adminClient         *adminservicetest.MockClient
		clusterMetadata     *cluster.MockMetadata
		executionManager    *mocks.ExecutionManager
		nDCHistoryResender  *xdc.MockNDCHistoryResender
		historyRereplicator *xdc.MockHistoryRereplicator

		taskHandler *taskExecutorImpl
	}
)

func TestTaskExecutorSuite(t *testing.T) {
	s := new(taskExecutorSuite)
	suite.Run(t, s)
}

func (s *taskExecutorSuite) SetupSuite() {

}

func (s *taskExecutorSuite) TearDownSuite() {

}

func (s *taskExecutorSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.currentCluster = "test"
	s.config = config.NewForTest()

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfo{
			ShardID:                0,
			RangeID:                1,
			ReplicationAckLevel:    0,
			ReplicationDLQAckLevel: map[string]int64{"test": -1},
		},
		s.config,
	)

	s.mockDomainCache = s.mockShard.Resource.DomainCache
	s.mockClientBean = s.mockShard.Resource.ClientBean
	s.adminClient = s.mockShard.Resource.RemoteAdminClient
	s.clusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.executionManager = s.mockShard.Resource.ExecutionMgr
	s.nDCHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.historyRereplicator = &xdc.MockHistoryRereplicator{}

	s.mockEngine = engine.NewMockEngine(s.controller)

	s.historyClient = historyservicetest.NewMockClient(s.controller)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()

	s.taskHandler = NewTaskExecutor(
		s.mockShard,
		s.mockDomainCache,
		s.nDCHistoryResender,
		s.historyRereplicator,
		s.mockEngine,
		metricsClient,
		s.mockShard.GetLogger(),
	).(*taskExecutorImpl)
}

func (s *taskExecutorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.Finish(s.T())
}

func (s *taskExecutorSuite) TestConvertRetryTaskError_OK() {
	err := &shared.RetryTaskError{}
	_, ok := s.taskHandler.convertRetryTaskError(err)
	s.True(ok)
}

func (s *taskExecutorSuite) TestConvertRetryTaskError_NotOK() {
	err := &shared.RetryTaskV2Error{}
	_, ok := s.taskHandler.convertRetryTaskError(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestConvertRetryTaskV2Error_OK() {
	err := &shared.RetryTaskV2Error{}
	_, ok := s.taskHandler.convertRetryTaskV2Error(err)
	s.True(ok)
}

func (s *taskExecutorSuite) TestConvertRetryTaskV2Error_NotOK() {
	err := &shared.RetryTaskError{}
	_, ok := s.taskHandler.convertRetryTaskV2Error(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestFilterTask() {
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
	ok, err := s.taskHandler.filterTask(domainID, false)
	s.NoError(err)
	s.True(ok)
}

func (s *taskExecutorSuite) TestFilterTask_Error() {
	domainID := uuid.New()
	s.mockDomainCache.EXPECT().
		GetDomainByID(domainID).
		Return(nil, fmt.Errorf("test"))
	ok, err := s.taskHandler.filterTask(domainID, false)
	s.Error(err)
	s.False(ok)
}

func (s *taskExecutorSuite) TestFilterTask_EnforceApply() {
	domainID := uuid.New()
	ok, err := s.taskHandler.filterTask(domainID, true)
	s.NoError(err)
	s.True(ok)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask() {
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
	request := &history.SyncActivityRequest{
		DomainId:   common.StringPtr(domainID),
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}

	s.mockEngine.EXPECT().SyncActivity(gomock.Any(), request).Return(nil).Times(1)
	_, err := s.taskHandler.execute(s.currentCluster, task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcessTaskOnce_HistoryReplicationTask() {
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
	request := &history.ReplicateEventsRequest{
		DomainUUID: common.StringPtr(domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
		SourceCluster:     common.StringPtr("test"),
		ForceBufferEvents: common.BoolPtr(false),
	}

	s.mockEngine.EXPECT().ReplicateEvents(gomock.Any(), request).Return(nil).Times(1)
	_, err := s.taskHandler.execute(s.currentCluster, task, true)
	s.NoError(err)
}

func (s *taskExecutorSuite) TestProcess_HistoryV2ReplicationTask() {
	domainID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicator.ReplicationTask{
		TaskType: replicator.ReplicationTaskTypeHistoryV2.Ptr(),
		HistoryTaskV2Attributes: &replicator.HistoryTaskV2Attributes{
			DomainId:   common.StringPtr(domainID),
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}
	request := &history.ReplicateEventsV2Request{
		DomainUUID: common.StringPtr(domainID),
		WorkflowExecution: &shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	}

	s.mockEngine.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil).Times(1)
	_, err := s.taskHandler.execute(s.currentCluster, task, true)
	s.NoError(err)
}
