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

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

type (
	replicationTaskExecutorSuite struct {
		suite.Suite
		*require.Assertions
		controller *gomock.Controller

		currentCluster     string
		mockResource       *resource.Test
		mockShard          *shard.ContextTest
		mockEngine         *shard.MockEngine
		config             *configs.Config
		historyClient      *historyservicemock.MockHistoryServiceClient
		mockNamespaceCache *cache.MockNamespaceCache
		mockClientBean     *client.MockBean
		adminClient        *adminservicemock.MockAdminServiceClient
		clusterMetadata    *cluster.MockMetadata
		nDCHistoryResender *xdc.MockNDCHistoryResender

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
	s.currentCluster = cluster.TestCurrentClusterName

	s.config = NewDynamicConfigForTest()
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistence.ShardInfoWithFailover{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:             0,
				RangeId:             1,
				ReplicationAckLevel: 0,
				ReplicationDlqAckLevel: map[string]int64{
					cluster.TestAlternativeClusterName: persistence.EmptyQueueMessageID,
				},
			}},
		s.config,
	)
	s.mockEngine = shard.NewMockEngine(s.controller)
	s.mockResource = s.mockShard.Resource
	s.mockNamespaceCache = s.mockResource.NamespaceCache
	s.mockClientBean = s.mockResource.ClientBean
	s.adminClient = s.mockResource.RemoteAdminClient
	s.clusterMetadata = s.mockResource.ClusterMetadata
	s.nDCHistoryResender = xdc.NewMockNDCHistoryResender(s.controller)

	s.historyClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	metricsClient := metrics.NewClient(tally.NoopScope, metrics.History)
	s.clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.replicationTaskHandler = newReplicationTaskExecutor(
		s.currentCluster,
		s.mockShard,
		s.mockNamespaceCache,
		s.nDCHistoryResender,
		s.mockEngine,
		metricsClient,
		s.mockShard.GetLogger(),
	).(*replicationTaskExecutorImpl)
}

func (s *replicationTaskExecutorSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *replicationTaskExecutorSuite) TestFilterTask_Apply() {
	namespaceID := uuid.New()
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(cache.NewGlobalNamespaceCacheEntryForTest(
			nil,
			nil,
			&persistencespb.NamespaceReplicationConfig{Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			}},
			0,
			s.clusterMetadata,
		), nil)
	ok, err := s.replicationTaskHandler.filterTask(namespaceID, false)
	s.NoError(err)
	s.True(ok)
}

func (s *replicationTaskExecutorSuite) TestFilterTask_NotApply() {
	namespaceID := uuid.New()
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(cache.NewGlobalNamespaceCacheEntryForTest(
			nil,
			nil,
			&persistencespb.NamespaceReplicationConfig{Clusters: []string{cluster.TestAlternativeClusterName}},
			0,
			s.clusterMetadata,
		), nil)
	ok, err := s.replicationTaskHandler.filterTask(namespaceID, false)
	s.NoError(err)
	s.False(ok)
}

func (s *replicationTaskExecutorSuite) TestFilterTask_Error() {
	namespaceID := uuid.New()
	s.mockNamespaceCache.EXPECT().
		GetNamespaceByID(namespaceID).
		Return(nil, fmt.Errorf("random error"))
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
				NamespaceId:        namespaceID,
				WorkflowId:         workflowID,
				RunId:              runID,
				Version:            1234,
				ScheduledId:        2345,
				ScheduledTime:      nil,
				StartedId:          2346,
				StartedTime:        nil,
				LastHeartbeatTime:  nil,
				Attempt:            10,
				LastFailure:        nil,
				LastWorkerIdentity: "",
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
		NamespaceId:        namespaceID,
		WorkflowId:         workflowID,
		RunId:              runID,
		Version:            1234,
		ScheduledId:        2345,
		ScheduledTime:      nil,
		StartedId:          2346,
		StartedTime:        nil,
		LastHeartbeatTime:  nil,
		Attempt:            10,
		LastFailure:        nil,
		LastWorkerIdentity: "",
	}

	s.mockEngine.EXPECT().SyncActivity(gomock.Any(), request).Return(nil)
	_, err := s.replicationTaskHandler.execute(task, true)
	s.NoError(err)
}

func (s *replicationTaskExecutorSuite) TestProcessTaskOnce_SyncActivityReplicationTask_Resend() {
	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{
			SyncActivityTaskAttributes: &replicationspb.SyncActivityTaskAttributes{
				NamespaceId:        namespaceID,
				WorkflowId:         workflowID,
				RunId:              runID,
				Version:            1234,
				ScheduledId:        2345,
				ScheduledTime:      nil,
				StartedId:          2346,
				StartedTime:        nil,
				LastHeartbeatTime:  nil,
				Attempt:            10,
				LastFailure:        nil,
				LastWorkerIdentity: "",
			},
		},
	}
	request := &historyservice.SyncActivityRequest{
		NamespaceId:        namespaceID,
		WorkflowId:         workflowID,
		RunId:              runID,
		Version:            1234,
		ScheduledId:        2345,
		ScheduledTime:      nil,
		StartedId:          2346,
		StartedTime:        nil,
		LastHeartbeatTime:  nil,
		Attempt:            10,
		LastFailure:        nil,
		LastWorkerIdentity: "",
	}

	resendErr := serviceerrors.NewRetryReplication(
		"some random error message",
		namespaceID,
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.mockEngine.EXPECT().SyncActivity(gomock.Any(), request).Return(resendErr)
	s.nDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		namespaceID,
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.mockEngine.EXPECT().SyncActivity(gomock.Any(), request).Return(nil)
	_, err := s.replicationTaskHandler.execute(task, true)
	s.NoError(err)
}

func (s *replicationTaskExecutorSuite) TestProcess_HistoryReplicationTask() {
	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{
			HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
				NamespaceId:         namespaceID,
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{233, 2333}},
				Events:              nil,
				NewRunEvents:        nil,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{233, 2333}},
		Events:              nil,
		NewRunEvents:        nil,
	}

	s.mockEngine.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil)
	_, err := s.replicationTaskHandler.execute(task, true)
	s.NoError(err)
}

func (s *replicationTaskExecutorSuite) TestProcess_HistoryReplicationTask_Resend() {
	namespaceID := uuid.New()
	workflowID := uuid.New()
	runID := uuid.New()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryTaskV2Attributes{
			HistoryTaskV2Attributes: &replicationspb.HistoryTaskV2Attributes{
				NamespaceId:         namespaceID,
				WorkflowId:          workflowID,
				RunId:               runID,
				VersionHistoryItems: []*historyspb.VersionHistoryItem{{233, 2333}},
				Events:              nil,
				NewRunEvents:        nil,
			},
		},
	}
	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		VersionHistoryItems: []*historyspb.VersionHistoryItem{{233, 2333}},
		Events:              nil,
		NewRunEvents:        nil,
	}

	resendErr := serviceerrors.NewRetryReplication(
		"some random error message",
		namespaceID,
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.mockEngine.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(resendErr)
	s.nDCHistoryResender.EXPECT().SendSingleWorkflowHistory(
		namespaceID,
		workflowID,
		runID,
		123,
		234,
		345,
		456,
	)
	s.mockEngine.EXPECT().ReplicateEventsV2(gomock.Any(), request).Return(nil)
	_, err := s.replicationTaskHandler.execute(task, true)
	s.NoError(err)
}
