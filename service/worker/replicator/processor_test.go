// Copyright (c) 2017 Uber Technologies, Inc.
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

package replicator

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	"github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/domain"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	messageMocks "github.com/temporalio/temporal/common/messaging/mocks"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
	"github.com/temporalio/temporal/common/task"
	"github.com/temporalio/temporal/common/xdc"
)

type (
	replicationTaskProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller                  *gomock.Controller
		mockSequentialTaskProcessor *task.MockProcessor
		mockHistoryClient           *historyservicemock.MockHistoryServiceClient
		mockDomainCache             *cache.MockDomainCache
		mockNDCResender             *xdc.MockNDCHistoryResender

		currentCluster string
		sourceCluster  string
		config         *Config
		logger         log.Logger
		metricsClient  metrics.Client

		mockMsg                           *messageMocks.Message
		mockDomainReplicationTaskExecutor *domain.MockReplicationTaskExecutor

		mockRereplicator *xdc.MockHistoryRereplicator

		processor *replicationTaskProcessor
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
	s.controller = gomock.NewController(s.T())
	s.mockSequentialTaskProcessor = task.NewMockProcessor(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockDomainCache = cache.NewMockDomainCache(s.controller)
	s.mockNDCResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.mockDomainCache.EXPECT().GetDomainByID(gomock.Any()).Return(
		cache.NewGlobalDomainCacheEntryForTest(
			&persistence.DomainInfo{},
			&persistence.DomainConfig{},
			&persistence.DomainReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []*persistence.ClusterReplicationConfig{
					{ClusterName: cluster.TestCurrentClusterName},
					{ClusterName: cluster.TestAlternativeClusterName},
				},
			},
			123,
			nil,
		),
		nil,
	).AnyTimes()

	s.logger = loggerimpl.NewDevelopmentForTest(s.Suite)
	s.config = &Config{
		ReplicatorTaskConcurrency:     dynamicconfig.GetIntPropertyFn(10),
		ReplicationTaskContextTimeout: dynamicconfig.GetDurationPropertyFn(30 * time.Second),
	}
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.Worker)

	s.mockMsg = &messageMocks.Message{}
	s.mockMsg.On("Partition").Return(int32(0))
	s.mockMsg.On("Offset").Return(int64(0))
	s.mockDomainReplicationTaskExecutor = domain.NewMockReplicationTaskExecutor(s.controller)
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}

	s.currentCluster = cluster.TestAlternativeClusterName
	s.sourceCluster = cluster.TestCurrentClusterName

	s.processor = newReplicationTaskProcessor(
		s.currentCluster,
		s.sourceCluster,
		"some random consumer name",
		nil,
		s.config,
		s.logger,
		s.metricsClient,
		s.mockDomainReplicationTaskExecutor,
		s.mockRereplicator,
		s.mockNDCResender,
		s.mockHistoryClient,
		s.mockDomainCache,
		s.mockSequentialTaskProcessor,
	)
}

func (s *replicationTaskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockMsg.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_BadEncoding() {
	s.mockMsg.On("Value").Return([]byte("some random bad encoded message"))
	s.mockMsg.On("Nack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Domain_Success() {
	replicationAttr := &replication.DomainTaskAttributes{
		DomainOperation: enums.DomainOperationUpdate,
		Id:              "some random domain ID",
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeDomain,
		Attributes: &replication.ReplicationTask_DomainTaskAttributes{DomainTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockDomainReplicationTaskExecutor.EXPECT().Execute(replicationAttr).Return(nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Domain_FailedThenSuccess() {
	replicationAttr := &replication.DomainTaskAttributes{
		DomainOperation: enums.DomainOperationUpdate,
		Id:              "some random domain ID",
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeDomain,
		Attributes: &replication.ReplicationTask_DomainTaskAttributes{DomainTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockDomainReplicationTaskExecutor.EXPECT().Execute(replicationAttr).Return(errors.New("some random error")).Times(1)
	s.mockDomainReplicationTaskExecutor.EXPECT().Execute(replicationAttr).Return(nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_Success() {
	replicationAttr := &replication.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		Timestamp:     time.Now().UnixNano(),
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeSyncShardStatus,
		Attributes: &replication.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockHistoryClient.EXPECT().SyncShardStatus(
		gomock.Any(),
		&historyservice.SyncShardStatusRequest{
			SourceCluster: replicationAttr.SourceCluster,
			ShardId:       replicationAttr.ShardId,
			Timestamp:     replicationAttr.Timestamp,
		},
	).Return(nil, nil).Times(1)
	s.mockMsg.On("Ack").Return(nil, nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_Success_Overdue() {
	replicationAttr := &replication.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		Timestamp:     time.Now().Add(-2 * dropSyncShardTaskTimeThreshold).UnixNano(),
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeSyncShardStatus,
		Attributes: &replication.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_FailedThenSuccess() {
	replicationAttr := &replication.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		Timestamp:     time.Now().UnixNano(),
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeSyncShardStatus,
		Attributes: &replication.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockHistoryClient.EXPECT().SyncShardStatus(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error")).Times(1)
	s.mockHistoryClient.EXPECT().SyncShardStatus(
		gomock.Any(),
		&historyservice.SyncShardStatusRequest{
			SourceCluster: replicationAttr.SourceCluster,
			ShardId:       replicationAttr.ShardId,
			Timestamp:     replicationAttr.Timestamp,
		},
	).Return(nil, nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_Success() {
	replicationAttr := &replication.SyncActivityTaskAttributes{
		DomainId:          "some random domain ID",
		WorkflowId:        "some random workflow ID",
		RunId:             "some random run ID",
		Version:           1234,
		ScheduledId:       1235,
		ScheduledTime:     time.Now().UnixNano(),
		StartedId:         1236,
		StartedTime:       time.Now().UnixNano(),
		LastHeartbeatTime: time.Now().UnixNano(),
		Details:           []byte("some random details"),
		Attempt:           1048576,
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeSyncActivity,
		Attributes: &replication.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_FailedThenSuccess() {
	replicationAttr := &replication.SyncActivityTaskAttributes{
		DomainId:          "some random domain ID",
		WorkflowId:        "some random workflow ID",
		RunId:             "some random run ID",
		Version:           1234,
		ScheduledId:       1235,
		ScheduledTime:     time.Now().UnixNano(),
		StartedId:         1236,
		StartedTime:       time.Now().UnixNano(),
		LastHeartbeatTime: time.Now().UnixNano(),
		Details:           []byte("some random details"),
		Attempt:           1048576,
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeSyncActivity,
		Attributes: &replication.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_History_Success() {
	replicationAttr := &replication.HistoryTaskAttributes{
		TargetClusters:  []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:        "some random domain ID",
		WorkflowId:      "some random workflow ID",
		RunId:           "some random run ID",
		Version:         1394,
		FirstEventId:    728,
		NextEventId:     1015,
		ReplicationInfo: map[string]*replication.ReplicationInfo{},
		History: &commonproto.History{
			Events: []*commonproto.HistoryEvent{{EventId: 1}},
		},
		NewRunHistory: nil,
		ResetWorkflow: true,
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeHistory,
		Attributes: &replication.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_History_FailedThenSuccess() {
	replicationAttr := &replication.HistoryTaskAttributes{
		TargetClusters:  []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:        "some random domain ID",
		WorkflowId:      "some random workflow ID",
		RunId:           "some random run ID",
		Version:         1394,
		FirstEventId:    728,
		NextEventId:     1015,
		ReplicationInfo: map[string]*replication.ReplicationInfo{},
		History: &commonproto.History{
			Events: []*commonproto.HistoryEvent{{EventId: 1}},
		},
		NewRunHistory: nil,
		ResetWorkflow: true,
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeHistory,
		Attributes: &replication.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_HistoryMetadata_Success() {
	replicationAttr := &replication.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       "some random domain ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		FirstEventId:   728,
		NextEventId:    1015,
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeHistoryMetadata,
		Attributes: &replication.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_HistoryMetadata_FailedThenSuccess() {
	replicationAttr := &replication.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       "some random domain ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		FirstEventId:   728,
		NextEventId:    1015,
	}
	replicationTask := &replication.ReplicationTask{
		TaskType:   enums.ReplicationTaskTypeHistoryMetadata,
		Attributes: &replication.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}
