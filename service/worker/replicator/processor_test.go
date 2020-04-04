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
	eventpb "go.temporal.io/temporal-proto/event"

	"github.com/temporalio/temporal/.gen/proto/historyservice"
	"github.com/temporalio/temporal/.gen/proto/historyservicemock"
	replicationgenpb "github.com/temporalio/temporal/.gen/proto/replication"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	messageMocks "github.com/temporalio/temporal/common/messaging/mocks"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/namespace"
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
		mockNamespaceCache          *cache.MockNamespaceCache
		mockNDCResender             *xdc.MockNDCHistoryResender

		currentCluster string
		sourceCluster  string
		config         *Config
		logger         log.Logger
		metricsClient  metrics.Client

		mockMsg                              *messageMocks.Message
		mockNamespaceReplicationTaskExecutor *namespace.MockReplicationTaskExecutor

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
	s.mockNamespaceCache = cache.NewMockNamespaceCache(s.controller)
	s.mockNDCResender = xdc.NewMockNDCHistoryResender(s.controller)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(
		cache.NewGlobalNamespaceCacheEntryForTest(
			&persistence.NamespaceInfo{},
			&persistence.NamespaceConfig{},
			&persistence.NamespaceReplicationConfig{
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
	s.mockNamespaceReplicationTaskExecutor = namespace.NewMockReplicationTaskExecutor(s.controller)
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
		s.mockNamespaceReplicationTaskExecutor,
		s.mockRereplicator,
		s.mockNDCResender,
		s.mockHistoryClient,
		s.mockNamespaceCache,
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

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Namespace_Success() {
	replicationAttr := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: replicationgenpb.NamespaceOperation_Update,
		Id:                 "some random namespace ID",
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_Namespace,
		Attributes: &replicationgenpb.ReplicationTask_NamespaceTaskAttributes{NamespaceTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockNamespaceReplicationTaskExecutor.EXPECT().Execute(replicationAttr).Return(nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Namespace_FailedThenSuccess() {
	replicationAttr := &replicationgenpb.NamespaceTaskAttributes{
		NamespaceOperation: replicationgenpb.NamespaceOperation_Update,
		Id:                 "some random namespace ID",
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_Namespace,
		Attributes: &replicationgenpb.ReplicationTask_NamespaceTaskAttributes{NamespaceTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockNamespaceReplicationTaskExecutor.EXPECT().Execute(replicationAttr).Return(errors.New("some random error")).Times(1)
	s.mockNamespaceReplicationTaskExecutor.EXPECT().Execute(replicationAttr).Return(nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_Success() {
	replicationAttr := &replicationgenpb.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		Timestamp:     time.Now().UnixNano(),
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_SyncShardStatus,
		Attributes: &replicationgenpb.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
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
	replicationAttr := &replicationgenpb.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		Timestamp:     time.Now().Add(-2 * dropSyncShardTaskTimeThreshold).UnixNano(),
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_SyncShardStatus,
		Attributes: &replicationgenpb.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_FailedThenSuccess() {
	replicationAttr := &replicationgenpb.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		Timestamp:     time.Now().UnixNano(),
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_SyncShardStatus,
		Attributes: &replicationgenpb.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
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
	replicationAttr := &replicationgenpb.SyncActivityTaskAttributes{
		NamespaceId:       "some random namespace ID",
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
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_SyncActivity,
		Attributes: &replicationgenpb.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_FailedThenSuccess() {
	replicationAttr := &replicationgenpb.SyncActivityTaskAttributes{
		NamespaceId:       "some random namespace ID",
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
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_SyncActivity,
		Attributes: &replicationgenpb.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_History_Success() {
	replicationAttr := &replicationgenpb.HistoryTaskAttributes{
		TargetClusters:  []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		NamespaceId:     "some random namespace ID",
		WorkflowId:      "some random workflow ID",
		RunId:           "some random run ID",
		Version:         1394,
		FirstEventId:    728,
		NextEventId:     1015,
		ReplicationInfo: map[string]*replicationgenpb.ReplicationInfo{},
		History: &eventpb.History{
			Events: []*eventpb.HistoryEvent{{EventId: 1}},
		},
		NewRunHistory: nil,
		ResetWorkflow: true,
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_History,
		Attributes: &replicationgenpb.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_History_FailedThenSuccess() {
	replicationAttr := &replicationgenpb.HistoryTaskAttributes{
		TargetClusters:  []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		NamespaceId:     "some random namespace ID",
		WorkflowId:      "some random workflow ID",
		RunId:           "some random run ID",
		Version:         1394,
		FirstEventId:    728,
		NextEventId:     1015,
		ReplicationInfo: map[string]*replicationgenpb.ReplicationInfo{},
		History: &eventpb.History{
			Events: []*eventpb.HistoryEvent{{EventId: 1}},
		},
		NewRunHistory: nil,
		ResetWorkflow: true,
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_History,
		Attributes: &replicationgenpb.ReplicationTask_HistoryTaskAttributes{HistoryTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_HistoryMetadata_Success() {
	replicationAttr := &replicationgenpb.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		NamespaceId:    "some random namespace ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		FirstEventId:   728,
		NextEventId:    1015,
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_HistoryMetadata,
		Attributes: &replicationgenpb.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_HistoryMetadata_FailedThenSuccess() {
	replicationAttr := &replicationgenpb.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		NamespaceId:    "some random namespace ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		FirstEventId:   728,
		NextEventId:    1015,
	}
	replicationTask := &replicationgenpb.ReplicationTask{
		TaskType:   replicationgenpb.ReplicationTaskType_HistoryMetadata,
		Attributes: &replicationgenpb.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}
