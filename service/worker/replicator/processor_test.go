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

package replicator

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/loggerimpl"
	messageMocks "go.temporal.io/server/common/messaging/mocks"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/service/dynamicconfig"
	"go.temporal.io/server/common/task"
	"go.temporal.io/server/common/xdc"
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
			&persistencespb.NamespaceInfo{},
			&persistencespb.NamespaceConfig{},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
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
		s.mockNDCResender,
		s.mockHistoryClient,
		s.mockNamespaceCache,
		s.mockSequentialTaskProcessor,
	)
}

func (s *replicationTaskProcessorSuite) TearDownTest() {
	s.controller.Finish()
	s.mockMsg.AssertExpectations(s.T())
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_BadEncoding() {
	s.mockMsg.On("Value").Return([]byte("some random bad encoded message"))
	s.mockMsg.On("Nack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Namespace_Success() {
	replicationAttr := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Id:                 "some random namespace ID",
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{NamespaceTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockNamespaceReplicationTaskExecutor.EXPECT().Execute(replicationAttr).Return(nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Namespace_FailedThenSuccess() {
	replicationAttr := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Id:                 "some random namespace ID",
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{NamespaceTaskAttributes: replicationAttr},
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
	replicationAttr := &replicationspb.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		StatusTime:    timestamp.TimeNowPtrUtc(),
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockHistoryClient.EXPECT().SyncShardStatus(
		gomock.Any(),
		&historyservice.SyncShardStatusRequest{
			SourceCluster: replicationAttr.SourceCluster,
			ShardId:       replicationAttr.ShardId,
			StatusTime:    replicationAttr.StatusTime,
		},
	).Return(nil, nil).Times(1)
	s.mockMsg.On("Ack").Return(nil, nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_Success_Overdue() {
	replicationAttr := &replicationspb.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		StatusTime:    timestamp.TimeNowPtrUtcAddDuration(-2 * dropSyncShardTaskTimeThreshold),
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_FailedThenSuccess() {
	replicationAttr := &replicationspb.SyncShardStatusTaskAttributes{
		SourceCluster: "some random source cluster",
		ShardId:       2333,
		StatusTime:    timestamp.TimeNowPtrUtc(),
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_SYNC_SHARD_STATUS_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncShardStatusTaskAttributes{SyncShardStatusTaskAttributes: replicationAttr},
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
			StatusTime:    replicationAttr.StatusTime,
		},
	).Return(nil, nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_Success() {
	replicationAttr := &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:       "some random namespace ID",
		WorkflowId:        "some random workflow ID",
		RunId:             "some random run ID",
		Version:           1234,
		ScheduledId:       1235,
		ScheduledTime:     timestamp.TimeNowPtrUtc(),
		StartedId:         1236,
		StartedTime:       timestamp.TimeNowPtrUtc(),
		LastHeartbeatTime: timestamp.TimeNowPtrUtc(),
		Details:           payloads.EncodeString("some random details"),
		Attempt:           1048576,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_FailedThenSuccess() {
	replicationAttr := &replicationspb.SyncActivityTaskAttributes{
		NamespaceId:       "some random namespace ID",
		WorkflowId:        "some random workflow ID",
		RunId:             "some random run ID",
		Version:           1234,
		ScheduledId:       1235,
		ScheduledTime:     timestamp.TimeNowPtrUtc(),
		StartedId:         1236,
		StartedTime:       timestamp.TimeNowPtrUtc(),
		LastHeartbeatTime: timestamp.TimeNowPtrUtc(),
		Details:           payloads.EncodeString("some random details"),
		Attempt:           1048576,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_SYNC_ACTIVITY_TASK,
		Attributes: &replicationspb.ReplicationTask_SyncActivityTaskAttributes{SyncActivityTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_HistoryMetadata_Success() {
	replicationAttr := &replicationspb.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		NamespaceId:    "some random namespace ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		FirstEventId:   728,
		NextEventId:    1015,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_HistoryMetadata_FailedThenSuccess() {
	replicationAttr := &replicationspb.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		NamespaceId:    "some random namespace ID",
		WorkflowId:     "some random workflow ID",
		RunId:          "some random run ID",
		FirstEventId:   728,
		NextEventId:    1015,
	}
	replicationTask := &replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_HISTORY_METADATA_TASK,
		Attributes: &replicationspb.ReplicationTask_HistoryMetadataTaskAttributes{HistoryMetadataTaskAttributes: replicationAttr},
	}
	replicationTaskBinary, err := replicationTask.Marshal()
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockSequentialTaskProcessor.EXPECT().Submit(gomock.Any()).Return(nil).Times(1)

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}
