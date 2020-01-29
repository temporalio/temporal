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
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	commonproto "go.temporal.io/temporal-proto/common"
	"go.temporal.io/temporal-proto/enums"

	h "github.com/temporalio/temporal/.gen/go/history"
	"github.com/temporalio/temporal/.gen/go/history/historyservicetest"
	"github.com/temporalio/temporal/.gen/go/replicator"
	"github.com/temporalio/temporal/.gen/go/shared"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/cluster"
	"github.com/temporalio/temporal/common/codec"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/loggerimpl"
	messageMocks "github.com/temporalio/temporal/common/messaging/mocks"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
	"github.com/temporalio/temporal/common/task"
	"github.com/temporalio/temporal/common/xdc"
	"github.com/temporalio/temporal/service/frontend/adapter"
)

type (
	replicationTaskProcessorSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		mockHistoryClient *historyservicetest.MockClient
		mockDomainCache   *cache.MockDomainCache
		mockNDCResender   *xdc.MockNDCHistoryResender

		currentCluster string
		sourceCluster  string
		config         *Config
		logger         log.Logger
		metricsClient  metrics.Client
		msgEncoder     codec.BinaryEncoder

		mockMsg              *messageMocks.Message
		mockDomainReplicator *MockDomainReplicator

		mockSequentialTaskProcessor *task.MockSequentialTaskProcessor
		mockRereplicator            *xdc.MockHistoryRereplicator

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
	s.mockHistoryClient = historyservicetest.NewMockClient(s.controller)
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
	s.msgEncoder = codec.NewThriftRWEncoder()

	s.mockMsg = &messageMocks.Message{}
	s.mockMsg.On("Partition").Return(int32(0))
	s.mockMsg.On("Offset").Return(int64(0))
	s.mockDomainReplicator = &MockDomainReplicator{}
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}
	s.mockSequentialTaskProcessor = &task.MockSequentialTaskProcessor{}

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
		s.mockDomainReplicator,
		s.mockRereplicator,
		s.mockNDCResender,
		s.mockHistoryClient,
		s.mockDomainCache,
		s.mockSequentialTaskProcessor,
	)
}

func (s *replicationTaskProcessorSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.mockDomainReplicator.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
	s.mockSequentialTaskProcessor.AssertExpectations(s.T())

	s.controller.Finish()
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_BadEncoding() {
	s.mockMsg.On("Value").Return([]byte("some random bad encoded message"))
	s.mockMsg.On("Nack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Domain_Success() {
	replicationAttr := &commonproto.DomainTaskAttributes{
		DomainOperation: enums.DomainOperationUpdate,
		Id:              "some random domain ID",
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
		DomainTaskAttributes: adapter.ToThriftDomainTaskAttributes(replicationAttr),
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockDomainReplicator.On("HandleReceivingTask", replicationAttr).Return(nil).Once()
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Domain_FailedThenSuccess() {
	replicationAttr := &commonproto.DomainTaskAttributes{
		DomainOperation: enums.DomainOperationUpdate,
		Id:              "some random domain ID",
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
		DomainTaskAttributes: adapter.ToThriftDomainTaskAttributes(replicationAttr),
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockDomainReplicator.On("HandleReceivingTask", replicationAttr).Return(errors.New("some random error")).Once()
	s.mockDomainReplicator.On("HandleReceivingTask", replicationAttr).Return(nil).Once()
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_Success() {
	replicationAttr := &replicator.SyncShardStatusTaskAttributes{
		SourceCluster: common.StringPtr("some random source cluster"),
		ShardId:       common.Int64Ptr(2333),
		Timestamp:     common.Int64Ptr(time.Now().UnixNano()),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                      replicator.ReplicationTaskTypeSyncShardStatus.Ptr(),
		SyncShardStatusTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockHistoryClient.EXPECT().SyncShardStatus(
		gomock.Any(),
		&h.SyncShardStatusRequest{
			SourceCluster: replicationAttr.SourceCluster,
			ShardId:       replicationAttr.ShardId,
			Timestamp:     replicationAttr.Timestamp,
		},
	).Return(nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_Success_Overdue() {
	replicationAttr := &replicator.SyncShardStatusTaskAttributes{
		SourceCluster: common.StringPtr("some random source cluster"),
		ShardId:       common.Int64Ptr(2333),
		Timestamp:     common.Int64Ptr(time.Now().Add(-2 * dropSyncShardTaskTimeThreshold).UnixNano()),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                      replicator.ReplicationTaskTypeSyncShardStatus.Ptr(),
		SyncShardStatusTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncShard_FailedThenSuccess() {
	replicationAttr := &replicator.SyncShardStatusTaskAttributes{
		SourceCluster: common.StringPtr("some random source cluster"),
		ShardId:       common.Int64Ptr(2333),
		Timestamp:     common.Int64Ptr(time.Now().UnixNano()),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                      replicator.ReplicationTaskTypeSyncShardStatus.Ptr(),
		SyncShardStatusTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockHistoryClient.EXPECT().SyncShardStatus(gomock.Any(), gomock.Any()).Return(errors.New("some random error")).Times(1)
	s.mockHistoryClient.EXPECT().SyncShardStatus(
		gomock.Any(),
		&h.SyncShardStatusRequest{
			SourceCluster: replicationAttr.SourceCluster,
			ShardId:       replicationAttr.ShardId,
			Timestamp:     replicationAttr.Timestamp,
		},
	).Return(nil).Times(1)
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_Success() {
	replicationAttr := &replicator.SyncActivityTaskAttributes{
		DomainId:          common.StringPtr("some random domain ID"),
		WorkflowId:        common.StringPtr("some random workflow ID"),
		RunId:             common.StringPtr("some random run ID"),
		Version:           common.Int64Ptr(1234),
		ScheduledId:       common.Int64Ptr(1235),
		ScheduledTime:     common.Int64Ptr(time.Now().UnixNano()),
		StartedId:         common.Int64Ptr(1236),
		StartedTime:       common.Int64Ptr(time.Now().UnixNano()),
		LastHeartbeatTime: common.Int64Ptr(time.Now().UnixNano()),
		Details:           []byte("some random details"),
		Attempt:           common.Int32Ptr(1048576),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                   replicator.ReplicationTaskTypeSyncActivity.Ptr(),
		SyncActivityTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_FailedThenSuccess() {
	replicationAttr := &replicator.SyncActivityTaskAttributes{
		DomainId:          common.StringPtr("some random domain ID"),
		WorkflowId:        common.StringPtr("some random workflow ID"),
		RunId:             common.StringPtr("some random run ID"),
		Version:           common.Int64Ptr(1234),
		ScheduledId:       common.Int64Ptr(1235),
		ScheduledTime:     common.Int64Ptr(time.Now().UnixNano()),
		StartedId:         common.Int64Ptr(1236),
		StartedTime:       common.Int64Ptr(time.Now().UnixNano()),
		LastHeartbeatTime: common.Int64Ptr(time.Now().UnixNano()),
		Details:           []byte("some random details"),
		Attempt:           common.Int32Ptr(1048576),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                   replicator.ReplicationTaskTypeSyncActivity.Ptr(),
		SyncActivityTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(errors.New("some random error")).Once()
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_History_Success() {
	replicationAttr := &replicator.HistoryTaskAttributes{
		TargetClusters:  []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:        common.StringPtr("some random domain ID"),
		WorkflowId:      common.StringPtr("some random workflow ID"),
		RunId:           common.StringPtr("some random run ID"),
		Version:         common.Int64Ptr(1394),
		FirstEventId:    common.Int64Ptr(728),
		NextEventId:     common.Int64Ptr(1015),
		ReplicationInfo: map[string]*shared.ReplicationInfo{},
		History: &shared.History{
			Events: []*shared.HistoryEvent{&shared.HistoryEvent{EventId: common.Int64Ptr(1)}},
		},
		NewRunHistory: nil,
		ResetWorkflow: common.BoolPtr(true),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:              replicator.ReplicationTaskTypeHistory.Ptr(),
		HistoryTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_History_FailedThenSuccess() {
	replicationAttr := &replicator.HistoryTaskAttributes{
		TargetClusters:  []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:        common.StringPtr("some random domain ID"),
		WorkflowId:      common.StringPtr("some random workflow ID"),
		RunId:           common.StringPtr("some random run ID"),
		Version:         common.Int64Ptr(1394),
		FirstEventId:    common.Int64Ptr(728),
		NextEventId:     common.Int64Ptr(1015),
		ReplicationInfo: map[string]*shared.ReplicationInfo{},
		History: &shared.History{
			Events: []*shared.HistoryEvent{&shared.HistoryEvent{EventId: common.Int64Ptr(1)}},
		},
		NewRunHistory: nil,
		ResetWorkflow: common.BoolPtr(true),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:              replicator.ReplicationTaskTypeHistory.Ptr(),
		HistoryTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(errors.New("some random error")).Once()
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_HistoryMetadata_Success() {
	replicationAttr := &replicator.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       common.StringPtr("some random domain ID"),
		WorkflowId:     common.StringPtr("some random workflow ID"),
		RunId:          common.StringPtr("some random run ID"),
		FirstEventId:   common.Int64Ptr(728),
		NextEventId:    common.Int64Ptr(1015),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                      replicator.ReplicationTaskTypeHistoryMetadata.Ptr(),
		HistoryMetadataTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_HistoryMetadata_FailedThenSuccess() {
	replicationAttr := &replicator.HistoryMetadataTaskAttributes{
		TargetClusters: []string{cluster.TestCurrentClusterName, cluster.TestAlternativeClusterName},
		DomainId:       common.StringPtr("some random domain ID"),
		WorkflowId:     common.StringPtr("some random workflow ID"),
		RunId:          common.StringPtr("some random run ID"),
		FirstEventId:   common.Int64Ptr(728),
		NextEventId:    common.Int64Ptr(1015),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:                      replicator.ReplicationTaskTypeHistoryMetadata.Ptr(),
		HistoryMetadataTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(errors.New("some random error")).Once()
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}
