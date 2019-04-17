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
	"github.com/uber/cadence/common/log"
	"go.uber.org/zap"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/replicator"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/codec"
	messageMocks "github.com/uber/cadence/common/messaging/mocks"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"github.com/uber/cadence/common/task"
	"github.com/uber/cadence/common/xdc"
)

type (
	replicationTaskProcessorSuite struct {
		suite.Suite
		currentCluster string
		sourceCluster  string
		config         *Config
		logger         log.Logger
		metricsClient  metrics.Client
		msgEncoder     codec.BinaryEncoder

		mockMsg                     *messageMocks.Message
		mockDomainReplicator        *MockDomainReplicator
		mockHistoryClient           *mocks.HistoryClient
		mockRereplicator            *xdc.MockHistoryRereplicator
		mockSequentialTaskProcessor *task.MockSequentialTaskProcessor

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
	zapLogger, err := zap.NewDevelopment()
	s.Require().NoError(err)
	s.logger = log.NewLogger(zapLogger)
	s.config = &Config{
		ReplicatorTaskConcurrency: dynamicconfig.GetIntPropertyFn(10),
	}
	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.Worker)
	s.msgEncoder = codec.NewThriftRWEncoder()

	s.mockMsg = &messageMocks.Message{}
	s.mockMsg.On("Partition").Return(int32(0))
	s.mockMsg.On("Offset").Return(int64(0))
	s.mockDomainReplicator = &MockDomainReplicator{}
	s.mockHistoryClient = &mocks.HistoryClient{}
	s.mockRereplicator = &xdc.MockHistoryRereplicator{}
	s.mockSequentialTaskProcessor = &task.MockSequentialTaskProcessor{}

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
		s.mockHistoryClient,
		s.mockSequentialTaskProcessor,
	)
}

func (s *replicationTaskProcessorSuite) TearDownTest() {
	s.mockMsg.AssertExpectations(s.T())
	s.mockDomainReplicator.AssertExpectations(s.T())
	s.mockHistoryClient.AssertExpectations(s.T())
	s.mockRereplicator.AssertExpectations(s.T())
	s.mockSequentialTaskProcessor.AssertExpectations(s.T())

}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_BadEncoding() {
	s.mockMsg.On("Value").Return([]byte("some random bad encoded message"))
	s.mockMsg.On("Nack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Domain_Success() {
	replicationAttr := &replicator.DomainTaskAttributes{
		DomainOperation: replicator.DomainOperationUpdate.Ptr(),
		ID:              common.StringPtr("some random domain ID"),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
		DomainTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockDomainReplicator.On("HandleReceivingTask", replicationAttr).Return(nil).Once()
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_Domain_FailedThenSuccess() {
	replicationAttr := &replicator.DomainTaskAttributes{
		DomainOperation: replicator.DomainOperationUpdate.Ptr(),
		ID:              common.StringPtr("some random domain ID"),
	}
	replicationTask := &replicator.ReplicationTask{
		TaskType:             replicator.ReplicationTaskTypeDomain.Ptr(),
		DomainTaskAttributes: replicationAttr,
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
	s.mockHistoryClient.On(
		"SyncShardStatus",
		mock.Anything,
		&h.SyncShardStatusRequest{
			SourceCluster: replicationAttr.SourceCluster,
			ShardId:       replicationAttr.ShardId,
			Timestamp:     replicationAttr.Timestamp,
		},
	).Return(nil).Once()
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
	s.mockHistoryClient.On("SyncShardStatus", mock.Anything, mock.Anything).Return(errors.New("some random error")).Once()
	s.mockHistoryClient.On(
		"SyncShardStatus",
		mock.Anything,
		&h.SyncShardStatusRequest{
			SourceCluster: replicationAttr.SourceCluster,
			ShardId:       replicationAttr.ShardId,
			Timestamp:     replicationAttr.Timestamp,
		},
	).Return(nil).Once()
	s.mockMsg.On("Ack").Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_Success() {
	replicationAttr := &replicator.SyncActicvityTaskAttributes{
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
		TaskType:                    replicator.ReplicationTaskTypeSyncActivity.Ptr(),
		SyncActicvityTaskAttributes: replicationAttr,
	}
	replicationTaskBinary, err := s.msgEncoder.Encode(replicationTask)
	s.Nil(err)
	s.mockMsg.On("Value").Return(replicationTaskBinary)
	s.mockSequentialTaskProcessor.On("Submit", mock.Anything).Return(nil).Once()

	s.processor.decodeMsgAndSubmit(s.mockMsg)
}

func (s *replicationTaskProcessorSuite) TestDecodeMsgAndSubmit_SyncActivity_FailedThenSuccess() {
	replicationAttr := &replicator.SyncActicvityTaskAttributes{
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
		TaskType:                    replicator.ReplicationTaskTypeSyncActivity.Ptr(),
		SyncActicvityTaskAttributes: replicationAttr,
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
		NewRunHistory:           nil,
		EventStoreVersion:       common.Int32Ptr(144),
		NewRunEventStoreVersion: nil,
		ResetWorkflow:           common.BoolPtr(true),
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
		NewRunHistory:           nil,
		EventStoreVersion:       common.Int32Ptr(144),
		NewRunEventStoreVersion: nil,
		ResetWorkflow:           common.BoolPtr(true),
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
