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

package replication

import (
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/tests"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

type (
	streamSenderSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		server        *historyservicemock.MockHistoryService_StreamWorkflowReplicationMessagesServer
		shardContext  *shard.MockContext
		historyEngine *shard.MockEngine
		taskConverter *MockSourceTaskConverter

		clientShardKey ClusterShardKey
		serverShardKey ClusterShardKey

		streamSender *StreamSenderImpl
		config       *configs.Config
	}
)

func TestStreamSenderSuite(t *testing.T) {
	s := new(streamSenderSuite)
	suite.Run(t, s)
}

func (s *streamSenderSuite) SetupSuite() {
}

func (s *streamSenderSuite) TearDownSuite() {
}

func (s *streamSenderSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.server = historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesServer(s.controller)
	s.shardContext = shard.NewMockContext(s.controller)
	s.historyEngine = shard.NewMockEngine(s.controller)
	s.taskConverter = NewMockSourceTaskConverter(s.controller)
	s.config = tests.NewDynamicConfig()

	s.clientShardKey = NewClusterShardKey(rand.Int31(), 1)
	s.serverShardKey = NewClusterShardKey(rand.Int31(), 1)
	s.shardContext.EXPECT().GetEngine(gomock.Any()).Return(s.historyEngine, nil).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()

	s.streamSender = NewStreamSender(
		s.server,
		s.shardContext,
		s.historyEngine,
		s.taskConverter,
		"target_cluster",
		2,
		s.clientShardKey,
		s.serverShardKey,
		s.config,
	)
}

func (s *streamSenderSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_SingleStack_Success() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{{
				Range: &persistencespb.QueueSliceRange{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
					),
					ExclusiveMax: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(math.MaxInt64),
					),
				},
				Predicate: &persistencespb.Predicate{
					PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
					Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
				},
			}},
		},
	).Return(nil)
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(
		readerID,
		replicationState.InclusiveLowWatermark-1,
		replicationState.InclusiveLowWatermarkTime.AsTime(),
	).Return(nil)

	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.NoError(err)
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_SingleStack_Error() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}

	var ownershipLost error
	if rand.Float64() < 0.5 {
		ownershipLost = &persistence.ShardOwnershipLostError{}
	} else {
		ownershipLost = serviceerrors.NewShardOwnershipLost("", "")
	}

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{{
				Range: &persistencespb.QueueSliceRange{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
					),
					ExclusiveMax: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(math.MaxInt64),
					),
				},
				Predicate: &persistencespb.Predicate{
					PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
					Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
				},
			}},
		},
	).Return(ownershipLost)

	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.Error(err)
	s.Equal(ownershipLost, err)
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_TieredStack_Success() {
	s.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	inclusiveWatermark := int64(1234)
	timestamp := timestamppb.New(time.Unix(0, rand.Int63()))
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     inclusiveWatermark,
		InclusiveLowWatermarkTime: timestamp,
		HighPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     inclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		},
		LowPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     inclusiveWatermark + 10,
			InclusiveLowWatermarkTime: timestamp,
		},
	}

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.HighPriorityState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.LowPriorityState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
			},
		},
	).Return(nil)
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(
		readerID,
		replicationState.InclusiveLowWatermark-1,
		replicationState.InclusiveLowWatermarkTime.AsTime(),
	).Return(nil)

	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.NoError(err)
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_TieredStack_Error() {
	s.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	inclusiveWatermark := int64(1234)
	timestamp := timestamppb.New(time.Unix(0, rand.Int63()))
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     inclusiveWatermark,
		InclusiveLowWatermarkTime: timestamp,
		HighPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     inclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		},
		LowPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     inclusiveWatermark + 10,
			InclusiveLowWatermarkTime: timestamp,
		},
	}

	var ownershipLost error
	if rand.Float64() < 0.5 {
		ownershipLost = &persistence.ShardOwnershipLostError{}
	} else {
		ownershipLost = serviceerrors.NewShardOwnershipLost("", "")
	}

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		&persistencespb.QueueReaderState{
			Scopes: []*persistencespb.QueueSliceScope{
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.HighPriorityState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
				{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.LowPriorityState.InclusiveLowWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				},
			},
		},
	).Return(ownershipLost)

	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.Error(err)
	s.Equal(ownershipLost, err)
}

func (s *streamSenderSuite) TestSendCatchUp_SingleStack() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 1
	s.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(beginInclusiveWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			},
		},
	}, true)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	)

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	taskID, err := s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_UNSPECIFIED)
	s.NoError(err)
	s.Equal(endExclusiveWatermark, taskID)
}

func (s *streamSenderSuite) TestSendCatchUp_TieredStack_SingleReaderScope() {
	s.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 1
	s.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: {
				Scopes: []*persistencespb.QueueSliceScope{{ // only has one scope
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(beginInclusiveWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					},
					Predicate: &persistencespb.Predicate{
						PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
						Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
					},
				}},
			},
		},
	}, true).Times(2)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	).Times(2)

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil).Times(2)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	}).Times(2)

	highPriorityCatchupTaskID, highPriorityCatchupErr := s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_HIGH)
	lowPriorityCatchupTaskID, lowPriorityCatchupErr := s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_LOW)
	s.NoError(highPriorityCatchupErr)
	s.Equal(endExclusiveWatermark, highPriorityCatchupTaskID)
	s.NoError(lowPriorityCatchupErr)
	s.Equal(endExclusiveWatermark, lowPriorityCatchupTaskID)
}

func (s *streamSenderSuite) TestSendCatchUp_TieredStack_TieredReaderScope() {
	s.streamSender.isTieredStackEnabled = true
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	beginInclusiveWatermarkHighPriority := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermarkHighPriority + 1
	beginInclusiveWatermarkLowPriority := beginInclusiveWatermarkHighPriority - 100
	s.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: {
				Scopes: []*persistencespb.QueueSliceScope{
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkLowPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
							Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
						},
					},
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkHighPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
							Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
						},
					},
					{
						Range: &persistencespb.QueueSliceRange{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkLowPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						},
						Predicate: &persistencespb.Predicate{
							PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
							Attributes:    &persistencespb.Predicate_UniversalPredicateAttributes{},
						},
					},
				},
			},
		},
	}, true).Times(2)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	).Times(2)

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)

	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermarkHighPriority,
		endExclusiveWatermark,
	).Return(iter, nil).Times(1)

	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermarkLowPriority,
		endExclusiveWatermark,
	).Return(iter, nil).Times(1)

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	}).Times(2)

	lowPriorityCatchupTaskID, lowPriorityCatchupErr := s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_LOW)
	highPriorityCatchupTaskID, highPriorityCatchupErr := s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_HIGH)
	s.NoError(highPriorityCatchupErr)
	s.Equal(endExclusiveWatermark, highPriorityCatchupTaskID)
	s.NoError(lowPriorityCatchupErr)
	s.Equal(endExclusiveWatermark, lowPriorityCatchupTaskID)
}

func (s *streamSenderSuite) TestSendCatchUp_SingleStack_NoReaderState() {
	endExclusiveWatermark := int64(1234)
	s.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates:                 map[int64]*persistencespb.QueueReaderState{},
	}, true)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	)

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	taskID, err := s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_UNSPECIFIED)
	s.NoError(err)
	s.Equal(endExclusiveWatermark, taskID)
}

func (s *streamSenderSuite) TestSendCatchUp_TieredStack_NoReaderState() {
	s.streamSender.isTieredStackEnabled = true
	endExclusiveWatermark := int64(1234)
	s.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates:                 map[int64]*persistencespb.QueueReaderState{},
	}, true).Times(2)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	).Times(2)

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	}).Times(2)

	taskID, err := s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_HIGH)
	s.NoError(err)
	s.Equal(endExclusiveWatermark, taskID)
	taskID, err = s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_LOW)
	s.NoError(err)
	s.Equal(endExclusiveWatermark, taskID)
}

func (s *streamSenderSuite) TestSendCatchUp_SingleStack_NoQueueState() {
	endExclusiveWatermark := int64(1234)
	s.shardContext.EXPECT().GetQueueState(
		tasks.CategoryReplication,
	).Return(nil, false)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	)

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	taskID, err := s.streamSender.sendCatchUp(enumsspb.TASK_PRIORITY_UNSPECIFIED)
	s.NoError(err)
	s.Equal(endExclusiveWatermark, taskID)
}

func (s *streamSenderSuite) TestSendLive() {
	channel := make(chan struct{})
	watermark0 := rand.Int63()
	watermark1 := watermark0 + 1 + rand.Int63n(100)
	watermark2 := watermark1 + 1 + rand.Int63n(100)

	gomock.InOrder(
		s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
			tasks.NewImmediateKey(watermark1),
		),
		s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
			tasks.NewImmediateKey(watermark2),
		),
	)
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	gomock.InOrder(
		s.historyEngine.EXPECT().GetReplicationTasksIter(
			gomock.Any(),
			string(s.clientShardKey.ClusterID),
			watermark0,
			watermark1,
		).Return(iter, nil),
		s.historyEngine.EXPECT().GetReplicationTasksIter(
			gomock.Any(),
			string(s.clientShardKey.ClusterID),
			watermark1,
			watermark2,
		).Return(iter, nil),
	)
	gomock.InOrder(
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(watermark1, resp.GetMessages().ExclusiveHighWatermark)
			s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(watermark2, resp.GetMessages().ExclusiveHighWatermark)
			s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)
	go func() {
		channel <- struct{}{}
		channel <- struct{}{}
		s.streamSender.shutdownChan.Shutdown()
	}()
	err := s.streamSender.sendLive(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		channel,
		watermark0,
	)
	s.Nil(err)
	s.True(!s.streamSender.IsValid())
}

func (s *streamSenderSuite) TestSendTasks_Noop() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendTasks_WithoutTasks() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return nil
	})

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendTasks_WithTasks() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := tasks.NewMockTask(s.controller)
	item1 := tasks.NewMockTask(s.controller)
	item2 := tasks.NewMockTask(s.controller)
	item3 := tasks.NewMockTask(s.controller)
	item0.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item1.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item2.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item3.EXPECT().GetNamespaceID().Return("2").AnyTimes()
	item0.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	item1.EXPECT().GetWorkflowID().Return("3").AnyTimes()
	item2.EXPECT().GetWorkflowID().Return("2").AnyTimes()
	item3.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	task0 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}
	task2 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark + 2,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2, item3}, nil, nil
		},
	)
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("2")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster"},
		}, 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item0).Return(task0, nil)
	s.taskConverter.EXPECT().Convert(item1).Times(0)
	s.taskConverter.EXPECT().Convert(item2).Return(task2, nil)
	s.taskConverter.EXPECT().Convert(item3).Times(0)
	gomock.InOrder(
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task0},
					ExclusiveHighWatermark:     task0.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task0.VisibilityTime,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task2},
					ExclusiveHighWatermark:     task2.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task2.VisibilityTime,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
			s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendTasks_TieredStack_HighPriority() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_LOW,
	}

	item1 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_HIGH,
	}
	item2 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_LOW,
	}
	task1 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item1).Return(task1, nil)

	gomock.InOrder(
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task1},
					ExclusiveHighWatermark:     task1.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task1.VisibilityTime,
					Priority:                   enumsspb.TASK_PRIORITY_HIGH,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
			s.Equal(enumsspb.TASK_PRIORITY_HIGH, resp.GetMessages().Priority)
			s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_HIGH,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendTasks_TieredStack_LowPriority() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_LOW,
	}
	item1 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_HIGH,
	}
	item2 := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: "1",
			WorkflowID:  "1",
		},
		Priority: enumsspb.TASK_PRIORITY_LOW,
	}

	task0 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}
	task2 := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item0).Return(task0, nil)
	s.taskConverter.EXPECT().Convert(item0).Return(task2, nil)

	gomock.InOrder(
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task0},
					ExclusiveHighWatermark:     task0.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task0.VisibilityTime,
					Priority:                   enumsspb.TASK_PRIORITY_LOW,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{task2},
					ExclusiveHighWatermark:     task2.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: task2.VisibilityTime,
					Priority:                   enumsspb.TASK_PRIORITY_LOW,
				},
			},
		}).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
			s.Equal(enumsspb.TASK_PRIORITY_LOW, resp.GetMessages().Priority)
			s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
			return nil
		}),
	)

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_LOW,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendEventLoop_Panic_ShouldCaptureAsError() {
	s.historyEngine.EXPECT().SubscribeReplicationNotification().Do(func() {
		panic("panic")
	})
	err := s.streamSender.sendEventLoop(enumsspb.TASK_PRIORITY_UNSPECIFIED)
	s.Error(err) // panic is captured as error
}

func (s *streamSenderSuite) TestRecvEventLoop_Panic_ShouldCaptureAsError() {
	s.streamSender.shutdownChan = nil // mimic nil pointer panic
	err := s.streamSender.recvEventLoop()
	s.Error(err) // panic is captured as error
}

func (s *streamSenderSuite) TestSendEventLoop_StreamSendError_ShouldReturnStreamError() {
	beginInclusiveWatermark := rand.Int63()
	endExclusiveWatermark := beginInclusiveWatermark

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		s.NotNil(resp.GetMessages().ExclusiveHighWatermarkTime)
		return errors.New("rpc error")
	})

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.Error(err, "rpc error")
	s.IsType(&StreamError{}, err)
}

func (s *streamSenderSuite) TestRecvEventLoop_RpcError_ShouldReturnStreamError() {
	s.server.EXPECT().Recv().Return(nil, errors.New("rpc error"))
	err := s.streamSender.recvEventLoop()
	s.Error(err)
	s.Error(err, "rpc error")
	s.IsType(&StreamError{}, err)
}
