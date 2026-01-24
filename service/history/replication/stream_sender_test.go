package replication

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	streamSenderSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		server        *historyservicemock.MockHistoryService_StreamWorkflowReplicationMessagesServer
		shardContext  *historyi.MockShardContext
		historyEngine *historyi.MockEngine
		taskConverter *MockSourceTaskConverter

		clientShardKey ClusterShardKey
		serverShardKey ClusterShardKey

		streamSender         *StreamSenderImpl
		senderFlowController *MockSenderFlowController
		config               *configs.Config
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
	s.server.EXPECT().Context().Return(context.Background()).AnyTimes()
	s.shardContext = historyi.NewMockShardContext(s.controller)
	s.historyEngine = historyi.NewMockEngine(s.controller)
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
		quotas.NoopRequestRateLimiter,
		s.taskConverter,
		"target_cluster",
		2,
		s.clientShardKey,
		s.serverShardKey,
		s.config,
	)
	s.senderFlowController = NewMockSenderFlowController(s.controller)
	s.streamSender.flowController = s.senderFlowController
}

func (s *streamSenderSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_SingleStack_Success() {
	s.streamSender.isTieredStackEnabled = false
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	replicationState := replicationspb.SyncReplicationState_builder{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}.Build()

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		persistencespb.QueueReaderState_builder{
			Scopes: []*persistencespb.QueueSliceScope{persistencespb.QueueSliceScope_builder{
				Range: persistencespb.QueueSliceRange_builder{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.GetInclusiveLowWatermark()),
					),
					ExclusiveMax: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(math.MaxInt64),
					),
				}.Build(),
				Predicate: persistencespb.Predicate_builder{
					PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
					UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
				}.Build(),
			}.Build()},
		}.Build(),
	).Return(nil)
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(
		readerID,
		replicationState.GetInclusiveLowWatermark()-1,
		replicationState.GetInclusiveLowWatermarkTime().AsTime(),
	).Return(nil)

	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.NoError(err)
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_SingleStack_Error() {
	s.streamSender.isTieredStackEnabled = false
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	replicationState := replicationspb.SyncReplicationState_builder{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}.Build()

	var ownershipLost error
	if rand.Float64() < 0.5 {
		ownershipLost = &persistence.ShardOwnershipLostError{}
	} else {
		ownershipLost = serviceerrors.NewShardOwnershipLost("", "")
	}

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		persistencespb.QueueReaderState_builder{
			Scopes: []*persistencespb.QueueSliceScope{persistencespb.QueueSliceScope_builder{
				Range: persistencespb.QueueSliceRange_builder{
					InclusiveMin: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(replicationState.GetInclusiveLowWatermark()),
					),
					ExclusiveMax: shard.ConvertToPersistenceTaskKey(
						tasks.NewImmediateKey(math.MaxInt64),
					),
				}.Build(),
				Predicate: persistencespb.Predicate_builder{
					PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
					UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
				}.Build(),
			}.Build()},
		}.Build(),
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
	lowPriorityInclusiveWatermark := int64(1234)
	highPriorityInclusiveWatermark := lowPriorityInclusiveWatermark + 10

	timestamp := timestamppb.New(time.Unix(0, rand.Int63()))
	replicationState := replicationspb.SyncReplicationState_builder{
		InclusiveLowWatermark:     lowPriorityInclusiveWatermark,
		InclusiveLowWatermarkTime: timestamp,
		HighPriorityState: replicationspb.ReplicationState_builder{
			InclusiveLowWatermark:     highPriorityInclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		}.Build(),
		LowPriorityState: replicationspb.ReplicationState_builder{
			InclusiveLowWatermark:     lowPriorityInclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		}.Build(),
	}.Build()
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(replicationState).Return().Times(1)

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		persistencespb.QueueReaderState_builder{
			Scopes: []*persistencespb.QueueSliceScope{
				persistencespb.QueueSliceScope_builder{
					Range: persistencespb.QueueSliceRange_builder{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					}.Build(),
					Predicate: persistencespb.Predicate_builder{
						PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
						UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
					}.Build(),
				}.Build(),
				persistencespb.QueueSliceScope_builder{
					Range: persistencespb.QueueSliceRange_builder{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.GetHighPriorityState().GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					}.Build(),
					Predicate: persistencespb.Predicate_builder{
						PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
						UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
					}.Build(),
				}.Build(),
				persistencespb.QueueSliceScope_builder{
					Range: persistencespb.QueueSliceRange_builder{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.GetLowPriorityState().GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					}.Build(),
					Predicate: persistencespb.Predicate_builder{
						PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
						UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
					}.Build(),
				}.Build(),
			},
		}.Build(),
	).Return(nil)
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(
		readerID,
		replicationState.GetHighPriorityState().GetInclusiveLowWatermark()-1,
		replicationState.GetInclusiveLowWatermarkTime().AsTime(),
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
	replicationState := replicationspb.SyncReplicationState_builder{
		InclusiveLowWatermark:     inclusiveWatermark,
		InclusiveLowWatermarkTime: timestamp,
		HighPriorityState: replicationspb.ReplicationState_builder{
			InclusiveLowWatermark:     inclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		}.Build(),
		LowPriorityState: replicationspb.ReplicationState_builder{
			InclusiveLowWatermark:     inclusiveWatermark + 10,
			InclusiveLowWatermarkTime: timestamp,
		}.Build(),
	}.Build()

	var ownershipLost error
	if rand.Float64() < 0.5 {
		ownershipLost = &persistence.ShardOwnershipLostError{}
	} else {
		ownershipLost = serviceerrors.NewShardOwnershipLost("", "")
	}
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(replicationState).Return().Times(1)

	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(
		readerID,
		persistencespb.QueueReaderState_builder{
			Scopes: []*persistencespb.QueueSliceScope{
				persistencespb.QueueSliceScope_builder{
					Range: persistencespb.QueueSliceRange_builder{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					}.Build(),
					Predicate: persistencespb.Predicate_builder{
						PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
						UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
					}.Build(),
				}.Build(),
				persistencespb.QueueSliceScope_builder{
					Range: persistencespb.QueueSliceRange_builder{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.GetHighPriorityState().GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					}.Build(),
					Predicate: persistencespb.Predicate_builder{
						PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
						UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
					}.Build(),
				}.Build(),
				persistencespb.QueueSliceScope_builder{
					Range: persistencespb.QueueSliceRange_builder{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(replicationState.GetLowPriorityState().GetInclusiveLowWatermark()),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					}.Build(),
					Predicate: persistencespb.Predicate_builder{
						PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
						UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
					}.Build(),
				}.Build(),
			},
		}.Build(),
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
	).Return(persistencespb.QueueState_builder{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: persistencespb.QueueReaderState_builder{
				Scopes: []*persistencespb.QueueSliceScope{persistencespb.QueueSliceScope_builder{
					Range: persistencespb.QueueSliceRange_builder{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(beginInclusiveWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					}.Build(),
					Predicate: persistencespb.Predicate_builder{
						PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
						UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
					}.Build(),
				}.Build()},
			}.Build(),
		},
	}.Build(), true)
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
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
	).Return(persistencespb.QueueState_builder{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: persistencespb.QueueReaderState_builder{
				Scopes: []*persistencespb.QueueSliceScope{persistencespb.QueueSliceScope_builder{ // only has one scope
					Range: persistencespb.QueueSliceRange_builder{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(beginInclusiveWatermark),
						),
						ExclusiveMax: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(math.MaxInt64),
						),
					}.Build(),
					Predicate: persistencespb.Predicate_builder{
						PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
						UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
					}.Build(),
				}.Build()},
			}.Build(),
		},
	}.Build(), true).Times(2)
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
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
	).Return(persistencespb.QueueState_builder{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: persistencespb.QueueReaderState_builder{
				Scopes: []*persistencespb.QueueSliceScope{
					persistencespb.QueueSliceScope_builder{
						Range: persistencespb.QueueSliceRange_builder{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkLowPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						}.Build(),
						Predicate: persistencespb.Predicate_builder{
							PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
							UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
						}.Build(),
					}.Build(),
					persistencespb.QueueSliceScope_builder{
						Range: persistencespb.QueueSliceRange_builder{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkHighPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						}.Build(),
						Predicate: persistencespb.Predicate_builder{
							PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
							UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
						}.Build(),
					}.Build(),
					persistencespb.QueueSliceScope_builder{
						Range: persistencespb.QueueSliceRange_builder{
							InclusiveMin: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(beginInclusiveWatermarkLowPriority),
							),
							ExclusiveMax: shard.ConvertToPersistenceTaskKey(
								tasks.NewImmediateKey(math.MaxInt64),
							),
						}.Build(),
						Predicate: persistencespb.Predicate_builder{
							PredicateType:                enumsspb.PREDICATE_TYPE_UNIVERSAL,
							UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
						}.Build(),
					}.Build(),
				},
			}.Build(),
		},
	}.Build(), true).Times(2)
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
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
	).Return(persistencespb.QueueState_builder{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates:                 map[int64]*persistencespb.QueueReaderState{},
	}.Build(), true)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	)

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
	).Return(persistencespb.QueueState_builder{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates:                 map[int64]*persistencespb.QueueReaderState{},
	}.Build(), true).Times(2)
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(endExclusiveWatermark),
	).Times(2)

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
			s.Equal(watermark1, resp.GetMessages().GetExclusiveHighWatermark())
			s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
			return nil
		}),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(watermark2, resp.GetMessages().GetExclusiveHighWatermark())
			s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
	s.streamSender.isTieredStackEnabled = false
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
	item0.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item1.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item2.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item3.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item0.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	item1.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	item2.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	item3.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	task0 := replicationspb.ReplicationTask_builder{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}.Build()
	task2 := replicationspb.ReplicationTask_builder{
		SourceTaskId:   beginInclusiveWatermark + 2,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}.Build()

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2, item3}, nil, nil
		},
	)
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, persistencespb.NamespaceReplicationConfig_builder{
			Clusters: []string{"source_cluster", "target_cluster"},
		}.Build(), 100), nil).AnyTimes()
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("2")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, persistencespb.NamespaceReplicationConfig_builder{
			Clusters: []string{"source_cluster"},
		}.Build(), 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item0, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).Return(task0, nil)
	s.taskConverter.EXPECT().Convert(item1, s.clientShardKey.ClusterID, gomock.Any()).Times(0)
	s.taskConverter.EXPECT().Convert(item2, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).Return(task2, nil)
	s.taskConverter.EXPECT().Convert(item3, s.clientShardKey.ClusterID, gomock.Any()).Times(0)
	gomock.InOrder(
		s.server.EXPECT().Send(historyservice.StreamWorkflowReplicationMessagesResponse_builder{
			Messages: replicationspb.WorkflowReplicationMessages_builder{
				ReplicationTasks:           []*replicationspb.ReplicationTask{task0},
				ExclusiveHighWatermark:     task0.GetSourceTaskId() + 1,
				ExclusiveHighWatermarkTime: task0.GetVisibilityTime(),
			}.Build(),
		}.Build()).Return(nil),
		s.server.EXPECT().Send(historyservice.StreamWorkflowReplicationMessagesResponse_builder{
			Messages: replicationspb.WorkflowReplicationMessages_builder{
				ReplicationTasks:           []*replicationspb.ReplicationTask{task2},
				ExclusiveHighWatermark:     task2.GetSourceTaskId() + 1,
				ExclusiveHighWatermarkTime: task2.GetVisibilityTime(),
			}.Build(),
		}.Build()).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
			s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
	s.streamSender.isTieredStackEnabled = true
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
	task1 := replicationspb.ReplicationTask_builder{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_HIGH,
	}.Build()
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, persistencespb.NamespaceReplicationConfig_builder{
			Clusters: []string{"source_cluster", "target_cluster"},
		}.Build(), 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2}, nil, nil
		},
	)
	s.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_HIGH).Return(nil).Times(1)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item1, s.clientShardKey.ClusterID, item1.Priority).Return(task1, nil)

	gomock.InOrder(
		s.server.EXPECT().Send(historyservice.StreamWorkflowReplicationMessagesResponse_builder{
			Messages: replicationspb.WorkflowReplicationMessages_builder{
				ReplicationTasks:           []*replicationspb.ReplicationTask{task1},
				ExclusiveHighWatermark:     task1.GetSourceTaskId() + 1,
				ExclusiveHighWatermarkTime: task1.GetVisibilityTime(),
				Priority:                   enumsspb.TASK_PRIORITY_HIGH,
			}.Build(),
		}.Build()).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
			s.Equal(enumsspb.TASK_PRIORITY_HIGH, resp.GetMessages().GetPriority())
			s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
	s.streamSender.isTieredStackEnabled = true
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

	task0 := replicationspb.ReplicationTask_builder{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}.Build()
	task2 := replicationspb.ReplicationTask_builder{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
		Priority:       enumsspb.TASK_PRIORITY_LOW,
	}.Build()
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, persistencespb.NamespaceReplicationConfig_builder{
			Clusters: []string{"source_cluster", "target_cluster"},
		}.Build(), 100), nil).AnyTimes()
	mockRegistry.EXPECT().GetNamespaceName(namespace.ID("1")).Return(namespace.Name("test"), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0, item1, item2}, nil, nil
		},
	)
	s.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_LOW).Return(nil).Times(2)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item0, s.clientShardKey.ClusterID, item0.Priority).Return(task0, nil)
	s.taskConverter.EXPECT().Convert(item0, s.clientShardKey.ClusterID, item0.Priority).Return(task2, nil)

	gomock.InOrder(
		s.server.EXPECT().Send(historyservice.StreamWorkflowReplicationMessagesResponse_builder{
			Messages: replicationspb.WorkflowReplicationMessages_builder{
				ReplicationTasks:           []*replicationspb.ReplicationTask{task0},
				ExclusiveHighWatermark:     task0.GetSourceTaskId() + 1,
				ExclusiveHighWatermarkTime: task0.GetVisibilityTime(),
				Priority:                   enumsspb.TASK_PRIORITY_LOW,
			}.Build(),
		}.Build()).Return(nil),
		s.server.EXPECT().Send(historyservice.StreamWorkflowReplicationMessagesResponse_builder{
			Messages: replicationspb.WorkflowReplicationMessages_builder{
				ReplicationTasks:           []*replicationspb.ReplicationTask{task2},
				ExclusiveHighWatermark:     task2.GetSourceTaskId() + 1,
				ExclusiveHighWatermarkTime: task2.GetVisibilityTime(),
				Priority:                   enumsspb.TASK_PRIORITY_LOW,
			}.Build(),
		}.Build()).Return(nil),
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
			s.Equal(enumsspb.TASK_PRIORITY_LOW, resp.GetMessages().GetPriority())
			s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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
	s.historyEngine.EXPECT().SubscribeReplicationNotification("target_cluster").Do(func(_ string) {
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
		s.Equal(endExclusiveWatermark, resp.GetMessages().GetExclusiveHighWatermark())
		s.NotNil(resp.GetMessages().GetExclusiveHighWatermarkTime())
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

func (s *streamSenderSuite) TestLivenessMonitor() {
	s.streamSender.recvSignalChan <- struct{}{}
	livenessMonitor(
		s.streamSender.recvSignalChan,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetIntPropertyFn(1),
		s.streamSender.shutdownChan,
		s.streamSender.Stop,
		s.streamSender.logger,
	)
	s.False(s.streamSender.IsValid())
}
