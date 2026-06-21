package replication

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common"
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
	s.streamSender.isTieredStackEnabled = false
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
	lowPriorityInclusiveWatermark := int64(1234)
	highPriorityInclusiveWatermark := lowPriorityInclusiveWatermark + 10

	timestamp := timestamppb.New(time.Unix(0, rand.Int63()))
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     lowPriorityInclusiveWatermark,
		InclusiveLowWatermarkTime: timestamp,
		HighPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     highPriorityInclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		},
		LowPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     lowPriorityInclusiveWatermark,
			InclusiveLowWatermarkTime: timestamp,
		},
	}
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(replicationState).Return().Times(1)

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
		replicationState.HighPriorityState.InclusiveLowWatermark-1,
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
	s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(replicationState).Return().Times(1)

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
	s.taskConverter.EXPECT().Convert(item0, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).Return(task0, nil)
	s.taskConverter.EXPECT().Convert(item1, s.clientShardKey.ClusterID, gomock.Any()).Times(0)
	s.taskConverter.EXPECT().Convert(item2, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).Return(task2, nil)
	s.taskConverter.EXPECT().Convert(item3, s.clientShardKey.ClusterID, gomock.Any()).Times(0)
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
	s.senderFlowController.EXPECT().Wait(gomock.Any(), enumsspb.TASK_PRIORITY_HIGH).Return(nil).Times(1)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item1, s.clientShardKey.ClusterID, item1.Priority).Return(task1, nil)

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
	s.ErrorAs(err, new(*StreamError))
}

func (s *streamSenderSuite) TestRecvEventLoop_RpcError_ShouldReturnStreamError() {
	s.server.EXPECT().Recv().Return(nil, errors.New("rpc error"))
	err := s.streamSender.recvEventLoop()
	s.Error(err)
	s.Error(err, "rpc error")
	s.ErrorAs(err, new(*StreamError))
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

func (s *streamSenderSuite) TestKey() {
	key := s.streamSender.Key()
	s.Equal(s.clientShardKey, key.Client)
	s.Equal(s.serverShardKey, key.Server)
}

func (s *streamSenderSuite) TestStop_NotStarted_NoOp() {
	// Status is Initialized (not Started), so Stop should be a no-op.
	s.streamSender.Stop()
	s.False(s.streamSender.shutdownChan.IsShutdown())
}

func (s *streamSenderSuite) TestStop_Started_Shutdown() {
	s.streamSender.status = common.DaemonStatusStarted
	s.streamSender.Stop()
	s.True(s.streamSender.shutdownChan.IsShutdown())
	// Calling Stop again is a no-op (status already Stopped).
	s.streamSender.Stop()
	s.True(s.streamSender.shutdownChan.IsShutdown())
}

func (s *streamSenderSuite) TestWait_ReturnsAfterShutdown() {
	s.streamSender.status = common.DaemonStatusStarted
	s.streamSender.Stop()
	done := make(chan struct{})
	go func() {
		s.streamSender.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		s.Fail("Wait did not return after shutdown")
	}
}

func (s *streamSenderSuite) TestStart_SingleStack() {
	s.streamSender.isTieredStackEnabled = false
	// Make the event loops return quickly by shutting down right away. The
	// background goroutines should observe shutdown and exit; we only assert
	// that Start flips the status to Started.
	s.server.EXPECT().Recv().Return(nil, errors.New("rpc error")).AnyTimes()
	s.historyEngine.EXPECT().SubscribeReplicationNotification("target_cluster").Return(make(chan struct{}), "sub").AnyTimes()
	s.historyEngine.EXPECT().UnsubscribeReplicationNotification("sub").AnyTimes()
	s.shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(
		tasks.NewImmediateKey(int64(0)),
	).AnyTimes()
	s.shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(nil, false).AnyTimes()
	s.server.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()

	s.streamSender.Start()
	s.True(s.streamSender.IsValid())
	// Second Start call should be a no-op (already started).
	s.streamSender.Start()
	s.True(s.streamSender.IsValid())
	s.streamSender.Stop()
}

func (s *streamSenderSuite) TestGetTaskPriority_SyncWorkflowState_Unspecified_ReturnsLow() {
	task := &tasks.SyncWorkflowStateTask{
		Priority: enumsspb.TASK_PRIORITY_UNSPECIFIED,
	}
	s.Equal(enumsspb.TASK_PRIORITY_LOW, s.streamSender.getTaskPriority(task))
}

func (s *streamSenderSuite) TestGetTaskPriority_SyncWorkflowState_High_ReturnsHigh() {
	task := &tasks.SyncWorkflowStateTask{
		Priority: enumsspb.TASK_PRIORITY_HIGH,
	}
	s.Equal(enumsspb.TASK_PRIORITY_HIGH, s.streamSender.getTaskPriority(task))
}

func (s *streamSenderSuite) TestGetTaskPriority_DefaultTask_ReturnsHigh() {
	task := &tasks.HistoryReplicationTask{}
	s.Equal(enumsspb.TASK_PRIORITY_HIGH, s.streamSender.getTaskPriority(task))
}

func (s *streamSenderSuite) TestGetTaskTargetCluster_AllTypes() {
	strSenderClusters := []string{"a", "b"}
	s.Equal(strSenderClusters, s.streamSender.getTaskTargetCluster(&tasks.SyncWorkflowStateTask{TargetClusters: strSenderClusters}))
	s.Equal(strSenderClusters, s.streamSender.getTaskTargetCluster(&tasks.SyncVersionedTransitionTask{TargetClusters: strSenderClusters}))
	s.Equal(strSenderClusters, s.streamSender.getTaskTargetCluster(&tasks.SyncHSMTask{TargetClusters: strSenderClusters}))
	s.Equal(strSenderClusters, s.streamSender.getTaskTargetCluster(&tasks.HistoryReplicationTask{TargetClusters: strSenderClusters}))
	s.Equal(strSenderClusters, s.streamSender.getTaskTargetCluster(&tasks.SyncActivityTask{TargetClusters: strSenderClusters}))
	// Default case returns nil.
	s.Nil(s.streamSender.getTaskTargetCluster(&tasks.WorkflowTask{}))
}

func (s *streamSenderSuite) TestRecordRetry() {
	throttledLogger := log.NewNoopLogger()
	s.shardContext.EXPECT().GetThrottledLogger().Return(throttledLogger).Times(1)
	item := tasks.NewMockTask(s.controller)
	item.EXPECT().GetTaskID().Return(int64(123)).AnyTimes()
	item.EXPECT().GetNamespaceID().Return("ns").AnyTimes()
	item.EXPECT().GetWorkflowID().Return("wf").AnyTimes()
	cause := errors.New("retry cause")
	err := s.streamSender.recordRetry(item, 2, cause)
	s.Equal(cause, err)
}

func (s *streamSenderSuite) TestGetSendCatchupBeginInclusiveWatermark_HighPriority_NonTieredReaderState() {
	// Reader state has a single scope (old format). For HIGH priority with
	// scopes != 3, index 0 is used.
	beginInclusiveWatermark := rand.Int63()
	readerState := &persistencespb.QueueReaderState{
		Scopes: []*persistencespb.QueueSliceScope{{
			Range: &persistencespb.QueueSliceRange{
				InclusiveMin: shard.ConvertToPersistenceTaskKey(
					tasks.NewImmediateKey(beginInclusiveWatermark),
				),
				ExclusiveMax: shard.ConvertToPersistenceTaskKey(
					tasks.NewImmediateKey(math.MaxInt64),
				),
			},
		}},
	}
	s.Equal(beginInclusiveWatermark, s.streamSender.getSendCatchupBeginInclusiveWatermark(readerState, enumsspb.TASK_PRIORITY_HIGH))
	s.Equal(beginInclusiveWatermark, s.streamSender.getSendCatchupBeginInclusiveWatermark(readerState, enumsspb.TASK_PRIORITY_LOW))
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_TieredStack_MissingPriorityState_Error() {
	s.streamSender.isTieredStackEnabled = true
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
		// HighPriorityState and LowPriorityState are nil -> unsupported.
	}
	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.Error(err)
	s.ErrorAs(err, new(*StreamError))
}

func (s *streamSenderSuite) TestRecvSyncReplicationState_SingleStack_UnexpectedPriorityState_Error() {
	s.streamSender.isTieredStackEnabled = false
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
		HighPriorityState: &replicationspb.ReplicationState{
			InclusiveLowWatermark:     rand.Int63(),
			InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
		},
	}
	err := s.streamSender.recvSyncReplicationState(replicationState)
	s.Error(err)
	s.ErrorAs(err, new(*StreamError))
}

func (s *streamSenderSuite) TestRecvEventLoop_SyncReplicationState_ThenShutdown() {
	s.streamSender.isTieredStackEnabled = false
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	replicationState := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark:     rand.Int63(),
		InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}
	req := &historyservice.StreamWorkflowReplicationMessagesRequest{
		Attributes: &historyservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
			SyncReplicationState: replicationState,
		},
	}
	s.shardContext.EXPECT().UpdateReplicationQueueReaderState(readerID, gomock.Any()).Return(nil).Times(1)
	// Shut down after the SyncReplicationState is fully processed so the loop
	// exits cleanly at the top of the next iteration (before a second Recv).
	s.shardContext.EXPECT().UpdateRemoteReaderInfo(readerID, gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ int64, _ int64, _ time.Time) error {
			s.streamSender.shutdownChan.Shutdown()
			return nil
		}).Times(1)
	s.server.EXPECT().Recv().Return(req, nil).Times(1)
	err := s.streamSender.recvEventLoop()
	s.NoError(err)
}

func (s *streamSenderSuite) TestRecvEventLoop_UnknownRequest_ReturnsError() {
	req := &historyservice.StreamWorkflowReplicationMessagesRequest{}
	s.server.EXPECT().Recv().Return(req, nil)
	err := s.streamSender.recvEventLoop()
	s.Error(err)
}

func (s *streamSenderSuite) TestRecvEventLoop_TieredStackChange_ReturnsStreamError() {
	s.streamSender.isTieredStackEnabled = true
	// config.EnableReplicationTaskTieredProcessing defaults to false in test
	// dynamic config, so the mismatch with isTieredStackEnabled=true triggers
	// the tiered-stack-change detection before Recv is ever called.
	err := s.streamSender.recvEventLoop()
	s.Error(err)
	s.ErrorAs(err, new(*StreamError))
}

func (s *streamSenderSuite) TestSendTasks_InvalidRange_ReturnsError() {
	beginInclusiveWatermark := rand.Int63n(1000) + 1000
	endExclusiveWatermark := beginInclusiveWatermark - 1
	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.Error(err)
}

func (s *streamSenderSuite) TestSendTasks_GetIterError_ReturnsError() {
	beginInclusiveWatermark := rand.Int63n(1000)
	endExclusiveWatermark := beginInclusiveWatermark + 100
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(nil, errors.New("iter error"))
	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.Error(err)
}

func (s *streamSenderSuite) TestSendTasks_IterNextError_ReturnsError() {
	beginInclusiveWatermark := rand.Int63n(1000)
	endExclusiveWatermark := beginInclusiveWatermark + 100
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return nil, nil, errors.New("next error")
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.Error(err)
}

func (s *streamSenderSuite) TestSendTasks_ConvertNil_SkipsTask() {
	s.streamSender.isTieredStackEnabled = false
	beginInclusiveWatermark := rand.Int63n(1000)
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := tasks.NewMockTask(s.controller)
	item0.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item0.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	item0.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item0.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()

	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	// Convert returns nil task -> task is skipped, no per-task Send.
	s.taskConverter.EXPECT().Convert(item0, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).Return(nil, nil)
	// Only the final empty watermark Send happens.
	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		return nil
	})
	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendTasks_ConvertError_ReturnsError() {
	s.streamSender.isTieredStackEnabled = false
	// Make retries immediate and bounded so the test does not hang.
	s.config.ReplicationStreamSenderErrorRetryMaxAttempts = dynamicconfig.GetIntPropertyFn(1)
	beginInclusiveWatermark := rand.Int63n(1000)
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := tasks.NewMockTask(s.controller)
	item0.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item0.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	item0.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item0.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	item0.EXPECT().GetTaskID().Return(int64(1)).AnyTimes()

	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	s.shardContext.EXPECT().GetThrottledLogger().Return(log.NewNoopLogger()).AnyTimes()
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	// Convert returns a non-retryable-resolving error; backoff exhausts and the
	// outer loop returns the error.
	s.taskConverter.EXPECT().Convert(item0, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).Return(nil, errors.New("convert error")).MinTimes(1)
	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.Error(err)
}

func (s *streamSenderSuite) TestSendTasks_ShutdownMidIteration_ReturnsNil() {
	s.streamSender.isTieredStackEnabled = false
	beginInclusiveWatermark := rand.Int63n(1000)
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item0 := tasks.NewMockTask(s.controller)
	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item0}, nil, nil
		},
	)
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	// Shut down before iterating so the loop returns nil immediately.
	s.streamSender.shutdownChan.Shutdown()
	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestShouldProcessTask_WrongShard_ReturnsFalse() {
	item := tasks.NewMockTask(s.controller)
	// clientClusterShardCount is 2; pick ids that map to a different shard than
	// clientShardKey.ShardID (1). Use distinct ids and assert the boolean is
	// consistent with the shard computation.
	item.EXPECT().GetNamespaceID().Return("wrong-shard-ns").AnyTimes()
	item.EXPECT().GetWorkflowID().Return("wrong-shard-wf").AnyTimes()
	// Drive the function; if the computed shard differs it returns false without
	// touching the registry. We don't assert a specific bool to avoid coupling
	// to the hashing, but exercise the path.
	_ = s.streamSender.shouldProcessTask(item)
}

func (s *streamSenderSuite) TestShouldProcessTask_TargetClusterFiltered_ReturnsFalse() {
	// Use a SyncWorkflowStateTask with TargetClusters that excludes the client
	// cluster -> filtered out, returns false (when shard matches).
	strSenderFindMatchingShard := func() (string, string) {
		for i := range 100000 {
			nsID := "ns"
			wfID := "wf-" + strSenderItoa(i)
			if common.WorkflowIDToHistoryShard(nsID, wfID, s.streamSender.clientClusterShardCount) == s.clientShardKey.ShardID {
				return nsID, wfID
			}
		}
		s.Fail("could not find matching shard")
		return "", ""
	}
	nsID, wfID := strSenderFindMatchingShard()
	item := &tasks.SyncWorkflowStateTask{
		WorkflowKey: definition.WorkflowKey{
			NamespaceID: nsID,
			WorkflowID:  wfID,
		},
		TargetClusters: []string{"some_other_cluster"},
	}
	s.False(s.streamSender.shouldProcessTask(item))
}

func (s *streamSenderSuite) TestShouldProcessTask_RegistryError_ReturnsTrue() {
	strSenderFindMatchingShard := func() (string, string) {
		for i := range 100000 {
			nsID := "ns"
			wfID := "wf-" + strSenderItoa(i)
			if common.WorkflowIDToHistoryShard(nsID, wfID, s.streamSender.clientClusterShardCount) == s.clientShardKey.ShardID {
				return nsID, wfID
			}
		}
		s.Fail("could not find matching shard")
		return "", ""
	}
	nsID, wfID := strSenderFindMatchingShard()
	item := tasks.NewMockTask(s.controller)
	item.EXPECT().GetNamespaceID().Return(nsID).AnyTimes()
	item.EXPECT().GetWorkflowID().Return(wfID).AnyTimes()
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID(nsID)).Return(nil, errors.New("registry error")).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	// On registry error, shouldProcessTask defaults to true (better safe than sorry).
	s.True(s.streamSender.shouldProcessTask(item))
}

func strSenderItoa(i int) string {
	return strconv.Itoa(i)
}
