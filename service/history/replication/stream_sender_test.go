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
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/wideevents"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
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
	s.shardContext.EXPECT().GetThrottledLogger().Return(log.NewNoopLogger()).AnyTimes()

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

// TestRecvSyncReplicationState_ReaderGroupEquivalence pins the PR's core claim: for
// every ack shape, the reader-group path persists exactly the QueueReaderState and
// failover watermark the legacy path does.
func (s *streamSenderSuite) TestRecvSyncReplicationState_ReaderGroupEquivalence() {
	readerID := shard.ReplicationReaderIDFromClusterShardID(
		int64(s.clientShardKey.ClusterID),
		s.clientShardKey.ShardID,
	)
	for _, tiered := range []bool{false, true} {
		attr := &replicationspb.SyncReplicationState{
			InclusiveLowWatermark:     rand.Int63(),
			InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
		}
		if tiered {
			attr.HighPriorityState = &replicationspb.ReplicationState{
				InclusiveLowWatermark:     rand.Int63(),
				InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
			}
			attr.LowPriorityState = &replicationspb.ReplicationState{
				InclusiveLowWatermark:     rand.Int63(),
				InclusiveLowWatermarkTime: timestamppb.New(time.Unix(0, rand.Int63())),
			}
		}

		run := func(group *replicationReaderGroup) (*persistencespb.QueueReaderState, int64) {
			s.streamSender.isTieredStackEnabled = tiered
			s.streamSender.readerGroup = group
			if tiered {
				s.senderFlowController.EXPECT().RefreshReceiverFlowControlInfo(attr)
			}
			var gotState *persistencespb.QueueReaderState
			var gotTaskID int64
			s.shardContext.EXPECT().UpdateReplicationQueueReaderState(readerID, gomock.Any()).DoAndReturn(
				func(_ int64, state *persistencespb.QueueReaderState) error {
					gotState = state
					return nil
				})
			s.shardContext.EXPECT().UpdateRemoteReaderInfo(readerID, gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ int64, taskID int64, _ time.Time) error {
					gotTaskID = taskID
					return nil
				})
			s.NoError(s.streamSender.recvSyncReplicationState(attr))
			return gotState, gotTaskID
		}

		legacyState, legacyTaskID := run(nil)
		groupState, groupTaskID := run(newReplicationReaderGroup(s.shardContext, s.clientShardKey, tiered, log.NewNoopLogger()))
		s.True(proto.Equal(legacyState, groupState),
			"tiered=%v: reader group persisted %v, legacy persisted %v", tiered, groupState, legacyState)
		s.Equal(legacyTaskID, groupTaskID)
	}
	s.streamSender.readerGroup = nil
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
	s.NoError(err)
	s.False(s.streamSender.IsValid())
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

// setupSingleFailingTask wires a single replication task whose conversion always fails with
// convertErr, and bounds retries to one fast attempt so the give-up path is reached quickly.
func (s *streamSenderSuite) setupSingleFailingTask(convertErr error) (beginInclusiveWatermark, endExclusiveWatermark int64) {
	s.streamSender.isTieredStackEnabled = false
	s.config.ReplicationStreamSenderErrorRetryMaxAttempts = func() int { return 1 }
	s.config.ReplicationStreamSenderErrorRetryWait = func() time.Duration { return time.Millisecond }

	beginInclusiveWatermark = rand.Int63n(math.MaxInt32)
	endExclusiveWatermark = beginInclusiveWatermark + 100
	item := tasks.NewMockTask(s.controller)
	item.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	item.EXPECT().GetRunID().Return("run-1").AnyTimes()
	item.EXPECT().GetTaskID().Return(beginInclusiveWatermark).AnyTimes()
	item.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item}, nil, nil
		},
	)
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	// Only exercised when EmitReplicationLifecycleEvents is on (the skipped wide event resolves the
	// namespace name and the source cluster); harmless for tests that keep lifecycle events off.
	mockRegistry.EXPECT().GetNamespaceName(namespace.ID("1")).Return(namespace.Name("test-ns"), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	mockClusterMetadata := cluster.NewMockMetadata(s.controller)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("source_cluster").AnyTimes()
	s.shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata).AnyTimes()
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).
		Return(nil, convertErr).MinTimes(1)
	return beginInclusiveWatermark, endExclusiveWatermark
}

func (s *streamSenderSuite) TestSendTasks_SkipStuckTask_Enabled() {
	s.config.ReplicationStreamSenderSkipStuckTask = func() bool { return true }
	// A retryable error whose retries are exhausted must be skipped, and the stuck task's
	// payload must not be sent. The trailing watermark still advances the receiver past it.
	beginInclusiveWatermark, endExclusiveWatermark := s.setupSingleFailingTask(errors.New("boom"))

	s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
		s.Empty(resp.GetMessages().ReplicationTasks)
		s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
		return nil
	}).Times(1)

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.NoError(err)
}

func (s *streamSenderSuite) TestSendTasks_SkipStuckTask_EmitsWideEvent() {
	s.config.ReplicationStreamSenderSkipStuckTask = func() bool { return true }
	// With lifecycle events on, skipping a stuck task must emit exactly one terminal "skipped"
	// ReplicationLifecycle event carrying the source_cluster/source_shard/source_task_id join key,
	// the target_cluster now missing the state, and the mapped task_type.
	s.config.EmitReplicationLifecycleEvents = func() bool { return true }
	capture := &eventCaptureLogger{}
	s.shardContext.EXPECT().GetEventLogger().Return(capture).AnyTimes()

	beginInclusiveWatermark, _ := s.setupSingleFailingTask(errors.New("boom"))
	s.server.EXPECT().Send(gomock.Any()).Return(nil).Times(1)

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		beginInclusiveWatermark+100,
	)
	s.NoError(err)

	s.Require().Len(capture.records, 1)
	rec := capture.records[0]
	s.Equal(wideevents.ReplicationLifecycleEventName, rec.EventName())
	fields := map[string]otellog.Value{}
	rec.WalkAttributes(func(kv otellog.KeyValue) bool {
		fields[kv.Key] = kv.Value
		return true
	})
	s.Equal(string(wideevents.ReplicationSkipped), fields["phase"].AsString())
	// item is a HISTORY task, mapped to the shared task_type vocabulary rather than the raw enum name.
	s.Equal(wideevents.ReplTaskHistory, fields["task_type"].AsString())
	s.Equal(beginInclusiveWatermark, fields["source_task_id"].AsInt64())
	s.Equal("source_cluster", fields["source_cluster"].AsString())
	s.Equal(int64(s.serverShardKey.ShardID), fields["source_shard"].AsInt64())
	s.Equal("convert: boom", fields["error"].AsString())
	s.Equal(s.streamSender.clientClusterName, fields["target_cluster"].AsString())
	s.Equal(enumsspb.TASK_PRIORITY_UNSPECIFIED.String(), fields["priority"].AsString())
}

func (s *streamSenderSuite) TestSendTasks_SkipStuckTask_Disabled() {
	s.config.ReplicationStreamSenderSkipStuckTask = func() bool { return false }
	// With the flag off, the sender preserves today's behavior: it fails so the stream restarts.
	beginInclusiveWatermark, endExclusiveWatermark := s.setupSingleFailingTask(errors.New("boom"))

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.Error(err)
}

func (s *streamSenderSuite) TestIsSkippable() {
	// A task that could not be built (convertError) with an otherwise-retryable cause is
	// skippable: its source info is corrupt/unusable, so retrying or reconnecting will not help.
	s.True(isSkippable(&convertError{err: errors.New("version histories does not contains given item")}))

	// A convert failure whose underlying cause is a non-retryable infra/teardown error must NOT be
	// skipped; the stream must tear down so shard handoff / reconnect can proceed.
	s.False(isSkippable(&convertError{err: NewStreamError("boom", nil)}))
	s.False(isSkippable(&convertError{err: context.Canceled}))
	s.False(isSkippable(&convertError{err: &persistence.ShardOwnershipLostError{}}))

	// Non-convert failures (transient send or rate-limit errors) are never skippable, even when
	// retryable: dropping a task that would have succeeded on reconnect is silent data loss.
	s.False(isSkippable(errors.New("boom")))
	s.False(isSkippable(NewStreamError("boom", nil)))
	s.False(isSkippable(nil))
}

func (s *streamSenderSuite) TestSendTasks_SkipStuckTask_NonRetryableNotSkipped() {
	s.config.ReplicationStreamSenderSkipStuckTask = func() bool { return true }
	// Even with the flag on, a convert failure caused by a non-retryable infra/teardown error must
	// NOT be skipped; the stream must tear down so shard handoff / reconnect can proceed. We use a
	// shard-ownership-lost error because it is a non-retryable failure convert can realistically
	// return (unlike StreamError, which only arises on the send path).
	beginInclusiveWatermark, endExclusiveWatermark := s.setupSingleFailingTask(&persistence.ShardOwnershipLostError{})

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.Error(err)
}

// TestSendTasks_SkipStuckTask_StreamKeepsFlowingPastSkip is the core guarantee: skipping an
// unbuildable task must not wedge the stream. A batch of [stuckItem, okItem] where stuckItem fails
// to convert (skipped) must still send okItem's payload and then advance the watermark to the end.
func (s *streamSenderSuite) TestSendTasks_SkipStuckTask_StreamKeepsFlowingPastSkip() {
	s.streamSender.isTieredStackEnabled = false
	s.config.ReplicationStreamSenderSkipStuckTask = func() bool { return true }
	s.config.ReplicationStreamSenderErrorRetryMaxAttempts = func() int { return 1 }
	s.config.ReplicationStreamSenderErrorRetryWait = func() time.Duration { return time.Millisecond }

	beginInclusiveWatermark := rand.Int63n(math.MaxInt32)
	endExclusiveWatermark := beginInclusiveWatermark + 100

	stuckItem := tasks.NewMockTask(s.controller)
	okItem := tasks.NewMockTask(s.controller)
	for _, it := range []*tasks.MockTask{stuckItem, okItem} {
		it.EXPECT().GetNamespaceID().Return("1").AnyTimes()
		it.EXPECT().GetRunID().Return("run").AnyTimes()
		it.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
		it.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	}
	// Workflows "1" and "2" both hash to this sender's shard, so okItem sits behind the skipped
	// stuckItem in the same batch and can only send if the stream kept flowing past the skip.
	stuckItem.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	stuckItem.EXPECT().GetTaskID().Return(beginInclusiveWatermark).AnyTimes()
	okItem.EXPECT().GetWorkflowID().Return("2").AnyTimes()
	okItem.EXPECT().GetTaskID().Return(beginInclusiveWatermark + 1).AnyTimes()

	okTask := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark + 1,
		VisibilityTime: timestamppb.New(time.Unix(0, rand.Int63())),
	}

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{stuckItem, okItem}, nil, nil
		},
	)
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	// Times(1) (not MinTimes): with MaxAttempts=1 the stuck convert is attempted exactly once, and
	// bounding it prevents gomock from greedily matching okItem's Convert call to this expectation
	// (mock tasks are reflect.DeepEqual-equal, so an unbounded matcher would swallow both calls).
	s.taskConverter.EXPECT().Convert(stuckItem, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).
		Return(nil, errors.New("boom")).Times(1)
	s.taskConverter.EXPECT().Convert(okItem, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).
		Return(okTask, nil).Times(1)

	gomock.InOrder(
		// okItem is sent even though it sits behind the skipped stuckItem in the same batch.
		s.server.EXPECT().Send(&historyservice.StreamWorkflowReplicationMessagesResponse{
			Attributes: &historyservice.StreamWorkflowReplicationMessagesResponse_Messages{
				Messages: &replicationspb.WorkflowReplicationMessages{
					ReplicationTasks:           []*replicationspb.ReplicationTask{okTask},
					ExclusiveHighWatermark:     okTask.SourceTaskId + 1,
					ExclusiveHighWatermarkTime: okTask.VisibilityTime,
				},
			},
		}).Return(nil),
		// trailing watermark advances the receiver to the end of the range.
		s.server.EXPECT().Send(gomock.Any()).DoAndReturn(func(resp *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			s.Empty(resp.GetMessages().ReplicationTasks)
			s.Equal(endExclusiveWatermark, resp.GetMessages().ExclusiveHighWatermark)
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

// TestSendTasks_SkipStuckTask_SendFailureNotSkipped confirms the skip is scoped to convert: a task
// that builds fine but fails to send (a StreamError, not a convertError) is NOT skipped even with
// the flag on, so the stream tears down rather than silently dropping a task that could be resent.
func (s *streamSenderSuite) TestSendTasks_SkipStuckTask_SendFailureNotSkipped() {
	s.streamSender.isTieredStackEnabled = false
	s.config.ReplicationStreamSenderSkipStuckTask = func() bool { return true }
	s.config.ReplicationStreamSenderErrorRetryMaxAttempts = func() int { return 1 }
	s.config.ReplicationStreamSenderErrorRetryWait = func() time.Duration { return time.Millisecond }

	beginInclusiveWatermark := rand.Int63n(math.MaxInt32)
	endExclusiveWatermark := beginInclusiveWatermark + 100
	item := tasks.NewMockTask(s.controller)
	item.EXPECT().GetNamespaceID().Return("1").AnyTimes()
	item.EXPECT().GetWorkflowID().Return("1").AnyTimes()
	item.EXPECT().GetRunID().Return("run-1").AnyTimes()
	item.EXPECT().GetTaskID().Return(beginInclusiveWatermark).AnyTimes()
	item.EXPECT().GetVisibilityTime().Return(time.Now().UTC()).AnyTimes()
	item.EXPECT().GetType().Return(enumsspb.TASK_TYPE_REPLICATION_HISTORY).AnyTimes()
	task := &replicationspb.ReplicationTask{
		SourceTaskId:   beginInclusiveWatermark,
		VisibilityTime: timestamppb.New(time.Now().UTC()),
	}

	iter := collection.NewPagingIterator[tasks.Task](
		func(paginationToken []byte) ([]tasks.Task, []byte, error) {
			return []tasks.Task{item}, nil, nil
		},
	)
	mockRegistry := namespace.NewMockRegistry(s.controller)
	mockRegistry.EXPECT().GetNamespaceByID(namespace.ID("1")).Return(namespace.NewGlobalNamespaceForTest(
		nil, nil, &persistencespb.NamespaceReplicationConfig{
			Clusters: []string{"source_cluster", "target_cluster"},
		}, 100), nil).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry).AnyTimes()
	s.historyEngine.EXPECT().GetReplicationTasksIter(
		gomock.Any(),
		string(s.clientShardKey.ClusterID),
		beginInclusiveWatermark,
		endExclusiveWatermark,
	).Return(iter, nil)
	s.taskConverter.EXPECT().Convert(item, s.clientShardKey.ClusterID, enumsspb.TASK_PRIORITY_UNSPECIFIED).
		Return(task, nil).MinTimes(1)
	// The send fails; sendToStream wraps it as a (non-retryable) StreamError, which is not a
	// convertError, so isSkippable is false and the task must not be skipped.
	s.server.EXPECT().Send(gomock.Any()).Return(errors.New("send boom")).MinTimes(1)

	err := s.streamSender.sendTasks(
		enumsspb.TASK_PRIORITY_UNSPECIFIED,
		beginInclusiveWatermark,
		endExclusiveWatermark,
	)
	s.Error(err)
}

// eventCaptureLogger is a minimal OTEL logger that records emitted wide-event records for
// assertions in tests that exercise the ReplicationLifecycle "skipped" event.
type eventCaptureLogger struct {
	embedded.Logger
	records []otellog.Record
}

func (c *eventCaptureLogger) Emit(_ context.Context, r otellog.Record) {
	c.records = append(c.records, r)
}
func (c *eventCaptureLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}
