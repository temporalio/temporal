package replication

import (
	"errors"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	taskProcessorManagerSuite struct {
		suite.Suite
		*require.Assertions

		controller                        *gomock.Controller
		mockShard                         *historyi.MockShardContext
		mockEngine                        *historyi.MockEngine
		mockClientBean                    *client.MockBean
		mockClusterMetadata               *cluster.MockMetadata
		mockHistoryClient                 *historyservicemock.MockHistoryServiceClient
		mockReplicationTaskExecutor       *MockTaskExecutor
		mockReplicationTaskFetcherFactory *MockTaskFetcherFactory

		mockExecutionManager *persistence.MockExecutionManager

		shardID     int32
		shardOwner  string
		config      *configs.Config
		requestChan chan *replicationTaskRequest

		taskProcessorManager *taskProcessorManagerImpl
	}
)

func TestTaskProcessorManagerSuite(t *testing.T) {
	s := new(taskProcessorManagerSuite)
	suite.Run(t, s)
}

func (s *taskProcessorManagerSuite) SetupSuite() {
}

func (s *taskProcessorManagerSuite) TearDownSuite() {
}

func (s *taskProcessorManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.config = tests.NewDynamicConfig()
	s.requestChan = make(chan *replicationTaskRequest, 10)

	s.shardID = rand.Int31()
	s.shardOwner = "test-shard-owner"
	s.mockShard = historyi.NewMockShardContext(s.controller)
	s.mockEngine = historyi.NewMockEngine(s.controller)
	s.mockClientBean = client.NewMockBean(s.controller)

	s.mockReplicationTaskExecutor = NewMockTaskExecutor(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)
	s.mockReplicationTaskFetcherFactory = NewMockTaskFetcherFactory(s.controller)
	serializer := serialization.NewSerializer()
	s.mockClusterMetadata = cluster.NewMockMetadata(s.controller)
	s.mockClientBean.EXPECT().GetHistoryClient().Return(s.mockHistoryClient).AnyTimes()
	s.mockShard.EXPECT().GetClusterMetadata().Return(s.mockClusterMetadata).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()
	s.mockShard.EXPECT().GetHistoryClient().Return(nil).AnyTimes()
	s.mockShard.EXPECT().GetNamespaceRegistry().Return(namespace.NewMockRegistry(s.controller)).AnyTimes()
	s.mockShard.EXPECT().GetConfig().Return(s.config).AnyTimes()
	s.mockShard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	s.mockShard.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	s.mockShard.EXPECT().GetPayloadSerializer().Return(serializer).AnyTimes()
	s.mockExecutionManager = persistence.NewMockExecutionManager(s.controller)
	s.mockShard.EXPECT().GetExecutionManager().Return(s.mockExecutionManager).AnyTimes()
	s.mockShard.EXPECT().GetShardID().Return(s.shardID).AnyTimes()
	s.mockShard.EXPECT().GetOwner().Return(s.shardOwner).AnyTimes()
	s.taskProcessorManager = NewTaskProcessorManager(
		s.config,
		s.mockShard,
		s.mockEngine,
		nil,
		nil,
		s.mockClientBean,
		serializer,
		s.mockReplicationTaskFetcherFactory,
		func(params TaskExecutorParams) TaskExecutor {
			return s.mockReplicationTaskExecutor
		},
		testhooks.TestHooks{},
		NewExecutionManagerDLQWriter(s.mockExecutionManager),
	)
}

func (s *taskProcessorManagerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *taskProcessorManagerSuite) TestCleanupReplicationTask_Noop() {
	ackedTaskID := int64(12345)
	s.mockShard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(ackedTaskID + 2)).AnyTimes()
	s.mockShard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			shard.ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, s.shardID): {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackedTaskID + 1),
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

	s.taskProcessorManager.minTxAckedTaskID = ackedTaskID
	err := s.taskProcessorManager.cleanupReplicationTasks()
	s.NoError(err)
}

func (s *taskProcessorManagerSuite) TestCleanupReplicationTask_Cleanup() {
	ackedTaskID := int64(12345)
	s.mockShard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(ackedTaskID + 2)).AnyTimes()
	s.mockShard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			shard.ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, common.MapShardID(
				cluster.TestAllClusterInfo[cluster.TestCurrentClusterName].ShardCount,
				cluster.TestAllClusterInfo[cluster.TestAlternativeClusterName].ShardCount,
				s.shardID,
			)[0]): {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackedTaskID + 1),
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
	s.taskProcessorManager.minTxAckedTaskID = ackedTaskID - 1
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(
		gomock.Any(),
		&persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             s.shardID,
			TaskCategory:        tasks.CategoryReplication,
			ExclusiveMaxTaskKey: tasks.NewImmediateKey(ackedTaskID + 1),
		},
	).Return(nil).Times(1)
	err := s.taskProcessorManager.cleanupReplicationTasks()
	s.NoError(err)
}

func (s *taskProcessorManagerSuite) TestCleanupReplicationTask_NoQueueState() {
	// When GetQueueState returns ok=false, an empty queue state is synthesized and
	// every target reader falls back to minAckedTaskID=0. With a watermark of 1,
	// minAckedTaskID resolves to 0 which exceeds the default minTxAckedTaskID (-1),
	// so a RangeCompleteHistoryTasks call is expected.
	s.mockShard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).
		Return(tasks.NewImmediateKey(1)).AnyTimes()
	s.mockShard.EXPECT().GetQueueState(tasks.CategoryReplication).
		Return(nil, false)
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(
		gomock.Any(),
		gomock.Any(),
	).Return(nil).Times(1)

	err := s.taskProcessorManager.cleanupReplicationTasks()
	s.NoError(err)
	s.Equal(int64(0), s.taskProcessorManager.minTxAckedTaskID)
}

func (s *taskProcessorManagerSuite) TestCleanupReplicationTask_RangeCompleteError() {
	ackedTaskID := int64(12345)
	s.mockShard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).
		Return(tasks.NewImmediateKey(ackedTaskID + 2)).AnyTimes()
	s.mockShard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ExclusiveReaderHighWatermark: nil,
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			shard.ReplicationReaderIDFromClusterShardID(cluster.TestAlternativeClusterInitialFailoverVersion, common.MapShardID(
				cluster.TestAllClusterInfo[cluster.TestCurrentClusterName].ShardCount,
				cluster.TestAllClusterInfo[cluster.TestAlternativeClusterName].ShardCount,
				s.shardID,
			)[0]): {
				Scopes: []*persistencespb.QueueSliceScope{{
					Range: &persistencespb.QueueSliceRange{
						InclusiveMin: shard.ConvertToPersistenceTaskKey(
							tasks.NewImmediateKey(ackedTaskID + 1),
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
	s.taskProcessorManager.minTxAckedTaskID = ackedTaskID - 1
	taskProcMgrErr := errors.New("range-complete-failed")
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(
		gomock.Any(),
		gomock.Any(),
	).Return(taskProcMgrErr).Times(1)

	err := s.taskProcessorManager.cleanupReplicationTasks()
	s.ErrorIs(err, taskProcMgrErr)
	// minTxAckedTaskID is only advanced on success.
	s.Equal(ackedTaskID-1, s.taskProcessorManager.minTxAckedTaskID)
}

func (s *taskProcessorManagerSuite) TestStartStop_FetcherEnabled() {
	// Force the fetcher path on (default config has EnableReplicationStream=true).
	s.taskProcessorManager.enableFetcher = true

	// Make the cleanup loop fire quickly at least once.
	s.config.ReplicationTaskProcessorCleanupInterval = dynamicconfig.GetDurationPropertyFnFilteredByShardID(time.Millisecond)
	s.config.ReplicationTaskProcessorCleanupJitterCoefficient = dynamicconfig.GetFloatPropertyFnFilteredByShardID(0.0)
	s.config.ReplicationEnableDLQMetrics = dynamicconfig.GetBoolPropertyFn(false)

	// completeReplicationTaskLoop will invoke cleanupReplicationTasks; allow any persistence calls.
	s.mockShard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).
		Return(tasks.NewImmediateKey(1)).AnyTimes()
	s.mockShard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(nil, false).AnyTimes()
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	registered := make(chan struct{})
	s.mockClusterMetadata.EXPECT().RegisterMetadataChangeCallback(s.taskProcessorManager, gomock.Any()).
		Do(func(_ any, _ cluster.CallbackFn) { close(registered) }).Times(1)
	s.mockClusterMetadata.EXPECT().UnRegisterMetadataChangeCallback(s.taskProcessorManager).Times(1)

	s.taskProcessorManager.Start()
	<-registered

	// Starting again is a no-op (CAS fails); no extra register calls expected.
	s.taskProcessorManager.Start()

	// Inject a processor so Stop exercises its per-processor shutdown loop.
	taskProcMgrProcessor := NewMockTaskProcessor(s.controller)
	taskProcMgrProcessor.EXPECT().Stop().Times(1)
	s.taskProcessorManager.taskProcessorLock.Lock()
	s.taskProcessorManager.taskProcessors[cluster.TestAlternativeClusterName] = []TaskProcessor{taskProcMgrProcessor}
	s.taskProcessorManager.taskProcessorLock.Unlock()

	s.taskProcessorManager.Stop()
	// Stopping again is a no-op (CAS fails); no extra unregister calls expected.
	s.taskProcessorManager.Stop()
}

func (s *taskProcessorManagerSuite) TestStartStop_FetcherDisabled() {
	// Simulate replication stream enabled: no cluster metadata callback registration.
	s.taskProcessorManager.enableFetcher = false

	s.config.ReplicationTaskProcessorCleanupInterval = dynamicconfig.GetDurationPropertyFnFilteredByShardID(time.Hour)
	s.config.ReplicationTaskProcessorCleanupJitterCoefficient = dynamicconfig.GetFloatPropertyFnFilteredByShardID(0.0)
	s.config.ReplicationEnableDLQMetrics = dynamicconfig.GetBoolPropertyFn(false)

	s.taskProcessorManager.Start()
	s.taskProcessorManager.Stop()
}

func (s *taskProcessorManagerSuite) TestCompleteReplicationTaskLoop_CleanupError() {
	// Fire the cleanup quickly and make cleanup fail so the error branch runs.
	s.config.ReplicationTaskProcessorCleanupInterval = dynamicconfig.GetDurationPropertyFnFilteredByShardID(time.Millisecond)
	s.config.ReplicationTaskProcessorCleanupJitterCoefficient = dynamicconfig.GetFloatPropertyFnFilteredByShardID(0.0)

	s.mockShard.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).
		Return(tasks.NewImmediateKey(1)).AnyTimes()
	s.mockShard.EXPECT().GetQueueState(tasks.CategoryReplication).Return(nil, false).AnyTimes()

	cleaned := make(chan struct{}, 1)
	s.mockExecutionManager.EXPECT().RangeCompleteHistoryTasks(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ any, _ *persistence.RangeCompleteHistoryTasksRequest) error {
			select {
			case cleaned <- struct{}{}:
			default:
			}
			return errors.New("taskProcMgr-cleanup-error")
		}).MinTimes(1)

	go s.taskProcessorManager.completeReplicationTaskLoop()
	<-cleaned
	close(s.taskProcessorManager.shutdownChan)
}

func (s *taskProcessorManagerSuite) TestHandleClusterMetadataUpdate_RemoveCluster() {
	// Pre-populate an existing processor for the alternative cluster.
	taskProcMgrProcessor := NewMockTaskProcessor(s.controller)
	taskProcMgrProcessor.EXPECT().Stop().Times(1)
	s.taskProcessorManager.taskProcessors[cluster.TestAlternativeClusterName] = []TaskProcessor{taskProcMgrProcessor}

	oldMetadata := map[string]*cluster.ClusterInformation{
		cluster.TestCurrentClusterName:     {Enabled: true},
		cluster.TestAlternativeClusterName: {Enabled: true},
	}
	// newMetadata empty -> cluster only removed, not re-added.
	s.taskProcessorManager.handleClusterMetadataUpdate(oldMetadata, map[string]*cluster.ClusterInformation{})

	_, ok := s.taskProcessorManager.taskProcessors[cluster.TestAlternativeClusterName]
	s.False(ok)
}

func (s *taskProcessorManagerSuite) TestHandleClusterMetadataUpdate_AddCluster() {
	taskProcMgrFetcher := NewMocktaskFetcher(s.controller)
	taskProcMgrFetcher.EXPECT().getSourceCluster().Return(cluster.TestAlternativeClusterName).AnyTimes()
	taskProcMgrFetcher.EXPECT().getRequestChan().Return(s.requestChan).AnyTimes()
	taskProcMgrFetcher.EXPECT().getRateLimiter().Return(nil).AnyTimes()
	s.mockReplicationTaskFetcherFactory.EXPECT().GetOrCreateFetcher(cluster.TestAlternativeClusterName).
		Return(taskProcMgrFetcher).AnyTimes()

	// Use a small current shard ID so getSourceClusterShardIDs returns a non-empty list
	// (current shard count 8, alternative shard count 4).
	s.taskProcessorManager.taskPollerManager = newPollerManager(1, s.mockClusterMetadata)

	newMetadata := map[string]*cluster.ClusterInformation{
		cluster.TestCurrentClusterName:     {Enabled: true},
		cluster.TestAlternativeClusterName: {Enabled: true},
	}
	s.taskProcessorManager.handleClusterMetadataUpdate(map[string]*cluster.ClusterInformation{}, newMetadata)

	processors, ok := s.taskProcessorManager.taskProcessors[cluster.TestAlternativeClusterName]
	s.True(ok)
	s.NotEmpty(processors)

	// Clean up the started (real) task processors so their goroutines terminate.
	for _, p := range processors {
		p.Stop()
	}
}

func (s *taskProcessorManagerSuite) TestHandleClusterMetadataUpdate_SkipDisabledAndNilCluster() {
	// Disabled cluster and nil cluster info should be skipped: no processors created.
	newMetadata := map[string]*cluster.ClusterInformation{
		cluster.TestCurrentClusterName:     {Enabled: true},
		cluster.TestAlternativeClusterName: {Enabled: false},
		"taskProcMgrNilCluster":            nil,
	}
	s.taskProcessorManager.handleClusterMetadataUpdate(map[string]*cluster.ClusterInformation{}, newMetadata)

	s.Empty(s.taskProcessorManager.taskProcessors[cluster.TestAlternativeClusterName])
	s.Empty(s.taskProcessorManager.taskProcessors["taskProcMgrNilCluster"])
}

func (s *taskProcessorManagerSuite) TestHandleClusterMetadataUpdate_GetSourceShardIDsError() {
	// Give the poller manager its own metadata whose shard counts are not multiples,
	// so getSourceClusterShardIDs returns an error and the cluster is skipped.
	taskProcMgrPollerMeta := cluster.NewMockMetadata(s.controller)
	taskProcMgrPollerMeta.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	taskProcMgrPollerMeta.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
		cluster.TestCurrentClusterName:     {Enabled: true, ShardCount: 3},
		cluster.TestAlternativeClusterName: {Enabled: true, ShardCount: 4},
	}).AnyTimes()
	s.taskProcessorManager.taskPollerManager = newPollerManager(s.shardID, taskProcMgrPollerMeta)

	newMetadata := map[string]*cluster.ClusterInformation{
		cluster.TestCurrentClusterName:     {Enabled: true},
		cluster.TestAlternativeClusterName: {Enabled: true},
	}
	s.taskProcessorManager.handleClusterMetadataUpdate(map[string]*cluster.ClusterInformation{}, newMetadata)

	s.Empty(s.taskProcessorManager.taskProcessors[cluster.TestAlternativeClusterName])
}

func (s *taskProcessorManagerSuite) TestCheckReplicationDLQSize_Empty() {
	s.mockShard.EXPECT().GetReplicatorDLQAckLevel(cluster.TestAlternativeClusterName).Return(int64(0)).Times(1)
	s.mockExecutionManager.EXPECT().IsReplicationDLQEmpty(gomock.Any(), gomock.Any()).Return(true, nil).Times(1)

	s.taskProcessorManager.checkReplicationDLQSize()
}

func (s *taskProcessorManagerSuite) TestCheckReplicationDLQSize_NonEmpty() {
	s.mockShard.EXPECT().GetReplicatorDLQAckLevel(cluster.TestAlternativeClusterName).Return(int64(0)).Times(1)
	s.mockExecutionManager.EXPECT().IsReplicationDLQEmpty(gomock.Any(), gomock.Any()).Return(false, nil).Times(1)

	s.taskProcessorManager.checkReplicationDLQSize()
}

func (s *taskProcessorManagerSuite) TestCheckReplicationDLQSize_Error() {
	s.mockShard.EXPECT().GetReplicatorDLQAckLevel(cluster.TestAlternativeClusterName).Return(int64(0)).Times(1)
	s.mockExecutionManager.EXPECT().IsReplicationDLQEmpty(gomock.Any(), gomock.Any()).
		Return(false, errors.New("taskProcMgr-dlq-error")).Times(1)

	s.taskProcessorManager.checkReplicationDLQSize()
}
