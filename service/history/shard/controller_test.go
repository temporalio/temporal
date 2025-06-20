package shard

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	controllerSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockResource        *resourcetest.Test
		mockHistoryEngine   *historyi.MockEngine
		mockClusterMetadata *cluster.MockMetadata
		mockServiceResolver *membership.MockServiceResolver

		hostInfo          membership.HostInfo
		otherHostInfo     membership.HostInfo
		mockShardManager  *persistence.MockShardManager
		mockEngineFactory *MockEngineFactory

		config               *configs.Config
		logger               log.Logger
		shardController      *ControllerImpl
		mockHostInfoProvider *membership.MockHostInfoProvider
		metricsTestHandler   *metricstest.Handler
	}
)

func NewTestController(
	engineFactory *MockEngineFactory,
	config *configs.Config,
	resource *resourcetest.Test,
	hostInfoProvider *membership.MockHostInfoProvider,
	metricsTestHandler *metricstest.Handler,
) *ControllerImpl {
	contextFactory := ContextFactoryProvider(ContextFactoryParams{
		ArchivalMetadata:            resource.GetArchivalMetadata(),
		ClientBean:                  resource.GetClientBean(),
		ClusterMetadata:             resource.GetClusterMetadata(),
		Config:                      config,
		EngineFactory:               engineFactory,
		HistoryClient:               resource.GetHistoryClient(),
		HistoryServiceResolver:      resource.GetHistoryServiceResolver(),
		HostInfoProvider:            hostInfoProvider,
		Logger:                      resource.GetLogger(),
		MetricsHandler:              metricsTestHandler,
		NamespaceRegistry:           resource.GetNamespaceRegistry(),
		PayloadSerializer:           resource.GetPayloadSerializer(),
		PersistenceExecutionManager: resource.GetExecutionManager(),
		PersistenceShardManager:     resource.GetShardManager(),
		SaMapperProvider:            resource.GetSearchAttributesMapperProvider(),
		SaProvider:                  resource.GetSearchAttributesProvider(),
		ThrottledLogger:             resource.GetThrottledLogger(),
		TimeSource:                  resource.GetTimeSource(),
		TaskCategoryRegistry:        tasks.NewDefaultTaskCategoryRegistry(),
	})

	return ControllerProvider(
		config,
		resource.GetLogger(),
		resource.GetHistoryServiceResolver(),
		metricsTestHandler,
		resource.GetHostInfoProvider(),
		contextFactory,
	)
}

func TestShardControllerSuite(t *testing.T) {
	s := new(controllerSuite)
	suite.Run(t, s)
}

func (s *controllerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockResource = resourcetest.NewTest(s.controller, primitives.HistoryService)
	s.mockHistoryEngine = historyi.NewMockEngine(s.controller)
	s.mockEngineFactory = NewMockEngineFactory(s.controller)

	s.mockShardManager = s.mockResource.ShardMgr
	s.mockServiceResolver = s.mockResource.HistoryServiceResolver
	s.mockClusterMetadata = s.mockResource.ClusterMetadata
	s.hostInfo = s.mockResource.GetHostInfo()
	s.otherHostInfo = membership.NewHostInfoFromAddress("other")
	s.mockHostInfoProvider = s.mockResource.HostInfoProvider
	s.mockHostInfoProvider.EXPECT().HostInfo().Return(s.hostInfo).AnyTimes()

	s.logger = s.mockResource.Logger
	s.config = tests.NewDynamicConfig()
	metricsTestHandler, err := metricstest.NewHandler(log.NewNoopLogger(), metrics.ClientConfig{})
	s.NoError(err)
	s.metricsTestHandler = metricsTestHandler

	// when shard is initialized, it will use the 2 mock function below to initialize the "current" time of each cluster
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestSingleDCClusterInfo).AnyTimes()

	s.shardController = NewTestController(
		s.mockEngineFactory,
		s.config,
		s.mockResource,
		s.mockHostInfoProvider,
		s.metricsTestHandler,
	)
}

func (s *controllerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *controllerSuite) TestAcquireShardSuccess() {
	numShards := int32(8)
	s.config.NumberOfShards = numShards

	var myShards []int32
	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			mockEngine := historyi.NewMockEngine(s.controller)
			historyEngines[shardID] = mockEngine
			s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(membership.NewHostInfoFromAddress(ownerHost), nil)
		}
	}

	s.shardController.acquireShards(context.Background())
	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, shardID := range myShards {
		shard, err := s.shardController.GetShardByID(shardID)
		s.NoError(err)
		_, err = shard.GetEngine(ctx)
		s.NoError(err)
		count++
	}
	s.Equal(2, count)
}

func (s *controllerSuite) TestAcquireShardsConcurrently() {
	numShards := int32(10)
	s.config.NumberOfShards = numShards
	s.config.AcquireShardConcurrency = func() int {
		return 10
	}

	var myShards []int32
	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		hostID := shardID % 4
		if hostID == 0 {
			myShards = append(myShards, shardID)
			mockEngine := historyi.NewMockEngine(s.controller)
			historyEngines[shardID] = mockEngine
			s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
		} else {
			ownerHost := fmt.Sprintf("test-acquire-shard-host-%v", hostID)
			s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(
				membership.NewHostInfoFromAddress(ownerHost), nil,
			)
		}
	}

	s.shardController.acquireShards(context.Background())
	count := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, shardID := range myShards {
		shard, err := s.shardController.GetShardByID(shardID)
		s.NoError(err)
		_, err = shard.GetEngine(ctx)
		s.NoError(err)
		count++
	}
	s.Equal(2, count)
}

func (s *controllerSuite) TestAcquireShardLookupFailure() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards
	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
	}

	s.shardController.acquireShards(context.Background())
	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
		shard, err := s.shardController.GetShardByID(shardID)
		s.Error(err)
		s.Nil(shard)
	}
}

func (s *controllerSuite) TestAcquireShardRenewSuccess() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards

	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyi.NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
	}

	s.shardController.acquireShards(context.Background())

	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil)
	}
	s.shardController.acquireShards(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		shard, err := s.shardController.GetShardByID(shardID)
		s.NoError(err)
		s.NotNil(shard)
		engine, err := shard.GetEngine(ctx)
		s.NoError(err)
		s.NotNil(engine)
	}
}

func (s *controllerSuite) TestAcquireShardRenewLookupFailed() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards

	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyi.NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
	}

	s.shardController.acquireShards(context.Background())

	for shardID := int32(1); shardID <= numShards; shardID++ {
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(nil, errors.New("ring failure"))
	}
	s.shardController.acquireShards(context.Background())

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		shard, err := s.shardController.GetShardByID(shardID)
		s.NoError(err)
		s.NotNil(shard)
		engine, err := shard.GetEngine(ctx)
		s.NoError(err)
		s.NotNil(engine)
	}
}

func (s *controllerSuite) TestHistoryEngineClosed() {
	numShards := int32(4)
	s.config.NumberOfShards = numShards
	s.shardController = NewTestController(
		s.mockEngineFactory,
		s.config,
		s.mockResource,
		s.mockHostInfoProvider,
		s.metricsTestHandler,
	)
	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyi.NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s.mockServiceResolver.EXPECT().AddListener(shardControllerMembershipUpdateListenerName,
		gomock.Any()).Return(nil).AnyTimes()
	s.shardController.Start()
	s.shardController.acquireShards(context.Background())

	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := int32(1); shardID <= numShards; shardID++ {
					shard, err := s.shardController.GetShardByID(shardID)
					s.NoError(err)
					s.NotNil(shard)
					engine, err := shard.GetEngine(ctx)
					s.NoError(err)
					s.NotNil(engine)
				}
			}
			workerWG.Done()
		}()
	}

	workerWG.Wait()

	for shardID := int32(1); shardID <= 2; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.otherHostInfo, nil).AnyTimes()
		s.shardController.CloseShardByID(shardID)
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			for attempt := 0; attempt < 10; attempt++ {
				for shardID := int32(3); shardID <= numShards; shardID++ {
					shard, err := s.shardController.GetShardByID(shardID)
					s.NoError(err)
					s.NotNil(shard)
					engine, err := shard.GetEngine(ctx)
					s.NoError(err)
					s.NotNil(engine)
					time.Sleep(20 * time.Millisecond)
				}
			}
			workerWG.Done()
		}()
	}

	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			shardLost := false
			for attempt := 0; !shardLost && attempt < 10; attempt++ {
				for shardID := int32(1); shardID <= 2; shardID++ {
					_, err := s.shardController.GetShardByID(shardID)
					if err != nil {
						s.logger.Error("ShardLost", tag.Error(err))
						shardLost = true
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			s.True(shardLost)
			workerWG.Done()
		}()
	}

	workerWG.Wait()

	s.mockServiceResolver.EXPECT().RemoveListener(shardControllerMembershipUpdateListenerName).Return(nil).AnyTimes()
	for shardID := int32(3); shardID <= numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop().Return()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).AnyTimes()
	}
	s.shardController.Stop()
}

func (s *controllerSuite) TestShardControllerClosed() {
	numShards := int32(4)
	s.config.NumberOfShards = numShards
	s.shardController = NewTestController(
		s.mockEngineFactory,
		s.config,
		s.mockResource,
		s.mockHostInfoProvider,
		s.metricsTestHandler,
	)

	historyEngines := make(map[int32]*historyi.MockEngine)
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyi.NewMockEngine(s.controller)
		historyEngines[shardID] = mockEngine
		s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)
	}

	s.mockServiceResolver.EXPECT().AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).Return(nil).AnyTimes()
	s.shardController.Start()
	s.shardController.acquireShards(context.Background())

	var workerWG sync.WaitGroup
	for w := 0; w < 10; w++ {
		workerWG.Add(1)
		go func() {
			shardLost := false
			for attempt := 0; !shardLost && attempt < 10; attempt++ {
				for shardID := int32(1); shardID <= numShards; shardID++ {
					_, err := s.shardController.GetShardByID(shardID)
					if err != nil {
						s.logger.Error("ShardLost", tag.Error(err))
						shardLost = true
					}
					time.Sleep(20 * time.Millisecond)
				}
			}

			s.True(shardLost)
			workerWG.Done()
		}()
	}

	s.mockServiceResolver.EXPECT().RemoveListener(shardControllerMembershipUpdateListenerName).Return(nil).AnyTimes()
	for shardID := int32(1); shardID <= numShards; shardID++ {
		mockEngine := historyEngines[shardID]
		mockEngine.EXPECT().Stop()
		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).AnyTimes()
	}
	s.shardController.Stop()
	workerWG.Wait()
}

func (s *controllerSuite) TestShardExplicitUnload() {
	s.config.NumberOfShards = 1

	mockEngine := historyi.NewMockEngine(s.controller)
	mockEngine.EXPECT().Stop().AnyTimes()
	s.setupMocksForAcquireShard(1, mockEngine, 5, 6, false)

	shard, err := s.shardController.getOrCreateShardContext(1)
	s.NoError(err)
	s.Equal(1, len(s.shardController.ShardIDs()))

	shard.UnloadForOwnershipLost()

	for tries := 0; tries < 100 && len(s.shardController.ShardIDs()) != 0; tries++ {
		// removal from map happens asynchronously
		time.Sleep(1 * time.Millisecond)
	}
	s.Equal(0, len(s.shardController.ShardIDs()))
	s.False(shard.IsValid())
}

func (s *controllerSuite) TestShardExplicitUnloadCancelGetOrCreate() {
	s.config.NumberOfShards = 1

	mockEngine := historyi.NewMockEngine(s.controller)
	mockEngine.EXPECT().Stop().AnyTimes()

	shardID := int32(1)
	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil)

	ready := make(chan struct{})
	wasCanceled := make(chan bool)
	// GetOrCreateShard blocks for 5s or until canceled
	s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).DoAndReturn(
		func(ctx context.Context, req *persistence.GetOrCreateShardRequest) (*persistence.GetOrCreateShardResponse, error) {
			ready <- struct{}{}
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				wasCanceled <- false
				return nil, errors.New("timed out")
			case <-ctx.Done():
				wasCanceled <- true
				return nil, ctx.Err()
			}
		})

	// get shard, will start initializing in background
	shard, err := s.shardController.getOrCreateShardContext(1)
	s.NoError(err)

	<-ready
	// now shard is blocked on GetOrCreateShard
	s.False(shard.(*ContextImpl).engineFuture.Ready())

	start := time.Now()
	shard.UnloadForOwnershipLost() // this cancels the context so GetOrCreateShard returns immediately
	s.True(<-wasCanceled)
	s.Less(time.Since(start), 500*time.Millisecond)
}

func (s *controllerSuite) TestShardExplicitUnloadCancelAcquire() {
	s.config.NumberOfShards = 1

	mockEngine := historyi.NewMockEngine(s.controller)
	mockEngine.EXPECT().Stop().AnyTimes()

	shardID := int32(1)
	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil)
	// return success from GetOrCreateShard
	s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  s.hostInfo.Identity(),
				RangeId:                5,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueStates:            s.queueStates(),
			},
		}, nil)

	// acquire lease (UpdateShard) blocks for 5s
	ready := make(chan struct{})
	wasCanceled := make(chan bool)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, req *persistence.UpdateShardRequest) error {
			ready <- struct{}{}
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-timer.C:
				wasCanceled <- false
				return errors.New("timed out")
			case <-ctx.Done():
				wasCanceled <- true
				return ctx.Err()
			}
		})

	// get shard, will start initializing in background
	shard, err := s.shardController.getOrCreateShardContext(1)
	s.NoError(err)

	<-ready
	// now shard is blocked on UpdateShard
	s.False(shard.(*ContextImpl).engineFuture.Ready())

	start := time.Now()
	shard.UnloadForOwnershipLost() // this cancels the context so UpdateShard returns immediately
	s.True(<-wasCanceled)
	s.Less(time.Since(start), 500*time.Millisecond)
}

// Tests random concurrent sequence of shard load/acquire/unload to catch any race conditions
// that were not covered by specific tests.
func (s *controllerSuite) TestShardControllerFuzz() {
	s.config.NumberOfShards = 10

	s.mockServiceResolver.EXPECT().AddListener(shardControllerMembershipUpdateListenerName, gomock.Any()).Return(nil).AnyTimes()
	s.mockServiceResolver.EXPECT().RemoveListener(shardControllerMembershipUpdateListenerName).Return(nil).AnyTimes()

	// only for MockEngines: we just need to hook Start/Stop, not verify calls
	disconnectedMockController := gomock.NewController(nil)

	var engineStarts, engineStops atomic.Int64
	var getShards, closeContexts atomic.Int64

	for shardID := int32(1); shardID <= s.config.NumberOfShards; shardID++ {
		shardID := shardID
		queueStates := s.queueStates()

		s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).AnyTimes()
		s.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).DoAndReturn(func(shard historyi.ShardContext) historyi.Engine {
			mockEngine := historyi.NewMockEngine(disconnectedMockController)
			status := new(int32)
			// notification step is done after engine is created, so may not be called when test finishes
			mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).MaxTimes(2)
			mockEngine.EXPECT().Start().Do(func() {
				if !atomic.CompareAndSwapInt32(status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
					return
				}
				engineStarts.Add(1)
			}).AnyTimes()
			mockEngine.EXPECT().Stop().Do(func() {
				if !atomic.CompareAndSwapInt32(status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
					return
				}
				engineStops.Add(1)
			}).AnyTimes()
			return mockEngine
		}).AnyTimes()
		s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).DoAndReturn(
			func(ctx context.Context, req *persistence.GetOrCreateShardRequest) (*persistence.GetOrCreateShardResponse, error) {
				if ctx.Err() != nil {
					return nil, errors.New("already canceled")
				}
				// note that lifecycleCtx could be canceled right here
				getShards.Add(1)
				go func(lifecycleCtx context.Context) {
					<-lifecycleCtx.Done()
					closeContexts.Add(1)
				}(req.LifecycleContext)
				return &persistence.GetOrCreateShardResponse{
					ShardInfo: &persistencespb.ShardInfo{
						ShardId:                shardID,
						Owner:                  s.hostInfo.Identity(),
						RangeId:                5,
						ReplicationDlqAckLevel: map[string]int64{},
						QueueStates:            queueStates,
					},
				}, nil
			}).AnyTimes()
		s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
		s.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
			ShardID: shardID,
			RangeID: 6,
		}).Return(nil).AnyTimes()
	}

	randomLoadedShard := func() (int32, historyi.ShardContext) {
		s.shardController.Lock()
		defer s.shardController.Unlock()
		if len(s.shardController.historyShards) == 0 {
			return -1, nil
		}
		n := rand.Intn(len(s.shardController.historyShards))
		for id, shard := range s.shardController.historyShards {
			if n == 0 {
				return id, shard
			}
			n--
		}
		return -1, nil
	}

	worker := func(ctx context.Context) error {
		for ctx.Err() == nil {
			shardID := int32(rand.Intn(int(s.config.NumberOfShards))) + 1
			switch rand.Intn(5) {
			case 0:
				_, _ = s.shardController.GetShardByID(shardID)
			case 1:
				if shard, err := s.shardController.GetShardByID(shardID); err == nil {
					_, _ = shard.GetEngine(ctx)
				}
			case 2:
				if _, shard := randomLoadedShard(); shard != nil {
					shard.UnloadForOwnershipLost()
				}
			case 3:
				if id, _ := randomLoadedShard(); id >= 0 {
					s.shardController.CloseShardByID(id)
				}
			case 4:
				time.Sleep(10 * time.Millisecond)
			}
		}
		return ctx.Err()
	}

	s.shardController.Start()
	s.shardController.acquireShards(context.Background())

	var workers goro.Group
	for i := 0; i < 10; i++ {
		workers.Go(worker)
	}

	time.Sleep(3 * time.Second)

	workers.Cancel()
	workers.Wait()
	s.shardController.Stop()

	// check that things are good
	// wait for number of GetOrCreateShard calls to stabilize across 100ms, since there could
	// be some straggler acquireShard goroutines that call it even after shardController.Stop
	// (which will cancel all lifecycleCtxs).
	var prevGetShards int64
	s.Eventually(func() bool {
		thisGetShards := getShards.Load()
		ok := thisGetShards == prevGetShards && thisGetShards == closeContexts.Load()
		prevGetShards = thisGetShards
		return ok
	}, 1*time.Second, 100*time.Millisecond, "all contexts did not close")
	s.Eventually(func() bool {
		return engineStarts.Load() == engineStops.Load()
	}, 1*time.Second, 100*time.Millisecond, "engine start/stop")
}

func (s *controllerSuite) Test_GetOrCreateShard_InvalidShardID() {
	numShards := int32(2)
	s.config.NumberOfShards = numShards

	_, err := s.shardController.getOrCreateShardContext(0)
	s.ErrorIs(err, invalidShardIdLowerBound)

	_, err = s.shardController.getOrCreateShardContext(3)
	s.ErrorIs(err, invalidShardIdUpperBound)
}

func (s *controllerSuite) TestShardLingerTimeout() {
	shardID := int32(1)
	s.config.NumberOfShards = 1
	timeLimit := 1 * time.Second
	s.config.ShardLingerTimeLimit = dynamicconfig.GetDurationPropertyFn(timeLimit)

	historyEngines := make(map[int32]*historyi.MockEngine)
	mockEngine := historyi.NewMockEngine(s.controller)
	historyEngines[shardID] = mockEngine
	s.setupMocksForAcquireShard(shardID, mockEngine, 5, 6, true)

	s.shardController.acquireShards(context.Background())

	s.Len(s.shardController.ShardIDs(), 1)
	shard, err := s.shardController.getOrCreateShardContext(shardID)
	s.NoError(err)

	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress("newhost"), nil)

	mockEngine.EXPECT().Stop().Return()

	s.shardController.acquireShards(context.Background())
	// Wait for total of timeout plus 100ms of test fudge factor.

	// Should still have a valid shard before the timeout.
	time.Sleep(timeLimit / 2)
	s.True(shard.IsValid())
	s.Len(s.shardController.ShardIDs(), 1)

	// By now the timeout should have occurred.
	time.Sleep(timeLimit/2 + 100*time.Millisecond)
	s.Len(s.shardController.ShardIDs(), 0)
	s.False(shard.IsValid())

	s.Equal(float64(1), s.readMetricsCounter(
		metrics.ShardLingerTimeouts.Name(),
		metrics.OperationTag(metrics.HistoryShardControllerScope)))
}

func (s *controllerSuite) TestShardLingerSuccess() {
	shardID := int32(1)
	s.config.NumberOfShards = 1
	timeLimit := 1 * time.Second
	s.config.ShardLingerTimeLimit = dynamicconfig.GetDurationPropertyFn(timeLimit)

	checkQPS := 5
	s.config.ShardLingerOwnershipCheckQPS = dynamicconfig.GetIntPropertyFn(checkQPS)

	historyEngines := make(map[int32]*historyi.MockEngine)
	mockEngine := historyi.NewMockEngine(s.controller)
	historyEngines[shardID] = mockEngine

	mockEngine.EXPECT().Start().MinTimes(1)
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).MaxTimes(2)
	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2).MinTimes(1)
	s.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).Return(mockEngine).MinTimes(1)
	s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  s.hostInfo.Identity(),
				RangeId:                5,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueStates:            s.queueStates(),
			},
		}, nil).MinTimes(1)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), updateShardRequestMatcher(persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:                shardID,
			Owner:                  s.hostInfo.Identity(),
			RangeId:                6,
			StolenSinceRenew:       1,
			ReplicationDlqAckLevel: map[string]int64{},
			QueueStates:            s.queueStates(),
		},
		PreviousRangeID: 5,
	})).Return(nil).MinTimes(1)
	s.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: 6,
	}).Return(nil).Times(1)

	s.shardController.acquireShards(context.Background())
	s.Len(s.shardController.ShardIDs(), 1)
	shard, err := s.shardController.getOrCreateShardContext(shardID)
	s.NoError(err)

	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).
		Return(membership.NewHostInfoFromAddress("newhost"), nil)

	mockEngine.EXPECT().Stop().Return().MinTimes(1)

	// We mock 2 AssertShardOwnership calls in shardLingerThenClose.
	// The second one finds that the shard is no longer owned by the host, and unloads it.
	s.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: 6,
	}).Return(nil).Times(1)
	s.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: 6,
	}).DoAndReturn(func(_ context.Context, _ *persistence.AssertShardOwnershipRequest) error {
		shard.UnloadForOwnershipLost()
		return nil
	}).Times(1)

	s.shardController.acquireShards(context.Background())

	// Wait for two checks plus 100ms of test fudge factor.
	expectedWait := time.Second / time.Duration(checkQPS) * 2
	time.Sleep(expectedWait + 100*time.Millisecond)

	s.Len(s.shardController.ShardIDs(), 0)
}

// TestShardCounter verifies that we can subscribe to shard count updates, receive them when shards are acquired, and
// unsubscribe from the updates when needed.
func (s *controllerSuite) TestShardCounter() {
	const totalShards = 5
	s.config.NumberOfShards = totalShards

	var ownedShards atomic.Int32
	s.mockServiceResolver.EXPECT().Lookup(gomock.Any()).DoAndReturn(func(key string) (membership.HostInfo, error) {
		if i, err := strconv.Atoi(key); err != nil {
			return nil, err
		} else if i <= int(ownedShards.Load()) {
			return s.hostInfo, nil
		}
		return s.otherHostInfo, nil
	}).AnyTimes()

	mockEngine := historyi.NewMockEngine(s.controller)
	for i := range totalShards {
		s.setupMocksForAcquireShard(int32(i+1), mockEngine, 5, 6, false)
	}

	// subscribe to shard count updates
	sub1 := s.shardController.SubscribeShardCount()

	// validate that we get the initial shard count
	s.Empty(sub1.ShardCount(), "Should not publish shard count before acquiring shards")
	ownedShards.Store(2)
	s.shardController.acquireShards(context.Background())
	s.Equal(2, <-sub1.ShardCount(), "Should publish shard count after acquiring shards")
	s.Empty(sub1.ShardCount(), "Shard count channel should be drained")

	// acquire shards twice to validate that this does not block even if there's no capacity left on the channel
	ownedShards.Store(3)
	s.shardController.acquireShards(context.Background())
	ownedShards.Store(4)
	s.shardController.acquireShards(context.Background())
	s.Equal(3, <-sub1.ShardCount(), "Shard count is buffered, so we should only get the first value")
	s.Empty(sub1.ShardCount(), "Shard count channel should be drained")

	// unsubscribe and validate that the channel is closed, but the other subscriber is still receiving updates
	sub2 := s.shardController.SubscribeShardCount()
	sub1.Unsubscribe()
	ownedShards.Store(4)
	s.shardController.acquireShards(context.Background())
	_, ok := <-sub1.ShardCount()
	s.False(ok, "Channel should be closed because sub1 is canceled")
	sub1.Unsubscribe() // should not panic if called twice
	s.Equal(4, <-sub2.ShardCount(), "Should receive shard count updates on sub2 even if sub1 is canceled")
	sub2.Unsubscribe()
}

type readinessMockState struct {
	ownership   sync.Map
	assertError sync.Map
	assertDelay sync.Map
}

func (s *controllerSuite) setupMocksForReadiness() *readinessMockState {
	state := &readinessMockState{}
	state.ownership.Store(1, true)
	state.ownership.Store(3, true)
	state.ownership.Store(5, true)

	s.config.NumberOfShards = 5

	s.mockServiceResolver.EXPECT().Lookup(gomock.Any()).DoAndReturn(func(key string) (membership.HostInfo, error) {
		if i, err := strconv.Atoi(key); err != nil {
			return nil, err
		} else if owned, ok := state.ownership.Load(i); ok && owned.(bool) {
			return s.hostInfo, nil
		}
		return s.otherHostInfo, nil
	}).AnyTimes()

	state.ownership.Range(func(shardID, owned any) bool {
		if owned.(bool) {
			s.setupMockForReadiness(int32(shardID.(int)), state)
		}
		return true
	})

	return state
}

func (s *controllerSuite) setupMockForReadiness(shardID int32, state *readinessMockState) {
	mockEngine := historyi.NewMockEngine(s.controller)
	mockEngine.EXPECT().Start()
	mockEngine.EXPECT().Stop().AnyTimes()
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).AnyTimes()
	s.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).Return(mockEngine)
	s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  s.hostInfo.Identity(),
				RangeId:                5,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueStates:            s.queueStates(),
			},
		}, nil)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), updateShardRequestMatcher(persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:                shardID,
			Owner:                  s.hostInfo.Identity(),
			RangeId:                6,
			StolenSinceRenew:       1,
			ReplicationDlqAckLevel: map[string]int64{},
			QueueStates:            s.queueStates(),
		},
		PreviousRangeID: 5,
	})).Return(nil)

	// nolint:forbidigo // deliberately blocking
	s.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: 6,
	}).DoAndReturn(func(context.Context, *persistence.AssertShardOwnershipRequest) error {
		if delay, ok := state.assertDelay.Load(int(shardID)); ok {
			time.Sleep(delay.(time.Duration))
		}
		if err, ok := state.assertError.Load(int(shardID)); ok {
			return err.(error)
		}
		return nil
	}).MinTimes(1)
}

func (s *controllerSuite) TestReadiness_Ready() {
	_ = s.setupMocksForReadiness()

	// not ready yet
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	s.ErrorIs(s.shardController.InitialShardsAcquired(ctx), context.DeadlineExceeded)

	// acquire
	s.shardController.acquireShards(context.Background())

	// now should be ready
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.shardController.InitialShardsAcquired(ctx))
}

func (s *controllerSuite) TestReadiness_Error() {
	state := s.setupMocksForReadiness()

	// use an error that will not cause controller to re-acquire
	state.assertError.Store(3, serviceerror.NewResourceExhausted(0, ""))

	// acquire
	s.shardController.acquireShards(context.Background())

	// shard 3 failed in AssertShardOwnership even though it passed UpdateShard, should be not ready yet
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	s.ErrorIs(s.shardController.InitialShardsAcquired(ctx), context.DeadlineExceeded)

	// fix error and try again
	state.assertError.Delete(3)
	s.shardController.acquireShards(context.Background())

	// now should be ready
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.shardController.InitialShardsAcquired(ctx))
}

func (s *controllerSuite) TestReadiness_Blocked() {
	s.config.ShardIOConcurrency = dynamicconfig.GetIntPropertyFn(10) // allow second assert to run while first is blocked
	state := s.setupMocksForReadiness()

	state.assertDelay.Store(3, time.Hour)

	// acquire
	s.shardController.acquireShards(context.Background())

	// shard 3 is blocked in AssertShardOwnership
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	s.ErrorIs(s.shardController.InitialShardsAcquired(ctx), context.DeadlineExceeded)

	// acquire again (e.g. membership changed)
	state.assertDelay.Delete(3)
	s.shardController.acquireShards(context.Background())

	// now should be ready
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.shardController.InitialShardsAcquired(ctx))
}

func (s *controllerSuite) TestReadiness_MembershipChanged() {
	state := s.setupMocksForReadiness()

	state.assertDelay.Store(3, time.Hour)

	// acquire
	s.shardController.acquireShards(context.Background())

	// shard 3 is blocked in AssertShardOwnership
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	s.ErrorIs(s.shardController.InitialShardsAcquired(ctx), context.DeadlineExceeded)

	// change membership, now we own 2, 4, and 5, we don't care about 3 anymore
	state.ownership.Clear()
	state.ownership.Store(2, true)
	state.ownership.Store(4, true)
	state.ownership.Store(5, true)
	s.setupMockForReadiness(2, state)
	s.setupMockForReadiness(4, state)

	// acquire again
	s.shardController.acquireShards(context.Background())

	// now should be ready
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.NoError(s.shardController.InitialShardsAcquired(ctx))
}

func (s *controllerSuite) setupMocksForAcquireShard(
	shardID int32,
	mockEngine *historyi.MockEngine,
	currentRangeID, newRangeID int64,
	required bool,
) {

	queueStates := s.queueStates()

	minTimes := 0
	if required {
		minTimes = 1
	}

	// s.mockResource.ExecutionMgr.On("Close").Return()
	mockEngine.EXPECT().Start().MinTimes(minTimes)
	// notification step is done after engine is created, so may not be called when test finishes
	mockEngine.EXPECT().NotifyNewTasks(gomock.Any()).MaxTimes(2)
	s.mockServiceResolver.EXPECT().Lookup(convert.Int32ToString(shardID)).Return(s.hostInfo, nil).Times(2).MinTimes(minTimes)
	s.mockEngineFactory.EXPECT().CreateEngine(contextMatcher(shardID)).Return(mockEngine).MinTimes(minTimes)
	s.mockShardManager.EXPECT().GetOrCreateShard(gomock.Any(), getOrCreateShardRequestMatcher(shardID)).Return(
		&persistence.GetOrCreateShardResponse{
			ShardInfo: &persistencespb.ShardInfo{
				ShardId:                shardID,
				Owner:                  s.hostInfo.Identity(),
				RangeId:                currentRangeID,
				ReplicationDlqAckLevel: map[string]int64{},
				QueueStates:            queueStates,
			},
		}, nil).MinTimes(minTimes)
	s.mockShardManager.EXPECT().UpdateShard(gomock.Any(), updateShardRequestMatcher(persistence.UpdateShardRequest{
		ShardInfo: &persistencespb.ShardInfo{
			ShardId:                shardID,
			Owner:                  s.hostInfo.Identity(),
			RangeId:                newRangeID,
			StolenSinceRenew:       1,
			ReplicationDlqAckLevel: map[string]int64{},
			QueueStates:            queueStates,
		},
		PreviousRangeID: currentRangeID,
	})).Return(nil).MinTimes(minTimes)
	s.mockShardManager.EXPECT().AssertShardOwnership(gomock.Any(), &persistence.AssertShardOwnershipRequest{
		ShardID: shardID,
		RangeID: newRangeID,
	}).Return(nil).AnyTimes()
}

func (s *controllerSuite) queueStates() map[int32]*persistencespb.QueueState {
	return map[int32]*persistencespb.QueueState{
		int32(tasks.CategoryTransfer.ID()): {
			ReaderStates: nil,
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				FireTime: timestamppb.New(tasks.DefaultFireTime),
				TaskId:   rand.Int63(),
			},
		},
		int32(tasks.CategoryTimer.ID()): {
			ReaderStates: make(map[int64]*persistencespb.QueueReaderState),
			ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
				FireTime: timestamp.TimeNowPtrUtc(),
				TaskId:   rand.Int63(),
			},
		},
		int32(tasks.CategoryReplication.ID()): {
			ReaderStates: map[int64]*persistencespb.QueueReaderState{
				0: {
					Scopes: []*persistencespb.QueueSliceScope{
						{
							Range: &persistencespb.QueueSliceRange{
								InclusiveMin: &persistencespb.TaskKey{
									FireTime: timestamppb.New(tasks.DefaultFireTime),
									TaskId:   1000,
								},
								ExclusiveMax: &persistencespb.TaskKey{
									FireTime: timestamppb.New(tasks.DefaultFireTime),
									TaskId:   2000,
								},
							},
							Predicate: &persistencespb.Predicate{
								PredicateType: enumsspb.PREDICATE_TYPE_UNIVERSAL,
								Attributes: &persistencespb.Predicate_UniversalPredicateAttributes{
									UniversalPredicateAttributes: &persistencespb.UniversalPredicateAttributes{},
								},
							},
						},
					},
				},
				1: nil,
			},
			ExclusiveReaderHighWatermark: nil,
		},
	}
}

// This is needed to avoid race conditions when using this matcher, since
// fmt.Sprintf("%v"), used by gomock, would otherwise access private fields.
// See https://github.com/temporalio/temporal/issues/2777
var _ fmt.Stringer = (*ContextImpl)(nil)

type contextMatcher int32

func (s contextMatcher) Matches(x interface{}) bool {
	shardContext, ok := x.(historyi.ShardContext)
	return ok && shardContext.GetShardID() == int32(s)
}

func (s contextMatcher) String() string {
	return strconv.Itoa(int(s))
}

type getOrCreateShardRequestMatcher int32

func (s getOrCreateShardRequestMatcher) Matches(x interface{}) bool {
	req, ok := x.(*persistence.GetOrCreateShardRequest)
	return ok && req.ShardID == int32(s)
}

func (s getOrCreateShardRequestMatcher) String() string {
	return strconv.Itoa(int(s))
}

type updateShardRequestMatcher persistence.UpdateShardRequest

func (m updateShardRequestMatcher) Matches(x interface{}) bool {
	req, ok := x.(*persistence.UpdateShardRequest)
	if !ok {
		return false
	}

	// only compare the essential vars,
	// other vars like queue state / queue ack level should not be test in this util
	return m.PreviousRangeID == req.PreviousRangeID &&
		m.ShardInfo.ShardId == req.ShardInfo.ShardId &&
		strings.Contains(req.ShardInfo.Owner, m.ShardInfo.Owner) &&
		m.ShardInfo.RangeId == req.ShardInfo.RangeId &&
		m.ShardInfo.StolenSinceRenew == req.ShardInfo.StolenSinceRenew
}

func (m updateShardRequestMatcher) String() string {
	return fmt.Sprintf("%+v", (persistence.UpdateShardRequest)(m))
}

func (s *controllerSuite) readMetricsCounter(name string, nonSystemTags ...metrics.Tag) float64 {
	expectedSystemTags := []metrics.Tag{
		metrics.StringTag("otel_scope_name", "temporal"),
		metrics.StringTag("otel_scope_version", ""),
	}
	snapshot, err := s.metricsTestHandler.Snapshot()
	s.NoError(err)

	tags := append(nonSystemTags, expectedSystemTags...)
	value, err := snapshot.Counter(name+"_total", tags...)
	s.NoError(err)
	return value
}
