package shard

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type ContextTest struct {
	*ContextImpl

	Resource *resourcetest.Test

	MockEventsCache *events.MockCache
}

var _ historyi.ShardContext = (*ContextTest)(nil)

func NewTestContextWithTimeSource(
	ctrl *gomock.Controller,
	shardInfo *persistencespb.ShardInfo,
	config *configs.Config,
	timeSource clock.TimeSource,
) *ContextTest {
	result := NewTestContext(ctrl, shardInfo, config)
	result.timeSource = timeSource
	result.taskKeyManager.generator.timeSource = timeSource
	result.Resource.TimeSource = timeSource
	return result
}

func NewTestContext(
	ctrl *gomock.Controller,
	shardInfo *persistencespb.ShardInfo,
	config *configs.Config,
) *ContextTest {
	resourceTest := resourcetest.NewTest(ctrl, primitives.HistoryService)
	eventsCache := events.NewMockCache(ctrl)
	shard := newTestContext(
		resourceTest,
		eventsCache,
		ContextConfigOverrides{
			ShardInfo: shardInfo,
			Config:    config,
		},
	)
	return &ContextTest{
		Resource:        resourceTest,
		ContextImpl:     shard,
		MockEventsCache: eventsCache,
	}
}

type ContextConfigOverrides struct {
	ShardInfo        *persistencespb.ShardInfo
	Config           *configs.Config
	Registry         namespace.Registry
	ClusterMetadata  cluster.Metadata
	ExecutionManager persistence.ExecutionManager
}

type StubContext struct {
	ContextTest
	engine historyi.Engine
}

func NewStubContext(
	ctrl *gomock.Controller,
	overrides ContextConfigOverrides,
	engine historyi.Engine,
) *StubContext {
	resourceTest := resourcetest.NewTest(ctrl, primitives.HistoryService)
	eventsCache := events.NewMockCache(ctrl)
	shard := newTestContext(resourceTest, eventsCache, overrides)

	result := &StubContext{
		ContextTest: ContextTest{
			Resource:        resourceTest,
			ContextImpl:     shard,
			MockEventsCache: eventsCache,
		},
		engine: engine,
	}
	return result
}

func newTestContext(t *resourcetest.Test, eventsCache events.Cache, config ContextConfigOverrides) *ContextImpl {
	hostInfoProvider := t.GetHostInfoProvider()
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	if config.ShardInfo.QueueStates == nil {
		config.ShardInfo.QueueStates = make(map[int32]*persistencespb.QueueState)
	}
	registry := config.Registry
	if registry == nil {
		registry = t.GetNamespaceRegistry()
	}
	clusterMetadata := config.ClusterMetadata
	if clusterMetadata == nil {
		clusterMetadata = t.GetClusterMetadata()
	}
	executionManager := config.ExecutionManager
	if executionManager == nil {
		executionManager = t.ExecutionMgr
	}
	taskCategoryRegistry := tasks.NewDefaultTaskCategoryRegistry()
	taskCategoryRegistry.AddCategory(tasks.CategoryArchival)

	ctx := &ContextImpl{
		shardID:             config.ShardInfo.GetShardId(),
		owner:               config.ShardInfo.GetOwner(),
		stringRepr:          fmt.Sprintf("Shard(%d)", config.ShardInfo.GetShardId()),
		executionManager:    executionManager,
		metricsHandler:      t.MetricsHandler,
		eventsCache:         eventsCache,
		config:              config.Config,
		contextTaggedLogger: t.GetLogger(),
		throttledLogger:     t.GetThrottledLogger(),
		lifecycleCtx:        lifecycleCtx,
		lifecycleCancel:     lifecycleCancel,
		queueMetricEmitter:  sync.Once{},

		state:              contextStateAcquired,
		engineFuture:       future.NewFuture[historyi.Engine](),
		shardInfo:          config.ShardInfo,
		remoteClusterInfos: make(map[string]*remoteClusterInfo),
		handoverNamespaces: make(map[namespace.Name]*namespaceHandOverInfo),

		clusterMetadata:         clusterMetadata,
		timeSource:              t.TimeSource,
		namespaceRegistry:       registry,
		stateMachineRegistry:    hsm.NewRegistry(),
		chasmRegistry:           chasm.NewRegistry(t.GetLogger()),
		persistenceShardManager: t.GetShardManager(),
		clientBean:              t.GetClientBean(),
		saProvider:              t.GetSearchAttributesProvider(),
		saMapperProvider:        t.GetSearchAttributesMapperProvider(),
		historyClient:           t.GetHistoryClient(),
		payloadSerializer:       t.GetPayloadSerializer(),
		archivalMetadata:        t.GetArchivalMetadata(),
		hostInfoProvider:        hostInfoProvider,
		taskCategoryRegistry:    taskCategoryRegistry,
		ioSemaphore:             locks.NewPrioritySemaphore(1),
	}
	ctx.taskKeyManager = newTaskKeyManager(
		ctx.taskCategoryRegistry,
		ctx.timeSource,
		config.Config,
		ctx.GetLogger(),
		func() error {
			return ctx.renewRangeLocked(false)
		},
	)
	ctx.taskKeyManager.setRangeID(config.ShardInfo.RangeId)
	return ctx
}

// SetEngineForTest sets s.engine. Only used by tests.
func (s *ContextTest) SetEngineForTesting(engine historyi.Engine) {
	s.engineFuture.Set(engine, nil)
}

// SetEventsCacheForTesting sets s.eventsCache. Only used by tests.
func (s *ContextTest) SetEventsCacheForTesting(c events.Cache) {
	// for testing only, will only be called immediately after initialization
	s.eventsCache = c
}

// SetLoggers sets both s.throttledLogger and s.contextTaggedLogger. Only used by tests.
func (s *ContextTest) SetLoggers(l log.Logger) {
	s.throttledLogger = l
	s.contextTaggedLogger = l
}

// SetMetricsHandler sets  s.metricsHandler. Only used by tests.
func (s *ContextTest) SetMetricsHandler(h metrics.Handler) {
	s.metricsHandler = h
}

// SetHistoryClientForTesting sets history client. Only used by tests.
func (s *ContextTest) SetHistoryClientForTesting(client historyservice.HistoryServiceClient) {
	s.historyClient = client
}

// SetStateMachineRegistry sets the state machine registry on this shard.
func (s *ContextTest) SetStateMachineRegistry(reg *hsm.Registry) {
	s.stateMachineRegistry = reg
}

func (s *ContextTest) SetChasmRegistry(reg *chasm.Registry) {
	s.chasmRegistry = reg
}

func (s *ContextTest) SetClusterMetadata(metadata cluster.Metadata) {
	s.clusterMetadata = metadata
}

// StopForTest calls FinishStop(). In general only the controller
// should call that, but integration tests need to do it also to clean up any
// background acquireShard goroutines that may exist.
func (s *ContextTest) StopForTest() {
	s.FinishStop()
}

func (s *StubContext) GetEngine(_ context.Context) (historyi.Engine, error) {
	return s.engine, nil
}
