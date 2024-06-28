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

package shard

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/mock/gomock"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resourcetest"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tasks"
)

type ContextTest struct {
	*ContextImpl

	Resource *resourcetest.Test

	MockEventsCache *events.MockCache
}

var _ Context = (*ContextTest)(nil)

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
	engine Engine
}

func NewStubContext(
	ctrl *gomock.Controller,
	overrides ContextConfigOverrides,
	engine Engine,
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
	taskCategoryRegistry.AddCategory(tasks.CategoryOutbound)

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
		engineFuture:       future.NewFuture[Engine](),
		shardInfo:          config.ShardInfo,
		remoteClusterInfos: make(map[string]*remoteClusterInfo),
		handoverNamespaces: make(map[namespace.Name]*namespaceHandOverInfo),

		clusterMetadata:         clusterMetadata,
		timeSource:              t.TimeSource,
		namespaceRegistry:       registry,
		stateMachineRegistry:    hsm.NewRegistry(),
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
func (s *ContextTest) SetEngineForTesting(engine Engine) {
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

// SetHistoryClientForTesting sets history client. Only used by tests.
func (s *ContextTest) SetHistoryClientForTesting(client historyservice.HistoryServiceClient) {
	s.historyClient = client
}

// SetStateMachineRegistry sets the state machine registry on this shard.
func (s *ContextTest) SetStateMachineRegistry(reg *hsm.Registry) {
	s.stateMachineRegistry = reg
}

// StopForTest calls FinishStop(). In general only the controller
// should call that, but integration tests need to do it also to clean up any
// background acquireShard goroutines that may exist.
func (s *ContextTest) StopForTest() {
	s.FinishStop()
}

func (s *StubContext) GetEngine(_ context.Context) (Engine, error) {
	return s.engine, nil
}
