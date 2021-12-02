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
	"time"

	"github.com/golang/mock/gomock"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
)

type ContextTest struct {
	*ContextImpl

	Resource *resource.Test

	MockEventsCache *events.MockCache
}

var _ Context = (*ContextTest)(nil)

func NewTestContextWithTimeSource(
	ctrl *gomock.Controller,
	shardInfo *persistence.ShardInfoWithFailover,
	config *configs.Config,
	timeSource clock.TimeSource,
) *ContextTest {
	result := NewTestContext(ctrl, shardInfo, config)
	result.timeSource = timeSource
	result.Resource.TimeSource = timeSource
	return result
}

func NewTestContext(
	ctrl *gomock.Controller,
	shardInfo *persistence.ShardInfoWithFailover,
	config *configs.Config,
) *ContextTest {
	resource := resource.NewTest(ctrl, metrics.History)
	eventsCache := events.NewMockCache(ctrl)
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	shard := &ContextImpl{
		shardID:             shardInfo.GetShardId(),
		executionManager:    resource.ExecutionMgr,
		metricsClient:       resource.MetricsClient,
		eventsCache:         eventsCache,
		config:              config,
		contextTaggedLogger: resource.GetLogger(),
		throttledLogger:     resource.GetThrottledLogger(),
		lifecycleCtx:        lifecycleCtx,
		lifecycleCancel:     lifecycleCancel,

		state:                     contextStateAcquired,
		shardInfo:                 shardInfo,
		transferSequenceNumber:    1,
		transferMaxReadLevel:      0,
		maxTransferSequenceNumber: 100000,
		timerMaxReadLevelMap:      make(map[string]time.Time),
		remoteClusterInfos:        make(map[string]*remoteClusterInfo),
		handoverNamespaces:        make(map[string]*namespaceHandOverInfo),

		clusterMetadata:         resource.ClusterMetadata,
		timeSource:              resource.TimeSource,
		namespaceRegistry:       resource.GetNamespaceRegistry(),
		persistenceShardManager: resource.GetShardManager(),
		clientBean:              resource.GetClientBean(),
		saProvider:              resource.GetSearchAttributesProvider(),
		historyClient:           resource.GetHistoryClient(),
		archivalMetadata:        resource.GetArchivalMetadata(),
	}
	return &ContextTest{
		Resource:        resource,
		ContextImpl:     shard,
		MockEventsCache: eventsCache,
	}
}

// SetEngineForTest sets s.engine. Only used by tests.
func (s *ContextTest) SetEngineForTesting(engine Engine) {
	s.engine = engine
}

// SetEventsCacheForTesting sets s.eventsCache. Only used by tests.
func (s *ContextTest) SetEventsCacheForTesting(c events.Cache) {
	// for testing only, will only be called immediately after initialization
	s.eventsCache = c
}

// StopForTest calls private method stop(). In general only the controller should call stop, but integration
// tests need to do it also to clean up any background acquireShard goroutines that may exist.
func (s *ContextTest) StopForTest() {
	s.stop()
}
