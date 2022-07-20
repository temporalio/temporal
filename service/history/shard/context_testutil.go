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

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
)

type ContextTest struct {
	*ContextImpl

	Resource *resource.Test

	MockEventsCache      *events.MockCache
	MockHostInfoProvider *membership.MockHostInfoProvider
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
	resourceTest := resource.NewTest(ctrl, metrics.History)
	eventsCache := events.NewMockCache(ctrl)
	hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	lifecycleCtx, lifecycleCancel := context.WithCancel(context.Background())
	if shardInfo.QueueAckLevels == nil {
		shardInfo.QueueAckLevels = make(map[int32]*persistencespb.QueueAckLevel)
	}
	if shardInfo.QueueStates == nil {
		shardInfo.QueueStates = make(map[int32]*persistencespb.QueueState)
	}
	shard := &ContextImpl{
		shardID:             shardInfo.GetShardId(),
		executionManager:    resourceTest.ExecutionMgr,
		metricsClient:       resourceTest.MetricsClient,
		eventsCache:         eventsCache,
		config:              config,
		contextTaggedLogger: resourceTest.GetLogger(),
		throttledLogger:     resourceTest.GetThrottledLogger(),
		lifecycleCtx:        lifecycleCtx,
		lifecycleCancel:     lifecycleCancel,

		state:                              contextStateAcquired,
		engineFuture:                       future.NewFuture[Engine](),
		shardInfo:                          shardInfo,
		taskSequenceNumber:                 shardInfo.RangeId << int64(config.RangeSizeBits),
		immediateTaskExclusiveMaxReadLevel: shardInfo.RangeId << int64(config.RangeSizeBits),
		maxTaskSequenceNumber:              (shardInfo.RangeId + 1) << int64(config.RangeSizeBits),
		scheduledTaskMaxReadLevelMap:       make(map[string]time.Time),
		remoteClusterInfos:                 make(map[string]*remoteClusterInfo),
		handoverNamespaces:                 make(map[string]*namespaceHandOverInfo),

		clusterMetadata:         resourceTest.ClusterMetadata,
		timeSource:              resourceTest.TimeSource,
		namespaceRegistry:       resourceTest.GetNamespaceRegistry(),
		persistenceShardManager: resourceTest.GetShardManager(),
		clientBean:              resourceTest.GetClientBean(),
		saProvider:              resourceTest.GetSearchAttributesProvider(),
		saMapper:                resourceTest.GetSearchAttributesMapper(),
		historyClient:           resourceTest.GetHistoryClient(),
		archivalMetadata:        resourceTest.GetArchivalMetadata(),
		hostInfoProvider:        hostInfoProvider,
	}
	return &ContextTest{
		Resource:             resourceTest,
		ContextImpl:          shard,
		MockEventsCache:      eventsCache,
		MockHostInfoProvider: hostInfoProvider,
	}
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

// StopForTest calls private method finishStop(). In general only the controller
// should call that, but integration tests need to do it also to clean up any
// background acquireShard goroutines that may exist.
func (s *ContextTest) StopForTest() {
	s.finishStop()
}
