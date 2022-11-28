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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/searchattribute"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
)

const (
	shardControllerMembershipUpdateListenerName = "ShardController"
)

type (
	ControllerImpl struct {
		membershipUpdateCh  chan *membership.ChangedEvent
		engineFactory       EngineFactory
		status              int32
		shutdownWG          sync.WaitGroup
		shutdownCh          chan struct{}
		contextTaggedLogger log.Logger
		throttledLogger     log.Logger
		config              *configs.Config

		sync.RWMutex
		historyShards               map[int32]*ContextImpl
		logger                      log.Logger
		persistenceExecutionManager persistence.ExecutionManager
		persistenceShardManager     persistence.ShardManager
		clientBean                  client.Bean
		historyClient               historyservice.HistoryServiceClient
		historyServiceResolver      membership.ServiceResolver
		taggedMetricsHandler        metrics.MetricsHandler
		metricsHandler              metrics.MetricsHandler
		payloadSerializer           serialization.Serializer
		timeSource                  clock.TimeSource
		namespaceRegistry           namespace.Registry
		saProvider                  searchattribute.Provider
		saMapper                    searchattribute.Mapper
		clusterMetadata             cluster.Metadata
		archivalMetadata            archiver.ArchivalMetadata
		hostInfoProvider            membership.HostInfoProvider
		tracer                      trace.Tracer
	}
)

var _ Controller = (*ControllerImpl)(nil)

func (c *ControllerImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	if c.engineFactory == nil {
		panic("engineFactory was not injected")
	}

	hostIdentity := c.hostInfoProvider.HostInfo().Identity()
	c.contextTaggedLogger = log.With(c.logger, tag.ComponentShardController, tag.Address(hostIdentity))
	c.throttledLogger = log.With(c.throttledLogger, tag.ComponentShardController, tag.Address(hostIdentity))

	c.acquireShards()
	c.shutdownWG.Add(1)
	go c.shardManagementPump()

	if err := c.historyServiceResolver.AddListener(
		shardControllerMembershipUpdateListenerName,
		c.membershipUpdateCh,
	); err != nil {
		c.contextTaggedLogger.Error("Error adding listener", tag.Error(err))
	}

	c.contextTaggedLogger.Info("", tag.LifeCycleStarted)
}

func (c *ControllerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	close(c.shutdownCh)

	if err := c.historyServiceResolver.RemoveListener(
		shardControllerMembershipUpdateListenerName,
	); err != nil {
		c.contextTaggedLogger.Error("Error removing membership update listener", tag.Error(err), tag.OperationFailed)
	}

	if success := common.AwaitWaitGroup(&c.shutdownWG, time.Minute); !success {
		c.contextTaggedLogger.Warn("", tag.LifeCycleStopTimedout)
	}

	c.contextTaggedLogger.Info("", tag.LifeCycleStopped)
}

func (c *ControllerImpl) GetPingChecks() []common.PingCheck {
	return []common.PingCheck{{
		Name:    "shard controller",
		Timeout: 10 * time.Second,
		Ping: func() []common.Pingable {
			// we only need to read but get write lock to make sure we can
			c.Lock()
			defer c.Unlock()
			out := make([]common.Pingable, 0, len(c.historyShards))
			for _, shard := range c.historyShards {
				out = append(out, shard)
			}
			return out
		},
		MetricsName: metrics.ShardControllerLockLatency.GetMetricName(),
	}}
}

func (c *ControllerImpl) Status() int32 {
	return atomic.LoadInt32(&c.status)
}

// GetShardByID returns a shard context for the given namespace and workflow.
// The shard context may not have acquired a rangeid lease yet.
// Callers can use GetEngine on the shard to block on rangeid lease acquisition.
func (c *ControllerImpl) GetShardByNamespaceWorkflow(
	namespaceID namespace.ID,
	workflowID string,
) (Context, error) {
	shardID := c.config.GetShardID(namespaceID, workflowID)
	return c.GetShardByID(shardID)
}

// GetShardByID returns a shard context for the given shard id.
// The shard context may not have acquired a rangeid lease yet.
// Callers can use GetEngine on the shard to block on rangeid lease acquisition.
func (c *ControllerImpl) GetShardByID(
	shardID int32,
) (Context, error) {
	startTime := time.Now().UTC()
	defer func() {
		c.taggedMetricsHandler.Timer(metrics.GetEngineForShardLatency.GetMetricName()).Record(time.Since(startTime))
	}()

	return c.getOrCreateShardContext(shardID)
}

func (c *ControllerImpl) CloseShardByID(shardID int32) {
	startTime := time.Now().UTC()
	defer func() {
		c.taggedMetricsHandler.Timer(metrics.RemoveEngineForShardLatency.GetMetricName()).Record(time.Since(startTime))
	}()

	shard, newNumShards := c.removeShard(shardID, nil)
	// Stop the current shard, if it exists.
	if shard != nil {
		shard.contextTaggedLogger.Info("", tag.LifeCycleStopping, tag.ComponentShardContext, tag.ShardID(shardID))
		shard.finishStop()
		c.taggedMetricsHandler.Counter(metrics.ShardContextRemovedCounter.GetMetricName()).Record(1)
		shard.contextTaggedLogger.Info("", tag.LifeCycleStopped, tag.ComponentShardContext, tag.Number(newNumShards))
	}
}

func (c *ControllerImpl) shardClosedCallback(shard *ContextImpl) {
	startTime := time.Now().UTC()
	defer func() {
		c.taggedMetricsHandler.Timer(metrics.RemoveEngineForShardLatency.GetMetricName()).Record(time.Since(startTime))
	}()

	c.taggedMetricsHandler.Counter(metrics.ShardContextClosedCounter.GetMetricName()).Record(1)
	_, newNumShards := c.removeShard(shard.shardID, shard)

	// Whether shard was in the shards map or not, in both cases we should stop it.
	shard.contextTaggedLogger.Info("", tag.LifeCycleStopping, tag.ComponentShardContext, tag.ShardID(shard.shardID))
	shard.finishStop()
	c.taggedMetricsHandler.Counter(metrics.ShardContextRemovedCounter.GetMetricName()).Record(1)
	shard.contextTaggedLogger.Info("", tag.LifeCycleStopped, tag.ComponentShardContext, tag.Number(newNumShards))
}

// getOrCreateShardContext returns a shard context for the given shard ID, creating a new one
// if necessary. If a shard context is created, it will initialize in the background.
// This function won't block on rangeid lease acquisition.
func (c *ControllerImpl) getOrCreateShardContext(shardID int32) (*ContextImpl, error) {
	c.RLock()
	if shard, ok := c.historyShards[shardID]; ok {
		if shard.isValid() {
			c.RUnlock()
			return shard, nil
		}
		// if shard not valid then proceed to create a new one
	}
	c.RUnlock()

	ownerInfo, err := c.historyServiceResolver.Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return nil, err
	}

	hostInfo := c.hostInfoProvider.HostInfo()
	if ownerInfo.Identity() != hostInfo.Identity() {
		return nil, serviceerrors.NewShardOwnershipLost(ownerInfo.Identity(), hostInfo.GetAddress())
	}

	c.Lock()
	defer c.Unlock()

	// Check again with exclusive lock
	if shard, ok := c.historyShards[shardID]; ok {
		if shard.isValid() {
			return shard, nil
		}

		shard.Unload()
		delete(c.historyShards, shardID)
	}

	if atomic.LoadInt32(&c.status) == common.DaemonStatusStopped {
		return nil, fmt.Errorf("ControllerImpl for host '%v' shutting down", hostInfo.Identity())
	}

	shard, err := newContext(
		shardID,
		c.engineFactory,
		c.config,
		c.shardClosedCallback,
		c.logger,
		c.throttledLogger,
		c.persistenceExecutionManager,
		c.persistenceShardManager,
		c.clientBean,
		c.historyClient,
		c.metricsHandler,
		c.payloadSerializer,
		c.timeSource,
		c.namespaceRegistry,
		c.saProvider,
		c.saMapper,
		c.clusterMetadata,
		c.archivalMetadata,
		c.hostInfoProvider,
	)
	if err != nil {
		return nil, err
	}
	shard.start()
	c.historyShards[shardID] = shard
	c.taggedMetricsHandler.Counter(metrics.ShardContextCreatedCounter.GetMetricName()).Record(1)

	shard.contextTaggedLogger.Info("", tag.LifeCycleStarted, tag.ComponentShardContext)
	return shard, nil
}

func (c *ControllerImpl) removeShard(shardID int32, expected *ContextImpl) (*ContextImpl, int64) {
	c.Lock()
	defer c.Unlock()

	nShards := int64(len(c.historyShards))
	current, ok := c.historyShards[shardID]
	if !ok {
		return nil, nShards
	}
	if expected != nil && current != expected {
		// the shard comparison is a defensive check to make sure we are deleting
		// what we intend to delete.
		return nil, nShards
	}

	delete(c.historyShards, shardID)

	return current, nShards - 1
}

// shardManagementPump is the main event loop for
// ControllerImpl. It is responsible for acquiring /
// releasing shards in response to any event that can
// change the shard ownership. These events are
//
//	a. Ring membership change
//	b. Periodic ticker
//	c. ShardOwnershipLostError and subsequent ShardClosedEvents from engine
func (c *ControllerImpl) shardManagementPump() {
	defer c.shutdownWG.Done()

	acquireTicker := time.NewTicker(c.config.AcquireShardInterval())
	defer acquireTicker.Stop()

	for {
		select {
		case <-c.shutdownCh:
			c.doShutdown()
			return
		case <-acquireTicker.C:
			c.acquireShards()
		case changedEvent := <-c.membershipUpdateCh:
			c.taggedMetricsHandler.Counter(metrics.MembershipChangedCounter.GetMetricName()).Record(1)

			c.contextTaggedLogger.Info("", tag.ValueRingMembershipChangedEvent,
				tag.NumberProcessed(len(changedEvent.HostsAdded)),
				tag.NumberDeleted(len(changedEvent.HostsRemoved)),
				tag.Number(int64(len(changedEvent.HostsUpdated))))
			c.acquireShards()
		}
	}
}

func (c *ControllerImpl) acquireShards() {
	c.taggedMetricsHandler.Counter(metrics.AcquireShardsCounter.GetMetricName()).Record(1)
	startTime := time.Now().UTC()
	defer func() {
		c.taggedMetricsHandler.Timer(metrics.AcquireShardsLatency.GetMetricName()).Record(time.Since(startTime))
	}()

	tryAcquire := func(shardID int32) {
		info, err := c.historyServiceResolver.Lookup(convert.Int32ToString(shardID))
		if err != nil {
			c.contextTaggedLogger.Error("Error looking up host for shardID", tag.Error(err), tag.OperationFailed, tag.ShardID(shardID))
			return
		}
		if info.Identity() != c.hostInfoProvider.HostInfo().Identity() {
			// current host is not owner of shard, unload it if it is already loaded.
			c.CloseShardByID(shardID)
			return
		}
		shard, err := c.GetShardByID(shardID)
		if err != nil {
			c.taggedMetricsHandler.Counter(metrics.GetEngineForShardErrorCounter.GetMetricName()).Record(1)
			c.contextTaggedLogger.Error("Unable to create history shard context", tag.Error(err), tag.OperationFailed, tag.ShardID(shardID))
			return
		}

		// Wait up to 1s for the shard to acquire the rangeid lock.
		// After 1s we will move on but the shard will continue trying in the background.
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_, _ = shard.GetEngine(ctx)

		ctx, cancel = context.WithTimeout(context.Background(), shardIOTimeout)
		defer cancel()
		// trust the AssertOwnership will handle shard ownership lost
		_ = shard.AssertOwnership(ctx)
	}

	concurrency := util.Max(c.config.AcquireShardConcurrency(), 1)
	shardActionCh := make(chan int32, c.config.NumberOfShards)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	// Spawn workers that would lookup and add/remove shards concurrently.
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for shardID := range shardActionCh {
				select {
				case <-c.shutdownCh:
					return
				default:
					tryAcquire(shardID)
				}
			}
		}()
	}

	// Submit tasks to the channel.
	numShards := c.config.NumberOfShards
	randomStartOffset := rand.Int31n(numShards)
LoopSubmit:
	for index := int32(0); index < numShards; index++ {
		shardID := (index+randomStartOffset)%numShards + 1
		select {
		case <-c.shutdownCh:
			break LoopSubmit
		case shardActionCh <- shardID:
			// noop
		}
	}
	close(shardActionCh)
	// Wait until all shards are processed.
	wg.Wait()

	c.RLock()
	numOfOwnedShards := len(c.historyShards)
	c.RUnlock()

	c.taggedMetricsHandler.Gauge(metrics.NumShardsGauge.GetMetricName()).Record(float64(numOfOwnedShards))
}

func (c *ControllerImpl) doShutdown() {
	c.contextTaggedLogger.Info("", tag.LifeCycleStopping)
	c.Lock()
	defer c.Unlock()
	for _, shard := range c.historyShards {
		shard.finishStop()
	}
	c.historyShards = nil
}

func (c *ControllerImpl) ShardIDs() []int32 {
	c.RLock()
	defer c.RUnlock()

	ids := make([]int32, 0, len(c.historyShards))
	for id := range c.historyShards {
		ids = append(ids, id)
	}
	return ids
}

func IsShardOwnershipLostError(err error) bool {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		return true
	case *serviceerrors.ShardOwnershipLost:
		return true
	}

	return false
}
