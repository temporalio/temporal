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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

const (
	shardControllerMembershipUpdateListenerName = "ShardController"
)

type (
	ControllerImpl struct {
		resource.Resource

		membershipUpdateCh chan *membership.ChangedEvent
		engineFactory      EngineFactory
		status             int32
		shutdownWG         sync.WaitGroup
		shutdownCh         chan struct{}
		logger             log.Logger
		throttledLogger    log.Logger
		config             *configs.Config
		metricsScope       metrics.Scope

		sync.RWMutex
		historyShards map[int32]*ContextImpl
	}
)

func NewController(
	resource resource.Resource,
	factory EngineFactory,
	config *configs.Config,
) *ControllerImpl {
	hostIdentity := resource.GetHostInfo().Identity()
	return &ControllerImpl{
		Resource:           resource,
		status:             common.DaemonStatusInitialized,
		membershipUpdateCh: make(chan *membership.ChangedEvent, 10),
		engineFactory:      factory,
		historyShards:      make(map[int32]*ContextImpl),
		shutdownCh:         make(chan struct{}),
		logger:             log.With(resource.GetLogger(), tag.ComponentShardController, tag.Address(hostIdentity)),
		throttledLogger:    log.With(resource.GetThrottledLogger(), tag.ComponentShardController, tag.Address(hostIdentity)),
		config:             config,
		metricsScope:       resource.GetMetricsClient().Scope(metrics.HistoryShardControllerScope),
	}
}

func (c *ControllerImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	c.acquireShards()
	c.shutdownWG.Add(1)
	go c.shardManagementPump()

	if err := c.GetHistoryServiceResolver().AddListener(
		shardControllerMembershipUpdateListenerName,
		c.membershipUpdateCh,
	); err != nil {
		c.logger.Error("Error adding listener", tag.Error(err))
	}

	c.logger.Info("", tag.LifeCycleStarted)
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

	if err := c.GetHistoryServiceResolver().RemoveListener(
		shardControllerMembershipUpdateListenerName,
	); err != nil {
		c.logger.Error("Error removing membership update listener", tag.Error(err), tag.OperationFailed)
	}

	if success := common.AwaitWaitGroup(&c.shutdownWG, time.Minute); !success {
		c.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	c.logger.Info("", tag.LifeCycleStopped)
}

func (c *ControllerImpl) Status() int32 {
	return atomic.LoadInt32(&c.status)
}

func (c *ControllerImpl) GetEngine(namespaceID namespace.ID, workflowID string) (Engine, error) {
	shardID := c.config.GetShardID(namespaceID, workflowID)
	return c.GetEngineForShard(shardID)
}

func (c *ControllerImpl) GetEngineForShard(shardID int32) (Engine, error) {
	sw := c.metricsScope.StartTimer(metrics.GetEngineForShardLatency)
	defer sw.Stop()
	shard, err := c.getOrCreateShardContext(shardID)
	if err != nil {
		return nil, err
	}
	return shard.getOrCreateEngine()
}

func (c *ControllerImpl) CloseShardByID(shardID int32) {
	sw := c.metricsScope.StartTimer(metrics.RemoveEngineForShardLatency)
	defer sw.Stop()

	shard, newNumShards := c.removeShard(shardID, nil)
	// Stop the current shard, if it exists.
	if shard != nil {
		shard.logger.Info("", tag.LifeCycleStopping, tag.ComponentShardContext, tag.ShardID(shardID))
		shard.stop()
		c.metricsScope.IncCounter(metrics.ShardContextRemovedCounter)
		shard.logger.Info("", tag.LifeCycleStopped, tag.ComponentShardContext, tag.Number(newNumShards))
	}
}

func (c *ControllerImpl) shardClosedCallback(shard *ContextImpl) {
	sw := c.metricsScope.StartTimer(metrics.RemoveEngineForShardLatency)
	defer sw.Stop()

	c.metricsScope.IncCounter(metrics.ShardContextClosedCounter)

	_, newNumShards := c.removeShard(shard.shardID, shard)

	// Whether shard was in the shards map or not, in both cases we should stop it.
	shard.logger.Info("", tag.LifeCycleStopping, tag.ComponentShardContext, tag.ShardID(shard.shardID))
	shard.stop()
	c.metricsScope.IncCounter(metrics.ShardContextRemovedCounter)
	shard.logger.Info("", tag.LifeCycleStopped, tag.ComponentShardContext, tag.Number(newNumShards))
}

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

	info, err := c.GetHistoryServiceResolver().Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return nil, err
	}

	if info.Identity() != c.GetHostInfo().Identity() {
		return nil, serviceerrors.NewShardOwnershipLost(c.GetHostInfo().Identity(), info.GetAddress())
	}

	c.Lock()
	defer c.Unlock()

	// Check again with exclusive lock
	if shard, ok := c.historyShards[shardID]; ok {
		if shard.isValid() {
			return shard, nil
		}
	}

	if atomic.LoadInt32(&c.status) == common.DaemonStatusStopped {
		return nil, fmt.Errorf("ControllerImpl for host '%v' shutting down", c.GetHostInfo().Identity())
	}

	shard, err := newContext(
		c.Resource,
		shardID,
		c.engineFactory,
		c.config,
		c.shardClosedCallback,
	)
	if err != nil {
		return nil, err
	}
	shard.start()
	c.historyShards[shardID] = shard
	c.metricsScope.IncCounter(metrics.ShardContextCreatedCounter)

	shard.logger.Info("", tag.LifeCycleStarted, tag.ComponentShardContext)
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
//   a. Ring membership change
//   b. Periodic ticker
//   c. ShardOwnershipLostError and subsequent ShardClosedEvents from engine
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
			c.metricsScope.IncCounter(metrics.MembershipChangedCounter)

			c.logger.Info("", tag.ValueRingMembershipChangedEvent,
				tag.NumberProcessed(len(changedEvent.HostsAdded)),
				tag.NumberDeleted(len(changedEvent.HostsRemoved)),
				tag.Number(int64(len(changedEvent.HostsUpdated))))
			c.acquireShards()
		}
	}
}

func (c *ControllerImpl) acquireShards() {
	c.metricsScope.IncCounter(metrics.AcquireShardsCounter)
	sw := c.metricsScope.StartTimer(metrics.AcquireShardsLatency)
	defer sw.Stop()

	concurrency := common.MaxInt(c.config.AcquireShardConcurrency(), 1)
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
					if info, err := c.GetHistoryServiceResolver().Lookup(
						convert.Int32ToString(shardID),
					); err != nil {
						c.logger.Error("Error looking up host for shardID", tag.Error(err), tag.OperationFailed, tag.ShardID(shardID))
					} else {
						if info.Identity() == c.GetHostInfo().Identity() {
							if _, err := c.GetEngineForShard(shardID); err != nil {
								c.metricsScope.IncCounter(metrics.GetEngineForShardErrorCounter)
								c.logger.Error("Unable to create history shard engine", tag.Error(err), tag.OperationFailed, tag.ShardID(shardID))
							}
						}
						// TODO: If we're _not_ the owner for this shard, and we have it loaded, we should unload it.
					}
				}
			}
		}()
	}

	// Submit tasks to the channel.
LoopSubmit:
	for shardID := int32(1); shardID <= c.config.NumberOfShards; shardID++ {
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

	c.metricsScope.UpdateGauge(metrics.NumShardsGauge, float64(c.NumShards()))
}

func (c *ControllerImpl) doShutdown() {
	c.logger.Info("", tag.LifeCycleStopping)
	c.Lock()
	defer c.Unlock()
	for _, shard := range c.historyShards {
		shard.stop()
	}
	c.historyShards = nil
}

func (c *ControllerImpl) NumShards() int {
	c.RLock()
	defer c.RUnlock()
	return len(c.historyShards)
}

func (c *ControllerImpl) ShardIDs() []int32 {
	c.RLock()
	ids := []int32{}
	for id := range c.historyShards {
		id32 := int32(id)
		ids = append(ids, id32)
	}
	c.RUnlock()
	return ids
}

func IsShardOwnershipLostError(err error) bool {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		return true
	}

	return false
}
