// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/resource"
)

const (
	shardControllerMembershipUpdateListenerName = "ShardController"
)

type (
	shardController struct {
		resource.Resource

		membershipUpdateCh chan *membership.ChangedEvent
		engineFactory      EngineFactory
		shardClosedCh      chan int
		status             int32
		shutdownWG         sync.WaitGroup
		shutdownCh         chan struct{}
		logger             log.Logger
		throttledLogger    log.Logger
		config             *Config
		metricsScope       metrics.Scope

		sync.RWMutex
		historyShards map[int]*historyShardsItem
	}

	historyShardsItemStatus int

	historyShardsItem struct {
		resource.Resource

		shardID         int
		config          *Config
		logger          log.Logger
		throttledLogger log.Logger
		engineFactory   EngineFactory

		sync.RWMutex
		status historyShardsItemStatus
		engine Engine
	}
)

const (
	historyShardsItemStatusInitialized = iota
	historyShardsItemStatusStarted
	historyShardsItemStatusStopped
)

func newShardController(
	resource resource.Resource,
	factory EngineFactory,
	config *Config,
) *shardController {
	hostIdentity := resource.GetHostInfo().Identity()
	return &shardController{
		Resource:           resource,
		status:             common.DaemonStatusInitialized,
		membershipUpdateCh: make(chan *membership.ChangedEvent, 10),
		engineFactory:      factory,
		historyShards:      make(map[int]*historyShardsItem),
		shardClosedCh:      make(chan int, config.NumberOfShards),
		shutdownCh:         make(chan struct{}),
		logger:             resource.GetLogger().WithTags(tag.ComponentShardController, tag.Address(hostIdentity)),
		throttledLogger:    resource.GetThrottledLogger().WithTags(tag.ComponentShardController, tag.Address(hostIdentity)),
		config:             config,
		metricsScope:       resource.GetMetricsClient().Scope(metrics.HistoryShardControllerScope),
	}
}

func newHistoryShardsItem(
	resource resource.Resource,
	shardID int,
	factory EngineFactory,
	config *Config,
) (*historyShardsItem, error) {

	hostIdentity := resource.GetHostInfo().Identity()
	return &historyShardsItem{
		Resource:        resource,
		shardID:         shardID,
		status:          historyShardsItemStatusInitialized,
		engineFactory:   factory,
		config:          config,
		logger:          resource.GetLogger().WithTags(tag.ShardID(shardID), tag.Address(hostIdentity)),
		throttledLogger: resource.GetThrottledLogger().WithTags(tag.ShardID(shardID), tag.Address(hostIdentity)),
	}, nil
}

func (c *shardController) Start() {
	if !atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	c.acquireShards()
	c.shutdownWG.Add(1)
	go c.shardManagementPump()

	err := c.GetHistoryServiceResolver().AddListener(shardControllerMembershipUpdateListenerName, c.membershipUpdateCh)
	if err != nil {
		c.logger.Error("Error adding listener", tag.Error(err))
	}

	c.logger.Info("", tag.LifeCycleStarted)
}

func (c *shardController) Stop() {
	if !atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	if err := c.GetHistoryServiceResolver().RemoveListener(shardControllerMembershipUpdateListenerName); err != nil {
		c.logger.Error("Error removing membership update listener", tag.Error(err), tag.OperationFailed)
	}
	close(c.shutdownCh)

	if success := common.AwaitWaitGroup(&c.shutdownWG, time.Minute); !success {
		c.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	c.logger.Info("", tag.LifeCycleStopped)
}

func (c *shardController) GetEngine(workflowID string) (Engine, error) {
	shardID := c.config.GetShardID(workflowID)
	return c.getEngineForShard(shardID)
}

func (c *shardController) getEngineForShard(shardID int) (Engine, error) {
	sw := c.metricsScope.StartTimer(metrics.GetEngineForShardLatency)
	defer sw.Stop()
	item, err := c.getOrCreateHistoryShardItem(shardID)
	if err != nil {
		return nil, err
	}
	return item.getOrCreateEngine(c.shardClosedCh)
}

func (c *shardController) removeEngineForShard(shardID int) {
	sw := c.metricsScope.StartTimer(metrics.RemoveEngineForShardLatency)
	defer sw.Stop()
	item, _ := c.removeHistoryShardItem(shardID)
	if item != nil {
		item.stopEngine()
	}
}

func (c *shardController) getOrCreateHistoryShardItem(shardID int) (*historyShardsItem, error) {
	c.RLock()
	if item, ok := c.historyShards[shardID]; ok {
		if item.isValid() {
			c.RUnlock()
			return item, nil
		}
		// if item not valid then process to create a new one
	}
	c.RUnlock()

	c.Lock()
	defer c.Unlock()

	if item, ok := c.historyShards[shardID]; ok {
		if item.isValid() {
			return item, nil
		}
		// if item not valid then process to create a new one
	}

	if atomic.LoadInt32(&c.status) == common.DaemonStatusStopped {
		return nil, fmt.Errorf("shardController for host '%v' shutting down", c.GetHostInfo().Identity())
	}
	info, err := c.GetHistoryServiceResolver().Lookup(string(shardID))
	if err != nil {
		return nil, err
	}

	if info.Identity() == c.GetHostInfo().Identity() {
		shardItem, err := newHistoryShardsItem(
			c.Resource,
			shardID,
			c.engineFactory,
			c.config,
		)
		if err != nil {
			return nil, err
		}
		c.historyShards[shardID] = shardItem
		c.metricsScope.IncCounter(metrics.ShardItemCreatedCounter)

		shardItem.logger.Info("", tag.LifeCycleStarted, tag.ComponentShardItem)
		return shardItem, nil
	}

	return nil, createShardOwnershipLostError(c.GetHostInfo().Identity(), info.GetAddress())
}

func (c *shardController) removeHistoryShardItem(shardID int) (*historyShardsItem, error) {
	nShards := 0
	c.Lock()
	shardItem, ok := c.historyShards[shardID]
	if !ok {
		c.Unlock()
		return nil, fmt.Errorf("No item found to remove for shard: %v", shardID)
	}
	delete(c.historyShards, shardID)
	nShards = len(c.historyShards)
	c.Unlock()

	c.metricsScope.IncCounter(metrics.ShardItemRemovedCounter)

	shardItem.logger.Info("", tag.LifeCycleStopped, tag.ComponentShardItem, tag.Number(int64(nShards)))
	return shardItem, nil
}

// shardManagementPump is the main event loop for
// shardController. It is responsible for acquiring /
// releasing shards in response to any event that can
// change the shard ownership. These events are
//   a. Ring membership change
//   b. Periodic ticker
//   c. ShardOwnershipLostError and subsequent ShardClosedEvents from engine
func (c *shardController) shardManagementPump() {

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
		case shardID := <-c.shardClosedCh:
			c.metricsScope.IncCounter(metrics.ShardClosedCounter)
			c.logger.Info("", tag.LifeCycleStopping, tag.ComponentShard, tag.ShardID(shardID))
			c.removeEngineForShard(shardID)
			// The async close notifications can cause a race
			// between acquire/release when nodes are flapping
			// The impact of this race is un-necessary shard load/unloads
			// even though things will settle eventually
			// To reduce the chance of the race happening, lets
			// process all closed events at once before we attempt
			// to acquire new shards again
			c.processShardClosedEvents()
		}
	}
}

func (c *shardController) acquireShards() {

	c.metricsScope.IncCounter(metrics.AcquireShardsCounter)
	sw := c.metricsScope.StartTimer(metrics.AcquireShardsLatency)
	defer sw.Stop()

	concurrency := common.MaxInt(c.config.AcquireShardConcurrency(), 1)
	shardActionCh := make(chan int, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	// Spawn workers that would lookup and add/remove shards concurrently.
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for shardID := range shardActionCh {
				info, err := c.GetHistoryServiceResolver().Lookup(string(shardID))
				if err != nil {
					c.logger.Error("Error looking up host for shardID", tag.Error(err), tag.OperationFailed, tag.ShardID(shardID))
				} else {
					if info.Identity() == c.GetHostInfo().Identity() {
						_, err1 := c.getEngineForShard(shardID)
						if err1 != nil {
							c.metricsScope.IncCounter(metrics.GetEngineForShardErrorCounter)
							c.logger.Error("Unable to create history shard engine", tag.Error(err1), tag.OperationFailed, tag.ShardID(shardID))
						}
					} else {
						c.removeEngineForShard(shardID)
					}
				}
			}
		}()
	}
	// Submit tasks to the channel.
	for shardID := 0; shardID < c.config.NumberOfShards; shardID++ {
		shardActionCh <- shardID
	}
	close(shardActionCh)
	// Wait until all shards are processed.
	wg.Wait()

	c.metricsScope.UpdateGauge(metrics.NumShardsGauge, float64(c.numShards()))
}

func (c *shardController) doShutdown() {
	c.logger.Info("", tag.LifeCycleStopping)
	c.Lock()
	defer c.Unlock()
	for _, item := range c.historyShards {
		item.stopEngine()
	}
	c.historyShards = nil
}

func (c *shardController) processShardClosedEvents() {
	for {
		select {
		case shardID := <-c.shardClosedCh:
			c.metricsScope.IncCounter(metrics.ShardClosedCounter)
			c.logger.Info("", tag.LifeCycleStopping, tag.ComponentShard, tag.ShardID(shardID))
			c.removeEngineForShard(shardID)
		default:
			return
		}
	}
}

func (c *shardController) numShards() int {
	nShards := 0
	c.RLock()
	nShards = len(c.historyShards)
	c.RUnlock()
	return nShards
}

func (c *shardController) shardIDs() []int32 {
	c.RLock()
	ids := []int32{}
	for id := range c.historyShards {
		id32 := int32(id)
		ids = append(ids, id32)
	}
	c.RUnlock()
	return ids
}

func (i *historyShardsItem) getOrCreateEngine(shardClosedCh chan<- int) (Engine, error) {
	i.RLock()
	if i.status == historyShardsItemStatusStarted {
		defer i.RUnlock()
		return i.engine, nil
	}
	i.RUnlock()

	i.Lock()
	defer i.Unlock()
	switch i.status {
	case historyShardsItemStatusInitialized:
		i.logger.Info("", tag.LifeCycleStarting, tag.ComponentShardEngine)
		context, err := acquireShard(i, shardClosedCh)
		if err != nil {
			return nil, err
		}
		if context.PreviousShardOwnerWasDifferent() {
			i.GetMetricsClient().RecordTimer(metrics.ShardInfoScope, metrics.ShardItemAcquisitionLatency,
				context.GetCurrentTime(i.GetClusterMetadata().GetCurrentClusterName()).Sub(context.GetLastUpdatedTime()))
		}
		i.engine = i.engineFactory.CreateEngine(context)
		i.engine.Start()
		i.logger.Info("", tag.LifeCycleStarted, tag.ComponentShardEngine)
		i.status = historyShardsItemStatusStarted
		return i.engine, nil
	case historyShardsItemStatusStarted:
		return i.engine, nil
	case historyShardsItemStatusStopped:
		return nil, fmt.Errorf("shard %v for host '%v' is shut down", i.shardID, i.GetHostInfo().Identity())
	default:
		panic(i.logInvalidStatus())
	}
}

func (i *historyShardsItem) stopEngine() {
	i.Lock()
	defer i.Unlock()

	switch i.status {
	case historyShardsItemStatusInitialized:
		i.status = historyShardsItemStatusStopped
	case historyShardsItemStatusStarted:
		i.logger.Info("", tag.LifeCycleStopping, tag.ComponentShardEngine)
		i.engine.Stop()
		i.engine = nil
		i.logger.Info("", tag.LifeCycleStopped, tag.ComponentShardEngine)
		i.status = historyShardsItemStatusStopped
	case historyShardsItemStatusStopped:
		// no op
	default:
		panic(i.logInvalidStatus())
	}
}

func (i *historyShardsItem) isValid() bool {
	i.RLock()
	defer i.RUnlock()

	switch i.status {
	case historyShardsItemStatusInitialized, historyShardsItemStatusStarted:
		return true
	case historyShardsItemStatusStopped:
		return false
	default:
		panic(i.logInvalidStatus())
	}
}

func (i *historyShardsItem) logInvalidStatus() string {
	msg := fmt.Sprintf("Host '%v' encounter invalid status %v for shard item for shardID '%v'.",
		i.GetHostInfo().Identity(), i.status, i.shardID)
	i.logger.Error(msg)
	return msg
}

func isShardOwnershiptLostError(err error) bool {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		return true
	}

	return false
}
