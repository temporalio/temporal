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

package history

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/convert"
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
	shardController struct {
		resource.Resource

		membershipUpdateCh chan *membership.ChangedEvent
		engineFactory      EngineFactory
		status             int32
		shuttingDown       int32
		shutdownWG         sync.WaitGroup
		shutdownCh         chan struct{}
		logger             log.Logger
		throttledLogger    log.Logger
		config             *configs.Config
		metricsScope       metrics.Scope

		sync.RWMutex
		historyShards map[int32]*historyShardsItem
	}

	historyShardsItemStatus int

	historyShardsItem struct {
		resource.Resource

		shardID         int32
		config          *configs.Config
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
	config *configs.Config,
) *shardController {
	hostIdentity := resource.GetHostInfo().Identity()
	return &shardController{
		Resource:           resource,
		status:             common.DaemonStatusInitialized,
		membershipUpdateCh: make(chan *membership.ChangedEvent, 10),
		engineFactory:      factory,
		historyShards:      make(map[int32]*historyShardsItem),
		shutdownCh:         make(chan struct{}),
		logger:             resource.GetLogger().WithTags(tag.ComponentShardController, tag.Address(hostIdentity)),
		throttledLogger:    resource.GetThrottledLogger().WithTags(tag.ComponentShardController, tag.Address(hostIdentity)),
		config:             config,
		metricsScope:       resource.GetMetricsClient().Scope(metrics.HistoryShardControllerScope),
	}
}

func newHistoryShardsItem(
	resource resource.Resource,
	shardID int32,
	factory EngineFactory,
	config *configs.Config,
) (*historyShardsItem, error) {

	hostIdentity := resource.GetHostInfo().Identity()
	shardItem := &historyShardsItem{
		Resource:        resource,
		shardID:         shardID,
		status:          historyShardsItemStatusInitialized,
		engineFactory:   factory,
		config:          config,
		logger:          resource.GetLogger().WithTags(tag.ShardID(shardID), tag.Address(hostIdentity)),
		throttledLogger: resource.GetThrottledLogger().WithTags(tag.ShardID(shardID), tag.Address(hostIdentity)),
	}
	shardItem.logger = shardItem.logger.WithTags(tag.ShardItem(shardItem))
	shardItem.throttledLogger = shardItem.throttledLogger.WithTags(tag.ShardItem(shardItem))

	return shardItem, nil
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

	c.PrepareToStop()

	if err := c.GetHistoryServiceResolver().RemoveListener(shardControllerMembershipUpdateListenerName); err != nil {
		c.logger.Error("Error removing membership update listener", tag.Error(err), tag.OperationFailed)
	}
	close(c.shutdownCh)

	if success := common.AwaitWaitGroup(&c.shutdownWG, time.Minute); !success {
		c.logger.Warn("", tag.LifeCycleStopTimedout)
	}

	c.logger.Info("", tag.LifeCycleStopped)
}

// PrepareToStop starts the graceful shutdown process for controller
func (c *shardController) PrepareToStop() {
	atomic.StoreInt32(&c.shuttingDown, 1)
}

func (c *shardController) isShuttingDown() bool {
	return atomic.LoadInt32(&c.shuttingDown) != 0
}

func (c *shardController) GetEngine(namespaceID, workflowID string) (Engine, error) {
	shardID := c.config.GetShardID(namespaceID, workflowID)
	return c.getEngineForShard(shardID)
}

func (c *shardController) getEngineForShard(shardID int32) (Engine, error) {
	sw := c.metricsScope.StartTimer(metrics.GetEngineForShardLatency)
	defer sw.Stop()
	item, err := c.getOrCreateHistoryShardItem(shardID)
	if err != nil {
		return nil, err
	}
	return item.getOrCreateEngine(c.shardClosedCallback)
}

func (c *shardController) removeEngineForShard(shardID int32, shardItem *historyShardsItem) {
	sw := c.metricsScope.StartTimer(metrics.RemoveEngineForShardLatency)
	defer sw.Stop()
	currentShardItem, _ := c.removeHistoryShardItem(shardID, shardItem)
	if shardItem != nil {
		// if shardItem is not nil, then currentShardItem either equals to shardItem or is nil
		// in both cases, we need to stop the engine in shardItem
		shardItem.stopEngine()
		return
	}

	// if shardItem is nil, then stop the engine for the current shardItem, if exists
	if currentShardItem != nil {
		currentShardItem.stopEngine()
	}
}

func (c *shardController) shardClosedCallback(shardID int32, shardItem *historyShardsItem) {
	c.metricsScope.IncCounter(metrics.ShardClosedCounter)
	c.logger.Info("", tag.LifeCycleStopping, tag.ComponentShard, tag.ShardID(shardID))
	c.removeEngineForShard(shardID, shardItem)
}

func (c *shardController) getOrCreateHistoryShardItem(shardID int32) (*historyShardsItem, error) {
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

	if c.isShuttingDown() || atomic.LoadInt32(&c.status) == common.DaemonStatusStopped {
		return nil, fmt.Errorf("shardController for host '%v' shutting down", c.GetHostInfo().Identity())
	}
	info, err := c.GetHistoryServiceResolver().Lookup(convert.Int32ToString(shardID))
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

	return nil, serviceerrors.NewShardOwnershipLost(c.GetHostInfo().Identity(), info.GetAddress())
}

func (c *shardController) removeHistoryShardItem(shardID int32, shardItem *historyShardsItem) (*historyShardsItem, error) {
	nShards := 0
	c.Lock()
	defer c.Unlock()

	currentShardItem, ok := c.historyShards[shardID]
	if !ok {
		return nil, fmt.Errorf("No item found to remove for shard: %v", shardID)
	}
	if shardItem != nil && currentShardItem != shardItem {
		// the shardItem comparison is a defensive check to make sure we are deleting
		// what we intend to delete.
		return nil, fmt.Errorf("Current shardItem doesn't match the one we intend to delete for shard: %v", shardID)
	}

	delete(c.historyShards, shardID)
	nShards = len(c.historyShards)

	c.metricsScope.IncCounter(metrics.ShardItemRemovedCounter)

	currentShardItem.logger.Info("", tag.LifeCycleStopped, tag.ComponentShardItem, tag.Number(int64(nShards)))
	return currentShardItem, nil
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
		}
	}
}

func (c *shardController) acquireShards() {
	c.metricsScope.IncCounter(metrics.AcquireShardsCounter)
	sw := c.metricsScope.StartTimer(metrics.AcquireShardsLatency)
	defer sw.Stop()

	concurrency := common.MaxInt(c.config.AcquireShardConcurrency(), 1)
	shardActionCh := make(chan int32, concurrency)
	var wg sync.WaitGroup
	wg.Add(concurrency)
	// Spawn workers that would lookup and add/remove shards concurrently.
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for shardID := range shardActionCh {
				if c.isShuttingDown() {
					return
				}
				info, err := c.GetHistoryServiceResolver().Lookup(convert.Int32ToString(shardID))
				if err != nil {
					c.logger.Error("Error looking up host for shardID", tag.Error(err), tag.OperationFailed, tag.ShardID(shardID))
				} else {
					if info.Identity() == c.GetHostInfo().Identity() {
						_, err1 := c.getEngineForShard(shardID)
						if err1 != nil {
							c.metricsScope.IncCounter(metrics.GetEngineForShardErrorCounter)
							c.logger.Error("Unable to create history shard engine", tag.Error(err1), tag.OperationFailed, tag.ShardID(shardID))
						}
					}
				}
			}
		}()
	}
	// Submit tasks to the channel.
	for shardID := int32(1); shardID <= c.config.NumberOfShards; shardID++ {
		shardActionCh <- shardID
		if c.isShuttingDown() {
			return
		}
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

func (i *historyShardsItem) getOrCreateEngine(
	closeCallback func(int32, *historyShardsItem),
) (Engine, error) {
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
		context, err := acquireShard(i, closeCallback)
		if err != nil {
			// invalidate the shardItem so that the same shardItem won't be
			// used to create another shardContext
			i.logger.Info("", tag.LifeCycleStopped, tag.ComponentShardEngine)
			i.status = historyShardsItemStatusStopped
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

func (i *historyShardsItem) String() string {
	// use memory address as shard item's identity
	return fmt.Sprintf("%p", i)
}

func isShardOwnershipLostError(err error) bool {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		return true
	}

	return false
}
