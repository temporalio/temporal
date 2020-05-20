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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination controller_mock.go -self_package github.com/uber/cadence/service/history/shard

package shard

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/engine"
	"github.com/uber/cadence/service/history/resource"
)

const (
	shardControllerMembershipUpdateListenerName = "ShardController"
)

type (
	// EngineFactory is used to create an instance of sharded history engine
	EngineFactory interface {
		CreateEngine(Context) engine.Engine
	}

	// Controller controls history service shards
	Controller interface {
		common.Daemon

		// PrepareToStop starts the graceful shutdown process for controller
		PrepareToStop()

		GetEngine(workflowID string) (engine.Engine, error)
		GetEngineForShard(shardID int) (engine.Engine, error)
		RemoveEngineForShard(shardID int)

		// Following methods describes the current status of the controller
		// TODO: consider converting to a unified describe method
		Status() int32
		NumShards() int
		ShardIDs() []int32
	}

	controller struct {
		resource.Resource

		membershipUpdateCh chan *membership.ChangedEvent
		engineFactory      EngineFactory
		status             int32
		shuttingDown       int32
		shutdownWG         sync.WaitGroup
		shutdownCh         chan struct{}
		logger             log.Logger
		throttledLogger    log.Logger
		config             *config.Config
		metricsScope       metrics.Scope

		sync.RWMutex
		historyShards map[int]*historyShardsItem
	}

	historyShardsItemStatus int

	historyShardsItem struct {
		resource.Resource

		shardID         int
		config          *config.Config
		logger          log.Logger
		throttledLogger log.Logger
		engineFactory   EngineFactory

		sync.RWMutex
		status historyShardsItemStatus
		engine engine.Engine
	}
)

const (
	historyShardsItemStatusInitialized = iota
	historyShardsItemStatusStarted
	historyShardsItemStatusStopped
)

// NewShardController creates a new shard controller
func NewShardController(
	resource resource.Resource,
	factory EngineFactory,
	config *config.Config,
) Controller {
	hostIdentity := resource.GetHostInfo().Identity()
	return &controller{
		Resource:           resource,
		status:             common.DaemonStatusInitialized,
		membershipUpdateCh: make(chan *membership.ChangedEvent, 10),
		engineFactory:      factory,
		historyShards:      make(map[int]*historyShardsItem),
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
	config *config.Config,
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

func (c *controller) Start() {
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

func (c *controller) Stop() {
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

func (c *controller) PrepareToStop() {
	atomic.StoreInt32(&c.shuttingDown, 1)
}

func (c *controller) GetEngine(workflowID string) (engine.Engine, error) {
	shardID := c.config.GetShardID(workflowID)
	return c.GetEngineForShard(shardID)
}

func (c *controller) GetEngineForShard(shardID int) (engine.Engine, error) {
	sw := c.metricsScope.StartTimer(metrics.GetEngineForShardLatency)
	defer sw.Stop()
	item, err := c.getOrCreateHistoryShardItem(shardID)
	if err != nil {
		return nil, err
	}
	return item.getOrCreateEngine(c.shardClosedCallback)
}

func (c *controller) RemoveEngineForShard(shardID int) {
	c.removeEngineForShard(shardID, nil)
}

func (c *controller) Status() int32 {
	return atomic.LoadInt32(&c.status)
}

func (c *controller) NumShards() int {
	nShards := 0
	c.RLock()
	nShards = len(c.historyShards)
	c.RUnlock()
	return nShards
}

func (c *controller) ShardIDs() []int32 {
	c.RLock()
	ids := []int32{}
	for id := range c.historyShards {
		id32 := int32(id)
		ids = append(ids, id32)
	}
	c.RUnlock()
	return ids
}

func (c *controller) removeEngineForShard(shardID int, shardItem *historyShardsItem) {
	sw := c.metricsScope.StartTimer(metrics.RemoveEngineForShardLatency)
	defer sw.Stop()
	item, _ := c.removeHistoryShardItem(shardID)
	// the shardItem comparison is just a defensive check to make sure we are deleting
	// what we intend to delete. In the event that multiple callers call removeEngine / getEngine
	// concurrently, it is possible to reorder a delete/delete/add sequence into a delete/add/delete
	// sequence. This check is to protect against those scenarios.
	if item != nil && (item == shardItem || shardItem == nil) {
		item.stopEngine()
	}
}

func (c *controller) shardClosedCallback(shardID int, shardItem *historyShardsItem) {
	c.metricsScope.IncCounter(metrics.ShardClosedCounter)
	c.logger.Info("", tag.LifeCycleStopping, tag.ComponentShard, tag.ShardID(shardID))
	c.removeEngineForShard(shardID, shardItem)
}

func (c *controller) getOrCreateHistoryShardItem(shardID int) (*historyShardsItem, error) {
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
		return nil, fmt.Errorf("controller for host '%v' shutting down", c.GetHostInfo().Identity())
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

	return nil, CreateShardOwnershipLostError(c.GetHostInfo().Identity(), info.GetAddress())
}

func (c *controller) removeHistoryShardItem(shardID int) (*historyShardsItem, error) {
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
// controller. It is responsible for acquiring /
// releasing shards in response to any event that can
// change the shard ownership. These events are
//   a. Ring membership change
//   b. Periodic ticker
//   c. ShardOwnershipLostError and subsequent ShardClosedEvents from engine
func (c *controller) shardManagementPump() {

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

func (c *controller) acquireShards() {
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
				if c.isShuttingDown() {
					return
				}
				info, err := c.GetHistoryServiceResolver().Lookup(string(shardID))
				if err != nil {
					c.logger.Error("Error looking up host for shardID", tag.Error(err), tag.OperationFailed, tag.ShardID(shardID))
				} else {
					if info.Identity() == c.GetHostInfo().Identity() {
						_, err1 := c.GetEngineForShard(shardID)
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
	for shardID := 0; shardID < c.config.NumberOfShards; shardID++ {
		shardActionCh <- shardID
		if c.isShuttingDown() {
			return
		}
	}
	close(shardActionCh)
	// Wait until all shards are processed.
	wg.Wait()

	c.metricsScope.UpdateGauge(metrics.NumShardsGauge, float64(c.NumShards()))
}

func (c *controller) doShutdown() {
	c.logger.Info("", tag.LifeCycleStopping)
	c.Lock()
	defer c.Unlock()
	for _, item := range c.historyShards {
		item.stopEngine()
	}
	c.historyShards = nil
}

func (c *controller) isShuttingDown() bool {
	return atomic.LoadInt32(&c.shuttingDown) != 0
}

func (i *historyShardsItem) getOrCreateEngine(
	closeCallback func(int, *historyShardsItem),
) (engine.Engine, error) {
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

// IsShardOwnershiptLostError checks if a given error is shard ownership lost error
func IsShardOwnershiptLostError(err error) bool {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		return true
	}

	return false
}

// CreateShardOwnershipLostError creates a new shard ownership lost error
func CreateShardOwnershipLostError(
	currentHost string,
	ownerHost string,
) *h.ShardOwnershipLostError {

	shardLostErr := &h.ShardOwnershipLostError{}
	shardLostErr.Message = common.StringPtr(fmt.Sprintf("Shard is not owned by host: %v", currentHost))
	shardLostErr.Owner = common.StringPtr(ownerHost)

	return shardLostErr
}
