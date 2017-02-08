package history

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"code.uber.internal/devexp/minions/common"
	"code.uber.internal/devexp/minions/common/membership"
	"code.uber.internal/devexp/minions/common/persistence"
)

const (
	defaultAcquireInterval                      = time.Minute
	shardControllerMembershipUpdateListenerName = "ShardController"
)

type (
	shardController struct {
		numberOfShards      int
		host                *membership.HostInfo
		hServiceResolver    membership.ServiceResolver
		membershipUpdateCh  chan *membership.ChangedEvent
		acquireInterval     time.Duration
		shardMgr            persistence.ShardManager
		executionMgrFactory persistence.ExecutionManagerFactory
		engineFactory       EngineFactory
		isStarted           int32
		isStopped           int32
		shutdownWG          sync.WaitGroup
		shutdownCh          chan struct{}
		logger              bark.Logger

		sync.RWMutex
		historyShards map[int]*historyShardsItem
	}

	historyShardsItem struct {
		shardID             int
		shardMgr            persistence.ShardManager
		executionMgrFactory persistence.ExecutionManagerFactory
		engineFactory       EngineFactory
		logger              bark.Logger

		sync.RWMutex
		engine  Engine
		context ShardContext
	}
)

func newShardController(numberOfShards int, host *membership.HostInfo, resolver membership.ServiceResolver,
	shardMgr persistence.ShardManager, executionMgrFactory persistence.ExecutionManagerFactory,
	factory EngineFactory, logger bark.Logger) *shardController {
	return &shardController{
		numberOfShards:      numberOfShards,
		host:                host,
		hServiceResolver:    resolver,
		membershipUpdateCh:  make(chan *membership.ChangedEvent, 10),
		acquireInterval:     defaultAcquireInterval,
		shardMgr:            shardMgr,
		executionMgrFactory: executionMgrFactory,
		engineFactory:       factory,
		historyShards:       make(map[int]*historyShardsItem),
		shutdownCh:          make(chan struct{}),
		logger:              logger,
	}
}

func newHistoryShardsItem(shardID int, shardMgr persistence.ShardManager, executionMgrFactory persistence.ExecutionManagerFactory,
	factory EngineFactory, logger bark.Logger) *historyShardsItem {
	return &historyShardsItem{
		shardID:             shardID,
		shardMgr:            shardMgr,
		executionMgrFactory: executionMgrFactory,
		engineFactory:       factory,
		logger:              logger,
	}
}

func (c *shardController) Start() {
	if !atomic.CompareAndSwapInt32(&c.isStarted, 0, 1) {
		return
	}

	c.acquireShards()

	c.shutdownWG.Add(1)
	go c.acquireShardsPump()

	c.hServiceResolver.AddListener(shardControllerMembershipUpdateListenerName, c.membershipUpdateCh)

	c.logger.Info("ShardController started.")
}

func (c *shardController) Stop() {
	if !atomic.CompareAndSwapInt32(&c.isStopped, 0, 1) {
		return
	}

	if atomic.LoadInt32(&c.isStarted) == 1 {
		if err := c.hServiceResolver.RemoveListener(shardControllerMembershipUpdateListenerName); err != nil {
			c.logger.Warnf("Error removing membership update listerner: %v", err)
		}
		close(c.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&c.shutdownWG, time.Minute); !success {
		c.logger.Warn("ShardController timed out on shutdown.")
	}

	c.logger.Info("ShardController stopped.")
}

func (c *shardController) GetEngine(workflowID string) (Engine, error) {
	shardID := common.WorkflowIDToHistoryShard(workflowID, c.numberOfShards)
	return c.getEngineForShard(shardID)
}

func (c *shardController) getEngineForShard(shardID int) (Engine, error) {
	item, err := c.getOrCreateHistoryShardItem(shardID)
	if err != nil {
		return nil, err
	}

	return item.getOrCreateEngine()
}

func (c *shardController) removeEngineForShard(shardID int) {
	item, _ := c.removeHistoryShardItem(shardID)
	if item != nil {
		item.stopEngine()
	}
}

func (c *shardController) getOrCreateHistoryShardItem(shardID int) (*historyShardsItem, error) {
	c.RLock()
	if item, ok := c.historyShards[shardID]; ok {
		c.RUnlock()
		return item, nil
	}
	c.RUnlock()

	c.Lock()
	defer c.Unlock()

	if item, ok := c.historyShards[shardID]; ok {
		return item, nil
	}

	info, err := c.hServiceResolver.Lookup(string(shardID))
	if err != nil {
		return nil, err
	}

	if info.Identity() == c.host.Identity() {
		c.logger.Infof("Creating new history shard item.  Host: %v, ShardID: %v", info.Identity(), shardID)
		shardItem := newHistoryShardsItem(shardID, c.shardMgr, c.executionMgrFactory, c.engineFactory, c.logger)
		c.historyShards[shardID] = shardItem
		return shardItem, nil
	}

	return nil, fmt.Errorf("Shard is owned by different host: %v", info.Identity())
}

func (c *shardController) removeHistoryShardItem(shardID int) (*historyShardsItem, error) {
	c.Lock()
	defer c.Unlock()
	item, ok := c.historyShards[shardID]
	if ok {
		delete(c.historyShards, shardID)
		c.logger.Infof("Removing history shard item.  Host: %v, ShardID: %v, Count: %v", c.host.Identity(), shardID,
			len(c.historyShards))
		return item, nil
	}

	return nil, fmt.Errorf("No item found to remove for shard: %v", shardID)
}

func (c *shardController) acquireShardsPump() {
	defer c.shutdownWG.Done()

	acquireTicker := time.NewTicker(c.acquireInterval)
	defer acquireTicker.Stop()
	for {
		select {
		case <-c.shutdownCh:
			c.logger.Info("ShardController shutting down.")
			c.Lock()
			defer c.Unlock()

			for shardID, item := range c.historyShards {
				c.logger.Infof("Shutting down engine for shardID: %v", shardID)
				item.stopEngine()
			}
			c.historyShards = nil
			return
		case <-acquireTicker.C:
			c.acquireShards()
		case changedEvent := <-c.membershipUpdateCh:
			c.logger.Infof("Updating shards due to membership changed event: {Added: %v, Removed: %v, Updated: %v}",
				len(changedEvent.HostsAdded), len(changedEvent.HostsRemoved), len(changedEvent.HostsUpdated))
			c.acquireShards()
		}
	}
}

func (c *shardController) acquireShards() {
AcquireLoop:
	for shardID := 0; shardID < c.numberOfShards; shardID++ {
		info, err := c.hServiceResolver.Lookup(string(shardID))
		if err != nil {
			c.logger.Warnf("Error looking up host for shardID: %v, Err: %v", shardID, err)
			continue AcquireLoop
		}

		if info.Identity() == c.host.Identity() {
			_, err1 := c.getEngineForShard(shardID)
			if err1 != nil {
				c.logger.Warnf("Unable to create history shard engine: %v, Err: %v", shardID, err1)
				continue AcquireLoop
			}
		} else {
			c.removeEngineForShard(shardID)
		}
	}
}

func (i *historyShardsItem) getEngine() Engine {
	i.RLock()
	defer i.RUnlock()

	return i.engine
}

func (i *historyShardsItem) getOrCreateEngine() (Engine, error) {
	i.RLock()
	if i.engine != nil {
		i.RUnlock()
		return i.engine, nil
	}
	i.RUnlock()

	i.Lock()
	defer i.Unlock()

	if i.engine != nil {
		return i.engine, nil
	}

	executionMgr, err := i.executionMgrFactory.CreateExecutionManager(i.shardID)
	if err != nil {
		return nil, err
	}

	context, err := acquireShard(i.shardID, i.shardMgr, executionMgr, i.logger)
	if err != nil {
		return nil, err
	}

	i.logger.Infof("Creating new engine for shardID: %v", i.shardID)
	i.engine = i.engineFactory.CreateEngine(context)
	i.engine.Start()

	return i.engine, nil
}

func (i *historyShardsItem) stopEngine() {
	i.Lock()
	defer i.Unlock()

	if i.engine != nil {
		i.logger.Infof("Stopping engine for shardID: %v", i.shardID)
		i.engine.Stop()
	}
}
