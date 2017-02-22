package history

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/membership"
	"github.com/uber/cadence/common/persistence"
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
		shardClosedCh       chan int
		isStarted           int32
		isStopped           int32
		shutdownWG          sync.WaitGroup
		shutdownCh          chan struct{}
		logger              bark.Logger

		sync.RWMutex
		historyShards map[int]*historyShardsItem
		isStopping    bool
	}

	historyShardsItem struct {
		shardID             int
		shardMgr            persistence.ShardManager
		executionMgrFactory persistence.ExecutionManagerFactory
		engineFactory       EngineFactory
		host                *membership.HostInfo
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
		shardClosedCh:       make(chan int, numberOfShards),
		shutdownCh:          make(chan struct{}),
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueShardController,
		}),
	}
}

func newHistoryShardsItem(shardID int, shardMgr persistence.ShardManager, executionMgrFactory persistence.ExecutionManagerFactory,
	factory EngineFactory, host *membership.HostInfo, logger bark.Logger) *historyShardsItem {
	return &historyShardsItem{
		shardID:             shardID,
		shardMgr:            shardMgr,
		executionMgrFactory: executionMgrFactory,
		engineFactory:       factory,
		host:                host,
		logger: logger.WithFields(bark.Fields{
			tagHistoryShardID: shardID,
		}),
	}
}

func (c *shardController) Start() {
	if !atomic.CompareAndSwapInt32(&c.isStarted, 0, 1) {
		return
	}

	c.acquireShards()

	c.shutdownWG.Add(1)
	go c.shardManagementPump()

	c.hServiceResolver.AddListener(shardControllerMembershipUpdateListenerName, c.membershipUpdateCh)

	logShardControllerStartedEvent(c.logger, c.host.Identity())
}

func (c *shardController) Stop() {
	if !atomic.CompareAndSwapInt32(&c.isStopped, 0, 1) {
		return
	}

	c.Lock()
	c.isStopping = true
	c.Unlock()

	if atomic.LoadInt32(&c.isStarted) == 1 {
		if err := c.hServiceResolver.RemoveListener(shardControllerMembershipUpdateListenerName); err != nil {
			logOperationFailedEvent(c.logger, "Error removing membership update listerner", err)
		}
		close(c.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&c.shutdownWG, time.Minute); !success {
		logShardControllerShutdownTimedoutEvent(c.logger, c.host.Identity())
	}

	logShardControllerShutdownEvent(c.logger, c.host.Identity())
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

	return item.getOrCreateEngine(c.shardClosedCh)
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

	if c.isStopping {
		return nil, fmt.Errorf("ShardController for host '%v' shutting down.", c.host.Identity())
	}
	info, err := c.hServiceResolver.Lookup(string(shardID))
	if err != nil {
		return nil, err
	}

	if info.Identity() == c.host.Identity() {
		shardItem := newHistoryShardsItem(shardID, c.shardMgr, c.executionMgrFactory, c.engineFactory, c.host, c.logger)
		c.historyShards[shardID] = shardItem
		logShardItemCreatedEvent(shardItem.logger, info.Identity(), shardID)
		return shardItem, nil
	}

	return nil, fmt.Errorf("Shard is owned by different host: %v", info.Identity())
}

func (c *shardController) removeHistoryShardItem(shardID int) (*historyShardsItem, error) {
	c.Lock()
	defer c.Unlock()

	item, ok := c.historyShards[shardID]
	if !ok {
		return nil, fmt.Errorf("No item found to remove for shard: %v", shardID)
	}

	delete(c.historyShards, shardID)
	logShardItemRemovedEvent(item.logger, c.host.Identity(), shardID, len(c.historyShards))

	return item, nil
}

func (c *shardController) shardManagementPump() {
	defer c.shutdownWG.Done()

	acquireTicker := time.NewTicker(c.acquireInterval)
	defer acquireTicker.Stop()
	for {
		select {
		case <-c.shutdownCh:
			logShardControllerShuttingDownEvent(c.logger, c.host.Identity())
			c.Lock()
			defer c.Unlock()

			for _, item := range c.historyShards {
				item.stopEngine()
			}
			c.historyShards = nil
			return
		case <-acquireTicker.C:
			c.acquireShards()
		case changedEvent := <-c.membershipUpdateCh:
			logRingMembershipChangedEvent(c.logger, c.host.Identity(), len(changedEvent.HostsAdded),
				len(changedEvent.HostsRemoved), len(changedEvent.HostsUpdated))
			c.acquireShards()
		case shardID := <-c.shardClosedCh:
			logShardClosedEvent(c.logger, c.host.Identity(), shardID)
			c.removeEngineForShard(shardID)
		}
	}
}

func (c *shardController) acquireShards() {
AcquireLoop:
	for shardID := 0; shardID < c.numberOfShards; shardID++ {
		info, err := c.hServiceResolver.Lookup(string(shardID))
		if err != nil {
			logOperationFailedEvent(c.logger, fmt.Sprintf("Error looking up host for shardID: %v", shardID), err)
			continue AcquireLoop
		}

		if info.Identity() == c.host.Identity() {
			_, err1 := c.getEngineForShard(shardID)
			if err1 != nil {
				logOperationFailedEvent(c.logger, fmt.Sprintf("Unable to create history shard engine: %v", shardID),
					err1)
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

func (i *historyShardsItem) getOrCreateEngine(shardClosedCh chan<- int) (Engine, error) {
	i.RLock()
	if i.engine != nil {
		defer i.RUnlock()
		return i.engine, nil
	}
	i.RUnlock()

	i.Lock()
	defer i.Unlock()

	if i.engine != nil {
		return i.engine, nil
	}

	logShardEngineCreatingEvent(i.logger, i.host.Identity(), i.shardID)
	defer logShardEngineCreatedEvent(i.logger, i.host.Identity(), i.shardID)
	executionMgr, err := i.executionMgrFactory.CreateExecutionManager(i.shardID)
	if err != nil {
		return nil, err
	}

	context, err := acquireShard(i.shardID, i.shardMgr, executionMgr, i.host.Identity(), shardClosedCh, i.logger)
	if err != nil {
		return nil, err
	}

	i.engine = i.engineFactory.CreateEngine(context)
	i.engine.Start()

	return i.engine, nil
}

func (i *historyShardsItem) stopEngine() {
	logShardEngineStoppingEvent(i.logger, i.host.Identity(), i.shardID)
	defer logShardEngineStoppedEvent(i.logger, i.host.Identity(), i.shardID)
	i.Lock()
	defer i.Unlock()

	if i.engine != nil {
		i.engine.Stop()
		i.engine = nil
	}
}

func isShardOwnershiptLostError(err error) bool {
	switch err.(type) {
	case *persistence.ShardOwnershipLostError:
		return true
	}

	return false
}
