package history

import (
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
)

const (
	historyCacheInitialSize               = 256
	historyCacheMaxSize                   = 1 * 1024
	historyCacheTTL         time.Duration = time.Hour
)

type (
	historyCache struct {
		cache.Cache
		shard                 ShardContext
		executionManager      persistence.ExecutionManager
		disabled              bool
		logger                bark.Logger
	}
)

var (
	// ErrTryLock is a temporary error that is thrown by the API
	// when it loses the race to create workflow execution context
	ErrTryLock = &workflow.InternalServiceError{Message: "Failed to acquire lock, backoff and retry"}
)

func newHistoryCache(shard ShardContext, logger bark.Logger) *historyCache {
	opts := &cache.Options{}
	opts.InitialCapacity = historyCacheInitialSize
	opts.TTL = historyCacheTTL

	return &historyCache{
		Cache:                 cache.New(historyCacheMaxSize, opts),
		shard:                 shard,
		executionManager:      shard.GetExecutionManager(),
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueHistoryCacheComponent,
		}),
	}
}

func (c *historyCache) getOrCreateWorkflowExecution(execution workflow.WorkflowExecution) (*workflowExecutionContext,
	error) {
	// Test hook for disabling the cache
	if c.disabled {
		return newWorkflowExecutionContext(execution, c.shard, c.executionManager, c.logger), nil
	}

	key := execution.GetRunId()
	context, cacheHit := c.Get(key).(*workflowExecutionContext)
	if cacheHit {
		return context, nil
	}

	// Let's create the workflow execution context
	context = newWorkflowExecutionContext(execution, c.shard, c.executionManager, c.logger)
	context = c.PutIfNotExist(key, context).(*workflowExecutionContext)

	return context, nil
}
