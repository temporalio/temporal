package history

import (
	"time"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/persistence"

	"github.com/uber-common/bark"
	"github.com/uber/cadence/common"
)

const (
	historyCacheInitialSize               = 256
	historyCacheMaxSize                   = 1 * 1024
	historyCacheTTL         time.Duration = time.Hour
)

type (
	releaseWorkflowExecutionFunc func()

	historyCache struct {
		cache.Cache
		shard            ShardContext
		executionManager persistence.ExecutionManager
		disabled         bool
		logger           bark.Logger
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
	opts.Pin = true

	return &historyCache{
		Cache:            cache.New(historyCacheMaxSize, opts),
		shard:            shard,
		executionManager: shard.GetExecutionManager(),
		logger: logger.WithFields(bark.Fields{
			tagWorkflowComponent: tagValueHistoryCacheComponent,
		}),
	}
}

func (c *historyCache) getOrCreateWorkflowExecution(domainID string,
	execution workflow.WorkflowExecution) (*workflowExecutionContext, releaseWorkflowExecutionFunc, error) {
	if execution.GetWorkflowId() == "" {
		return nil, nil, &workflow.InternalServiceError{Message: "Can't load workflow execution.  WorkflowId not set."}
	}

	// RunID is not provided, lets try to retrieve the RunID for current active execution
	if execution.GetRunId() == "" {
		response, err := c.getCurrentExecutionWithRetry(&persistence.GetCurrentExecutionRequest{
			DomainID:   domainID,
			WorkflowID: execution.GetWorkflowId(),
		})

		if err != nil {
			return nil, nil, err
		}

		execution.RunId = common.StringPtr(response.RunID)
	}

	// Test hook for disabling the cache
	if c.disabled {
		return newWorkflowExecutionContext(domainID, execution, c.shard, c.executionManager, c.logger), func() {}, nil
	}

	key := execution.GetRunId()
	context, cacheHit := c.Get(key).(*workflowExecutionContext)
	if !cacheHit {
		// Let's create the workflow execution context
		context = newWorkflowExecutionContext(domainID, execution, c.shard, c.executionManager, c.logger)
		elem, err := c.PutIfNotExist(key, context)
		if err != nil {
			return nil, nil, err
		}
		context = elem.(*workflowExecutionContext)
	}

	// This will create a closure on every request.
	// Consider revisiting this if it causes too much GC activity
	releaseFunc := func() {
		context.Unlock()
		c.Release(key)
	}

	context.Lock()
	return context, releaseFunc, nil
}

func (c *historyCache) getCurrentExecutionWithRetry(
	request *persistence.GetCurrentExecutionRequest) (*persistence.GetCurrentExecutionResponse, error) {
	var response *persistence.GetCurrentExecutionResponse
	op := func() error {
		var err error
		response, err = c.executionManager.GetCurrentExecution(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		return nil, err
	}

	return response, nil
}
