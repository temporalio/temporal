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
	"context"
	"sync/atomic"

	"github.com/pborman/uuid"

	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/definition"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

type (
	releaseWorkflowExecutionFunc func(err error)

	historyCache struct {
		cache.Cache
		shard            ShardContext
		executionManager persistence.ExecutionManager
		disabled         bool
		logger           log.Logger
		metricsClient    metrics.Client
		config           *Config
	}
)

var noopReleaseFn releaseWorkflowExecutionFunc = func(err error) {}

const (
	cacheNotReleased int32 = 0
	cacheReleased    int32 = 1
)

func newHistoryCache(shard ShardContext) *historyCache {
	opts := &cache.Options{}
	config := shard.GetConfig()
	opts.InitialCapacity = config.HistoryCacheInitialSize()
	opts.TTL = config.HistoryCacheTTL()
	opts.Pin = true

	return &historyCache{
		Cache:            cache.New(config.HistoryCacheMaxSize(), opts),
		shard:            shard,
		executionManager: shard.GetExecutionManager(),
		logger:           shard.GetLogger().WithTags(tag.ComponentHistoryCache),
		metricsClient:    shard.GetMetricsClient(),
		config:           config,
	}
}

func (c *historyCache) getOrCreateCurrentWorkflowExecution(
	ctx context.Context,
	domainID string,
	workflowID string,
) (workflowExecutionContext, releaseWorkflowExecutionFunc, error) {

	scope := metrics.HistoryCacheGetOrCreateCurrentScope
	c.metricsClient.IncCounter(scope, metrics.CacheRequests)
	sw := c.metricsClient.StartTimer(scope, metrics.CacheLatency)
	defer sw.Stop()

	// using empty run ID as current workflow run ID
	runID := ""
	execution := workflow.WorkflowExecution{
		WorkflowId: common.StringPtr(workflowID),
		RunId:      common.StringPtr(runID),
	}

	return c.getOrCreateWorkflowExecutionInternal(
		ctx,
		domainID,
		execution,
		scope,
		true,
	)
}

// For analyzing mutableState, we have to try get workflowExecutionContext from cache and also load from database
func (c *historyCache) getAndCreateWorkflowExecution(
	ctx context.Context,
	domainID string,
	execution workflow.WorkflowExecution,
) (workflowExecutionContext, workflowExecutionContext, releaseWorkflowExecutionFunc, bool, error) {

	scope := metrics.HistoryCacheGetAndCreateScope
	c.metricsClient.IncCounter(scope, metrics.CacheRequests)
	sw := c.metricsClient.StartTimer(scope, metrics.CacheLatency)
	defer sw.Stop()

	if err := c.validateWorkflowExecutionInfo(domainID, &execution); err != nil {
		c.metricsClient.IncCounter(scope, metrics.CacheFailures)
		return nil, nil, nil, false, err
	}

	key := definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowId(), execution.GetRunId())
	contextFromCache, cacheHit := c.Get(key).(workflowExecutionContext)
	// TODO This will create a closure on every request.
	//  Consider revisiting this if it causes too much GC activity
	releaseFunc := noopReleaseFn
	// If cache hit, we need to lock the cache to prevent race condition
	if cacheHit {
		if err := contextFromCache.lock(ctx); err != nil {
			// ctx is done before lock can be acquired
			c.Release(key)
			c.metricsClient.IncCounter(metrics.HistoryCacheGetAndCreateScope, metrics.CacheFailures)
			c.metricsClient.IncCounter(metrics.HistoryCacheGetAndCreateScope, metrics.AcquireLockFailedCounter)
			return nil, nil, nil, false, err
		}
		releaseFunc = c.makeReleaseFunc(key, contextFromCache, false)
	} else {
		c.metricsClient.IncCounter(metrics.HistoryCacheGetAndCreateScope, metrics.CacheMissCounter)
	}

	// Note, the one loaded from DB is not put into cache and don't affect any behavior
	contextFromDB := newWorkflowExecutionContext(domainID, execution, c.shard, c.executionManager, c.logger)
	return contextFromCache, contextFromDB, releaseFunc, cacheHit, nil
}

func (c *historyCache) getOrCreateWorkflowExecutionForBackground(
	domainID string,
	execution workflow.WorkflowExecution,
) (workflowExecutionContext, releaseWorkflowExecutionFunc, error) {

	return c.getOrCreateWorkflowExecution(context.Background(), domainID, execution)
}

func (c *historyCache) getOrCreateWorkflowExecution(
	ctx context.Context,
	domainID string,
	execution workflow.WorkflowExecution,
) (workflowExecutionContext, releaseWorkflowExecutionFunc, error) {

	scope := metrics.HistoryCacheGetOrCreateScope
	c.metricsClient.IncCounter(scope, metrics.CacheRequests)
	sw := c.metricsClient.StartTimer(scope, metrics.CacheLatency)
	defer sw.Stop()

	if err := c.validateWorkflowExecutionInfo(domainID, &execution); err != nil {
		c.metricsClient.IncCounter(scope, metrics.CacheFailures)
		return nil, nil, err
	}

	return c.getOrCreateWorkflowExecutionInternal(
		ctx,
		domainID,
		execution,
		scope,
		false,
	)
}

func (c *historyCache) getOrCreateWorkflowExecutionInternal(
	ctx context.Context,
	domainID string,
	execution workflow.WorkflowExecution,
	scope int,
	forceClearContext bool,
) (workflowExecutionContext, releaseWorkflowExecutionFunc, error) {

	// Test hook for disabling the cache
	if c.disabled {
		return newWorkflowExecutionContext(domainID, execution, c.shard, c.executionManager, c.logger), noopReleaseFn, nil
	}

	key := definition.NewWorkflowIdentifier(domainID, execution.GetWorkflowId(), execution.GetRunId())
	workflowCtx, cacheHit := c.Get(key).(workflowExecutionContext)
	if !cacheHit {
		c.metricsClient.IncCounter(scope, metrics.CacheMissCounter)
		// Let's create the workflow execution workflowCtx
		workflowCtx = newWorkflowExecutionContext(domainID, execution, c.shard, c.executionManager, c.logger)
		elem, err := c.PutIfNotExist(key, workflowCtx)
		if err != nil {
			c.metricsClient.IncCounter(scope, metrics.CacheFailures)
			return nil, nil, err
		}
		workflowCtx = elem.(workflowExecutionContext)
	}

	// TODO This will create a closure on every request.
	//  Consider revisiting this if it causes too much GC activity
	releaseFunc := c.makeReleaseFunc(key, workflowCtx, forceClearContext)

	if err := workflowCtx.lock(ctx); err != nil {
		// ctx is done before lock can be acquired
		c.Release(key)
		c.metricsClient.IncCounter(scope, metrics.CacheFailures)
		c.metricsClient.IncCounter(scope, metrics.AcquireLockFailedCounter)
		return nil, nil, err
	}
	return workflowCtx, releaseFunc, nil
}

func (c *historyCache) validateWorkflowExecutionInfo(
	domainID string,
	execution *workflow.WorkflowExecution,
) error {

	if execution.GetWorkflowId() == "" {
		return &workflow.BadRequestError{Message: "Can't load workflow execution.  WorkflowId not set."}
	}

	// RunID is not provided, lets try to retrieve the RunID for current active execution
	if execution.GetRunId() == "" {
		response, err := c.getCurrentExecutionWithRetry(&persistence.GetCurrentExecutionRequest{
			DomainID:   domainID,
			WorkflowID: execution.GetWorkflowId(),
		})

		if err != nil {
			return err
		}

		execution.RunId = common.StringPtr(response.RunID)
	} else if uuid.Parse(execution.GetRunId()) == nil { // immediately return if invalid runID
		return &workflow.BadRequestError{Message: "RunID is not valid UUID."}
	}
	return nil
}

func (c *historyCache) makeReleaseFunc(
	key definition.WorkflowIdentifier,
	context workflowExecutionContext,
	forceClearContext bool,
) func(error) {

	status := cacheNotReleased
	return func(err error) {
		if atomic.CompareAndSwapInt32(&status, cacheNotReleased, cacheReleased) {
			if rec := recover(); rec != nil {
				context.clear()
				context.unlock()
				c.Release(key)
				panic(rec)
			} else {
				if err != nil || forceClearContext {
					// TODO see issue #668, there are certain type or errors which can bypass the clear
					context.clear()
				}
				context.unlock()
				c.Release(key)
			}
		}
	}
}

func (c *historyCache) getCurrentExecutionWithRetry(
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.GetCurrentExecutionResponse, error) {

	c.metricsClient.IncCounter(metrics.HistoryCacheGetCurrentExecutionScope, metrics.CacheRequests)
	sw := c.metricsClient.StartTimer(metrics.HistoryCacheGetCurrentExecutionScope, metrics.CacheLatency)
	defer sw.Stop()

	var response *persistence.GetCurrentExecutionResponse
	op := func() error {
		var err error
		response, err = c.executionManager.GetCurrentExecution(request)

		return err
	}

	err := backoff.Retry(op, persistenceOperationRetryPolicy, common.IsPersistenceTransientError)
	if err != nil {
		c.metricsClient.IncCounter(metrics.HistoryCacheGetCurrentExecutionScope, metrics.CacheFailures)
		return nil, err
	}

	return response, nil
}
