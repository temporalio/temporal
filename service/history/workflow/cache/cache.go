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

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination cache_mock.go

package cache

import (
	"context"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	// ReleaseCacheFunc must be called to release the workflow context from the cache.
	// Make sure not to access the mutable state or workflow context after releasing back to the cache.
	// If there is any error when using the mutable state (e.g. mutable state is mutated and dirty), call release with
	// the error so the in-memory copy will be thrown away.
	ReleaseCacheFunc func(err error)

	Cache interface {
		GetOrCreateCurrentWorkflowExecution(
			ctx context.Context,
			shardContext shard.Context,
			namespaceID namespace.ID,
			workflowID string,
			lockPriority workflow.LockPriority,
		) (ReleaseCacheFunc, error)

		GetOrCreateWorkflowExecution(
			ctx context.Context,
			shardContext shard.Context,
			namespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			lockPriority workflow.LockPriority,
		) (workflow.Context, ReleaseCacheFunc, error)
	}

	CacheImpl struct {
		cache.Cache

		nonUserContextLockTimeout time.Duration
	}

	NewCacheFn func(config *configs.Config) Cache

	Key struct {
		// Those are exported because some unit tests uses the cache directly.
		// TODO: Update the unit tests and make those fields private.
		WorkflowKey definition.WorkflowKey
		ShardUUID   string
	}
)

var NoopReleaseFn ReleaseCacheFunc = func(err error) {}

const (
	cacheNotReleased int32 = 0
	cacheReleased    int32 = 1
)

const (
	workflowLockTimeoutTailTime = 500 * time.Millisecond
)

func NewHostLevelCache(
	config *configs.Config,
) Cache {
	return newCache(
		config.HistoryCacheMaxSize(),
		config.HistoryCacheTTL(),
		config.HistoryCacheNonUserContextLockTimeout(),
	)
}

func NewShardLevelCache(
	config *configs.Config,
) Cache {
	return newCache(
		config.HistoryShardLevelCacheMaxSize(),
		config.HistoryCacheTTL(),
		config.HistoryCacheNonUserContextLockTimeout(),
	)
}

func newCache(
	size int,
	ttl time.Duration,
	nonUserContextLockTimeout time.Duration,
) Cache {
	opts := &cache.Options{}
	opts.TTL = ttl
	opts.Pin = true

	return &CacheImpl{
		Cache:                     cache.New(size, opts),
		nonUserContextLockTimeout: nonUserContextLockTimeout,
	}
}

func (c *CacheImpl) GetOrCreateCurrentWorkflowExecution(
	ctx context.Context,
	shardContext shard.Context,
	namespaceID namespace.ID,
	workflowID string,
	lockPriority workflow.LockPriority,
) (ReleaseCacheFunc, error) {

	if err := c.validateWorkflowID(workflowID); err != nil {
		return nil, err
	}

	handler := shardContext.GetMetricsHandler().WithTags(
		metrics.OperationTag(metrics.HistoryCacheGetOrCreateCurrentScope),
		metrics.CacheTypeTag(metrics.MutableStateCacheTypeTagValue),
	)
	handler.Counter(metrics.CacheRequests.Name()).Record(1)
	start := time.Now()
	defer func() { handler.Timer(metrics.CacheLatency.Name()).Record(time.Since(start)) }()

	execution := commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		// using empty run ID as current workflow run ID
		RunId: "",
	}

	_, weReleaseFn, err := c.getOrCreateWorkflowExecutionInternal(
		ctx,
		shardContext,
		namespaceID,
		&execution,
		handler,
		true,
		lockPriority,
	)

	metrics.ContextCounterAdd(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name(), time.Since(start).Nanoseconds())

	return weReleaseFn, err
}

func (c *CacheImpl) GetOrCreateWorkflowExecution(
	ctx context.Context,
	shardContext shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	lockPriority workflow.LockPriority,
) (workflow.Context, ReleaseCacheFunc, error) {

	if err := c.validateWorkflowExecutionInfo(ctx, shardContext, namespaceID, execution); err != nil {
		return nil, nil, err
	}

	handler := shardContext.GetMetricsHandler().WithTags(
		metrics.OperationTag(metrics.HistoryCacheGetOrCreateScope),
		metrics.CacheTypeTag(metrics.MutableStateCacheTypeTagValue),
	)
	handler.Counter(metrics.CacheRequests.Name()).Record(1)
	start := time.Now()
	defer func() { handler.Timer(metrics.CacheLatency.Name()).Record(time.Since(start)) }()

	weCtx, weReleaseFunc, err := c.getOrCreateWorkflowExecutionInternal(
		ctx,
		shardContext,
		namespaceID,
		execution,
		handler,
		false,
		lockPriority,
	)

	metrics.ContextCounterAdd(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name(), time.Since(start).Nanoseconds())

	return weCtx, weReleaseFunc, err
}

func (c *CacheImpl) getOrCreateWorkflowExecutionInternal(
	ctx context.Context,
	shardContext shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	handler metrics.Handler,
	forceClearContext bool,
	lockPriority workflow.LockPriority,
) (workflow.Context, ReleaseCacheFunc, error) {

	cacheKey := Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId()),
		ShardUUID:   shardContext.GetOwner(),
	}
	workflowCtx, cacheHit := c.Get(cacheKey).(workflow.Context)
	if !cacheHit {
		handler.Counter(metrics.CacheMissCounter.Name()).Record(1)
		// Let's create the workflow execution workflowCtx
		workflowCtx = workflow.NewContext(shardContext.GetConfig(), cacheKey.WorkflowKey, shardContext.GetLogger(), shardContext.GetThrottledLogger(), shardContext.GetMetricsHandler())
		elem, err := c.PutIfNotExist(cacheKey, workflowCtx)
		if err != nil {
			handler.Counter(metrics.CacheFailures.Name()).Record(1)
			return nil, nil, err
		}
		workflowCtx = elem.(workflow.Context)
	}
	// TODO This will create a closure on every request.
	//  Consider revisiting this if it causes too much GC activity
	releaseFunc := c.makeReleaseFunc(cacheKey, shardContext, workflowCtx, forceClearContext, lockPriority)

	if err := c.lockWorkflowExecution(ctx, workflowCtx, cacheKey, lockPriority); err != nil {
		handler.Counter(metrics.CacheFailures.Name()).Record(1)
		handler.Counter(metrics.AcquireLockFailedCounter.Name()).Record(1)
		return nil, nil, err
	}

	return workflowCtx, releaseFunc, nil
}

func (c *CacheImpl) lockWorkflowExecution(
	ctx context.Context,
	workflowCtx workflow.Context,
	cacheKey Key,
	lockPriority workflow.LockPriority,
) error {
	// skip if there is no deadline
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		if headers.GetCallerInfo(ctx).CallerType != headers.CallerTypeAPI {
			newDeadline := time.Now().Add(c.nonUserContextLockTimeout)
			if newDeadline.Before(deadline) {
				ctx, cancel = context.WithDeadline(ctx, newDeadline)
				defer cancel()
			}
		} else {
			newDeadline := deadline.Add(-workflowLockTimeoutTailTime)
			if newDeadline.After(time.Now()) {
				ctx, cancel = context.WithDeadline(ctx, newDeadline)
				defer cancel()
			}
		}
	}

	if err := workflowCtx.Lock(ctx, lockPriority); err != nil {
		// ctx is done before lock can be acquired
		c.Release(cacheKey)
		return consts.ErrResourceExhaustedBusyWorkflow
	}
	return nil
}

func (c *CacheImpl) makeReleaseFunc(
	cacheKey Key,
	shardContext shard.Context,
	context workflow.Context,
	forceClearContext bool,
	lockPriority workflow.LockPriority,
) func(error) {

	status := cacheNotReleased
	return func(err error) {
		if atomic.CompareAndSwapInt32(&status, cacheNotReleased, cacheReleased) {
			if rec := recover(); rec != nil {
				context.Clear()
				context.Unlock(lockPriority)
				c.Release(cacheKey)
				panic(rec)
			} else {
				if err != nil || forceClearContext {
					// TODO see issue #668, there are certain type or errors which can bypass the clear
					context.Clear()
					context.Unlock(lockPriority)
					c.Release(cacheKey)
				} else {
					isDirty := context.IsDirty()
					if isDirty {
						context.Clear()
						logger := log.With(shardContext.GetLogger(), tag.ComponentHistoryCache)
						logger.Error("Cache encountered dirty mutable state transaction",
							tag.WorkflowNamespaceID(context.GetWorkflowKey().NamespaceID),
							tag.WorkflowID(context.GetWorkflowKey().WorkflowID),
							tag.WorkflowRunID(context.GetWorkflowKey().RunID),
						)
					}
					context.Unlock(lockPriority)
					c.Release(cacheKey)
					if isDirty {
						panic("Cache encountered dirty mutable state transaction")
					}
				}
			}
		}
	}
}

func (c *CacheImpl) validateWorkflowExecutionInfo(
	ctx context.Context,
	shardContext shard.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
) error {

	if err := c.validateWorkflowID(execution.GetWorkflowId()); err != nil {
		return err
	}

	// RunID is not provided, lets try to retrieve the RunID for current active execution
	if execution.GetRunId() == "" {
		response, err := shardContext.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
			ShardID:     shardContext.GetShardID(),
			NamespaceID: namespaceID.String(),
			WorkflowID:  execution.GetWorkflowId(),
		})

		if err != nil {
			return err
		}

		execution.RunId = response.RunID
	} else if uuid.Parse(execution.GetRunId()) == nil { // immediately return if invalid runID
		return serviceerror.NewInvalidArgument("RunId is not valid UUID.")
	}
	return nil
}

func (c *CacheImpl) validateWorkflowID(
	workflowID string,
) error {
	if workflowID == "" {
		return serviceerror.NewInvalidArgument("Can't load workflow execution.  WorkflowId not set.")
	}

	if !utf8.ValidString(workflowID) {
		// We know workflow cannot exist with invalid utf8 string as WorkflowID.
		return serviceerror.NewNotFound("Workflow not exists.")
	}

	return nil
}
