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
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	ReleaseCacheFunc func(err error)

	Cache interface {
		GetOrCreateWorkflowExecution(
			ctx context.Context,
			namespaceID namespace.ID,
			execution commonpb.WorkflowExecution,
			caller workflow.CallerType,
		) (workflow.Context, ReleaseCacheFunc, error)
	}

	CacheImpl struct {
		cache.Cache
		shard          shard.Context
		logger         log.Logger
		metricsHandler metrics.MetricsHandler
		config         *configs.Config
	}

	NewCacheFn func(shard shard.Context) Cache
)

var NoopReleaseFn ReleaseCacheFunc = func(err error) {}

const (
	cacheNotReleased int32 = 0
	cacheReleased    int32 = 1
)

func NewCache(shard shard.Context) Cache {
	opts := &cache.Options{}
	config := shard.GetConfig()
	opts.InitialCapacity = config.HistoryCacheInitialSize()
	opts.TTL = config.HistoryCacheTTL()
	opts.Pin = true

	return &CacheImpl{
		Cache:          cache.New(config.HistoryCacheMaxSize(), opts),
		shard:          shard,
		logger:         log.With(shard.GetLogger(), tag.ComponentHistoryCache),
		metricsHandler: shard.GetMetricsHandler().WithTags(metrics.CacheTypeTag(metrics.MutableStateCacheTypeTagValue)),
		config:         config,
	}
}

func (c *CacheImpl) GetOrCreateWorkflowExecution(
	ctx context.Context,
	namespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
	caller workflow.CallerType,
) (workflow.Context, ReleaseCacheFunc, error) {

	if err := c.validateWorkflowExecutionInfo(ctx, namespaceID, &execution); err != nil {
		return nil, nil, err
	}

	handler := c.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryCacheGetOrCreateScope))
	handler.Counter(metrics.CacheRequests.GetMetricName()).Record(1)
	start := time.Now()
	defer func() { handler.Timer(metrics.CacheLatency.GetMetricName()).Record(time.Since(start)) }()

	weCtx, weReleaseFunc, err := c.getOrCreateWorkflowExecutionInternal(
		ctx,
		namespaceID,
		execution,
		handler,
		false,
		caller,
	)

	metrics.ContextCounterAdd(ctx, metrics.HistoryWorkflowExecutionCacheLatency.GetMetricName(), time.Since(start).Nanoseconds())

	return weCtx, weReleaseFunc, err
}

func (c *CacheImpl) getOrCreateWorkflowExecutionInternal(
	ctx context.Context,
	namespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
	handler metrics.MetricsHandler,
	forceClearContext bool,
	caller workflow.CallerType,
) (workflow.Context, ReleaseCacheFunc, error) {

	key := definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId())
	workflowCtx, cacheHit := c.Get(key).(workflow.Context)
	if !cacheHit {
		handler.Counter(metrics.CacheMissCounter.GetMetricName()).Record(1)
		// Let's create the workflow execution workflowCtx
		workflowCtx = workflow.NewContext(c.shard, key, c.logger)
		elem, err := c.PutIfNotExist(key, workflowCtx)
		if err != nil {
			handler.Counter(metrics.CacheFailures.GetMetricName()).Record(1)
			return nil, nil, err
		}
		workflowCtx = elem.(workflow.Context)
	}

	// TODO This will create a closure on every request.
	//  Consider revisiting this if it causes too much GC activity
	releaseFunc := c.makeReleaseFunc(key, workflowCtx, forceClearContext, caller)

	if err := workflowCtx.Lock(ctx, caller); err != nil {
		// ctx is done before lock can be acquired
		c.Release(key)
		handler.Counter(metrics.CacheFailures.GetMetricName()).Record(1)
		handler.Counter(metrics.AcquireLockFailedCounter.GetMetricName()).Record(1)
		return nil, nil, err
	}
	return workflowCtx, releaseFunc, nil
}

func (c *CacheImpl) makeReleaseFunc(
	key definition.WorkflowKey,
	context workflow.Context,
	forceClearContext bool,
	caller workflow.CallerType,
) func(error) {

	status := cacheNotReleased
	return func(err error) {
		if atomic.CompareAndSwapInt32(&status, cacheNotReleased, cacheReleased) {
			if rec := recover(); rec != nil {
				context.Clear()
				context.Unlock(caller)
				c.Release(key)
				panic(rec)
			} else {
				if err != nil || forceClearContext {
					// TODO see issue #668, there are certain type or errors which can bypass the clear
					context.Clear()
				}
				context.Unlock(caller)
				c.Release(key)
			}
		}
	}
}

func (c *CacheImpl) validateWorkflowExecutionInfo(
	ctx context.Context,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
) error {

	if execution.GetWorkflowId() == "" {
		return serviceerror.NewInvalidArgument("Can't load workflow execution.  WorkflowId not set.")
	}

	if !utf8.ValidString(execution.GetWorkflowId()) {
		// We know workflow cannot exist with invalid utf8 string as WorkflowID.
		return serviceerror.NewNotFound("Workflow not exists.")
	}

	// RunID is not provided, lets try to retrieve the RunID for current active execution
	if execution.GetRunId() == "" {
		response, err := c.shard.GetCurrentExecution(ctx, &persistence.GetCurrentExecutionRequest{
			ShardID:     c.shard.GetShardID(),
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
