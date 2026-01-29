//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination cache_mock.go

package cache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/cache"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/finalizer"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

type (
	Cache interface {
		GetOrCreateCurrentExecution(
			ctx context.Context,
			shardContext historyi.ShardContext,
			namespaceID namespace.ID,
			workflowID string,
			archetypeID chasm.ArchetypeID,
			lockPriority locks.Priority,
		) (historyi.ReleaseWorkflowContextFunc, error)

		GetOrCreateWorkflowExecution(
			ctx context.Context,
			shardContext historyi.ShardContext,
			namespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			lockPriority locks.Priority,
		) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error)

		GetOrCreateChasmExecution(
			ctx context.Context,
			shardContext historyi.ShardContext,
			namespaceID namespace.ID,
			execution *commonpb.WorkflowExecution,
			archetypeID chasm.ArchetypeID,
			lockPriority locks.Priority,
		) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error)
	}

	cacheImpl struct {
		cache.Cache

		onPut                     func(wfContext *historyi.WorkflowContext)
		onEvict                   func(wfContext *historyi.WorkflowContext)
		nonUserContextLockTimeout time.Duration
	}
	cacheItem struct {
		shardId   int32
		wfContext historyi.WorkflowContext
		finalizer *finalizer.Finalizer
	}

	Key struct {
		// Those are exported because some unit tests uses the cache directly.
		// TODO: Update the unit tests and make those fields private.
		WorkflowKey definition.WorkflowKey
		ArchetypeID chasm.ArchetypeID
		ShardUUID   string
	}
)

var NoopReleaseFn historyi.ReleaseWorkflowContextFunc = func(err error) {}

const (
	cacheNotReleased int32 = 0
	cacheReleased    int32 = 1
)

const (
	workflowLockTimeoutTailTime = 500 * time.Millisecond
)

func NewHostLevelCache(
	config *configs.Config,
	logger log.Logger,
	handler metrics.Handler,
) Cache {
	maxSize := config.HistoryHostLevelCacheMaxSize()
	if config.HistoryCacheLimitSizeBased {
		maxSize = config.HistoryHostLevelCacheMaxSizeBytes()
	}
	opts := &cache.Options{
		TTL:             config.HistoryCacheTTL(),
		Pin:             true,
		BackgroundEvict: config.HistoryCacheBackgroundEvict,
		OnPut: func(val any) {
			//revive:disable-next-line:unchecked-type-assertion
			item := val.(*cacheItem)
			if item.finalizer == nil {
				return // should only happen in unit tests
			}
			wfKey := item.wfContext.GetWorkflowKey()
			err := item.finalizer.Register(wfKey.String(), func(ctx context.Context) error {
				if err := item.wfContext.Lock(ctx, locks.PriorityHigh); err != nil {
					return err
				}
				defer item.wfContext.Unlock()
				item.wfContext.Clear()
				return nil
			})
			if err != nil {
				logger.Debug("cache failed to register callback in finalizer",
					tag.Error(err), tag.ShardID(item.shardId))
			}
		},
		OnEvict: func(val any) {
			//revive:disable-next-line:unchecked-type-assertion
			item := val.(*cacheItem)
			if item.finalizer == nil {
				return // should only happen in unit tests
			}
			wfKey := item.wfContext.GetWorkflowKey()
			err := item.finalizer.Deregister(wfKey.String())
			if err != nil {
				// debug level since this is very common: the cache item was registered with a finalizer
				// that has been finalized since then and is therefore no longer accepting any calls
				logger.Debug("cache failed to de-register callback in finalizer",
					tag.Error(err), tag.ShardID(item.shardId))
			}
		},
	}

	taggedHandler := handler.WithTags(metrics.CacheTypeTag(metrics.MutableStateCacheTypeTagValue))
	c := cache.NewWithMetrics(maxSize, opts, taggedHandler)
	return &cacheImpl{
		Cache:                     c,
		nonUserContextLockTimeout: config.HistoryCacheNonUserContextLockTimeout(),
	}
}

func (c *cacheImpl) stop() {
	c.Cache.(cache.StoppableCache).Stop()
}

func (c *cacheImpl) GetOrCreateWorkflowExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	lockPriority locks.Priority,
) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {
	return c.GetOrCreateChasmExecution(
		ctx,
		shardContext,
		namespaceID,
		execution,
		chasm.WorkflowArchetypeID,
		lockPriority,
	)
}

func (c *cacheImpl) GetOrCreateCurrentExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	workflowID string,
	archetypeID chasm.ArchetypeID,
	lockPriority locks.Priority,
) (historyi.ReleaseWorkflowContextFunc, error) {
	if err := c.validateWorkflowID(workflowID); err != nil {
		return nil, err
	}

	handler := shardContext.GetMetricsHandler().WithTags(
		metrics.OperationTag(metrics.HistoryCacheGetOrCreateCurrentScope),
		metrics.CacheTypeTag(metrics.MutableStateCacheTypeTagValue),
		metrics.NamespaceIDTag(namespaceID.String()),
	)
	metrics.CacheRequests.With(handler).Record(1)
	start := time.Now()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(start)) }()

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
		archetypeID,
		handler,
		true,
		lockPriority,
	)

	metrics.ContextCounterAdd(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name(),
		time.Since(start).Nanoseconds())

	return weReleaseFn, err
}

func (c *cacheImpl) GetOrCreateChasmExecution(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
	lockPriority locks.Priority,
) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {

	if err := c.validateWorkflowExecutionInfo(ctx, shardContext, namespaceID, execution, archetypeID, lockPriority); err != nil {
		return nil, nil, err
	}

	handler := shardContext.GetMetricsHandler().WithTags(
		metrics.OperationTag(metrics.HistoryCacheGetOrCreateScope),
		metrics.CacheTypeTag(metrics.MutableStateCacheTypeTagValue),
		metrics.NamespaceIDTag(namespaceID.String()),
	)
	metrics.CacheRequests.With(handler).Record(1)
	start := time.Now()
	defer func() { metrics.CacheLatency.With(handler).Record(time.Since(start)) }()

	weCtx, weReleaseFunc, err := c.getOrCreateWorkflowExecutionInternal(
		ctx,
		shardContext,
		namespaceID,
		execution,
		archetypeID,
		handler,
		false,
		lockPriority,
	)

	metrics.ContextCounterAdd(ctx, metrics.HistoryWorkflowExecutionCacheLatency.Name(),
		time.Since(start).Nanoseconds())

	return weCtx, weReleaseFunc, err
}

func (c *cacheImpl) getOrCreateWorkflowExecutionInternal(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
	handler metrics.Handler,
	forceClearContext bool,
	lockPriority locks.Priority,
) (historyi.WorkflowContext, historyi.ReleaseWorkflowContextFunc, error) {

	if !softassert.That(
		shardContext.GetLogger(),
		archetypeID != chasm.UnspecifiedArchetypeID,
		"Creating execution cache key with unspecified archetype ID",
	) {
		archetypeID = chasm.WorkflowArchetypeID
	}

	cacheKey := Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId()),
		ArchetypeID: archetypeID,
		ShardUUID:   shardContext.GetOwner(),
	}
	item, cacheHit := c.Get(cacheKey).(*cacheItem)
	var workflowCtx historyi.WorkflowContext
	if cacheHit {
		workflowCtx = item.wfContext
	} else {
		metrics.CacheMissCounter.With(handler).Record(1)
		workflowCtx = workflow.NewContext(
			shardContext.GetConfig(),
			cacheKey.WorkflowKey,
			archetypeID,
			shardContext.GetLogger(),
			shardContext.GetThrottledLogger(),
			shardContext.GetMetricsHandler(),
		)

		var err error
		value := &cacheItem{shardId: shardContext.GetShardID(), wfContext: workflowCtx, finalizer: shardContext.GetFinalizer()}
		existing, err := c.PutIfNotExist(cacheKey, value)
		if err != nil {
			metrics.CacheFailures.With(handler).Record(1)
			return nil, nil, err
		}
		//nolint:revive
		workflowCtx = existing.(*cacheItem).wfContext
	}

	if err := c.lockWorkflowExecution(ctx, workflowCtx, cacheKey, lockPriority); err != nil {
		metrics.CacheFailures.With(handler).Record(1)
		metrics.AcquireLockFailedCounter.With(handler).Record(1)
		return nil, nil, err
	}

	// TODO This will create a closure on every request.
	//  Consider revisiting this if it causes too much GC activity
	releaseFunc := c.makeReleaseFunc(cacheKey, shardContext, workflowCtx, forceClearContext, handler, time.Now())

	return workflowCtx, releaseFunc, nil
}

func (c *cacheImpl) lockWorkflowExecution(
	ctx context.Context,
	workflowCtx historyi.WorkflowContext,
	cacheKey Key,
	lockPriority locks.Priority,
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

func (c *cacheImpl) makeReleaseFunc(
	cacheKey Key,
	shardContext historyi.ShardContext,
	wfContext historyi.WorkflowContext,
	forceClearContext bool,
	handler metrics.Handler,
	acquireTime time.Time,
) func(error) {

	status := cacheNotReleased
	return func(err error) {
		if atomic.CompareAndSwapInt32(&status, cacheNotReleased, cacheReleased) {
			defer func() {
				metrics.HistoryWorkflowExecutionCacheLockHoldDuration.With(handler).Record(time.Since(acquireTime))
			}()
			if rec := recover(); rec != nil {
				wfContext.Clear()
				wfContext.Unlock()
				c.Release(cacheKey)
				panic(rec)
			} else {
				if err != nil || forceClearContext {
					// TODO see issue #668, there are certain type or errors which can bypass the clear
					wfContext.Clear()
					wfContext.Unlock()
					c.Release(cacheKey)
				} else {
					isDirty := wfContext.IsDirty()
					if isDirty {
						wfContext.Clear()
						logger := log.With(shardContext.GetLogger(), tag.ComponentHistoryCache)
						logger.Error("Cache encountered dirty mutable state transaction",
							tag.WorkflowNamespaceID(wfContext.GetWorkflowKey().NamespaceID),
							tag.WorkflowID(wfContext.GetWorkflowKey().WorkflowID),
							tag.WorkflowRunID(wfContext.GetWorkflowKey().RunID),
						)
					}
					wfContext.Unlock()
					c.Release(cacheKey)
					if isDirty {
						panic("Cache encountered dirty mutable state transaction")
					}
				}
			}
		}
	}
}

func (c *cacheImpl) validateWorkflowExecutionInfo(
	ctx context.Context,
	shardContext historyi.ShardContext,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	archetypeID chasm.ArchetypeID,
	lockPriority locks.Priority,
) error {

	if err := c.validateWorkflowID(execution.GetWorkflowId()); err != nil {
		return err
	}

	// RunID is not provided, lets try to retrieve the RunID for current active execution
	if execution.GetRunId() == "" {
		runID, err := GetCurrentRunID(
			ctx,
			shardContext,
			c,
			namespaceID.String(),
			execution.GetWorkflowId(),
			archetypeID,
			lockPriority,
		)
		if err != nil {
			return err
		}

		execution.RunId = runID
	} else if uuid.Validate(execution.GetRunId()) != nil { // immediately return if invalid runID
		return serviceerror.NewInvalidArgument("RunId is not valid UUID.")
	}
	return nil
}

func (c *cacheImpl) validateWorkflowID(
	workflowID string,
) error {
	if workflowID == "" {
		return serviceerror.NewInvalidArgument("Can't load workflow execution.  WorkflowId not set.")
	}
	return nil
}

func GetCurrentRunID(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowCache Cache,
	namespaceID string,
	workflowID string,
	archetypeID chasm.ArchetypeID,
	lockPriority locks.Priority,
) (runID string, retErr error) {
	currentRelease, err := workflowCache.GetOrCreateCurrentExecution(
		ctx,
		shardContext,
		namespace.ID(namespaceID),
		workflowID,
		archetypeID,
		lockPriority,
	)
	if err != nil {
		return "", err
	}
	defer func() { currentRelease(retErr) }()

	resp, err := shardContext.GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     shardContext.GetShardID(),
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			ArchetypeID: archetypeID,
		},
	)
	if err != nil {
		return "", err
	}
	return resp.RunID, nil
}

func (c *cacheItem) CacheSize() int {
	if sg, ok := c.wfContext.(cache.SizeGetter); ok {
		return sg.CacheSize()
	}
	return 0
}
