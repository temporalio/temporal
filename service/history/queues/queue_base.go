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

package queues

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/quotas"
	hshard "go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const (
	DefaultReaderId = common.DefaultQueueReaderID

	// Non-default readers will use critical pending task count * this coefficient
	// as its max pending task count so that their loading will never trigger pending
	// task alert & action
	nonDefaultReaderMaxPendingTaskCoefficient = 0.8

	queueIOTimeout = 5 * time.Second * debug.TimeoutMultiplier

	// Force creating new slice every forceNewSliceDuration
	// so that the last slice in the default reader won't grow
	// infinitely.
	// The benefit of forcing new slice is:
	// 1. As long as the last slice won't grow infinitly, task loading
	// for that slice will complete and it's scope (both range and
	// predicate) is able to shrink
	// 2. Current task loading implementation can only unload the entire
	// slice. If there's only one slice, we may unload all tasks for a
	// given namespace.
	forceNewSliceDuration = 5 * time.Minute
)

type (
	queueState struct {
		readerScopes                 map[int64][]Scope
		exclusiveReaderHighWatermark tasks.Key
	}

	queueBase struct {
		shard hshard.Context

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		category       tasks.Category
		options        *Options
		scheduler      Scheduler
		rescheduler    Rescheduler
		timeSource     clock.TimeSource
		monitor        *monitorImpl
		mitigator      *mitigatorImpl
		logger         log.Logger
		metricsHandler metrics.Handler

		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		lastRangeID                    int64
		exclusiveDeletionHighWatermark tasks.Key
		nonReadableScope               Scope
		readerRateLimiter              quotas.RequestRateLimiter
		readerGroup                    *ReaderGroup
		nextForceNewSliceTime          time.Time

		checkpointRetrier backoff.Retrier
		checkpointTimer   *time.Timer

		alertCh <-chan *Alert
	}

	Options struct {
		ReaderOptions
		MonitorOptions

		MaxPollRPS                          dynamicconfig.IntPropertyFn
		MaxPollInterval                     dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient    dynamicconfig.FloatPropertyFn
		CheckpointInterval                  dynamicconfig.DurationPropertyFn
		CheckpointIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		MaxReaderCount                      dynamicconfig.IntPropertyFn
	}
)

func newQueueBase(
	shard hshard.Context,
	category tasks.Category,
	paginationFnProvider PaginationFnProvider,
	scheduler Scheduler,
	rescheduler Rescheduler,
	priorityAssigner PriorityAssigner,
	executor Executor,
	options *Options,
	hostReaderRateLimiter quotas.RequestRateLimiter,
	completionFn ReaderCompletionFn,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *queueBase {
	var readerScopes map[int64][]Scope
	var exclusiveReaderHighWatermark tasks.Key
	if persistenceState, ok := shard.GetQueueState(category); ok {
		queueState := FromPersistenceQueueState(persistenceState)

		readerScopes = queueState.readerScopes
		exclusiveReaderHighWatermark = queueState.exclusiveReaderHighWatermark
	} else {
		ackLevel := tasks.NewKey(tasks.DefaultFireTime, 0)
		if category.Type() == tasks.CategoryTypeImmediate {
			// convert to exclusive ack level
			ackLevel = ackLevel.Next()
		}

		exclusiveReaderHighWatermark = ackLevel
	}

	timeSource := shard.GetTimeSource()
	executableInitializer := func(readerID int64, t tasks.Task) Executable {
		return NewExecutable(
			readerID,
			t,
			executor,
			scheduler,
			rescheduler,
			priorityAssigner,
			timeSource,
			shard.GetNamespaceRegistry(),
			shard.GetClusterMetadata(),
			logger,
			metricsHandler,
		)
	}

	monitor := newMonitor(category.Type(), timeSource, &options.MonitorOptions)
	readerRateLimiter := newShardReaderRateLimiter(
		options.MaxPollRPS,
		hostReaderRateLimiter,
		int64(options.MaxReaderCount()),
	)
	readerInitializer := func(readerID int64, slices []Slice) Reader {
		readerOptions := options.ReaderOptions // make a copy
		if readerID != DefaultReaderId {
			// non-default reader should not trigger task unloading
			// otherwise those readers will keep loading, hit pending task count limit, unload, throttle, load, etc...
			// use a limit lower than the critical pending task count instead
			readerOptions.MaxPendingTasksCount = func() int {
				return int(float64(options.PendingTasksCriticalCount()) * nonDefaultReaderMaxPendingTaskCoefficient)
			}
		}

		return NewReader(
			readerID,
			slices,
			&readerOptions,
			scheduler,
			rescheduler,
			timeSource,
			readerRateLimiter,
			monitor,
			completionFn,
			logger,
			metricsHandler,
		)
	}

	resetReaderScope := false
	exclusiveDeletionHighWatermark := exclusiveReaderHighWatermark
	readerGroup := NewReaderGroup(shard.GetShardID(), shard.GetOwner(), category, readerInitializer, shard.GetExecutionManager())
	for readerID, scopes := range readerScopes {
		if len(scopes) == 0 {
			continue
		}

		slices := make([]Slice, 0, len(scopes))
		for _, scope := range scopes {
			slices = append(slices, NewSlice(paginationFnProvider, executableInitializer, monitor, scope))
		}
		if _, err := readerGroup.NewReader(readerID, slices...); err != nil {
			// we are not able to re-create the scopes & readers we previously have
			// but we may still be able to run with only one reader.
			// Pick the lowest key among all scopes and start from there
			logger.Error("Failed to create history queue reader on initialization", tag.QueueReaderID(readerID), tag.Error(err))

			resetReaderScope = true

			// don't break here, still need to update exclusiveDeletionHighWatermark
		}

		exclusiveDeletionHighWatermark = tasks.MinKey(exclusiveDeletionHighWatermark, scopes[0].Range.InclusiveMin)
	}
	if resetReaderScope {
		// start from the lowest key of all reader scopes
		exclusiveReaderHighWatermark = exclusiveDeletionHighWatermark

		// some readers may already be created
		// stop them and create a new empty reader group
		readerGroup.Stop()
		readerGroup = NewReaderGroup(shard.GetShardID(), shard.GetOwner(), category, readerInitializer, shard.GetExecutionManager())
	}

	mitigator := newMitigator(readerGroup, monitor, logger, metricsHandler, options.MaxReaderCount)

	return &queueBase{
		shard: shard,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		category:       category,
		options:        options,
		scheduler:      scheduler,
		rescheduler:    rescheduler,
		timeSource:     shard.GetTimeSource(),
		monitor:        monitor,
		mitigator:      mitigator,
		logger:         logger,
		metricsHandler: metricsHandler,

		paginationFnProvider:  paginationFnProvider,
		executableInitializer: executableInitializer,

		lastRangeID:                    -1, // start from an invalid rangeID
		exclusiveDeletionHighWatermark: exclusiveDeletionHighWatermark,
		nonReadableScope: NewScope(
			NewRange(exclusiveReaderHighWatermark, tasks.MaximumKey),
			predicates.Universal[tasks.Task](),
		),
		readerRateLimiter: readerRateLimiter,
		readerGroup:       readerGroup,

		// pollTimer and checkpointTimer are initialized on Start()
		checkpointRetrier: backoff.NewRetrier(
			createCheckpointRetryPolicy(),
			backoff.SystemClock,
		),

		alertCh: monitor.AlertCh(),
	}
}

func (p *queueBase) Start() {
	p.rescheduler.Start()
	p.readerGroup.Start()

	p.checkpointTimer = time.NewTimer(backoff.Jitter(
		p.options.CheckpointInterval(),
		p.options.CheckpointIntervalJitterCoefficient(),
	))
}

func (p *queueBase) Stop() {
	p.monitor.Close()
	p.readerGroup.Stop()
	p.rescheduler.Stop()
	p.checkpointTimer.Stop()
}

func (p *queueBase) Category() tasks.Category {
	return p.category
}

func (p *queueBase) FailoverNamespace(
	namespaceID string,
) {
	p.rescheduler.Reschedule(namespaceID)
}

func (p *queueBase) processNewRange() {
	var newMaxKey tasks.Key
	switch categoryType := p.category.Type(); categoryType {
	case tasks.CategoryTypeImmediate:
		newMaxKey = p.shard.GetImmediateQueueExclusiveHighReadWatermark()
	case tasks.CategoryTypeScheduled:
		var err error
		if newMaxKey, err = p.shard.UpdateScheduledQueueExclusiveHighReadWatermark(); err != nil {
			p.logger.Error("Unable to process new range", tag.Error(err))
			return
		}
	default:
		panic(fmt.Sprintf("Unknown task category type: %v", categoryType.String()))
	}

	reader, err := p.readerGroup.GetOrCreateReader(DefaultReaderId)
	if err != nil {
		p.logger.Error("Unable to create default reader", tag.Error(err), tag.QueueReaderID(DefaultReaderId))
		return
	}

	slices := make([]Slice, 0, 1)
	if p.nonReadableScope.CanSplitByRange(newMaxKey) {
		var newReadScope Scope
		newReadScope, p.nonReadableScope = p.nonReadableScope.SplitByRange(newMaxKey)
		slices = append(slices, NewSlice(
			p.paginationFnProvider,
			p.executableInitializer,
			p.monitor,
			newReadScope,
		))
	}

	if now := p.timeSource.Now(); now.After(p.nextForceNewSliceTime) {
		reader.AppendSlices(slices...)
		p.nextForceNewSliceTime = now.Add(forceNewSliceDuration)
	} else {
		reader.MergeSlices(slices...)
	}
}

func (p *queueBase) checkpoint() {
	p.readerGroup.ForEach(func(_ int64, r Reader) {
		r.ShrinkSlices()
	})

	// Run slicePredicateAction to move slices with non-universal predicate to non-default reader
	// so that upon shard reload, task loading for those slices won't block other slices in the default reader.
	_ = runAction(
		newSlicePredicateAction(p.monitor, p.mitigator.maxReaderCount()),
		p.readerGroup,
		p.metricsHandler,
		p.logger,
	)

	readerScopes := make(map[int64][]Scope)
	newExclusiveDeletionHighWatermark := p.nonReadableScope.Range.InclusiveMin
	for readerID, reader := range p.readerGroup.Readers() {
		scopes := reader.Scopes()

		if len(scopes) == 0 && readerID != DefaultReaderId {
			p.readerGroup.RemoveReader(readerID)
			continue
		}

		readerScopes[readerID] = scopes
		if len(scopes) != 0 {
			newExclusiveDeletionHighWatermark = tasks.MinKey(newExclusiveDeletionHighWatermark, scopes[0].Range.InclusiveMin)
		}
	}
	p.metricsHandler.Histogram(metrics.QueueReaderCountHistogram.GetMetricName(), metrics.QueueReaderCountHistogram.GetMetricUnit()).
		Record(int64(len(readerScopes)))
	p.metricsHandler.Histogram(metrics.QueueSliceCountHistogram.GetMetricName(), metrics.QueueSliceCountHistogram.GetMetricUnit()).
		Record(int64(p.monitor.GetTotalSliceCount()))
	p.metricsHandler.Histogram(metrics.PendingTasksCounter.GetMetricName(), metrics.PendingTasksCounter.GetMetricUnit()).
		Record(int64(p.monitor.GetTotalPendingTaskCount()))

	p.updateReaderProgress(readerScopes)

	// NOTE: Must range complete task first.
	// Otherwise, if state is updated first, later deletion fails and shard get reloaded
	// some tasks will never be deleted.
	//
	// Emit metric before the deletion watermark comparsion so we have the emit even if there's no task
	// for the queue
	p.metricsHandler.Counter(metrics.TaskBatchCompleteCounter.GetMetricName()).Record(1)
	if newExclusiveDeletionHighWatermark.CompareTo(p.exclusiveDeletionHighWatermark) > 0 ||
		p.updateShardRangeID() {
		// when shard rangeID is updated, perform range completion again in case the underlying persistence implementation
		// serves traffic based on the persisted shardInfo
		err := p.rangeCompleteTasks(p.exclusiveDeletionHighWatermark, newExclusiveDeletionHighWatermark)
		if err != nil {
			p.resetCheckpointTimer(err)
			return
		}

		p.exclusiveDeletionHighWatermark = newExclusiveDeletionHighWatermark
	}

	err := p.updateQueueState(readerScopes)
	p.resetCheckpointTimer(err)
}

func (p *queueBase) updateReaderProgress(
	readerScopes map[int64][]Scope,
) {
	// NOTE: A reader has progress = X means that reader will
	// never try to load/process tasks with key < X.
	// If a reader's first slice has scope start from Y,
	// it's possible that later a slice contains key < Y get moved into it.
	// In general, the minKey of a reader's first slice's scope is not it's progress.

	// However, since slices only move from reader with lower ID to higher ID,
	// progress of reader with ID X = min(
	//     progress of reader with ID < X if exists,
	//     minKey of the first slice's scope,
	// )
	ctx, cancel := newQueueIOContext()
	defer cancel()

	readerIDs := make([]int64, 0, len(readerScopes))
	for readerID := range readerScopes {
		readerIDs = append(readerIDs, readerID)
	}
	slices.Sort(readerIDs)

	progress := tasks.MaximumKey
	for _, readerID := range readerIDs {
		scopes := readerScopes[readerID]

		var minKey tasks.Key
		if len(scopes) == 0 {
			// this should only happen to default reader
			minKey = p.nonReadableScope.Range.InclusiveMin
		} else {
			minKey = scopes[0].Range.InclusiveMin
		}

		progress = tasks.MinKey(progress, minKey)
		p.shard.GetExecutionManager().UpdateHistoryTaskReaderProgress(ctx, &persistence.UpdateHistoryTaskReaderProgressRequest{
			ShardID:                    p.shard.GetShardID(),
			ShardOwner:                 p.shard.GetOwner(),
			TaskCategory:               p.category,
			ReaderID:                   readerID,
			InclusiveMinPendingTaskKey: progress,
		})
	}
}

func (p *queueBase) updateShardRangeID() bool {
	newRangeID := p.shard.GetRangeID()
	if p.lastRangeID < newRangeID {
		p.lastRangeID = newRangeID
		return true
	}
	return false
}

func (p *queueBase) rangeCompleteTasks(
	oldExclusiveDeletionHighWatermark tasks.Key,
	newExclusiveDeletionHighWatermark tasks.Key,
) error {
	if p.category.Type() == tasks.CategoryTypeScheduled {
		oldExclusiveDeletionHighWatermark.TaskID = 0
		newExclusiveDeletionHighWatermark.TaskID = 0
	}

	ctx, cancel := newQueueIOContext()
	defer cancel()

	if err := p.shard.GetExecutionManager().RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
		ShardID:             p.shard.GetShardID(),
		TaskCategory:        p.category,
		InclusiveMinTaskKey: oldExclusiveDeletionHighWatermark,
		ExclusiveMaxTaskKey: newExclusiveDeletionHighWatermark,
	}); err != nil {
		p.logger.Error("Error range completing queue task", tag.Error(err))
		return err
	}
	return nil
}

func (p *queueBase) updateQueueState(
	readerScopes map[int64][]Scope,
) error {
	p.metricsHandler.Counter(metrics.AckLevelUpdateCounter.GetMetricName()).Record(1)
	for readerID, scopes := range readerScopes {
		if len(scopes) == 0 {
			delete(readerScopes, readerID)
		}
	}

	err := p.shard.SetQueueState(p.category, ToPersistenceQueueState(&queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: p.nonReadableScope.Range.InclusiveMin,
	}))
	if err != nil {
		p.metricsHandler.Counter(metrics.AckLevelUpdateFailedCounter.GetMetricName()).Record(1)
		p.logger.Error("Error updating queue state", tag.Error(err), tag.OperationFailed)
	}
	return err
}

func (p *queueBase) resetCheckpointTimer(checkPointErr error) {
	if checkPointErr != nil {
		backoff := p.checkpointRetrier.NextBackOff()
		p.checkpointTimer.Reset(backoff)
		return
	}

	p.checkpointRetrier.Reset()
	p.checkpointTimer.Reset(backoff.Jitter(
		p.options.CheckpointInterval(),
		p.options.CheckpointIntervalJitterCoefficient(),
	))
}

func (p *queueBase) handleAlert(alert *Alert) {
	if alert == nil {
		return
	}

	p.mitigator.Mitigate(*alert)

	// checkpoint the action taken & update reader progress
	p.checkpoint()

	// reader may be able to load more tasks after progress is updated
	p.notifyReaders()
}

func (p *queueBase) notifyReaders() {
	p.readerGroup.ForEach(func(_ int64, r Reader) {
		r.Notify()
	})
}

func createCheckpointRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond).
		WithMaximumInterval(5 * time.Second).
		WithExpirationInterval(backoff.NoInterval)

	return policy
}

func newQueueIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), queueIOTimeout)
	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundCallerInfo)
	return ctx, cancel
}
