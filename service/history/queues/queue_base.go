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
	"sync"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
)

const (
	DefaultReaderId = 0

	// Non-default readers will use critical pending task count * this coefficient
	// as its max pending task count so that their loading will never trigger pending
	// task alert & action
	nonDefaultReaderMaxPendingTaskCoefficient = 0.8

	queueIOTimeout = 5 * time.Second

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
		readerScopes                 map[int32][]Scope
		exclusiveReaderHighWatermark tasks.Key
	}

	queueBase struct {
		shard shard.Context

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
		metricsHandler metrics.MetricsHandler

		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		exclusiveDeletionHighWatermark tasks.Key
		nonReadableScope               Scope
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
		TaskMaxRetryCount                   dynamicconfig.IntPropertyFn
	}
)

func newQueueBase(
	shard shard.Context,
	category tasks.Category,
	paginationFnProvider PaginationFnProvider,
	scheduler Scheduler,
	priorityAssigner PriorityAssigner,
	executor Executor,
	options *Options,
	hostReaderRateLimiter quotas.RequestRateLimiter,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
) *queueBase {
	var readerScopes map[int32][]Scope
	var exclusiveReaderHighWatermark tasks.Key
	if persistenceState, ok := shard.GetQueueState(category); ok {
		queueState := FromPersistenceQueueState(persistenceState)

		readerScopes = queueState.readerScopes
		exclusiveReaderHighWatermark = queueState.exclusiveReaderHighWatermark
	} else {
		ackLevel := shard.GetQueueAckLevel(category)
		if category.Type() == tasks.CategoryTypeImmediate {
			// convert to exclusive ack level
			ackLevel = ackLevel.Next()
		}

		exclusiveReaderHighWatermark = ackLevel
	}

	timeSource := shard.GetTimeSource()
	rescheduler := NewRescheduler(
		scheduler,
		timeSource,
		logger,
		metricsHandler,
	)

	monitor := newMonitor(category.Type(), &options.MonitorOptions)
	mitigator := newMitigator(monitor, logger, metricsHandler, options.MaxReaderCount)

	executableInitializer := func(readerID int32, t tasks.Task) Executable {
		return NewExecutable(
			readerID,
			t,
			nil,
			executor,
			scheduler,
			rescheduler,
			priorityAssigner,
			timeSource,
			shard.GetNamespaceRegistry(),
			logger,
			metricsHandler,
			options.TaskMaxRetryCount,
			shard.GetConfig().NamespaceCacheRefreshInterval,
		)
	}

	readerInitializer := func(readerID int32, slices []Slice) Reader {
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
			newShardReaderRateLimiter(
				options.MaxPollRPS,
				hostReaderRateLimiter,
				options.MaxReaderCount(),
			),
			monitor,
			logger,
			metricsHandler,
		)
	}

	exclusiveDeletionHighWatermark := exclusiveReaderHighWatermark
	readerGroup := NewReaderGroup(readerInitializer)
	for readerID, scopes := range readerScopes {
		if len(scopes) == 0 {
			continue
		}

		slices := make([]Slice, 0, len(scopes))
		for _, scope := range scopes {
			slices = append(slices, NewSlice(paginationFnProvider, executableInitializer, monitor, scope))
		}
		readerGroup.NewReader(readerID, slices...)

		exclusiveDeletionHighWatermark = tasks.MinKey(exclusiveDeletionHighWatermark, scopes[0].Range.InclusiveMin)
	}

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

		exclusiveDeletionHighWatermark: exclusiveDeletionHighWatermark,
		nonReadableScope: NewScope(
			NewRange(exclusiveReaderHighWatermark, tasks.MaximumKey),
			predicates.Universal[tasks.Task](),
		),
		readerGroup: readerGroup,

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

	p.checkpointTimer = time.NewTimer(backoff.JitDuration(
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
	namespaceIDs map[string]struct{},
) {
	p.rescheduler.Reschedule(namespaceIDs)
}

func (p *queueBase) LockTaskProcessing() {
	// no-op
}

func (p *queueBase) UnlockTaskProcessing() {
	// no-op
}

func (p *queueBase) processNewRange() {
	newMaxKey := p.shard.GetQueueExclusiveHighReadWatermark(
		p.category,
		p.shard.GetClusterMetadata().GetCurrentClusterName(),
	)

	if !p.nonReadableScope.CanSplitByRange(newMaxKey) {
		return
	}

	var newReadScope Scope
	newReadScope, p.nonReadableScope = p.nonReadableScope.SplitByRange(newMaxKey)
	newSlice := NewSlice(
		p.paginationFnProvider,
		p.executableInitializer,
		p.monitor,
		newReadScope,
	)

	reader, ok := p.readerGroup.ReaderByID(DefaultReaderId)
	if !ok {
		p.readerGroup.NewReader(DefaultReaderId, newSlice)
		return
	}

	if now := p.timeSource.Now(); now.After(p.nextForceNewSliceTime) {
		reader.AppendSlices(newSlice)
		p.nextForceNewSliceTime = now.Add(forceNewSliceDuration)
	} else {
		reader.MergeSlices(newSlice)
	}
}

func (p *queueBase) checkpoint() {
	for _, reader := range p.readerGroup.Readers() {
		reader.ShrinkSlices()
	}
	// Run slicePredicateAction to move slices with non-universal predicate to non-default reader
	// so that upon shard reload, task loading for those slices won't block other slices in the default
	// reader.
	newSlicePredicateAction(p.monitor, p.mitigator.maxReaderCount()).Run(p.readerGroup)

	readerScopes := make(map[int32][]Scope)
	newExclusiveDeletionHighWatermark := p.nonReadableScope.Range.InclusiveMin
	for readerID, reader := range p.readerGroup.Readers() {
		scopes := reader.Scopes()

		if len(scopes) == 0 {
			p.readerGroup.RemoveReader(readerID)
			continue
		}

		readerScopes[readerID] = scopes
		for _, scope := range scopes {
			newExclusiveDeletionHighWatermark = tasks.MinKey(newExclusiveDeletionHighWatermark, scope.Range.InclusiveMin)
		}
	}
	p.metricsHandler.Histogram(QueueReaderCountHistogram, metrics.Dimensionless).Record(int64(len(readerScopes)))
	p.metricsHandler.Histogram(QueueSliceCountHistogram, metrics.Dimensionless).Record(int64(p.monitor.GetTotalSliceCount()))
	p.metricsHandler.Histogram(PendingTasksCounter, metrics.Dimensionless).Record(int64(p.monitor.GetTotalPendingTaskCount()))

	// NOTE: Must range complete task first.
	// Otherwise, if state is updated first, later deletion fails and shard get reloaded
	// some tasks will never be deleted.
	//
	// Emit metric before the deletion watermark comparsion so we have the emit even if there's no task
	// for the queue
	p.metricsHandler.Counter(TaskBatchCompleteCounter).Record(1)
	if newExclusiveDeletionHighWatermark.CompareTo(p.exclusiveDeletionHighWatermark) > 0 {
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
	readerScopes map[int32][]Scope,
) error {
	p.metricsHandler.Counter(AckLevelUpdateCounter).Record(1)
	err := p.shard.UpdateQueueState(p.category, ToPersistenceQueueState(&queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: p.nonReadableScope.Range.InclusiveMin,
	}))
	if err != nil {
		p.metricsHandler.Counter(AckLevelUpdateFailedCounter).Record(1)
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
	p.checkpointTimer.Reset(backoff.JitDuration(
		p.options.CheckpointInterval(),
		p.options.CheckpointIntervalJitterCoefficient(),
	))
}

func (p *queueBase) handleAlert(alert *Alert) {
	// Upon getting an Alert from monitor,
	// send it to the mitigator for deduping and generating the corresponding Action.
	// Then run the returned Action to resolve the Alert.

	if alert == nil {
		return
	}

	action := p.mitigator.Mitigate(*alert)
	if action == nil {
		return
	}

	action.Run(p.readerGroup)
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
