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
	defaultReaderId = 0

	queueIOTimeout = 5 * time.Second
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
		rescheduler    Rescheduler
		timeSource     clock.TimeSource
		monitor        *monitorImpl
		mitigator      *mitigatorImpl
		logger         log.Logger
		metricsHandler metrics.MetricsHandler

		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		inclusiveLowWatermark tasks.Key
		nonReadableScope      Scope
		readerGroup           *ReaderGroup
		lastPollTime          time.Time

		checkpointRetrier backoff.Retrier
		checkpointTimer   *time.Timer
		pollTimer         *time.Timer

		alertCh <-chan *Alert
	}

	Options struct {
		ReaderOptions
		MonitorOptions

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
	rateLimiter quotas.RateLimiter,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
) *queueBase {
	var readerScopes map[int32][]Scope
	var exclusiveHighWatermark tasks.Key
	if persistenceState, ok := shard.GetQueueState(category); ok {
		queueState := FromPersistenceQueueState(persistenceState)

		readerScopes = queueState.readerScopes
		exclusiveHighWatermark = queueState.exclusiveReaderHighWatermark
	} else {
		ackLevel := shard.GetQueueAckLevel(category)
		if category.Type() == tasks.CategoryTypeImmediate {
			// convert to exclusive ack level
			ackLevel = ackLevel.Next()
		}

		exclusiveHighWatermark = ackLevel
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

	executableInitializer := func(t tasks.Task) Executable {
		return NewExecutable(
			t,
			nil,
			executor,
			scheduler,
			rescheduler,
			priorityAssigner,
			timeSource,
			shard.GetNamespaceRegistry(),
			logger,
			options.TaskMaxRetryCount,
			QueueTypeUnknown, // we don't care about queue type
			shard.GetConfig().NamespaceCacheRefreshInterval,
		)
	}

	readerInitializer := func(readerID int32, slices []Slice) Reader {
		return NewReader(
			readerID,
			slices,
			&options.ReaderOptions,
			scheduler,
			rescheduler,
			timeSource,
			rateLimiter,
			monitor,
			logger,
			metricsHandler,
		)
	}

	inclusiveLowWatermark := exclusiveHighWatermark
	readerGroup := NewReaderGroup(readerInitializer)
	for readerID, scopes := range readerScopes {
		slices := make([]Slice, 0, len(scopes))
		for _, scope := range scopes {
			slices = append(slices, NewSlice(paginationFnProvider, executableInitializer, scope))
		}
		readerGroup.NewReader(readerID, slices...)

		if len(scopes) != 0 {
			inclusiveLowWatermark = tasks.MinKey(inclusiveLowWatermark, scopes[0].Range.InclusiveMin)
		}
	}

	return &queueBase{
		shard: shard,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		category:       category,
		options:        options,
		rescheduler:    rescheduler,
		timeSource:     shard.GetTimeSource(),
		monitor:        monitor,
		mitigator:      mitigator,
		logger:         logger,
		metricsHandler: metricsHandler,

		paginationFnProvider:  paginationFnProvider,
		executableInitializer: executableInitializer,

		inclusiveLowWatermark: inclusiveLowWatermark,
		nonReadableScope: NewScope(
			NewRange(exclusiveHighWatermark, tasks.MaximumKey),
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

	p.pollTimer = time.NewTimer(backoff.JitDuration(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
	p.checkpointTimer = time.NewTimer(backoff.JitDuration(
		p.options.CheckpointInterval(),
		p.options.CheckpointIntervalJitterCoefficient(),
	))
}

func (p *queueBase) Stop() {
	p.monitor.Close()
	p.readerGroup.Stop()
	p.rescheduler.Stop()
	p.pollTimer.Stop()
	p.checkpointTimer.Stop()
}

func (p *queueBase) Category() tasks.Category {
	return p.category
}

func (p *queueBase) FailoverNamespace(
	namespaceIDs map[string]struct{},
) {
	// TODO: reschedule all tasks for namespaces that becomes active
	// no-op
}

func (p *queueBase) LockTaskProcessing() {
	// no-op
}

func (p *queueBase) UnlockTaskProcessing() {
	// no-op
}

func (p *queueBase) processPollTimer() {
	if p.lastPollTime.Add(p.options.MaxPollInterval()).Before(p.timeSource.Now()) {
		p.processNewRange()
	}

	p.pollTimer.Reset(backoff.JitDuration(
		p.options.MaxPollInterval(),
		p.options.MaxPollIntervalJitterCoefficient(),
	))
}

func (p *queueBase) processNewRange() {
	newMaxKey := p.shard.GetQueueExclusiveHighReadWatermark(
		p.category,
		p.shard.GetClusterMetadata().GetCurrentClusterName(),
	)

	if !p.nonReadableScope.CanSplitByRange(newMaxKey) {
		return
	}

	p.lastPollTime = p.timeSource.Now()

	var newReadScope Scope
	newReadScope, p.nonReadableScope = p.nonReadableScope.SplitByRange(newMaxKey)
	newSlice := NewSlice(
		p.paginationFnProvider,
		p.executableInitializer,
		newReadScope,
	)

	reader, ok := p.readerGroup.ReaderByID(defaultReaderId)
	if !ok {
		p.readerGroup.NewReader(defaultReaderId, newSlice)
	} else {
		reader.MergeSlices(newSlice)
	}
}

func (p *queueBase) checkpoint() {
	var err error
	defer func() {
		if err == nil {
			p.checkpointRetrier.Reset()
			p.checkpointTimer.Reset(backoff.JitDuration(
				p.options.CheckpointInterval(),
				p.options.CheckpointIntervalJitterCoefficient(),
			))
		} else {
			backoff := p.checkpointRetrier.NextBackOff()
			p.checkpointTimer.Reset(backoff)
		}
	}()

	totalSlices := 0
	readerScopes := make(map[int32][]Scope)
	newInclusiveLowWatermark := tasks.MaximumKey
	for id, reader := range p.readerGroup.Readers() {
		reader.ShrinkSlices()
		scopes := reader.Scopes()

		totalSlices += len(scopes)
		readerScopes[id] = scopes
		for _, scope := range scopes {
			newInclusiveLowWatermark = tasks.MinKey(newInclusiveLowWatermark, scope.Range.InclusiveMin)
		}
	}

	// NOTE: Must range complete task first.
	// Otherwise, if state is updated first, later deletion fails and shard get reloaded
	// some tasks will never be deleted.
	if newInclusiveLowWatermark != tasks.MaximumKey && newInclusiveLowWatermark.CompareTo(p.inclusiveLowWatermark) > 0 {
		p.metricsHandler.Counter(TaskBatchCompleteCounter).Record(1)

		persistenceMinTaskKey := p.inclusiveLowWatermark
		persistenceMaxTaskKey := newInclusiveLowWatermark
		if p.category.Type() == tasks.CategoryTypeScheduled {
			persistenceMinTaskKey = tasks.NewKey(p.inclusiveLowWatermark.FireTime, 0)
			persistenceMaxTaskKey = tasks.NewKey(newInclusiveLowWatermark.FireTime, 0)
		}

		ctx, cancel := newQueueIOContext()
		defer cancel()

		if err = p.shard.GetExecutionManager().RangeCompleteHistoryTasks(ctx, &persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             p.shard.GetShardID(),
			TaskCategory:        p.category,
			InclusiveMinTaskKey: persistenceMinTaskKey,
			ExclusiveMaxTaskKey: persistenceMaxTaskKey,
		}); err != nil {
			p.logger.Error("Error range completing queue task", tag.Error(err))
			return
		}

		p.inclusiveLowWatermark = newInclusiveLowWatermark
	}

	p.metricsHandler.Counter(AckLevelUpdateCounter).Record(1)
	// p.metricsHandler.Histogram(PendingTasksCounter, metrics.Dimensionless).Record(int64(p.monitor.GetPendingTasksCount()))
	err = p.shard.UpdateQueueState(p.category, ToPersistenceQueueState(&queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: p.nonReadableScope.Range.InclusiveMin,
	}))
	if err != nil {
		p.metricsHandler.Counter(AckLevelUpdateFailedCounter).Record(1)
		p.logger.Error("Error updating queue state", tag.Error(err), tag.OperationFailed)
	}
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
