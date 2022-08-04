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

		category    tasks.Category
		options     *Options
		rescheduler Rescheduler
		timeSource  clock.TimeSource
		logger      log.Logger

		paginationFnProvider  PaginationFnProvider
		executableInitializer ExecutableInitializer

		inclusiveLowWatermark tasks.Key
		nonReadableScope      Scope
		readerGroup           *ReaderGroup
		lastPollTime          time.Time

		checkpointRetrier backoff.Retrier
		checkpointTimer   *time.Timer
		pollTimer         *time.Timer
	}

	Options struct {
		ReaderOptions

		MaxPollInterval                     dynamicconfig.DurationPropertyFn
		MaxPollIntervalJitterCoefficient    dynamicconfig.FloatPropertyFn
		CheckpointInterval                  dynamicconfig.DurationPropertyFn
		CheckpointIntervalJitterCoefficient dynamicconfig.FloatPropertyFn
		TaskMaxRetryCount                   dynamicconfig.IntPropertyFn
	}
)

func newQueueBase(
	shard shard.Context,
	category tasks.Category,
	paginationFnProvider PaginationFnProvider,
	scheduler Scheduler,
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

	executableInitializer := func(t tasks.Task) Executable {
		return NewExecutable(
			t,
			nil,
			executor,
			scheduler,
			rescheduler,
			timeSource,
			logger,
			options.TaskMaxRetryCount,
			QueueTypeUnknown, // we don't care about queue type
			shard.GetConfig().NamespaceCacheRefreshInterval,
		)
	}

	readerInitializer := func(readerID int32, slices []Slice) Reader {
		return NewReader(
			slices,
			&options.ReaderOptions,
			scheduler,
			rescheduler,
			timeSource,
			rateLimiter,
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

		category:    category,
		options:     options,
		rescheduler: rescheduler,
		timeSource:  shard.GetTimeSource(),
		logger:      logger,

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
			return
		}

		p.inclusiveLowWatermark = newInclusiveLowWatermark
	}

	err = p.shard.UpdateQueueState(p.category, ToPersistenceQueueState(&queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: p.nonReadableScope.Range.InclusiveMin,
	}))
}

func createCheckpointRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(100 * time.Millisecond)
	policy.SetMaximumInterval(5 * time.Second)
	policy.SetExpirationInterval(backoff.NoInterval)

	return policy
}

func newQueueIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), queueIOTimeout)
	ctx = headers.SetCallerInfo(ctx, headers.NewCallerInfo(headers.CallerTypeBackground))
	return ctx, cancel
}
