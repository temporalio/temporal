package queues

import (
	"context"
	"math"
	"sync"
	"time"

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
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

const (
	DefaultReaderId = common.DefaultQueueReaderID

	// Non-default readers will use critical pending task count * (this multiplier ^ readerID)
	// as its max pending task count so that their loading will never trigger pending
	// task alert & action
	maxPendingTaskMultiplier = 0.8
	minMaxPendingTaskCount   = 1000

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
		shard historyi.ShardContext

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
		grouper        Grouper
		logger         log.Logger
		metricsHandler metrics.Handler

		paginationFnProvider PaginationFnProvider
		executableFactory    ExecutableFactory

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
		MoveGroupTaskCountBase              dynamicconfig.IntPropertyFn
		MoveGroupTaskCountMultiplier        dynamicconfig.FloatPropertyFn
	}
)

func newQueueBase(
	shard historyi.ShardContext,
	category tasks.Category,
	paginationFnProvider PaginationFnProvider,
	scheduler Scheduler,
	rescheduler Rescheduler,
	executableFactory ExecutableFactory,
	options *Options,
	hostReaderRateLimiter quotas.RequestRateLimiter,
	completionFn ReaderCompletionFn,
	grouper Grouper,
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

	monitor := newMonitor(category.Type(), shard.GetTimeSource(), &options.MonitorOptions)
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

			// Use lower maxPendingTaskCount for lower reader to guarantee that higher reader can
			// always have some tasks loaded.
			readerOptions.MaxPendingTasksCount = func() int {
				return max(
					minMaxPendingTaskCount,
					int(float64(options.PendingTasksCriticalCount())*
						math.Pow(maxPendingTaskMultiplier, float64(readerID))),
				)
			}
		}

		return NewReader(
			readerID,
			slices,
			&readerOptions,
			scheduler,
			rescheduler,
			shard.GetTimeSource(),
			readerRateLimiter,
			monitor,
			completionFn,
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
			slices = append(slices, NewSlice(paginationFnProvider, executableFactory, monitor, scope, grouper, options.ReaderOptions.MaxPredicateSize))
		}
		readerGroup.NewReader(readerID, slices...)

		exclusiveDeletionHighWatermark = tasks.MinKey(exclusiveDeletionHighWatermark, scopes[0].Range.InclusiveMin)
	}

	mitigator := newMitigator(readerGroup, monitor, logger, metricsHandler, options.MaxReaderCount, grouper)

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
		grouper:        grouper,
		logger:         logger,
		metricsHandler: metricsHandler,

		paginationFnProvider: paginationFnProvider,
		executableFactory:    executableFactory,

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
			clock.NewRealTimeSource(),
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
	newMaxKey := p.shard.GetQueueExclusiveHighReadWatermark(p.category)

	slices := make([]Slice, 0, 1)
	if p.nonReadableScope.CanSplitByRange(newMaxKey) {
		var newReadScope Scope
		newReadScope, p.nonReadableScope = p.nonReadableScope.SplitByRange(newMaxKey)
		slices = append(slices, NewSlice(
			p.paginationFnProvider,
			p.executableFactory,
			p.monitor,
			newReadScope,
			p.grouper,
			p.options.ReaderOptions.MaxPredicateSize,
		))
	}

	reader, ok := p.readerGroup.ReaderByID(DefaultReaderId)
	if !ok {
		p.readerGroup.NewReader(DefaultReaderId, slices...)
		return
	}

	if now := p.timeSource.Now(); now.After(p.nextForceNewSliceTime) {
		reader.AppendSlices(slices...)
		p.nextForceNewSliceTime = now.Add(forceNewSliceDuration)
	} else {
		reader.MergeSlices(slices...)
	}
}

func (p *queueBase) checkpoint() {
	var tasksCompleted int
	p.readerGroup.ForEach(func(_ int64, r Reader) {
		tasksCompleted += r.ShrinkSlices()
	})

	var checkpointAction Action
	maxReaderCount := p.options.MaxReaderCount()
	if taskCountBase := p.options.MoveGroupTaskCountBase(); taskCountBase > 0 {
		// Run an action to proactively move task group with high pending task to non-default reader
		// so that upon shard reload, those groups won't block other tasks in the default reader from
		// being loaded.
		checkpointAction = newMoveGroupAction(maxReaderCount, p.grouper, taskCountBase, p.options.MoveGroupTaskCountMultiplier(), p.logger)
	} else {
		// Run slicePredicateAction to move slices with non-universal predicate to non-default reader
		// so that upon shard reload, task loading for those slices won't block other slices in the default reader.
		checkpointAction = newSlicePredicateAction(p.monitor, maxReaderCount)
	}

	runAction(checkpointAction, p.readerGroup, p.metricsHandler)

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
	metrics.QueueReaderCountHistogram.With(p.metricsHandler).Record(int64(len(readerScopes)))
	metrics.QueueSliceCountHistogram.With(p.metricsHandler).Record(int64(p.monitor.GetTotalSliceCount()))
	metrics.PendingTasksCounter.With(p.metricsHandler).Record(int64(p.monitor.GetTotalPendingTaskCount()))

	// NOTE: Must range-complete task first.
	// Otherwise, if state is updated first, later deletion fails and the shard gets reloaded.
	// Some tasks will never be deleted.
	//
	// Emit metric before the deletion watermark comparison so we have the emit even if there's no task
	// for the queue.
	metrics.TaskBatchCompleteCounter.With(p.metricsHandler).Record(1)
	if newExclusiveDeletionHighWatermark.CompareTo(p.exclusiveDeletionHighWatermark) > 0 ||
		(p.updateShardRangeID() && newExclusiveDeletionHighWatermark.CompareTo(tasks.MinimumKey) > 0) {
		// When shard rangeID is updated, perform range completion again in case the underlying persistence implementation
		// serves traffic based on the persisted shardInfo.
		err := p.rangeCompleteTasks(p.exclusiveDeletionHighWatermark, newExclusiveDeletionHighWatermark)
		if err != nil {
			p.resetCheckpointTimer(err)
			return
		}

		p.exclusiveDeletionHighWatermark = newExclusiveDeletionHighWatermark
	}

	err := p.updateQueueState(tasksCompleted, readerScopes)
	p.resetCheckpointTimer(err)
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
	tasksCompleted int,
	readerScopes map[int64][]Scope,
) error {
	metrics.AckLevelUpdateCounter.With(p.metricsHandler).Record(1)
	for readerID, scopes := range readerScopes {
		if len(scopes) == 0 {
			delete(readerScopes, readerID)
		}
	}

	err := p.shard.SetQueueState(p.category, tasksCompleted, ToPersistenceQueueState(&queueState{
		readerScopes:                 readerScopes,
		exclusiveReaderHighWatermark: p.nonReadableScope.Range.InclusiveMin,
	}))
	if err != nil {
		metrics.AckLevelUpdateFailedCounter.With(p.metricsHandler).Record(1)
		p.logger.Error("Error updating queue state", tag.Error(err), tag.OperationFailed)
	}
	return err
}

func (p *queueBase) resetCheckpointTimer(checkPointErr error) {
	if checkPointErr != nil {
		delay := p.checkpointRetrier.NextBackOff(checkPointErr)
		p.checkpointTimer.Reset(delay)
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
	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundHighCallerInfo)
	return ctx, cancel
}
