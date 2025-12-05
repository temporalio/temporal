package scheduler

import (
	"fmt"
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	BackfillerTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor
	}

	BackfillerTaskExecutor struct {
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		specProcessor  SpecProcessor
	}
)

func NewBackfillerTaskExecutor(opts BackfillerTaskExecutorOptions) *BackfillerTaskExecutor {
	return &BackfillerTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		specProcessor:  opts.SpecProcessor,
	}
}

func (b *BackfillerTaskExecutor) Validate(
	ctx chasm.Context,
	backfiller *Backfiller,
	attrs chasm.TaskAttributes,
	_ *schedulerpb.BackfillerTask,
) (bool, error) {
	return validateTaskHighWaterMark(backfiller.GetLastProcessedTime(), attrs.ScheduledTime)
}

func (b *BackfillerTaskExecutor) Execute(
	ctx chasm.MutableContext,
	backfiller *Backfiller,
	_ chasm.TaskAttributes,
	_ *schedulerpb.BackfillerTask,
) error {
	defer func() { backfiller.Attempt++ }()

	scheduler := backfiller.Scheduler.Get(ctx)
	logger := newTaggedLogger(b.baseLogger, scheduler)

	invoker := scheduler.Invoker.Get(ctx)

	// If the buffer is already full, don't move the watermark at all, just back off
	// and retry.
	tweakables := b.config.Tweakables(scheduler.Namespace)
	limit, err := b.allowedBufferedStarts(ctx, scheduler, invoker, tweakables)
	if err != nil {
		return err
	}
	if limit <= 0 {
		// Fire a timer task to retry after backoff.
		b.rescheduleBackfill(ctx, backfiller)
		return nil
	}

	// Process backfills, returning BufferedStarts.
	var result backfillProgressResult
	switch backfiller.RequestType() {
	case RequestTypeBackfill:
		result, err = b.processBackfill(ctx, scheduler, backfiller, limit)
	case RequestTypeTrigger:
		result, err = b.processTrigger(ctx, scheduler, backfiller)
	default:
		return queueerrors.NewUnprocessableTaskError(fmt.Sprintf("unknown backfill type: %v", backfiller.RequestType()))
	}
	if err != nil {
		return queueerrors.NewUnprocessableTaskError(fmt.Sprintf("failed to process backfill: %s", err.Error()))
	}

	// Enqueue new BufferedStarts on the Invoker, if we have any.
	if len(result.BufferedStarts) > 0 {
		invoker.EnqueueBufferedStarts(ctx, result.BufferedStarts)
	}

	// If we're complete, we can delete this Backfiller component and return without
	// any more tasks.
	if result.Complete {
		logger.Debug("backfill complete, deleting Backfiller",
			tag.NewStringTag("backfill-id", backfiller.GetBackfillId()))
		delete(scheduler.Backfillers, backfiller.GetBackfillId())
		return nil
	}

	// Otherwise, update watermark and reschedule.
	backfiller.LastProcessedTime = timestamppb.New(result.LastProcessedTime)
	b.rescheduleBackfill(ctx, backfiller)

	return nil
}

func (b *BackfillerTaskExecutor) rescheduleBackfill(ctx chasm.MutableContext, backfiller *Backfiller) {
	backoffTime := ctx.Now(backfiller).Add(b.backoffDelay(backfiller))
	ctx.AddTask(backfiller, chasm.TaskAttributes{
		ScheduledTime: backoffTime,
	}, &schedulerpb.BackfillerTask{})
}

// processBackfill processes a Backfiller's BackfillRequest.
func (b *BackfillerTaskExecutor) processBackfill(
	_ chasm.MutableContext,
	scheduler *Scheduler,
	backfiller *Backfiller,
	limit int,
) (result backfillProgressResult, err error) {
	request := backfiller.GetBackfillRequest()

	// Restore high watermark if we've already started processing the backfill.
	var startTime time.Time
	lastProcessed := backfiller.GetLastProcessedTime()
	if backfiller.GetAttempt() > 0 {
		startTime = lastProcessed.AsTime()
	} else {
		// On the first attempt, the start time is set slightly behind in order to make
		// the backfill start time inclusive.
		startTime = request.GetStartTime().AsTime().Add(-1 * time.Millisecond)
	}
	endTime := request.GetEndTime().AsTime()
	specResult, err := b.specProcessor.ProcessTimeRange(
		scheduler,
		startTime,
		endTime,
		request.GetOverlapPolicy(),
		scheduler.WorkflowID(),
		backfiller.GetBackfillId(),
		true,
		&limit,
	)
	if err != nil {
		return
	}

	next := specResult.NextWakeupTime
	if next.IsZero() || next.After(endTime) {
		result.Complete = true
	} else {
		// More to backfill, indicating the buffer is full. Set the high watermark, and
		// apply a backoff time before attempting to continue filling.
		result.LastProcessedTime = specResult.LastActionTime
	}
	result.BufferedStarts = specResult.BufferedStarts

	return
}

// backoffDelay returns the amount of delay that should be added when retrying.
func (b *BackfillerTaskExecutor) backoffDelay(backfiller *Backfiller) time.Duration {
	// Increment GetAttempt here early, to avoid needing to increment
	// backfiller.Attempt wherever backoffDelay's result is needed.
	return b.config.RetryPolicy().ComputeNextDelay(0, int(backfiller.GetAttempt()+1), nil)
}

// processTrigger processes a Backfiller's TriggerImmediatelyRequest.
func (b *BackfillerTaskExecutor) processTrigger(
	_ chasm.MutableContext,
	scheduler *Scheduler,
	backfiller *Backfiller,
) (result backfillProgressResult, err error) {
	request := backfiller.GetTriggerRequest()
	overlapPolicy := scheduler.resolveOverlapPolicy(request.GetOverlapPolicy())

	// Add a single manual start and mark the Backfiller as complete. For batch
	// backfill requests, a deterministic start time is trivial as they follow the
	// schedule. For immediate trigger requests, the `LastProcessedTime` (set to
	// "now" when the Backfiller is spawned to handle a request) is used for start
	// time determinism.
	nowpb := backfiller.GetLastProcessedTime()
	now := nowpb.AsTime()
	requestID := generateRequestID(scheduler, backfiller.GetBackfillId(), now, now)
	result.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   nowpb,
			ActualTime:    nowpb,
			DesiredTime:   nowpb,
			OverlapPolicy: overlapPolicy,
			Manual:        true,
			RequestId:     requestID,
		},
	}
	result.Complete = true

	return
}

// allowedBufferedStarts returns the number of BufferedStarts that the Backfiller should
// buffer, taking into account buffer limits and concurrent backfills.
func (b *BackfillerTaskExecutor) allowedBufferedStarts(
	ctx chasm.Context,
	scheduler *Scheduler,
	invoker *Invoker,
	tweakables Tweakables,
) (int, error) {
	// Count the number of Backfillers active.
	backfillerCount := 0
	for _, field := range scheduler.Backfillers {
		b := field.Get(ctx)

		// Don't count trigger-immediately requests, as they only fire a single start.
		if b.RequestType() == RequestTypeBackfill {
			backfillerCount++
		}
	}

	// Prevents a division by 0.
	backfillerCount = max(1, backfillerCount)

	// Give half the available buffer to backfillers, distributed evenly.
	return max(0, ((tweakables.MaxBufferSize/2)/backfillerCount)-len(invoker.GetBufferedStarts())), nil
}
