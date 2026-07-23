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
	schedulescommon "go.temporal.io/server/common/schedules"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	BackfillerTaskHandlerOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor
	}

	BackfillerTaskHandler struct {
		chasm.PureTaskHandlerBase
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		specProcessor  SpecProcessor
	}
)

func NewBackfillerTaskHandler(opts BackfillerTaskHandlerOptions) *BackfillerTaskHandler {
	return &BackfillerTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		specProcessor:  opts.SpecProcessor,
	}
}

// BackfillerTask invalidation reasons. Limited cardinality for ReasonTag.
const (
	backfillerInvalidatedStaleHWM metrics.ReasonString = "stale_hwm"
)

func (b *BackfillerTaskHandler) Validate(
	ctx chasm.Context,
	backfiller *Backfiller,
	attrs chasm.TaskInvocation,
	_ *schedulerpb.BackfillerTask,
) (bool, error) {
	if backfiller.Scheduler.Get(ctx).WorkflowMigration != nil {
		return false, nil
	}
	valid, err := validateTaskHighWaterMark(backfiller.GetLastProcessedTime(), attrs.ScheduledTime)
	if err != nil {
		return false, err
	}
	if !valid {
		newTaggedMetricsHandler(b.metricsHandler, backfiller.Scheduler.Get(ctx)).
			Counter(metrics.ScheduleBackfillerTask.Name()).
			Record(1, metrics.OutcomeTag(outcomeInvalidated), metrics.ReasonTag(backfillerInvalidatedStaleHWM))
	}
	return valid, nil
}

func (b *BackfillerTaskHandler) Execute(
	ctx chasm.MutableContext,
	backfiller *Backfiller,
	_ chasm.TaskAttributes,
	_ *schedulerpb.BackfillerTask,
) error {
	defer func() { backfiller.Attempt++ }()

	scheduler := backfiller.Scheduler.Get(ctx)
	logger := newTaggedLogger(b.baseLogger, scheduler)
	metricsHandler := newTaggedMetricsHandler(b.metricsHandler, scheduler)
	metricsHandler.Counter(metrics.ScheduleBackfillerTask.Name()).Record(1, metrics.OutcomeTag(outcomeFired), metrics.ReasonTag(reasonNone))

	invoker := scheduler.Invoker.Get(ctx)

	backfiller.getOrCreateEventLog(ctx).LogEvent(ctx, "backfillerTask executed")

	// If the buffer is already full, don't move the watermark at all, just back off
	// and retry.
	tweakables := b.config.Tweakables(scheduler.Namespace)
	limit, err := b.allowedBufferedStarts(ctx, scheduler, invoker, tweakables)
	if err != nil {
		return err
	}
	if limit <= 0 {
		// Buffer is full, back off and retry later. Unlike the generator, the
		// backfiller doesn't drop actions - it will retry after backoff.
		logger.Debug("Buffer full, backing off backfill",
			tag.String("backfill-id", backfiller.GetBackfillId()))
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
			tag.String("backfill-id", backfiller.GetBackfillId()))
		delete(scheduler.Backfillers, backfiller.GetBackfillId())
		metricsHandler.Counter(metrics.ScheduleBackfillerCompleted.Name()).Record(1)

		// Revive the Generator so it can re-evaluate idle/close eligibility now
		// that this backfiller is gone.
		scheduler.Generator.Get(ctx).Generate(ctx)
		return nil
	}

	// Otherwise, update watermark and reschedule.
	backfiller.LastProcessedTime = timestamppb.New(result.LastProcessedTime)
	b.rescheduleBackfill(ctx, backfiller)

	return nil
}

func (b *BackfillerTaskHandler) rescheduleBackfill(ctx chasm.MutableContext, backfiller *Backfiller) {
	backoffTime := ctx.Now(backfiller).Add(b.backoffDelay(backfiller))
	backfiller.scheduleTask(ctx, backoffTime)
}

// processBackfill processes a Backfiller's BackfillRequest.
func (b *BackfillerTaskHandler) processBackfill(
	_ chasm.MutableContext,
	scheduler *Scheduler,
	backfiller *Backfiller,
	limit int,
) (result backfillProgressResult, err error) {
	request := backfiller.GetBackfillRequest()

	endTime := request.GetEndTime().AsTime()
	// Resume from the high watermark only once genuine progress has been recorded.
	// The watermark is left unset until a batch is actually processed (see Execute),
	// so a fresh or capacity-stalled backfiller starts from the range start.
	var startTime time.Time
	lastProcessed := backfiller.GetLastProcessedTime()
	if hasRecordedProgress(lastProcessed) {
		startTime = lastProcessed.AsTime()
	} else {
		// On the first attempt, start slightly behind to make the range inclusive.
		startTime = request.GetStartTime().AsTime().Add(-1 * time.Millisecond)
	}
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

// hasRecordedProgress reports whether a backfiller's high watermark reflects a
// batch that was actually processed. An unset (nil or zero) watermark means no
// progress yet - a fresh backfiller, or one whose only attempts were buffer-full
// stalls - so processing must (re)start from the requested range start rather
// than resume.
func hasRecordedProgress(lastProcessed *timestamppb.Timestamp) bool {
	return lastProcessed != nil && (lastProcessed.GetSeconds() != 0 || lastProcessed.GetNanos() != 0)
}

// backoffDelay returns the amount of delay that should be added when retrying.
func (b *BackfillerTaskHandler) backoffDelay(backfiller *Backfiller) time.Duration {
	// Increment GetAttempt here early, to avoid needing to increment
	// backfiller.Attempt wherever backoffDelay's result is needed.
	return b.config.RetryPolicy().ComputeNextDelay(0, int(backfiller.GetAttempt()+1), nil)
}

// processTrigger processes a Backfiller's TriggerImmediatelyRequest.
func (b *BackfillerTaskHandler) processTrigger(
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
	workflowID := schedulescommon.GenerateWorkflowID(scheduler.WorkflowID(), now)
	result.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   nowpb,
			ActualTime:    nowpb,
			DesiredTime:   nowpb,
			OverlapPolicy: overlapPolicy,
			Manual:        true,
			RequestId:     requestID,
			WorkflowId:    workflowID,
		},
	}
	result.Complete = true

	return
}

// allowedBufferedStarts returns the number of BufferedStarts that the Backfiller should
// buffer, taking into account buffer limits and concurrent backfills.
func (b *BackfillerTaskHandler) allowedBufferedStarts(
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

	// Give half the available buffer to backfillers, distributed evenly, minus
	// Generator reserve space.
	pending := max(0, len(invoker.GetBufferedStarts())-recentActionCount)
	return max(0, ((tweakables.MaxBufferSize/2)/backfillerCount)-pending-tweakables.GeneratorBufferReserveSize), nil
}
