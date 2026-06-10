package scheduler

import (
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	GeneratorTaskHandlerOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor
		SpecBuilder    *scheduler.SpecBuilder
	}

	GeneratorTaskHandler struct {
		chasm.PureTaskHandlerBase
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		SpecProcessor  SpecProcessor
		specBuilder    *scheduler.SpecBuilder
	}
)

func NewGeneratorTaskHandler(opts GeneratorTaskHandlerOptions) *GeneratorTaskHandler {
	return &GeneratorTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		SpecProcessor:  opts.SpecProcessor,
		specBuilder:    opts.SpecBuilder,
	}
}

func (g *GeneratorTaskHandler) Execute(
	ctx chasm.MutableContext,
	generator *Generator,
	_ chasm.TaskAttributes,
	_ *schedulerpb.GeneratorTask,
) error {
	scheduler := generator.Scheduler.Get(ctx)
	logger := newTaggedLogger(g.baseLogger, scheduler)
	metricsHandler := newTaggedMetricsHandler(g.metricsHandler, scheduler)
	invoker := scheduler.Invoker.Get(ctx)

	now := ctx.Now(generator)

	generator.EventLog.Get(ctx).LogEvent(ctx, "generatorTask executed")

	// If we have no last processed time, this is a new schedule.
	if generator.LastProcessedTime == nil {
		createdAt := timestamppb.New(now)
		generator.LastProcessedTime = createdAt
		scheduler.Info.CreateTime = createdAt

		g.logSchedule(ctx, logger, "starting schedule", generator, scheduler)
	}

	// If the high water mark is earlier than when a schedule was updated, we must skip any actions that hadn't
	// yet been processed.
	if scheduler.Info.GetUpdateTime().AsTime().After(generator.LastProcessedTime.AsTime()) {
		generator.LastProcessedTime = scheduler.Info.GetUpdateTime()
	}

	// Process time range between last high water mark and system time.
	t1 := generator.LastProcessedTime.AsTime()
	t2 := now.UTC()
	if t2.Before(t1) {
		logger.Error("time went backwards",
			tag.Stringer("time", t1),
			tag.Stringer("time", t2))
		t2 = t1
	}

	// Generate BufferedStarts and determine the next HWM. Actions are skipped when
	// they can't be taken (paused, or limited and without any remaining actions).
	result, err := g.SpecProcessor.ProcessTimeRange(
		scheduler,
		t1, t2,
		scheduler.overlapPolicy(),
		scheduler.WorkflowID(),
		"",
		false,
		nil,
	)
	if err != nil {
		// An error here should be impossible, send to the DLQ.
		return queueerrors.NewUnprocessableTaskError(
			fmt.Sprintf("failed to process a time range: %s", err.Error()))
	}

	// Emit metrics and update state for any dropped actions.
	if result.DroppedCount > 0 {
		generator.EventLog.Get(ctx).LogEvent(ctx,
			fmt.Sprintf("buffer overrun, dropped %d actions", result.DroppedCount))
		logger.Warn("Buffer overrun, dropping actions",
			tag.Int64("dropped-count", result.DroppedCount))
		metricsHandler.Counter(metrics.ScheduleBufferOverruns.Name()).Record(result.DroppedCount)
		scheduler.Info.BufferDropped += result.DroppedCount
	}

	// Enqueue newly-generated buffered starts.
	if len(result.BufferedStarts) > 0 {
		invoker.EnqueueBufferedStarts(ctx, result.BufferedStarts)
	}

	// Write the new high water mark and future action times.
	generator.LastProcessedTime = timestamppb.New(result.LastActionTime)
	generator.UpdateFutureActionTimes(ctx, g.specBuilder)

	// Schedule the next timer task. Three outcomes are possible:
	// - isIdle: the schedule is done; arm the idle task to close it.
	// - NextWakeupTime is set: arm the next generator tick.
	// - Neither: Hold open without a task. This requires both that
	//   isHeldOpen is true (paused or backfill pending) AND that no spec
	//   wakeup is available, e.g. a paused manual-only schedule. IdleTime=0
	//   also lands here. An external trigger (Patch.Unpause, Update, or a
	//   BackfillerTask's completion-time Generate call) revives us.
	idleTimeTotal := g.config.Tweakables(scheduler.Namespace).IdleTime
	idleExpiration, isIdle := scheduler.getIdleExpiration(ctx, idleTimeTotal, result.NextWakeupTime)
	if isIdle {
		// Schedule is complete, no need for another buffer task. We keep the schedule's
		// backing mutable state explicitly open for the idle period, during which the
		// customer can describe/modify/restart the schedule.
		//
		// Once the idle timer expires, we close the component.
		generator.EventLog.Get(ctx).LogEvent(ctx,
			fmt.Sprintf("scheduled idle task for %s", idleExpiration.Format(time.RFC3339)))
		ctx.AddTask(scheduler, chasm.TaskAttributes{
			ScheduledTime: idleExpiration,
		}, &schedulerpb.SchedulerIdleTask{
			IdleTimeTotal: durationpb.New(idleTimeTotal),
		})
		// Record the idle-close deadline so it can be surfaced as the
		// ScheduleIdleCloseTime search attribute for stuck-schedule detection.
		scheduler.IdleCloseTime = timestamppb.New(idleExpiration)

		return nil
	}

	// Not idle: the schedule has work again (or is being held open), so it's
	// no longer pending an idle close.
	scheduler.IdleCloseTime = nil

	if !result.NextWakeupTime.IsZero() {
		// Keep the generator task perpetually scheduled. When paused, the next
		// fire will simply advance the HWM without appending actions (handled in
		// ProcessTimeRange).
		generator.scheduleTask(ctx, result.NextWakeupTime)
	}
	// else: hold open without a task; see the comment block above.

	return nil
}

func (g *GeneratorTaskHandler) logSchedule(ctx chasm.MutableContext, logger log.Logger, msg string, generator *Generator, sched *Scheduler) {
	spec := jsonStringer{sched.Schedule.Spec}
	policies := jsonStringer{sched.Schedule.Policies}

	generator.EventLog.Get(ctx).LogEvent(ctx, fmt.Sprintf("%s:\nSpec: %s\nPolicies: %s\n", msg, spec, policies))
	logger.Info(msg,
		tag.Stringer("spec", spec),
		tag.Stringer("policies", policies))
}

func (g *GeneratorTaskHandler) Validate(
	ctx chasm.Context,
	generator *Generator,
	attrs chasm.TaskAttributes,
	_ *schedulerpb.GeneratorTask,
) (bool, error) {
	return validateTaskHighWaterMark(
		generator.GetLastProcessedTime(),
		attrs.ScheduledTime,
	)
}
