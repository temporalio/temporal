package scheduler

import (
	"fmt"

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
	GeneratorTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor
		SpecBuilder    *scheduler.SpecBuilder
	}

	GeneratorTaskExecutor struct {
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		SpecProcessor  SpecProcessor
		specBuilder    *scheduler.SpecBuilder
	}
)

func NewGeneratorTaskExecutor(opts GeneratorTaskExecutorOptions) *GeneratorTaskExecutor {
	return &GeneratorTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		SpecProcessor:  opts.SpecProcessor,
		specBuilder:    opts.SpecBuilder,
	}
}

func (g *GeneratorTaskExecutor) Execute(
	ctx chasm.MutableContext,
	generator *Generator,
	_ chasm.TaskAttributes,
	_ *schedulerpb.GeneratorTask,
) error {
	scheduler := generator.Scheduler.Get(ctx)
	logger := newTaggedLogger(g.baseLogger, scheduler)
	metricsHandler := newTaggedMetricsHandler(g.metricsHandler, scheduler)
	invoker := scheduler.Invoker.Get(ctx)

	// If we have no last processed time, this is a new schedule.
	if generator.LastProcessedTime == nil {
		createdAt := timestamppb.New(ctx.Now(generator))
		generator.LastProcessedTime = createdAt
		scheduler.Info.CreateTime = createdAt

		g.logSchedule(logger, "starting schedule", scheduler)
	}

	// If the high water mark is earlier than when a schedule was updated, we must skip any actions that hadn't
	// yet been processed.
	if scheduler.Info.GetUpdateTime().AsTime().After(generator.LastProcessedTime.AsTime()) {
		generator.LastProcessedTime = scheduler.Info.GetUpdateTime()
	}

	// Process time range between last high water mark and system time.
	t1 := generator.LastProcessedTime.AsTime()
	t2 := ctx.Now(generator).UTC()
	if t2.Before(t1) {
		logger.Error("time went backwards",
			tag.Stringer("time", t1),
			tag.Stringer("time", t2))
		t2 = t1
	}

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

	// Check if the schedule has gone idle.
	idleTimeTotal := g.config.Tweakables(scheduler.Namespace).IdleTime
	idleExpiration, isIdle := scheduler.getIdleExpiration(ctx, idleTimeTotal, result.NextWakeupTime)
	if isIdle {
		// Schedule is complete, no need for another buffer task. We keep the schedule's
		// backing mutable state explicitly open for a the idle period, during which the
		// customer can describe/modify/restart the schedule.
		//
		// Once the idle timer expires, we close the component.
		ctx.AddTask(scheduler, chasm.TaskAttributes{
			ScheduledTime: idleExpiration,
		}, &schedulerpb.SchedulerIdleTask{
			IdleTimeTotal: durationpb.New(idleTimeTotal),
		})
		return nil
	}

	// No more tasks if we're paused.
	if scheduler.Schedule.State.Paused {
		return nil
	}

	// Another buffering task is added if we aren't completely out of actions or paused.
	generator.scheduleTask(ctx, result.NextWakeupTime)

	return nil
}

func (g *GeneratorTaskExecutor) logSchedule(logger log.Logger, msg string, scheduler *Scheduler) {
	logger.Debug(msg,
		tag.Stringer("spec", jsonStringer{scheduler.Schedule.Spec}),
		tag.Stringer("policies", jsonStringer{scheduler.Schedule.Policies}))
}

func (g *GeneratorTaskExecutor) Validate(
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
