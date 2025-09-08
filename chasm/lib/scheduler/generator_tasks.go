package scheduler

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	GeneratorTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor
	}

	GeneratorTaskExecutor struct {
		GeneratorTaskExecutorOptions
	}
)

func NewGeneratorTaskExecutor(opts GeneratorTaskExecutorOptions) *GeneratorTaskExecutor {
	return &GeneratorTaskExecutor{
		GeneratorTaskExecutorOptions: opts,
	}
}

func (g *GeneratorTaskExecutor) Execute(
	ctx chasm.MutableContext,
	generator *Generator,
	_ chasm.TaskAttributes,
	_ *schedulerpb.GeneratorTask,
) error {
	scheduler, err := generator.Scheduler.Get(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w",
			serviceerror.NewInternal("scheduler tree missing node"),
			err)
	}
	logger := newTaggedLogger(g.BaseLogger, scheduler)

	invoker, err := scheduler.Invoker.Get(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w",
			serviceerror.NewInternal("scheduler tree missing node"),
			err)
	}

	// If we have no last processed time, this is a new schedule.
	if generator.LastProcessedTime == nil {
		createdAt := timestamppb.New(ctx.Now(generator))
		generator.LastProcessedTime = createdAt
		scheduler.Info.CreateTime = createdAt

		g.logSchedule(logger, "starting schedule", scheduler)
	}

	// Process time range between last high water mark and system time.
	t1 := generator.LastProcessedTime.AsTime()
	t2 := ctx.Now(generator).UTC()
	if t2.Before(t1) {
		logger.Warn("time went backwards",
			tag.NewStringerTag("time", t1),
			tag.NewStringerTag("time", t2))
		t2 = t1
	}

	result, err := g.SpecProcessor.ProcessTimeRange(scheduler, t1, t2, scheduler.overlapPolicy(), "", false, nil)
	if err != nil {
		// An error here should be impossible, send to the DLQ.
		logger.Error("error processing time range", tag.Error(err))
		return fmt.Errorf("%w: %w",
			serviceerror.NewInternalf("failed to process a time range"),
			err)
	}

	// Enqueue newly-generated buffered starts.
	if len(result.BufferedStarts) > 0 {
		invoker.EnqueueBufferedStarts(ctx, result.BufferedStarts)
	}

	// Write the new high water mark.
	generator.LastProcessedTime = timestamppb.New(result.LastActionTime)

	// Check if the schedule has gone idle.
	idleTimeTotal := g.Config.Tweakables(scheduler.Namespace).IdleTime
	idleExpiration, isIdle := scheduler.getIdleExpiration(ctx, idleTimeTotal, result.NextWakeupTime)
	if isIdle {
		// Schedule is complete, no need for another buffer task. We keep the schedule's
		// backing mutable state explicitly open for a the idle period, during which the
		// customer can describe/modify/restart the schedule.
		//
		// Once the idle timer expires, we close the component.
		ctx.AddTask(scheduler, chasm.TaskAttributes{
			ScheduledTime: idleExpiration,
		}, &schedulerpb.SchedulerIdleTask{})
		return nil
	}

	// No more tasks if we're paused.
	if scheduler.Schedule.State.Paused {
		return nil
	}

	// Another buffering task is added if we aren't completely out of actions or paused.
	ctx.AddTask(generator, chasm.TaskAttributes{
		ScheduledTime: result.NextWakeupTime,
	}, &schedulerpb.GeneratorTask{})

	return nil
}

func (g *GeneratorTaskExecutor) logSchedule(logger log.Logger, msg string, scheduler *Scheduler) {
	logger.Debug(msg,
		tag.NewStringerTag("spec", jsonStringer{scheduler.Schedule.Spec}),
		tag.NewStringerTag("policies", jsonStringer{scheduler.Schedule.Policies}))
}

func (g *GeneratorTaskExecutor) Validate(
	ctx chasm.Context,
	generator *Generator,
	_ chasm.TaskAttributes,
	_ *schedulerpb.GeneratorTask,
) (bool, error) {
	return validateTaskHighWaterMark(generator.GetLastProcessedTime(), ctx.Now(generator))
}
