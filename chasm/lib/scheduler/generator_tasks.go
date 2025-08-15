package scheduler

import (
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/fx"
	"google.golang.org/protobuf/encoding/protojson"
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
	_ *schedulespb.GeneratorTask,
) error {
	scheduler, err := generator.Scheduler.Get(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w",
			serviceerror.NewInternal("Scheduler tree missing node"),
			err)
	}
	logger := newTaggedLogger(g.BaseLogger, scheduler)

	invoker, err := scheduler.Invoker.Get(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w",
			serviceerror.NewInternal("Scheduler tree missing node"),
			err)
	}

	// If we have no last processed time, this is a new schedule.
	if generator.LastProcessedTime == nil {
		createdAt := timestamppb.New(ctx.Now(generator))
		generator.LastProcessedTime = createdAt
		scheduler.Info.CreateTime = createdAt

		g.logSchedule(logger, "Starting schedule", scheduler)
	}

	// Process time range between last high water mark and system time.
	t1 := generator.LastProcessedTime.AsTime()
	t2 := ctx.Now(generator).UTC()
	if t2.Before(t1) {
		logger.Warn("Time went backwards",
			tag.NewStringerTag("time", t1),
			tag.NewStringerTag("time", t2))
		t2 = t1
	}

	result, err := g.SpecProcessor.ProcessTimeRange(scheduler, t1, t2, scheduler.overlapPolicy(), "", false, nil)
	if err != nil {
		// An error here should be impossible, send to the DLQ.
		logger.Error("Error processing time range", tag.Error(err))
		return fmt.Errorf("%w: %w",
			serviceerror.NewInternalf("Failed to process a time range"),
			err)
	}

	// Enqueue newly-generated buffered starts.
	if len(result.BufferedStarts) > 0 {
		invoker.EnqueueBufferedStarts(ctx, result.BufferedStarts)
	}

	// Write the new high water mark.
	generator.LastProcessedTime = timestamppb.New(result.LastActionTime)

	_, inRetention := g.getRetentionExpiration(ctx, scheduler, result.NextWakeupTime)
	if inRetention {
		// We're in retention, no need for another buffer task.
		//
		// TODO - add a timer to close this scheduler tree. V1 scheduler waits this
		// period before exiting, whereas we just stop pumping new tasks.
		return nil
	}

	// No more tasks if we're paused.
	if scheduler.Schedule.State.Paused {
		return nil
	}

	// Another buffering task is added if we aren't completely out of actions or paused.
	ctx.AddTask(generator, chasm.TaskAttributes{
		ScheduledTime: result.NextWakeupTime,
	}, &schedulespb.GeneratorTask{})

	return nil
}

func (g *GeneratorTaskExecutor) logSchedule(logger log.Logger, msg string, scheduler *Scheduler) {
	// Log spec as json since it's more readable than the Go representation.
	specJson, _ := protojson.Marshal(scheduler.Schedule.Spec)
	policiesJson, _ := protojson.Marshal(scheduler.Schedule.Policies)
	logger.Debug(msg,
		tag.NewStringTag("spec", string(specJson)),
		tag.NewStringTag("policies", string(policiesJson)))
}

func (g *GeneratorTaskExecutor) Validate(
	ctx chasm.Context,
	generator *Generator,
	_ chasm.TaskAttributes,
	_ *schedulespb.GeneratorTask,
) (bool, error) {
	return validateTaskHighWaterMark(generator.GetLastProcessedTime(), ctx.Now(generator))
}

// getRetentionExpiration returns a retention time and the boolean value of
// 'true' for when a schedule is in retention (pending soft delete).
func (g *GeneratorTaskExecutor) getRetentionExpiration(
	ctx chasm.Context,
	scheduler *Scheduler,
	nextWakeup time.Time,
) (time.Time, bool) {
	retentionTime := g.Config.Tweakables(scheduler.Namespace).RetentionTime

	// If RetentionTime is not set or the schedule is paused or nextWakeup time is not zero
	// or there are more actions to take, there is no need for retention.
	if retentionTime == 0 ||
		scheduler.Schedule.State.Paused ||
		(!nextWakeup.IsZero() && scheduler.useScheduledAction(false)) ||
		scheduler.hasMoreAllowAllBackfills(ctx) {
		return time.Time{}, false
	}

	return scheduler.getLastEventTime().Add(retentionTime), true
}
