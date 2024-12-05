package generator

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	schedpb "go.temporal.io/server/api/schedule/v1"
	servercommon "go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/scheduler2/common"
	"go.temporal.io/server/components/scheduler2/core"
	"go.temporal.io/server/components/scheduler2/executor"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	scheduler1 "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	TaskExecutorOptions struct {
		fx.In

		Config         *common.Config
		MetricsHandler metrics.Handler
		Logger         log.Logger
		SpecBuilder    *scheduler1.SpecBuilder
	}

	taskExecutor struct {
		TaskExecutorOptions
	}
)

func RegisterExecutor(registry *hsm.Registry, options TaskExecutorOptions) error {
	e := taskExecutor{options}
	if err := hsm.RegisterImmediateExecutor(registry, e.executeBufferTask); err != nil {
		return err
	}
	return nil
}

func (e taskExecutor) runBufferLoop(ctx context.Context, env hsm.Environment, ref hsm.Ref) (wakeupTime time.Time, err error) {
	wakeupTime = time.Now()
	scheduler, err := common.LoadSchedulerFromParent(ctx, env, ref)
	if err != nil {
		return
	}
	tweakables := e.Config.Tweakables(scheduler.Namespace)

	generator, err := e.loadGenerator(ctx, env, ref)
	if err != nil {
		return
	}

	// If we have no last processed time, this is a new schedule.
	if generator.LastProcessedTime == nil {
		generator.LastProcessedTime = timestamppb.Now()
		// TODO - update schedule info with create time

		e.logSchedule("Starting schedule", scheduler)
	}

	// Process time range between last high water mark and system time.
	t1 := generator.LastProcessedTime.AsTime()
	t2 := time.Now()
	if t2.Before(t1) {
		e.Logger.Warn("Time went backwards",
			tag.NewStringTag("time", t1.String()),
			tag.NewStringTag("time", t2.String()))
		t2 = t1
	}

	res, err := e.processTimeRange(scheduler, tweakables, t1, t2, nil)
	if err != nil {
		// An error here should be impossible, send to the DLQ.
		e.Logger.Error("Error processing time range", tag.Error(err))

		err = fmt.Errorf(
			"%w: %w",
			err,
			serviceerror.NewInternal("Scheduler's Generator failed to process a time range"),
		)
		return
	}
	generator.LastProcessedTime = timestamppb.New(res.LastActionTime)
	generator.NextInvocationTime = timestamppb.New(res.NextWakeupTime)
	wakeupTime = res.NextWakeupTime

	err = env.Access(ctx, ref, hsm.AccessWrite, func(generatorNode *hsm.Node) error {
		// Check if an update was applied to the scheduler while actions were being
		// buffered.
		refreshedScheduler, err := hsm.MachineData[core.Scheduler](generatorNode.Parent)
		if err != nil {
			return err
		}
		if refreshedScheduler.ConflictToken != scheduler.ConflictToken {
			// Schedule was updated while processing, drop this task as the update will have
			// generated another.
			return fmt.Errorf(
				"%w: Scheduler state was updated while buffering actions",
				consts.ErrStaleReference,
			)
		}

		// Transition the executor sub state machine to execute the new buffered actions.
		executorNode, err := generatorNode.Parent.Child([]hsm.Key{executor.MachineKey})
		if err != nil {
			return fmt.Errorf(
				"%w: %w",
				err,
				serviceerror.NewInternal("Scheduler is missing its Executor node"),
			)
		}
		err = hsm.MachineTransition(executorNode, func(e executor.Executor) (hsm.TransitionOutput, error) {
			return executor.TransitionExecute.Apply(e, executor.EventExecute{
				Node:            executorNode,
				BufferedActions: res.BufferedStarts,
			})
		})
		if err != nil {
			return fmt.Errorf(
				"%w: unable to transition Executor to Executing state",
				err,
			)
		}

		// Write Generator internal state, flushing the high water mark to persistence.
		// if we fail conflict here (after transitioning Executor), that's fine; buffered
		// actions are idempotent through their generated request IDs.
		return hsm.MachineTransition(generatorNode, func(g Generator) (hsm.TransitionOutput, error) {
			g.GeneratorInternal = generator.GeneratorInternal
			return hsm.TransitionOutput{}, nil
		})
	})
	return
}

func (e taskExecutor) executeBufferTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task BufferTask) error {
	// The buffer task runs indefinitely (unless runBufferLoop fails).
	for {
		wakeupTime, err := e.runBufferLoop(ctx, env, ref)
		if err != nil {
			return err
		}

		e.Logger.Debug("Sleeping after buffering",
			tag.NewTimeTag("wakeupTime", wakeupTime))
		time.Sleep(wakeupTime.Sub(time.Now()))
	}
}

func (e taskExecutor) logSchedule(msg string, scheduler core.Scheduler) {
	// Log spec as json since it's more readable than the Go representation.
	specJson, _ := protojson.Marshal(scheduler.Schedule.Spec)
	policiesJson, _ := protojson.Marshal(scheduler.Schedule.Policies)
	e.Logger.Info(msg,
		tag.NewStringTag("spec", string(specJson)),
		tag.NewStringTag("policies", string(policiesJson)))
}

type processedTimeRangeResult struct {
	NextWakeupTime time.Time
	LastActionTime time.Time
	BufferedStarts []*schedpb.BufferedStart
}

// processTimeRange generates buffered actions according to the schedule spec for
// the given time range.
func (e taskExecutor) processTimeRange(
	scheduler core.Scheduler,
	tweakables common.Tweakables,
	start, end time.Time,
	limit *int,
) (*processedTimeRangeResult, error) {
	overlapPolicy := scheduler.OverlapPolicy()

	e.Logger.Debug("processTimeRange",
		tag.NewTimeTag("start", start),
		tag.NewTimeTag("end", end),
		tag.NewAnyTag("overlap-policy", overlapPolicy),
		tag.NewBoolTag("manual", false))

	// Peek at paused/remaining actions state and don't bother if we're not going to
	// take an action now. (Don't count as missed catchup window either.)
	// Skip over entire time range if paused or no actions can be taken.
	if !scheduler.UseScheduledAction(false) {
		// Use end as last action time so that we don't reprocess time spent paused.
		next, err := e.getNextTime(scheduler, end)
		if err != nil {
			return nil, err
		}

		return &processedTimeRangeResult{
			NextWakeupTime: next.Next,
			LastActionTime: end,
			BufferedStarts: nil,
		}, nil
	}

	catchupWindow := catchupWindow(scheduler, tweakables)
	lastAction := start
	var next scheduler1.GetNextTimeResult
	var bufferedStarts []*schedpb.BufferedStart
	for next, err := e.getNextTime(scheduler, start); err == nil && !(next.Next.IsZero() || next.Next.After(end)); next, err = e.getNextTime(scheduler, next.Next) {
		if scheduler.Info.UpdateTime.AsTime().After(next.Next) {
			// If we've received an update that took effect after the LastProcessedTime high
			// water mark, discard actions that were scheduled to kick off before the update.
			continue
		}

		if end.Sub(next.Next) > catchupWindow {
			e.Logger.Warn("Schedule missed catchup window",
				tag.NewTimeTag("now", end),
				tag.NewTimeTag("time", next.Next))
			e.MetricsHandler.Counter(metrics.ScheduleMissedCatchupWindow.Name()).Record(1)

			// TODO - update Info.MissedCatchupWindow
			// s.Info.MissedCatchupWindow++
			// or write that to the generator's persisted state?
			continue
		}

		bufferedStarts = append(bufferedStarts, &schedpb.BufferedStart{
			NominalTime:   timestamppb.New(next.Nominal),
			ActualTime:    timestamppb.New(next.Next),
			OverlapPolicy: overlapPolicy,
			Manual:        false,
			RequestId:     common.GenerateRequestID(scheduler, "", next.Nominal, next.Next),
		})
		lastAction = next.Next

		if limit != nil {
			if (*limit)--; *limit <= 0 {
				break
			}
		}
	}

	return &processedTimeRangeResult{
		NextWakeupTime: next.Next,
		LastActionTime: lastAction,
		BufferedStarts: bufferedStarts,
	}, nil
}

func catchupWindow(s core.Scheduler, tweakables common.Tweakables) time.Duration {
	cw := s.Schedule.Policies.CatchupWindow
	if cw == nil {
		return tweakables.DefaultCatchupWindow
	}

	return max(cw.AsDuration(), tweakables.MinCatchupWindow)
}

// getNextTime returns the next time result, or an error if the schedule cannot be compiled.
func (e taskExecutor) getNextTime(s core.Scheduler, after time.Time) (scheduler1.GetNextTimeResult, error) {
	spec, err := s.CompiledSpec(e.SpecBuilder)
	if err != nil {
		e.Logger.Error("Invalid schedule", tag.Error(err))
		return scheduler1.GetNextTimeResult{}, err
	}

	return spec.GetNextTime(s.JitterSeed(), after), nil
}

// loadGenerator loads the Generator's persisted state, returning a cloned copy.
func (e taskExecutor) loadGenerator(ctx context.Context, env hsm.Environment, ref hsm.Ref) (generator Generator, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		prevGenerator, err := hsm.MachineData[Generator](node)
		generator = Generator{
			GeneratorInternal: servercommon.CloneProto(prevGenerator.GeneratorInternal),
		}
		return err
	})
	return
}
