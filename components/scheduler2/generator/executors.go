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
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/components/scheduler2/common"
	"go.temporal.io/server/components/scheduler2/core"
	"go.temporal.io/server/components/scheduler2/executor"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"
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
	if err := hsm.RegisterTimerExecutor(registry, e.executeSleepTask); err != nil {
		return err
	}
	if err := hsm.RegisterImmediateExecutor(registry, e.executeBufferTask); err != nil {
		return err
	}
	return nil
}

func (e taskExecutor) executeSleepTask(env hsm.Environment, node *hsm.Node, task SleepTask) error {
	return hsm.MachineTransition(node, func(g Generator) (hsm.TransitionOutput, error) {
		return TransitionBuffer.Apply(g, EventBuffer{
			Node: node,
		})
	})
}

func (e taskExecutor) executeBufferTask(ctx context.Context, env hsm.Environment, ref hsm.Ref, task BufferTask) error {
	scheduler, err := common.LoadSchedulerFromParent(ctx, env, ref)
	if err != nil {
		return err
	}
	tweakables := e.Config.Tweakables(scheduler.Namespace)

	generator, err := e.loadGenerator(ctx, env, ref)
	if err != nil {
		return err
	}

	// if we have no last processed time, this is a new schedule
	if generator.LastProcessedTime == nil {
		generator.LastProcessedTime = timestamppb.Now()
		// TODO - update schedule info with create time

		e.logSchedule("Starting schedule", scheduler)
	}

	// process time range between last high water mark and system time
	t1 := timestamp.TimeValue(generator.LastProcessedTime)
	t2 := time.Now()
	if t2.Before(t1) {
		e.Logger.Warn("Time went backwards",
			tag.NewStringTag("time", t1.String()),
			tag.NewStringTag("time", t2.String()))
		t2 = t1
	}

	res, err := e.processTimeRange(scheduler, tweakables, t1, t2, nil)
	if err != nil {
		// An error here should be impossible and go to the DLQ
		e.Logger.Error("Error processing time range", tag.Error(err))

		return fmt.Errorf(
			"%w: %w",
			err,
			serviceerror.NewInternal("Scheduler's Generator failed to process a time range"),
		)
	}
	generator.LastProcessedTime = timestamppb.New(res.LastActionTime)
	generator.NextInvocationTime = timestamppb.New(res.NextWakeupTime)

	return env.Access(ctx, ref, hsm.AccessWrite, func(generatorNode *hsm.Node) error {
		// Check for scheduler version conflict. We don't need to bump it, as the
		// Generator's buffered actions are idempotent when pushed to the Executor, and the
		// Generator's own state is also versioned. This helps Generator quickly react to
		// schedule updates/pauses.
		refreshedScheduler, err := hsm.MachineData[core.Scheduler](generatorNode.Parent)
		if err != nil {
			return err
		}
		if refreshedScheduler.ConflictToken != scheduler.ConflictToken {
			// schedule was updated while processing, retry
			return fmt.Errorf(
				"%w: Scheduler state was updated while buffering actions",
				consts.ErrStaleState,
			)
		}

		// transition the executor substate machine to execute the new buffered actions
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
				BufferedActions: []*schedpb.BufferedStart{},
			})
		})
		if err != nil {
			return fmt.Errorf(
				"%w: unable to transition Executor to Executing state",
				err,
			)
		}

		// transition the generator back to waiting with new wait times. if we fail
		// conflict here (after transitioning Executor), that's fine; buffered actions
		// are idempotent through their generated request IDs
		return hsm.MachineTransition(generatorNode, func(g Generator) (hsm.TransitionOutput, error) {
			if g.ConflictToken != generator.ConflictToken {
				return hsm.TransitionOutput{}, fmt.Errorf(
					"%w: conflicting Generator state while buffering actions",
					consts.ErrStaleState,
				)
			}

			g.GeneratorInternalState = generator.GeneratorInternalState
			g.ConflictToken++
			return TransitionSleep.Apply(g, EventSleep{
				Node:     generatorNode,
				Deadline: generator.NextInvocationTime.AsTime(),
			})
		})
	})
}

func (e taskExecutor) logSchedule(msg string, scheduler core.Scheduler) {
	// log spec as json since it's more readable than the Go representation
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

// Processes the given time range, generating buffered actions according to the
// schedule spec.
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

	catchupWindow := e.catchupWindow(scheduler, tweakables)

	// Peek at paused/remaining actions state and don't bother if we're not going to
	// take an action now. (Don't count as missed catchup window either.)
	// Skip over entire time range if paused or no actions can be taken.
	if !scheduler.CanTakeScheduledAction(false) {
		// use end as last action time so that we don't reprocess time spent paused
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

	lastAction := start
	var next scheduler1.GetNextTimeResult
	var bufferedStarts []*schedpb.BufferedStart
	for next, err := e.getNextTime(scheduler, start); err == nil && !(next.Next.IsZero() || next.Next.After(end)); next, err = e.getNextTime(scheduler, next.Next) {
		if scheduler.Info.UpdateTime.AsTime().After(next.Next) {
			// We're reprocessing since the most recent event after an update. Discard actions before
			// the update time (which was just set to "now"). This doesn't have to be guarded with
			// hasMinVersion because this condition couldn't happen in previous versions.
			continue
		}

		if end.Sub(next.Next) > catchupWindow {
			e.Logger.Warn("Schedule missed catchup window", tag.NewTimeTag("now", end), tag.NewTimeTag("time", next.Next))
			e.MetricsHandler.Counter(metrics.ScheduleMissedCatchupWindow.Name()).Record(1)

			// TODO - update Info.MissedCatchupWindow
			// s.Info.MissedCatchupWindow++
			// or write that to the generator's persisted state?
			continue
		}

		bufferedStarts = append(bufferedStarts, &schedpb.BufferedStart{
			NominalTime:   timestamppb.New(next.Nominal),
			ActualTime:    timestamppb.New(next.Next),
			OverlapPolicy: scheduler.OverlapPolicy(),
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

func (e taskExecutor) catchupWindow(s core.Scheduler, tweakables common.Tweakables) time.Duration {
	cw := s.Schedule.Policies.CatchupWindow
	if cw == nil {
		return tweakables.DefaultCatchupWindow
	}

	return max(cw.AsDuration(), tweakables.MinCatchupWindow)
}

// Returns the next time result, or an error if the schedule cannot be compiled.
func (e taskExecutor) getNextTime(s core.Scheduler, after time.Time) (scheduler.GetNextTimeResult, error) {
	spec, err := s.CompiledSpec(e.SpecBuilder)
	if err != nil {
		e.Logger.Error("Invalid schedule", tag.Error(err))
		return scheduler1.GetNextTimeResult{}, err
	}

	return spec.GetNextTime(s.JitterSeed(), after), nil
}

// Loads the generator's persisted state, returning a cloned copy.
func (e taskExecutor) loadGenerator(ctx context.Context, env hsm.Environment, ref hsm.Ref) (generator Generator, err error) {
	err = env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		prevGenerator, err := hsm.MachineData[Generator](node)
		generator = Generator{
			GeneratorInternalState: servercommon.CloneProto(prevGenerator.GeneratorInternalState),
		}
		return err
	})
	return
}
