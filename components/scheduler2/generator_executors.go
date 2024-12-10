package scheduler2

import (
	"fmt"
	"time"

	"go.temporal.io/api/serviceerror"
	servercommon "go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/hsm"
	"go.uber.org/fx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	GeneratorTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		Logger         log.Logger
		SpecProcessor  *SpecProcessor
	}

	generatorTaskExecutor struct {
		GeneratorTaskExecutorOptions
	}
)

func RegisterGeneratorExecutors(registry *hsm.Registry, options GeneratorTaskExecutorOptions) error {
	e := generatorTaskExecutor{options}
	if err := hsm.RegisterTimerExecutor(registry, e.executeBufferTask); err != nil {
		return err
	}
	return nil
}

func (e generatorTaskExecutor) executeBufferTask(env hsm.Environment, node *hsm.Node, task BufferTask) error {
	schedulerNode := node.Parent
	scheduler, err := e.loadScheduler(schedulerNode)
	if err != nil {
		return err
	}

	generator, err := e.loadGenerator(node)
	if err != nil {
		return err
	}

	// If we have no last processed time, this is a new schedule.
	if generator.LastProcessedTime == nil {
		generator.LastProcessedTime = timestamppb.Now()
		// TODO - update schedule info with create time

		e.logSchedule("Starting schedule", scheduler)
	}

	// Process time range between last high water mark and system time.
	t1 := generator.LastProcessedTime.AsTime()
	t2 := time.Now().UTC()
	if t2.Before(t1) {
		e.Logger.Warn("Time went backwards",
			tag.NewStringTag("time", t1.String()),
			tag.NewStringTag("time", t2.String()))
		t2 = t1
	}

	res, err := e.SpecProcessor.ProcessTimeRange(scheduler, t1, t2, false, nil)
	if err != nil {
		// An error here should be impossible, send to the DLQ.
		e.Logger.Error("Error processing time range", tag.Error(err))

		return fmt.Errorf(
			"%w: %w",
			err,
			serviceerror.NewInternal("Scheduler's Generator failed to process a time range"),
		)
	}

	// Transition the executor sub state machine to execute the new buffered actions.
	executorNode, err := schedulerNode.Child([]hsm.Key{ExecutorMachineKey})
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			err,
			serviceerror.NewInternal("Scheduler is missing its Executor node"),
		)
	}
	err = hsm.MachineTransition(executorNode, func(e Executor) (hsm.TransitionOutput, error) {
		return TransitionExecute.Apply(e, EventExecute{
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
	// Another buffering task is also added. if we fail here (after transitioning
	// Executor), that's fine; buffered actions are idempotent through their generated
	// request IDs.
	err = hsm.MachineTransition(node, func(g Generator) (hsm.TransitionOutput, error) {
		wakeupTime := res.NextWakeupTime
		g.LastProcessedTime = timestamppb.New(res.LastActionTime)
		g.NextInvocationTime = timestamppb.New(wakeupTime)

		e.Logger.Debug("Sleeping after buffering",
			tag.NewTimeTag("wakeupTime", wakeupTime))

		return TransitionBuffer.Apply(g, EventBuffer{
			Node:     node,
			Deadline: wakeupTime,
		})
	})
	if err != nil {
		return fmt.Errorf(
			"%w: unable to transition Generator",
			err,
		)
	}

	return nil
}

func (e generatorTaskExecutor) logSchedule(msg string, scheduler Scheduler) {
	// Log spec as json since it's more readable than the Go representation.
	specJson, _ := protojson.Marshal(scheduler.Schedule.Spec)
	policiesJson, _ := protojson.Marshal(scheduler.Schedule.Policies)
	e.Logger.Info(msg,
		tag.NewStringTag("spec", string(specJson)),
		tag.NewStringTag("policies", string(policiesJson)))
}

// loadGenerator loads the Generator's persisted state, returning a cloned copy.
func (e generatorTaskExecutor) loadGenerator(node *hsm.Node) (Generator, error) {
	prevGenerator, err := hsm.MachineData[Generator](node)
	if err != nil {
		return Generator{}, err
	}

	return Generator{
		GeneratorInternal: servercommon.CloneProto(prevGenerator.GeneratorInternal),
	}, nil
}

// loadScheduler loads the Scheduler's persisted state, returning a cloned copy.
func (e generatorTaskExecutor) loadScheduler(node *hsm.Node) (Scheduler, error) {
	prevScheduler, err := hsm.MachineData[Scheduler](node)
	if err != nil {
		return Scheduler{}, err
	}

	return Scheduler{
		SchedulerInternal: servercommon.CloneProto(prevScheduler.SchedulerInternal),
	}, nil
}
