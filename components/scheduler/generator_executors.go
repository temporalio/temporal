// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package scheduler

import (
	"fmt"

	"go.temporal.io/api/serviceerror"
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
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor
	}

	generatorTaskExecutor struct {
		GeneratorTaskExecutorOptions
	}
)

func RegisterGeneratorExecutors(registry *hsm.Registry, options GeneratorTaskExecutorOptions) error {
	e := generatorTaskExecutor{
		GeneratorTaskExecutorOptions: options,
	}
	return hsm.RegisterTimerExecutor(registry, e.executeBufferTask)
}

func (e generatorTaskExecutor) executeBufferTask(env hsm.Environment, node *hsm.Node, task BufferTask) error {
	schedulerNode := node.Parent
	scheduler, err := loadScheduler(schedulerNode)
	if err != nil {
		return err
	}
	// Prepended with common scheduler attributes.
	logger := newTaggedLogger(e.BaseLogger, scheduler)

	generator, err := e.loadGenerator(node)
	if err != nil {
		return err
	}

	// If we have no last processed time, this is a new schedule.
	if generator.LastProcessedTime == nil {
		generator.LastProcessedTime = timestamppb.New(env.Now())
		// TODO - update schedule info with create time

		e.logSchedule(logger, "Starting schedule", scheduler)
	}

	// Process time range between last high water mark and system time.
	t1 := generator.LastProcessedTime.AsTime()
	t2 := env.Now().UTC()
	if t2.Before(t1) {
		logger.Warn("Time went backwards",
			tag.NewStringTag("time", t1.String()),
			tag.NewStringTag("time", t2.String()))
		t2 = t1
	}

	res, err := e.SpecProcessor.ProcessTimeRange(scheduler, t1, t2, false, nil)
	if err != nil {
		// An error here should be impossible, send to the DLQ.
		logger.Error("Error processing time range", tag.Error(err))

		return fmt.Errorf(
			"%w: %w",
			serviceerror.NewInternal("Scheduler's Generator failed to process a time range"),
			err,
		)
	}

	// Transition the executor sub state machine to execute the new buffered actions.
	executorNode, err := schedulerNode.Child([]hsm.Key{InvokerMachineKey})
	if err != nil {
		return fmt.Errorf(
			"%w: %w",
			serviceerror.NewInternal("Scheduler is missing its Executor node"),
			err,
		)
	}
	err = hsm.MachineTransition(executorNode, func(e Invoker) (hsm.TransitionOutput, error) {
		return TransitionEnqueue.Apply(e, EventEnqueue{
			Node:           executorNode,
			BufferedStarts: res.BufferedStarts,
		})
	})
	if err != nil {
		return fmt.Errorf(
			"unable to transition Executor to Executing state: %w",
			err,
		)
	}

	// Write Generator internal state, flushing the high water mark to persistence.
	// Another buffering task is also added.
	err = hsm.MachineTransition(node, func(g Generator) (hsm.TransitionOutput, error) {
		wakeupTime := res.NextWakeupTime
		g.LastProcessedTime = timestamppb.New(res.LastActionTime)
		g.NextInvocationTime = timestamppb.New(wakeupTime)

		logger.Debug("Sleeping after buffering",
			tag.NewTimeTag("wakeupTime", wakeupTime))

		return g.output()
	})
	if err != nil {
		return fmt.Errorf(
			"%w: unable to transition Generator",
			err,
		)
	}

	return nil
}

func (e generatorTaskExecutor) logSchedule(logger log.Logger, msg string, scheduler Scheduler) {
	// Log spec as json since it's more readable than the Go representation.
	specJson, _ := protojson.Marshal(scheduler.Schedule.Spec)
	policiesJson, _ := protojson.Marshal(scheduler.Schedule.Policies)
	logger.Debug(msg,
		tag.NewStringTag("spec", string(specJson)),
		tag.NewStringTag("policies", string(policiesJson)))
}

// loadGenerator loads the Generator's persisted state.
func (e generatorTaskExecutor) loadGenerator(node *hsm.Node) (Generator, error) {
	prevGenerator, err := hsm.MachineData[Generator](node)
	if err != nil {
		return Generator{}, err
	}

	return Generator{
		GeneratorInternal: prevGenerator.GeneratorInternal,
	}, nil
}
