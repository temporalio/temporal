// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func RegisterExecutor(
	registry *hsm.Registry,
	executorOptions TaskExecutorOptions,
	config *Config,
) error {
	exec := taskExecutor{options: executorOptions, config: config}
	if err := hsm.RegisterTimerExecutor(
		registry,
		exec.executeSchedulerWaitTask); err != nil {
		return err
	}
	return hsm.RegisterImmediateExecutor(registry, exec.executeSchedulerRunTask)
}

type (
	TaskExecutorOptions struct {
		metricsHandler metrics.Handler
		logger         log.Logger
		frontendClient workflowservice.WorkflowServiceClient
		historyClient  resource.HistoryClient
	}

	taskExecutor struct {
		options TaskExecutorOptions
		config  *Config
	}
)

func (e taskExecutor) executeSchedulerWaitTask(
	env hsm.Environment,
	node *hsm.Node,
	task SchedulerWaitTask,
) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.MachineTransition(node, func(scheduler *Scheduler) (hsm.TransitionOutput, error) {
		return TransitionSchedulerActivate.Apply(scheduler, EventSchedulerActivate{})
	})
}

func (e taskExecutor) executeSchedulerRunTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task SchedulerActivateTask,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
			return err
		}
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}

		s.populateTransientFieldsIfAbsent(e.options.logger, e.options.metricsHandler, e.options.frontendClient, e.options.historyClient)

		if s.Args.State.LastProcessedTime == nil {
			// log these as json since it's more readable than the Go representation
			specJson, _ := protojson.Marshal(s.Args.Schedule.Spec)
			policiesJson, _ := protojson.Marshal(s.Args.Schedule.Policies)
			s.logger.Info("Starting schedule", tag.NewStringTag("spec", string(specJson)), tag.NewStringTag("policies", string(policiesJson)))
			s.Args.State.LastProcessedTime = timestamppb.Now()
			s.Args.State.ConflictToken = scheduler.InitialConflictToken
			s.Args.Info.CreateTime = s.Args.State.LastProcessedTime
		}

		t1 := timestamp.TimeValue(s.Args.State.LastProcessedTime)
		t2 := time.Now()
		if t2.Before(t1) {
			// Time went backwards. Currently this can only happen across a continue-as-new boundary.
			s.logger.Warn("Time went backwards", tag.NewStringTag("time", t1.String()), tag.NewStringTag("time", t2.String()))
			t2 = t1
		}
		nextWakeup, lastAction := s.processTimeRange(
			t1, t2,
			// resolve this to the schedule's policy as late as possible
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
			false,
			nil,
		)
		s.Args.State.LastProcessedTime = timestamppb.New(lastAction)
		// process backfills if we have any too
		s.processBackfills()
		// try starting workflows in the buffer
		//nolint:revive
		for s.processBuffer() {
		}
		s.NextInvocationTime = timestamppb.New(nextWakeup)

		return hsm.MachineTransition(node, func(scheduler *Scheduler) (hsm.TransitionOutput, error) {
			return TransitionSchedulerWait.Apply(scheduler, EventSchedulerWait{})
		})
	})
}
