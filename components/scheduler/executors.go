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
	"go.temporal.io/server/service/history/hsm"
)

func RegisterExecutor(
	registry *hsm.Registry,
	activeExecutorOptions ActiveExecutorOptions,
	standbyExecutorOptions StandbyExecutorOptions,
	config *Config,
) error {
	activeExec := activeExecutor{options: activeExecutorOptions, config: config}
	standbyExec := standbyExecutor{options: standbyExecutorOptions}
	if err := hsm.RegisterTimerExecutors(
		registry,
		activeExec.executeSchedulerWaitTask,
		standbyExec.executeSchedulerWaitTask,
	); err != nil {
		return err
	}
	return hsm.RegisterImmediateExecutor(registry, activeExec.executeSchedulerRunTask)
}

type (
	ActiveExecutorOptions struct {
	}

	activeExecutor struct {
		options ActiveExecutorOptions
		config  *Config
	}
)

func (e activeExecutor) executeSchedulerWaitTask(
	env hsm.Environment,
	node *hsm.Node,
	task SchedulerWaitTask,
) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.MachineTransition(node, func(scheduler Scheduler) (hsm.TransitionOutput, error) {
		return TransitionSchedulerActivate.Apply(scheduler, EventSchedulerActivate{})
	})
}

func (e activeExecutor) executeSchedulerRunTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task SchedulerRunTask,
) error {
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
			return err
		}
		//scheduler, err := hsm.MachineData[Scheduler](node)
		//if err != nil {
		//	return err
		//}

		return hsm.MachineTransition(node, func(scheduler Scheduler) (hsm.TransitionOutput, error) {
			return TransitionSchedulerWait.Apply(scheduler, EventSchedulerWait{})
		})
	})
}

type (
	StandbyExecutorOptions struct{}

	standbyExecutor struct {
		options StandbyExecutorOptions
	}
)

func (e standbyExecutor) executeSchedulerWaitTask(
	env hsm.Environment,
	node *hsm.Node,
	task SchedulerWaitTask,
) error {
	panic("unimplemented")
}

func (e standbyExecutor) executeSchedulerRunTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task SchedulerRunTask,
) error {
	panic("unimplemented")
}
