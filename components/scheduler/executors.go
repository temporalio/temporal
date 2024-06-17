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
	"go.temporal.io/server/service/history/hsm"
)

func RegisterExecutor(
	registry *hsm.Registry,
	executorOptions TaskExecutorOptions,
	config *Config,
) error {
	activeExec := taskExecutor{options: executorOptions, config: config}
	return hsm.RegisterTimerExecutor(
		registry,
		activeExec.executeScheduleTask,
	)
}

type (
	TaskExecutorOptions struct {
	}

	taskExecutor struct {
		options TaskExecutorOptions
		config  *Config
	}
)

func (e taskExecutor) executeScheduleTask(
	env hsm.Environment,
	node *hsm.Node,
	task ScheduleTask,
) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	// TODO(Tianyu): Perform scheduler logic before scheduling self again
	return hsm.MachineTransition(node, func(scheduler Scheduler) (hsm.TransitionOutput, error) {
		return TransitionSchedulerActivate.Apply(scheduler, EventSchedulerActivate{})
	})
}
