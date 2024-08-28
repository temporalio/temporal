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

package dummy

import (
	"context"

	"go.temporal.io/server/service/history/hsm"
)

func RegisterExecutor(
	registry *hsm.Registry,
	taskExecutorOptions TaskExecutorOptions,
) error {
	activeExec := taskExecutor{
		TaskExecutorOptions: taskExecutorOptions,
	}
	if err := hsm.RegisterImmediateExecutor(
		registry,
		activeExec.executeImmediateTask,
	); err != nil {
		return err
	}
	return hsm.RegisterTimerExecutor(
		registry,
		activeExec.executeTimerTask,
	)
}

type (
	TaskExecutorOptions struct {
		ImmediateExecutor hsm.ImmediateExecutor[ImmediateTask]
		TimerExecutor     hsm.TimerExecutor[TimerTask]
	}

	taskExecutor struct {
		TaskExecutorOptions
	}
)

func (e taskExecutor) executeImmediateTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task ImmediateTask,
) error {
	return e.ImmediateExecutor(ctx, env, ref, task)
}

func (e taskExecutor) executeTimerTask(
	env hsm.Environment,
	node *hsm.Node,
	task TimerTask,
) error {
	return e.TimerExecutor(env, node, task)
}
