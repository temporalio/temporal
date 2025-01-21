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
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

type (
	BufferTask struct {
		deadline time.Time
	}

	// Fired when the Generator should buffer actions. After buffering, another buffer
	// task is usually created with a later deadline. The Generator will
	// alternate between sleeping and buffering without an explicit state
	// transition.
	EventBuffer struct {
		Node *hsm.Node

		Deadline time.Time
	}
)

const (
	TaskTypeBuffer = "scheduler.generator.Buffer"
)

var (
	_ hsm.Task = BufferTask{}
)

func (BufferTask) Type() string {
	return TaskTypeBuffer
}

func (b BufferTask) Deadline() time.Time {
	return b.deadline
}

func (BufferTask) Destination() string {
	return ""
}

func (BufferTask) Validate(_ *persistencespb.StateMachineRef, _ *hsm.Node) error {
	// Generator only has a single task/state, so no validation is done here.
	return nil
}

func (g Generator) tasks() ([]hsm.Task, error) {
	return []hsm.Task{BufferTask{deadline: g.NextInvocationTime.AsTime()}}, nil
}

func (g Generator) output() (hsm.TransitionOutput, error) {
	tasks, err := g.tasks()
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}
