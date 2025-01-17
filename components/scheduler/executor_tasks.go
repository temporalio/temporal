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

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
)

type (
	ExecuteTask struct {
		deadline time.Time
	}

	// EventExecute is fired when the executor should wake up and begin processing
	// pending buffered starts, including any that are set on BufferedStarts.
	// BufferedStarts are immediately appended to the Executor's queue as the
	// transition is applied.
	//
	// Execution can be delayed (such as in the event of backing off) by setting
	// Deadline to something other than env.Now().
	EventExecute struct {
		Node *hsm.Node

		Deadline       time.Time
		BufferedStarts []*schedulespb.BufferedStart
	}

	// EventWait is fired when the executor should return to a waiting state until
	// more actions are buffered.
	EventWait struct {
		Node *hsm.Node
	}
)

const (
	TaskTypeExecute = "scheduler.executor.Execute"
)

var (
	_ hsm.Task = ExecuteTask{}
)

func (ExecuteTask) Type() string {
	return TaskTypeExecute
}

func (e ExecuteTask) Deadline() time.Time {
	return e.deadline
}

func (ExecuteTask) Destination() string {
	return ""
}

func (ExecuteTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	return validateTaskTransition(node, TransitionExecute)
}

func (e Executor) tasks() ([]hsm.Task, error) {
	if e.State() == enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING {
		// If NextInvocationTime is nil/0, set deadline to the sentinel Immediate value,
		// instead of to the UNIX epoch after converting from protobuf timestamp.
		deadline := hsm.Immediate
		if e.GetNextInvocationTime().GetSeconds() > 0 {
			deadline = e.GetNextInvocationTime().AsTime().UTC()
		}

		return []hsm.Task{ExecuteTask{
			deadline: deadline,
		}}, nil
	}
	return nil, nil
}

func (e Executor) output() (hsm.TransitionOutput, error) {
	tasks, err := e.tasks()
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}
