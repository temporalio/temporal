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
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// The Executor sub state machine is responsible for executing buffered actions.
	Executor struct {
		*schedulespb.ExecutorInternal
	}

	// The machine definition provides serialization/deserialization and type information.
	executorMachineDefinition struct{}
)

const (
	// Unique identifier for the Executor sub state machine.
	ExecutorMachineType = "scheduler.Executor"
)

var (
	_ hsm.StateMachine[enumsspb.SchedulerExecutorState] = Executor{}
	_ hsm.StateMachineDefinition                        = &executorMachineDefinition{}

	// Each sub state machine is a singleton of the top-level Scheduler, accessed with
	// a fixed key.
	ExecutorMachineKey = hsm.Key{Type: ExecutorMachineType, ID: ""}
)

// NewExecutor returns an intialized Executor sub state machine, which should
// be parented under a Scheduler root node.
func NewExecutor() *Executor {
	var zero time.Time
	return &Executor{
		ExecutorInternal: &schedulespb.ExecutorInternal{
			State:              enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING,
			NextInvocationTime: timestamppb.New(zero),
			BufferedStarts:     []*schedulespb.BufferedStart{},
		},
	}
}

func (e Executor) State() enumsspb.SchedulerExecutorState {
	return e.ExecutorInternal.State
}

func (e Executor) SetState(state enumsspb.SchedulerExecutorState) {
	e.ExecutorInternal.State = state
}

func (e Executor) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return e.tasks()
}

func (executorMachineDefinition) Type() string {
	return ExecutorMachineType
}

func (executorMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Executor); ok {
		return proto.Marshal(state.ExecutorInternal)
	}
	return nil, fmt.Errorf("invalid executor state provided: %v", state)
}

func (executorMachineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedulespb.ExecutorInternal{}
	return Executor{state}, proto.Unmarshal(body, state)
}

func (executorMachineDefinition) CompareState(a any, b any) (int, error) {
	panic("TODO: CompareState not yet implemented for Executor")
}

// Transition to executing state to continue executing pending buffered actions,
// writing additional pending actions into the Executor's persistent state.
var TransitionExecute = hsm.NewTransition(
	[]enumsspb.SchedulerExecutorState{
		enumsspb.SCHEDULER_EXECUTOR_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING,
		enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING,
	},
	enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING,
	func(e Executor, event EventExecute) (hsm.TransitionOutput, error) {
		e.NextInvocationTime = timestamppb.New(event.Deadline)
		e.BufferedStarts = append(e.BufferedStarts, event.BufferedStarts...)

		return e.output()
	},
)

// Transition to waiting state. No new tasks will be created until the Executor
// is transitioned back to Executing state.
var TransitionWait = hsm.NewTransition(
	[]enumsspb.SchedulerExecutorState{
		enumsspb.SCHEDULER_EXECUTOR_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING,
		enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING,
	},
	enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING,
	func(e Executor, event EventWait) (hsm.TransitionOutput, error) {
		e.NextInvocationTime = nil
		return e.output()
	},
)
