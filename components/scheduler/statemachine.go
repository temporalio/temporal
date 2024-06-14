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
	"fmt"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
)

// Unique type identifier for this state machine.
var StateMachineType = hsm.MachineType{
	ID:   5,
	Name: "scheduler.Scheduler",
}

// MachineCollection creates a new typed [statemachines.Collection] for callbacks.
func MachineCollection(tree *hsm.Node) hsm.Collection[Scheduler] {
	return hsm.NewCollection[Scheduler](tree, StateMachineType.ID)
}

// NewScheduler creates a new scheduler in the WAITING state from given params.
func NewScheduler(args *schedspb.StartScheduleArgs) *Scheduler {
	result := &Scheduler{
		HsmSchedulerState: &schedspb.HsmSchedulerState{
			Args:     args,
			HsmState: enumsspb.SCHEDULER_STATE_WAITING,
		},
	}
	result.ensureFields()
	return result
}

func (s *Scheduler) State() enumsspb.SchedulerState {
	return s.HsmState
}

func (s *Scheduler) SetState(state enumsspb.SchedulerState) {
	s.HsmState = state
}

func (s *Scheduler) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch s.HsmState { // nolint:exhaustive
	case enumsspb.SCHEDULER_STATE_WAITING:
		return []hsm.Task{SchedulerWaitTask{Deadline: s.nextInvokeTime}}, nil
	case enumsspb.SCHEDULER_STATE_EXECUTING:
		return []hsm.Task{SchedulerRunTask{Destination: s.Args.State.Namespace}}, nil
	}
	return nil, nil
}

type stateMachineDefinition struct{}

func (stateMachineDefinition) Type() hsm.MachineType {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	state := &schedspb.HsmSchedulerState{}
	if err := proto.Unmarshal(d, state); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return &Scheduler{
		HsmSchedulerState: state,
		tweakables:        scheduler.CurrentTweakablePolicies,
		nextInvokeTime:    time.Time{},
	}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(*Scheduler); ok {
		return proto.Marshal(state.HsmSchedulerState)
	}
	return nil, fmt.Errorf("invalid scheduler state provided: %v", state) // nolint:goerr113
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

// EventSchedulerActivate is triggered when the scheduler state machine should wake up and perform work.
type EventSchedulerActivate struct{}

var TransitionSchedulerActivate = hsm.NewTransition(
	[]enumsspb.SchedulerState{enumsspb.SCHEDULER_STATE_WAITING},
	enumsspb.SCHEDULER_STATE_EXECUTING,
	func(scheduler *Scheduler, event EventSchedulerActivate) (hsm.TransitionOutput, error) {
		tasks, err := scheduler.RegenerateTasks(nil)
		return hsm.TransitionOutput{Tasks: tasks}, err
	})

// EventSchedulerWait is triggered when the scheduler state machine is done working and goes back to waiting.
type EventSchedulerWait struct{}

var TransitionSchedulerWait = hsm.NewTransition(
	[]enumsspb.SchedulerState{enumsspb.SCHEDULER_STATE_EXECUTING},
	enumsspb.SCHEDULER_STATE_WAITING,
	func(scheduler *Scheduler, event EventSchedulerWait) (hsm.TransitionOutput, error) {
		tasks, err := scheduler.RegenerateTasks(nil)
		return hsm.TransitionOutput{Tasks: tasks}, err
	})
