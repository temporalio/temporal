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
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Unique type identifier for this state machine.
const StateMachineType = "scheduler.Scheduler"

// MachineCollection creates a new typed [statemachines.Collection] for callbacks.
func MachineCollection(tree *hsm.Node) hsm.Collection[Scheduler] {
	return hsm.NewCollection[Scheduler](tree, StateMachineType)
}

// Callback state machine.
type Scheduler struct {
	*schedspb.HsmSchedulerState
}

// NewCallback creates a new callback in the STANDBY state from given params.
func NewScheduler(args *schedspb.StartScheduleArgs) Scheduler {
	return Scheduler{
		&schedspb.HsmSchedulerState{
			Args:     args,
			HsmState: enumsspb.SCHEDULER_STATE_WAITING,
		},
	}
}

func (s Scheduler) State() enumsspb.SchedulerState {
	return s.HsmState
}

func (s Scheduler) SetState(state enumsspb.SchedulerState) {
	s.HsmSchedulerState.HsmState = state
}

func (s Scheduler) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch s.HsmState { // nolint:exhaustive
	case enumsspb.SCHEDULER_STATE_WAITING:
		s.Args.State.LastProcessedTime = timestamppb.Now()
		// TODO(Tianyu): Replace with actual scheduler work
		fmt.Printf("Scheduler has been invoked\n")
		// TODO(Tianyu): Replace with actual scheduling logic
		nextInvokeTime := timestamppb.New(s.Args.State.LastProcessedTime.AsTime().Add(1 * time.Second))
		return []hsm.Task{ScheduleTask{Deadline: nextInvokeTime.AsTime()}}, nil
	}
	return nil, nil
}

type stateMachineDefinition struct{}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	state := &schedspb.HsmSchedulerState{}
	if err := proto.Unmarshal(d, state); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return Scheduler{state}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Scheduler); ok {
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
	enumsspb.SCHEDULER_STATE_WAITING,
	func(scheduler Scheduler, event EventSchedulerActivate) (hsm.TransitionOutput, error) {
		tasks, err := scheduler.RegenerateTasks(nil)
		return hsm.TransitionOutput{Tasks: tasks}, err
	})
