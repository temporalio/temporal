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
	schedpb "go.temporal.io/api/schedule/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

type Scheduler struct {
	*schedspb.HsmSchedulerState
	// cspec is not serialized as part of the state and created on demand per scheduler by the executor
	cspec *scheduler.CompiledSpec
}

// Unique type identifier for this state machine.
const StateMachineType = "scheduler.Scheduler"

// MachineCollection creates a new typed [statemachines.Collection] for callbacks.
func MachineCollection(tree *hsm.Node) hsm.Collection[*Scheduler] {
	return hsm.NewCollection[*Scheduler](tree, StateMachineType)
}

// NewScheduler creates a new scheduler in the WAITING state from given params.
func NewScheduler(args *schedspb.StartScheduleArgs, tweakables *HsmTweakables) *Scheduler {
	result := &Scheduler{
		HsmSchedulerState: &schedspb.HsmSchedulerState{
			Args:     args,
			HsmState: enumsspb.SCHEDULER_STATE_WAITING,
		},
	}
	if result.Args.Schedule == nil {
		result.Args.Schedule = &schedpb.Schedule{}
	}
	if result.Args.Schedule.Spec == nil {
		result.Args.Schedule.Spec = &schedpb.ScheduleSpec{}
	}
	if result.Args.Schedule.Action == nil {
		result.Args.Schedule.Action = &schedpb.ScheduleAction{}
	}
	if result.Args.Schedule.Policies == nil {
		result.Args.Schedule.Policies = &schedpb.SchedulePolicies{}
	}

	result.Args.Schedule.Policies.OverlapPolicy = result.resolveOverlapPolicy(result.Args.Schedule.Policies.OverlapPolicy)
	result.Args.Schedule.Policies.CatchupWindow = durationpb.New(result.getCatchupWindow(tweakables))

	if result.Args.Schedule.State == nil {
		result.Args.Schedule.State = &schedpb.ScheduleState{}
	}
	if result.Args.Info == nil {
		result.Args.Info = &schedpb.ScheduleInfo{}
	}
	if result.Args.State == nil {
		result.Args.State = &schedspb.InternalState{}
	}
	return result
}

func (s *Scheduler) resolveOverlapPolicy(overlapPolicy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		overlapPolicy = s.Args.Schedule.Policies.OverlapPolicy
	}
	if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		overlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return overlapPolicy
}

func (s *Scheduler) getCatchupWindow(tweakables *HsmTweakables) time.Duration {
	cw := s.Args.Schedule.Policies.CatchupWindow
	if cw == nil {
		return tweakables.DefaultCatchupWindow
	}
	if cw.AsDuration() < tweakables.MinCatchupWindow {
		return tweakables.MinCatchupWindow
	}
	return cw.AsDuration()
}

func (s *Scheduler) jitterSeed() string {
	return fmt.Sprintf("%s-%s", s.Args.State.NamespaceId, s.Args.State.ScheduleId)
}

func (s *Scheduler) identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.Args.State.Namespace, s.Args.State.ScheduleId)
}

func (s *Scheduler) State() enumsspb.SchedulerState {
	return s.HsmState
}

func (s *Scheduler) SetState(state enumsspb.SchedulerState) {
	s.HsmSchedulerState.HsmState = state
}

func (s *Scheduler) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch s.HsmState { // nolint:exhaustive
	case enumsspb.SCHEDULER_STATE_WAITING:
		return []hsm.Task{SchedulerWaitTask{Deadline: s.NextInvocationTime.AsTime()}}, nil
	case enumsspb.SCHEDULER_STATE_EXECUTING:
		return []hsm.Task{SchedulerActivateTask{Destination: s.Args.State.Namespace}}, nil
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
	return &Scheduler{
		HsmSchedulerState: state,
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
