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
	schedpb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/util"
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

// NewScheduler creates a new scheduler in the WAITING state from given params.
func NewScheduler(args *schedspb.StartScheduleArgs, tweakables *Tweakables) *Scheduler {
	s := &Scheduler{
		HsmSchedulerState: &schedspb.HsmSchedulerState{
			Args:     args,
			HsmState: enumsspb.SCHEDULER_STATE_WAITING,
		},
	}
	s.ensureFields(tweakables)
	return s
}

func (s *Scheduler) ensureFields(tweakables *Tweakables) {
	if s.Args.Schedule == nil {
		s.Args.Schedule = &schedpb.Schedule{}
	}
	if s.Args.Schedule.Spec == nil {
		s.Args.Schedule.Spec = &schedpb.ScheduleSpec{}
	}
	if s.Args.Schedule.Action == nil {
		s.Args.Schedule.Action = &schedpb.ScheduleAction{}
	}
	if s.Args.Schedule.Policies == nil {
		s.Args.Schedule.Policies = &schedpb.SchedulePolicies{}
	}

	s.Args.Schedule.Policies.OverlapPolicy = s.resolveOverlapPolicy(s.Args.Schedule.Policies.OverlapPolicy)
	s.Args.Schedule.Policies.CatchupWindow = durationpb.New(s.catchupWindow(tweakables))

	if s.Args.Schedule.State == nil {
		s.Args.Schedule.State = &schedpb.ScheduleState{}
	}
	if s.Args.Info == nil {
		s.Args.Info = &schedpb.ScheduleInfo{}
	}
	if s.Args.State == nil {
		s.Args.State = &schedspb.InternalState{}
	}
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

func (s *Scheduler) catchupWindow(tweakables *Tweakables) time.Duration {
	cw := s.Args.Schedule.Policies.CatchupWindow
	if cw == nil {
		return tweakables.DefaultCatchupWindow
	}
	cwDuration := cw.AsDuration()
	if cwDuration < tweakables.MinCatchupWindow {
		return tweakables.MinCatchupWindow
	}
	return cwDuration
}

func (s *Scheduler) recordAction(result *schedpb.ScheduleActionResult, nonOverlapping bool) {
	s.Args.Info.ActionCount++
	s.Args.Info.RecentActions = util.SliceTail(append(s.Args.Info.RecentActions, result), RecentActionCount)
	if nonOverlapping && result.StartWorkflowResult != nil {
		s.Args.Info.RunningWorkflows = append(s.Args.Info.RunningWorkflows, result.StartWorkflowResult)
	}
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
		return []hsm.Task{SchedulerProcessBufferTask{}, SchedulerWaitTask{Deadline: s.NextInvocationTime.AsTime()}}, nil
	case enumsspb.SCHEDULER_STATE_EXECUTING:
		// This task is done locally and do not need a destination
		return []hsm.Task{SchedulerActivateTask{}}, nil
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

// CompareState is required for the temporary state sync solution to work.
// Once transition history based replication is implemented, we won't need this method any more.
// That will likely come before we productionize the HSM based scheduler, but if it doesn't, this method will need to be
// implemented.
func (s stateMachineDefinition) CompareState(any, any) (int, error) {
	return 0, serviceerror.NewUnimplemented("CompareState not implemented for the scheduler state machine")
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

// EventSchedulerActivate is triggered when the scheduler state machine should wake up and perform work.
type EventSchedulerActivate struct {
	// Instruct the task to schedule more invocation into the future or treat itself as a one-off
	scheduleMore bool
}

var TransitionSchedulerActivate = hsm.NewTransition(
	[]enumsspb.SchedulerState{enumsspb.SCHEDULER_STATE_EXECUTING, enumsspb.SCHEDULER_STATE_WAITING},
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
