package core

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
)

type (
	// The top-level scheduler state machine is compromised of 3 substate machines:
	// - Generator: buffers actions according to the schedule specification
	// - Executor: executes buffered actions
	// - Backfiller: buffers actions according to requested backfills
	//
	// 	A running scheduler will always have exactly one of each of the above substate
	// machines mounted as nodes within the HSM tree. The top-level	machine itself
	// remains in a singular running state for its lifetime (all work is done within the
	// substate machines). The Scheduler state machine is only responsible for creating
	// the singleton substate machines.
	Scheduler struct {
		*schedspb.HsmSchedulerV2State

		// Locally-cached state
		compiledSpec *scheduler.CompiledSpec
	}

	// The machine definitions provide serialization/deserialization and type information.
	machineDefinition struct{}
)

const (
	// Unique identifier for top-level scheduler state machine.
	SchedulerMachineType = "scheduler.SchedulerV2"
)

var (
	_ hsm.StateMachine[enumsspb.Scheduler2State] = Scheduler{}
	_ hsm.StateMachineDefinition                 = &machineDefinition{}
)

// Registers state machine definitions with the HSM registry. Should be called
// during dependency injection.
func RegisterStateMachines(r *hsm.Registry) error {
	if err := r.RegisterMachine(machineDefinition{}); err != nil {
		return err
	}
	// TODO: add other state machines here
	return nil
}

// MachineCollection creates a new typed [statemachines.Collection] for operations.
func MachineCollection(tree *hsm.Node) hsm.Collection[Scheduler] {
	return hsm.NewCollection[Scheduler](tree, SchedulerMachineType)
}

func (s Scheduler) State() enumsspb.Scheduler2State {
	return s.HsmSchedulerV2State.State
}

func (s Scheduler) SetState(state enumsspb.Scheduler2State) {
	s.HsmSchedulerV2State.State = state
}

func (s Scheduler) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return nil, nil
}

func (machineDefinition) Type() string {
	return SchedulerMachineType
}

func (machineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Scheduler); ok {
		return proto.Marshal(state.HsmSchedulerV2State)
	}
	return nil, fmt.Errorf("invalid scheduler state provided: %v", state)
}

func (machineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedspb.HsmSchedulerV2State{}
	return Scheduler{
		HsmSchedulerV2State: state,
		compiledSpec:        nil,
	}, proto.Unmarshal(body, state)
}

// Returns:
//
// 0 when states are equal
// 1 when a is newer than b
// -1 when b is newer than a
func (machineDefinition) CompareState(a any, b any) (int, error) {
	s1, ok := a.(Scheduler)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Scheduler instance, got %v", hsm.ErrIncompatibleType, s1)
	}
	s2, ok := a.(Scheduler)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Scheduler instance, got %v", hsm.ErrIncompatibleType, s2)
	}

	if s1.State() > s2.State() {
		return 1, nil
	} else if s1.State() < s2.State() {
		return -1, nil
	}

	return 0, nil
}

// Returns true when the Scheduler should allow scheduled actions to be taken.
//
// When decrement is true, the schedule's state's `RemainingActions` counter
// is decremented and the conflict token is bumped.
func (s Scheduler) CanTakeScheduledAction(decrement bool) bool {
	// If paused, don't do anything
	if s.Schedule.State.Paused {
		return false
	}

	// If unlimited actions, allow
	if !s.Schedule.State.LimitedActions {
		return true
	}

	// Otherwise check and decrement limit
	if s.Schedule.State.RemainingActions > 0 {
		if decrement {
			s.Schedule.State.RemainingActions--
			s.ConflictToken++
		}
		return true
	}

	// No actions left
	return false
}

func (s Scheduler) CompiledSpec(specBuilder *scheduler.SpecBuilder) (*scheduler.CompiledSpec, error) {
	// cache compiled spec
	if s.compiledSpec == nil {
		cspec, err := specBuilder.NewCompiledSpec(s.Schedule.Spec)
		if err != nil {
			return nil, err
		}
		s.compiledSpec = cspec
	}

	return s.compiledSpec, nil
}

func (s Scheduler) JitterSeed() string {
	return fmt.Sprintf("%s-%s", s.NamespaceId, s.ScheduleId)
}

func (s Scheduler) Identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.Namespace, s.ScheduleId)
}

func (s Scheduler) OverlapPolicy() enumspb.ScheduleOverlapPolicy {
	policy := s.Schedule.Policies.OverlapPolicy
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return policy
}
