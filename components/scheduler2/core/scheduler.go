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
	// Scheduler is a top-level state machine compromised of 3 sub state machines:
	// - Generator: buffers actions according to the schedule specification
	// - Executor: executes buffered actions
	// - Backfiller: buffers actions according to requested backfills
	//
	// A running Scheduler will always have exactly one of each of the above sub state
	// machines mounted as nodes within the HSM tree. The top-level	machine itself
	// remains in a singular running state for its lifetime (all work is done within the
	// sub state machines). The Scheduler state machine is only responsible for creating
	// the singleton sub state machines.
	Scheduler struct {
		*schedspb.SchedulerInternal

		// Locally-cached state, invalidated whenever cacheConflictToken != ConflictToken.
		cacheConflictToken int64
		compiledSpec       *scheduler.CompiledSpec
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

// RegisterStateMachine registers state machine definitions with the HSM
// registry. Should be called during dependency injection.
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
	return s.SchedulerInternal.State
}

func (s Scheduler) SetState(state enumsspb.Scheduler2State) {
	s.SchedulerInternal.State = state
}

func (s Scheduler) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return nil, nil
}

func (machineDefinition) Type() string {
	return SchedulerMachineType
}

func (machineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Scheduler); ok {
		return proto.Marshal(state.SchedulerInternal)
	}
	return nil, fmt.Errorf("invalid scheduler state provided: %v", state)
}

func (machineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedspb.SchedulerInternal{}
	return Scheduler{
		SchedulerInternal: state,
		compiledSpec:      nil,
	}, proto.Unmarshal(body, state)
}

func (machineDefinition) CompareState(a any, b any) (int, error) {
	panic("TODO: CompareState not yet implemented for Scheduler")
}

// UseScheduledAction returns true when the Scheduler should allow scheduled
// actions to be taken.
//
// When decrement is true, the schedule's state's `RemainingActions` counter is
// decremented when an action can be taken. When decrement is false, no state
// is mutated.
func (s Scheduler) UseScheduledAction(decrement bool) bool {
	// If paused, don't do anything.
	if s.Schedule.State.Paused {
		return false
	}

	// If unlimited actions, allow.
	if !s.Schedule.State.LimitedActions {
		return true
	}

	// Otherwise check and decrement limit.
	if s.Schedule.State.RemainingActions > 0 {
		if decrement {
			s.Schedule.State.RemainingActions--

			// The conflict token is updated because a client might be in the process of
			// preparing an update request that increments their schedule's RemainingActions
			// field.
			s.UpdateConflictToken()
		}
		return true
	}

	// No actions left
	return false
}

func (s Scheduler) CompiledSpec(specBuilder *scheduler.SpecBuilder) (*scheduler.CompiledSpec, error) {
	s.validateCachedState()

	// Cache compiled spec.
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

// validateCachedState clears cached fields whenever the Scheduler's
// ConflictToken doesn't match its cacheConflictToken field. Validation is only
// as effective as the Scheduler's backing persisted state is up-to-date.
func (s Scheduler) validateCachedState() {
	if s.cacheConflictToken != s.ConflictToken {
		// Bust stale cached fields.
		s.compiledSpec = nil

		// We're now up-to-date.
		s.cacheConflictToken = s.ConflictToken
	}
}

// UpdateConflictToken bumps the Scheduler's conflict token. This has a side
// effect of invalidating the local cache. Use whenever applying a mutation that
// should invalidate other in-flight updates.
func (s Scheduler) UpdateConflictToken() {
	s.ConflictToken++
}
