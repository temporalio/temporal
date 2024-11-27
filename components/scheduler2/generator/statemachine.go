package generator

import (
	"fmt"

	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
)

type (
	// The Generator substate machine is responsible for buffering actions according
	// to the schedule's specification. Manually requested actions (from an immediate
	// request or backfill) are separately handled in the Backfiller substate machine.
	Generator struct {
		*schedspb.GeneratorInternalState
	}

	// The machine definition provides serialization/deserialization and type information.
	machineDefinition struct{}
)

const (
	// Unique identifier for the Generator substate machine.
	MachineType = "scheduler.Generator"
)

var (
	_ hsm.StateMachine[enumsspb.SchedulerGeneratorState] = Generator{}
	_ hsm.StateMachineDefinition                         = &machineDefinition{}

	// Each substate machine is a singleton of the top-level Scheduler, accessed with
	// a fixed key
	MachineKey = hsm.Key{Type: MachineType, ID: ""}
)

func (g Generator) State() enumsspb.SchedulerGeneratorState {
	return g.GeneratorInternalState.State
}

func (g Generator) SetState(state enumsspb.SchedulerGeneratorState) {
	g.GeneratorInternalState.State = state
}

func (g Generator) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return g.tasks()
}

func (machineDefinition) Type() string {
	return MachineType
}

func (machineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Generator); ok {
		return proto.Marshal(state.GeneratorInternalState)
	}
	return nil, fmt.Errorf("invalid generator state provided: %v", state)
}

func (machineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedspb.GeneratorInternalState{}
	return Generator{
		GeneratorInternalState: state,
	}, proto.Unmarshal(body, state)
}

// Returns:
//
// 0 when states are equal
// 1 when a is newer than b
// -1 when b is newer than a
func (machineDefinition) CompareState(a any, b any) (int, error) {
	s1, ok := a.(Generator)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Generator instance, got %v", hsm.ErrIncompatibleType, s1)
	}
	s2, ok := a.(Generator)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Generator instance, got %v", hsm.ErrIncompatibleType, s2)
	}

	if s1.State() > s2.State() {
		return 1, nil
	} else if s1.State() < s2.State() {
		return -1, nil
	}

	return 0, nil
}
