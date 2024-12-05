package generator

import (
	"fmt"

	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
)

type (
	// The Generator sub state machine is responsible for buffering actions according
	// to the schedule's specification. Manually requested actions (from an immediate
	// request or backfill) are separately handled in the Backfiller sub state machine.
	Generator struct {
		*schedspb.GeneratorInternal
	}

	// The machine definition provides serialization/deserialization and type information.
	machineDefinition struct{}
)

const (
	// Unique identifier for the Generator sub state machine.
	MachineType = "scheduler.Generator"
)

var (
	_ hsm.StateMachine[enumsspb.SchedulerGeneratorState] = Generator{}
	_ hsm.StateMachineDefinition                         = &machineDefinition{}

	// Each sub state machine is a singleton of the top-level Scheduler, accessed with
	// a fixed key
	MachineKey = hsm.Key{Type: MachineType, ID: ""}
)

func (g Generator) State() enumsspb.SchedulerGeneratorState {
	return g.GeneratorInternal.State
}

func (g Generator) SetState(state enumsspb.SchedulerGeneratorState) {
	g.GeneratorInternal.State = state
}

func (g Generator) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return g.tasks()
}

func (machineDefinition) Type() string {
	return MachineType
}

func (machineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Generator); ok {
		return proto.Marshal(state.GeneratorInternal)
	}
	return nil, fmt.Errorf("invalid generator state provided: %v", state)
}

func (machineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedspb.GeneratorInternal{}
	return Generator{
		GeneratorInternal: state,
	}, proto.Unmarshal(body, state)
}

func (machineDefinition) CompareState(a any, b any) (int, error) {
	panic("TODO: CompareState not yet implemented for Generator")
}

var TransitionBuffer = hsm.NewTransition(
	[]enumsspb.SchedulerGeneratorState{
		enumsspb.SCHEDULER_GENERATOR_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_GENERATOR_STATE_BUFFERING, // Allow re-entering the buffering state, to wake up immediately
	},
	enumsspb.SCHEDULER_GENERATOR_STATE_BUFFERING,
	func(g Generator, _ EventBuffer) (hsm.TransitionOutput, error) {
		g.NextInvocationTime = nil
		return g.output()
	},
)
