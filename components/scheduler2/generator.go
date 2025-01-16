package scheduler2

import (
	"fmt"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// The Generator sub state machine is responsible for buffering actions according
	// to the schedule's specification. Manually requested actions (from an immediate
	// request or backfill) are separately handled in the Backfiller sub state machine.
	Generator struct {
		*schedulespb.GeneratorInternal
	}

	// The machine definition provides serialization/deserialization and type information.
	generatorMachineDefinition struct{}

	GeneratorMachineState int
)

const (
	// Unique identifier for the Generator sub state machine.
	GeneratorMachineType = "scheduler.Generator"

	// The Generator has only a single running state.
	GeneratorMachineStateRunning GeneratorMachineState = 0
)

var (
	_ hsm.StateMachine[GeneratorMachineState] = Generator{}
	_ hsm.StateMachineDefinition              = &generatorMachineDefinition{}

	// Each sub state machine is a singleton of the top-level Scheduler, accessed with
	// a fixed key
	GeneratorMachineKey = hsm.Key{Type: GeneratorMachineType, ID: ""}
)

// NewGenerator returns an intialized Generator sub state machine, which should
// be parented under a Scheduler root node.
func NewGenerator() *Generator {
	return &Generator{
		GeneratorInternal: &schedulespb.GeneratorInternal{
			NextInvocationTime: timestamppb.Now(),
			LastProcessedTime:  timestamppb.Now(),
		},
	}
}

func (g Generator) State() GeneratorMachineState {
	return GeneratorMachineStateRunning
}

func (g Generator) SetState(_ GeneratorMachineState) {}

func (g Generator) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return g.tasks()
}

func (generatorMachineDefinition) Type() string {
	return GeneratorMachineType
}

func (generatorMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Generator); ok {
		return proto.Marshal(state.GeneratorInternal)
	}
	return nil, fmt.Errorf("invalid generator state provided: %v", state)
}

func (generatorMachineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedulespb.GeneratorInternal{}
	return Generator{
		GeneratorInternal: state,
	}, proto.Unmarshal(body, state)
}

func (generatorMachineDefinition) CompareState(a any, b any) (int, error) {
	panic("TODO: CompareState not yet implemented for Generator")
}
