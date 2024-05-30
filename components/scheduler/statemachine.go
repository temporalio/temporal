package scheduler

import (
	"fmt"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
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

// Callback state machine.
type Scheduler struct {
	*schedspb.HsmSchedulerState
}

func (c Scheduler) State() enumsspb.SchedulerState {
	return c.HsmState
}

func (c Scheduler) SetState(state enumsspb.SchedulerState) {
	c.HsmState = state
}

func (c Scheduler) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
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
	return Scheduler{state}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Scheduler); ok {
		return proto.Marshal(state.HsmSchedulerState)
	}
	return nil, fmt.Errorf("invalid callback provided: %v", state) // nolint:goerr113
}

// EventSchedulerActivate is triggered when the scheduler state machine should wake up and perform work
type EventSchedulerActivate struct{}

var TransitionSchedulerActivate = hsm.NewTransition(
	[]enumsspb.SchedulerState{enumsspb.SCHEDULER_STATE_WAITING},
	enumsspb.SCHEDULER_STATE_WAITING,
	func(scheduler Scheduler, event EventSchedulerActivate) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{Tasks: []hsm.Task{}}, nil
	})
