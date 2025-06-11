package dummy

import (
	"encoding/json"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/service/history/hsm"
)

type State int

const (
	State0 State = iota
	State1
)

var StateMachineType = "dummy.Dummy"

func MachineCollection(tree *hsm.Node) hsm.Collection[*Dummy] {
	return hsm.NewCollection[*Dummy](tree, StateMachineType)
}

// Dummy state machine.
// It's used for writing unit tests. Do not use in production!
type Dummy struct {
	CurrentState State
}

var _ hsm.StateMachine[State] = &Dummy{}

// NewDummy creates a new callback in the STANDBY state from given params.
func NewDummy() *Dummy {
	return &Dummy{
		CurrentState: State0,
	}
}

func (sm *Dummy) State() State {
	return sm.CurrentState
}

func (sm *Dummy) SetState(state State) {
	sm.CurrentState = state
}

func (sm Dummy) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	return []hsm.Task{}, nil
}

type stateMachineDefinition struct{}

var _ hsm.StateMachineDefinition = stateMachineDefinition{}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	sm := &Dummy{}
	err := json.Unmarshal(d, &sm)
	return sm, err
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	return json.Marshal(state)
}

func (stateMachineDefinition) CompareState(s1, s2 any) (int, error) {
	// This is just a dummy used in tests, CompareState is not used.
	return 0, serviceerror.NewUnimplemented("CompareState not implemented for dummy state machine")
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

type Event0 struct{}

var Transition0 = hsm.NewTransition(
	[]State{State0, State1},
	State0,
	func(sm *Dummy, e Event0) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	},
)

type Event1 struct{}

var Transition1 = hsm.NewTransition(
	[]State{State0, State1},
	State1,
	func(sm *Dummy, e Event1) (hsm.TransitionOutput, error) {
		return hsm.TransitionOutput{}, nil
	},
)
