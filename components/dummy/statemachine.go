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
