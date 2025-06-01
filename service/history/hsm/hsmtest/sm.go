package hsmtest

import (
	"fmt"
	"strings"
	"time"

	"go.temporal.io/server/service/history/hsm"
)

var (
	errInvalidStateType = fmt.Errorf("invalid state type")
)

type State string

const (
	State1 State = "state1"
	State2 State = "state2"
	State3 State = "state3"
	State4 State = "state4"
)

type Data struct {
	state State
}

func NewData(
	state State,
) *Data {
	return &Data{
		state: state,
	}
}

func (d *Data) State() State {
	return d.state
}

func (d *Data) SetState(s State) {
	d.state = s
}

func (d *Data) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return []hsm.Task{
		NewTask(
			hsm.TaskAttributes{Deadline: time.Now().Add(time.Hour)},
			false,
		),
		NewTask(
			hsm.TaskAttributes{Destination: string(d.state)},
			false,
		),
	}, nil
}

type Definition struct {
	typeName string
}

func NewDefinition(typeName string) Definition {
	return Definition{
		typeName: typeName,
	}
}

func (d Definition) Deserialize(b []byte) (any, error) {
	return &Data{State(string(b))}, nil
}

// Serialize implements hsm.StateMachineDefinition.
func (d Definition) Serialize(s any) ([]byte, error) {
	t, ok := s.(*Data)
	if !ok {
		return nil, errInvalidStateType
	}
	return []byte(t.state), nil
}

// Type implements hsm.StateMachineDefinition.
func (d Definition) Type() string {
	return d.typeName
}

func (d Definition) CompareState(s1 any, s2 any) (int, error) {
	t1, ok := s1.(*Data)
	if !ok {
		return 0, errInvalidStateType
	}

	t2, ok := s2.(*Data)
	if !ok {
		return 0, errInvalidStateType
	}

	return strings.Compare(string(t1.State()), string(t2.State())), nil
}
