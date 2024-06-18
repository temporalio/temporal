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
			hsm.TaskKindTimer{Deadline: time.Now().Add(time.Hour)},
			false,
		),
		NewTask(
			hsm.TaskKindOutbound{Destination: string(d.state)},
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

func (d Definition) CompareState(s1, s2 any) (int, error) {
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

// Type implements hsm.StateMachineDefinition.
func (d Definition) Type() string {
	return d.typeName
}
