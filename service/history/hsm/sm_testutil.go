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

package hsm

import (
	"encoding/json"
	"fmt"
	"time"
)

type (
	TestState string

	TestData struct {
		state TestState
	}

	TestDefinition struct {
		typeID int32
		name   string
	}

	TestTask struct {
		kind         TaskKind
		IsConcurrent bool
	}

	TestTaskSerializer struct{}
)

const (
	TestState1 TestState = "state1"
	TestState2 TestState = "state2"
	TestState3 TestState = "state3"
	TestState4 TestState = "state4"
)

const (
	TestTaskTypeID   = int32(4321)
	TestTaskTypeName = "test-task-type-name"
)

var (
	TestTaskType = TaskType{
		ID:   TestTaskTypeID,
		Name: TestTaskTypeName,
	}
)

func NewTestData(
	state TestState,
) *TestData {
	return &TestData{
		state: state,
	}
}

func (d *TestData) State() TestState {
	return d.state
}

func (d *TestData) SetState(s TestState) {
	d.state = s
}

func (d *TestData) RegenerateTasks(node *Node) ([]Task, error) {
	return []Task{
		NewTestTask(
			TaskKindTimer{Deadline: time.Now().Add(time.Hour)},
			false,
		),
		NewTestTask(
			TaskKindOutbound{Destination: string(d.state)},
			false,
		),
	}, nil
}

func NewTestDefinition(typeID int32) TestDefinition {
	return TestDefinition{
		typeID: typeID,
		name:   fmt.Sprintf("test-%d", typeID),
	}
}

func (d TestDefinition) Deserialize(b []byte) (any, error) {
	return &TestData{TestState(string(b))}, nil
}

// Serialize implements hsm.StateMachineDefinition.
func (d TestDefinition) Serialize(s any) ([]byte, error) {
	t, ok := s.(*TestData)
	if !ok {
		return nil, fmt.Errorf("invalid type")
	}
	return []byte(t.state), nil
}

// Type implements hsm.StateMachineDefinition.
func (d TestDefinition) Type() MachineType {
	return MachineType{
		ID:   d.typeID,
		Name: d.name,
	}
}

func NewTestTask(
	kind TaskKind,
	concurrent bool,
) *TestTask {
	return &TestTask{
		kind:         kind,
		IsConcurrent: concurrent,
	}
}

func (t *TestTask) Type() TaskType {
	return TestTaskType
}

func (t *TestTask) Kind() TaskKind {
	return t.kind
}

func (t *TestTask) Concurrent() bool {
	return t.IsConcurrent
}

func (s TestTaskSerializer) Serialize(t Task) ([]byte, error) {
	if t.Type() != TestTaskType {
		return nil, fmt.Errorf("invalid task type")
	}
	return json.Marshal(t)
}

func (s TestTaskSerializer) Deserialize(b []byte, kind TaskKind) (Task, error) {
	var t TestTask
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	t.kind = kind
	return &t, nil
}
