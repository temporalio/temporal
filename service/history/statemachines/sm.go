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

package statemachines

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/tasks"
)

// Task is a [tasks.Task] that is generated without a workflow key or version.
// The missing fields are meant to be filled in by the environment (e.g. mutable state).
type Task interface {
	tasks.Task
	SetWorkflowKey(definition.WorkflowKey)
	SetVersion(int64)
}

// Environment for executing state machine transitions.
type Environment interface {
	// GetNamespaceFailoverVersion returns the current namespace failover version.
	GetNamespaceFailoverVersion() int64
	// Schedule schedules a partial task. WorkflowKey and Version is set by the Environment.
	Schedule(task Task)
	// GetCurrentTime returns the current time.
	GetCurrentTime() time.Time
}

// MockEnvironment for testing purposes.
type MockEnvironment struct {
	// The current time to return in GetCurrentTime.
	CurrentTime time.Time
	// The version to return in GetVersion.
	Version int64
	// Tasks scheduled in this environment are appended to this slice.
	ScheduledTasks []Task
}

// GetCurrentTime implements Environment.
func (m *MockEnvironment) GetCurrentTime() time.Time {
	return m.CurrentTime
}

// GetNamespaceFailoverVersion implements Environment.
func (m *MockEnvironment) GetNamespaceFailoverVersion() int64 {
	return m.Version
}

// Schedule implements Environment.
func (m *MockEnvironment) Schedule(task Task) {
	m.ScheduledTasks = append(m.ScheduledTasks, task)
}

var _ Environment = &MockEnvironment{}

// ErrInvalidTransition is returned from [Transition.Apply] on an invalid state transition.
var ErrInvalidTransition = errors.New("invalid transition")

// Transition represents a state machine transition for an object of type T with state S and event E.
type Transition[T any, S comparable, E any] struct {
	// Adapter to get and set the state S on an object of type T.
	// If it is also a Transitioner, OnTransition hooks are applied.
	Adapter Adapter[T, S]
	// Source states that are valid for this transition.
	Src []S
	// Destination state.
	Dst S
	// Before hook, applied before running the OnTransition hook.
	Before func(T, E, Environment) error
	// After hook, applied after running the OnTransition hook.
	After func(T, E, Environment) error
}

// Possible returns a boolean indicating whether the transtion is possible for the current state.
func (t Transition[T, S, E]) Possible(data T) bool {
	return slices.Contains(t.Src, t.Adapter.GetState(data))
}

// Apply applies a transition event to the given data.
func (t Transition[T, S, E]) Apply(data T, event E, env Environment) error {
	prevState := t.Adapter.GetState(data)
	if !t.Possible(data) {
		return fmt.Errorf("%w from %v: %v", ErrInvalidTransition, prevState, event)
	}
	if t.Before != nil {
		if err := t.Before(data, event, env); err != nil {
			return err
		}
	}
	t.Adapter.SetState(data, t.Dst)
	if transitioner, canTransition := t.Adapter.(Transitioner[T, S]); canTransition {
		if err := transitioner.OnTransition(data, prevState, t.Dst, env); err != nil {
			return err
		}
	}
	if t.After != nil {
		if err := t.After(data, event, env); err != nil {
			return err
		}
	}
	return nil
}

// Adapter gets and sets state S from objects of type T.
type Adapter[T any, S comparable] interface {
	GetState(T) S
	SetState(T, S)
}

// Transitioner adds transition hooks.
type Transitioner[T any, S comparable] interface {
	// Transition hook, applied between the transition's Before and After hooks. Data will have already transitioned
	// to the new state.
	OnTransition(data T, from S, to S, env Environment) error
}
