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

package statemachines_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/statemachines"
)

type state string

const (
	state1 state = "state1"
	state2 state = "state2"
	state3 state = "state3"
	state4 state = "state4"
)

type data struct {
	state       state
	transitions []struct{ from, to state }
}

type event struct{ failBeforeHook, failAfterHook bool }

type adapter struct{}

func (adapter) GetState(d *data) state {
	return d.state
}

func (adapter) SetState(d *data, s state) {
	d.state = s
}

func (adapter) OnTransition(d *data, from, to state, env statemachines.Environment) error {
	d.transitions = append(d.transitions, struct {
		from state
		to   state
	}{from, to})
	return nil
}

var transition = statemachines.Transition[*data, state, event]{
	Adapter: adapter{},
	Src:     []state{state1, state2},
	Dst:     state3,
}

func TestTransition_Possible(t *testing.T) {
	d := &data{state: state4}
	require.False(t, transition.Possible(d))
	d = &data{state: state3}
	require.False(t, transition.Possible(d))
	d = &data{state: state1}
	require.True(t, transition.Possible(d))
	d = &data{state: state2}
	require.True(t, transition.Possible(d))
}

func TestTransition_WithoutBeforeAndAfterHooks(t *testing.T) {
	d := &data{state: state1}
	require.NoError(t, transition.Apply(d, event{}, &statemachines.MockEnvironment{}))
	require.Equal(t, state3, d.state)
	require.Equal(t, []struct{ from, to state }{{state1, state3}}, d.transitions)
}

func TestTransition_InvalidTransition(t *testing.T) {
	d := &data{state: state4}
	err := transition.Apply(d, event{}, &statemachines.MockEnvironment{})
	require.ErrorIs(t, err, statemachines.ErrInvalidTransition)
}

func TestTransition_WithHooks(t *testing.T) {
	transitionWithHooks := transition
	beforeErr := errors.New("before")
	afterErr := errors.New("after")

	transitionWithHooks.Before = func(d *data, e event, env statemachines.Environment) error {
		if e.failBeforeHook {
			return beforeErr
		}
		return nil
	}
	transitionWithHooks.After = func(d *data, e event, env statemachines.Environment) error {
		if e.failAfterHook {
			return afterErr
		}
		return nil
	}

	d := &data{state: state1}
	err := transitionWithHooks.Apply(d, event{failBeforeHook: true}, &statemachines.MockEnvironment{})
	require.ErrorIs(t, err, beforeErr)
	err = transitionWithHooks.Apply(d, event{failAfterHook: true}, &statemachines.MockEnvironment{})
	require.ErrorIs(t, err, afterErr)
}
