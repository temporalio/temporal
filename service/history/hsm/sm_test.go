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

package hsm_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/hsm"
)

type state string

const (
	state1 state = "state1"
	state2 state = "state2"
	state3 state = "state3"
	state4 state = "state4"
)

type data struct {
	state state
}

type event struct{ fail bool }

func (d *data) State() state {
	return d.state
}

func (d *data) SetState(s state) {
	d.state = s
}

func (d *data) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	panic("not implemented")
}

var handlerErr = errors.New("test")

var transition = hsm.NewTransition(
	[]state{state1, state2},
	state3,
	func(d *data, e event) (hsm.TransitionOutput, error) {
		if e.fail {
			return hsm.TransitionOutput{}, handlerErr
		}
		return hsm.TransitionOutput{}, nil
	},
)

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

func TestTransition_ValidTransition(t *testing.T) {
	d := &data{state: state1}
	_, err := transition.Apply(d, event{})
	require.NoError(t, err)
	require.Equal(t, state3, d.state)
}

func TestTransition_InvalidTransition(t *testing.T) {
	d := &data{state: state4}
	_, err := transition.Apply(d, event{})
	require.ErrorIs(t, err, hsm.ErrInvalidTransition)
	require.Equal(t, state4, d.state)
}

func TestTransition_HandlerError(t *testing.T) {
	d := &data{state: state1}
	_, err := transition.Apply(d, event{fail: true})
	require.ErrorIs(t, err, handlerErr)
}
