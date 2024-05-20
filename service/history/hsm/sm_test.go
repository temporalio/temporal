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
	"go.temporal.io/server/service/history/hsm/hsmtest"
)

type event struct{ fail bool }

var handlerErr = errors.New("test")

var transition = hsm.NewTransition(
	[]hsmtest.State{hsmtest.State1, hsmtest.State2},
	hsmtest.State3,
	func(d *hsmtest.Data, e event) (hsm.TransitionOutput, error) {
		if e.fail {
			return hsm.TransitionOutput{}, handlerErr
		}
		return hsm.TransitionOutput{}, nil
	},
)

func TestTransition_Possible(t *testing.T) {
	d := hsmtest.NewData(hsmtest.State4)
	require.False(t, transition.Possible(d))
	d = hsmtest.NewData(hsmtest.State3)
	require.False(t, transition.Possible(d))
	d = hsmtest.NewData(hsmtest.State1)
	require.True(t, transition.Possible(d))
	d = hsmtest.NewData(hsmtest.State2)
	require.True(t, transition.Possible(d))
}

func TestTransition_ValidTransition(t *testing.T) {
	d := hsmtest.NewData(hsmtest.State1)
	_, err := transition.Apply(d, event{})
	require.NoError(t, err)
	require.Equal(t, hsmtest.State3, d.State())
}

func TestTransition_InvalidTransition(t *testing.T) {
	d := hsmtest.NewData(hsmtest.State4)
	_, err := transition.Apply(d, event{})
	require.ErrorIs(t, err, hsm.ErrInvalidTransition)
	require.Equal(t, hsmtest.State4, d.State())
}

func TestTransition_HandlerError(t *testing.T) {
	d := hsmtest.NewData(hsmtest.State1)
	_, err := transition.Apply(d, event{fail: true})
	require.ErrorIs(t, err, handlerErr)
}
