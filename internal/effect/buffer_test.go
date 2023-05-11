// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package effect_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/internal/effect"
)

func TestBufferApplyOrder(t *testing.T) {
	var buf effect.Buffer
	state := make([]int, 0, 3)

	buf.OnAfterCommit(func(context.Context) { state = append(state, 0) })
	buf.OnAfterCommit(func(context.Context) { state = append(state, 1) })
	buf.OnAfterCommit(func(context.Context) { state = append(state, 2) })

	buf.Apply(context.TODO())

	require.ElementsMatch(t, []int{0, 1, 2}, state)
}

func TestBufferRollbackOrder(t *testing.T) {
	var buf effect.Buffer
	state := make([]int, 0, 3)

	buf.OnAfterRollback(func(context.Context) { state = append(state, 0) })
	buf.OnAfterRollback(func(context.Context) { state = append(state, 1) })
	buf.OnAfterRollback(func(context.Context) { state = append(state, 2) })

	buf.Cancel(context.TODO())

	require.ElementsMatch(t, []int{2, 1, 0}, state)
}

func TestBufferCancelAfterApply(t *testing.T) {
	var buf effect.Buffer
	var commit, rollback int
	buf.OnAfterCommit(func(context.Context) { commit++ })
	buf.OnAfterRollback(func(context.Context) { rollback++ })

	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())
	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())
	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())

	require.Equal(t, commit, 1)
	require.Equal(t, rollback, 0)
}

func TestBufferApplyAfterCancel(t *testing.T) {
	var buf effect.Buffer
	var commit, rollback int
	buf.OnAfterCommit(func(context.Context) { commit++ })
	buf.OnAfterRollback(func(context.Context) { rollback++ })

	buf.Cancel(context.TODO())
	buf.Apply(context.TODO())
	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())
	buf.Apply(context.TODO())
	buf.Cancel(context.TODO())

	require.Equal(t, commit, 0)
	require.Equal(t, rollback, 1)
}
