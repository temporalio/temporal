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

package shard

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/memory"
)

func TestPinning(t *testing.T) {
	type state struct {
		secret string
	}

	shard := &ContextImpl{}
	key := definition.WorkflowKey{
		NamespaceID: t.Name() + "-namespace",
		WorkflowID:  t.Name() + "-workflow_id",
		RunID:       t.Name() + "-run_id",
	}

	unpin := func() memory.Releaser {
		pinnedState := state{}
		unpin := memory.Hold(shard, key, &pinnedState)
		pinnedState.secret = t.Name()
		return unpin
	}()
	// pinnedState goes out of scope here - but should still exist because it is
	// pinned to the shard.Context
	fromPin, _, ok := memory.Recall[definition.WorkflowKey, *state](shard, key)
	require.True(t, ok)
	require.Equal(t, fromPin.secret, t.Name())

	unpin()
	{
		_, _, ok := memory.Recall[definition.WorkflowKey, *state](shard, key)
		require.False(t, ok)
	}
}
