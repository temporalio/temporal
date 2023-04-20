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

package memory_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/memory"
)

func TestMementoHoldRestore(t *testing.T) {
	type state struct{ secret string }

	caretaker := &memory.ThreadsafeCaretaker{}
	key := t.Name()
	release := func() memory.Releaser {
		memento := state{}
		release := memory.Hold(caretaker, key, &memento)
		memento.secret = t.Name()
		return release
	}()
	// memento goes out of scope here - but should still exist because it is
	// held by the caretaker
	restored, _, ok := memory.Recall[string, *state](caretaker, key)
	require.True(t, ok)
	require.Equal(t, restored.secret, t.Name())

	release()
	{
		_, _, ok := memory.Recall[string, *state](caretaker, key)
		require.False(t, ok)
	}
}

func TestPreventAccidentalMementoRelease(t *testing.T) {
	caretaker := &memory.ThreadsafeCaretaker{}
	key := t.Name()
	firstRelease := memory.Hold(caretaker, key, "first value")
	_ = memory.Hold(caretaker, key, "second value")

	// firstRelease is now stale due to the second assignment to the same key
	firstRelease()

	got, release, ok := memory.Recall[string, string](caretaker, key)
	require.Truef(t, ok, "memento under key %q should still be present "+
		"even though a Releaser for that key has been called - that "+
		"Releaser was for a different version of the key", key)
	require.Equal(t, got, "second value")

	release()
	_, _, ok = memory.Recall[string, string](caretaker, key)
	require.False(t, ok)
}
