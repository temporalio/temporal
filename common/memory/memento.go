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

package memory

import "sync"

type (
	// Caretaker represents the ability to hold and recall instances of any
	// piece of data, identified by a key. This is the memento pattern so we
	// adopt that naming scheme.
	Caretaker interface {
		// Hold instructs the Caretaker to form a strong reference to the object
		// provided. The reference will prevent garbage collection of the data.
		// The data can be recalled later by calling Recall with the same key as
		// is provded here. The strong reference created here can be removed by
		// calling the returned Releaser function. It is expected that clients
		// will not use this function directly but rather use the Hold free
		// function in this package.
		Hold(key any, data any) Releaser

		// Recall returns a memento previously registered with this Caretaker
		// via the Hold function. If the item exists, the third return value
		// will be `true`, otherwise it will be `false`. It is expected that
		// clients will not use this function directly but rather use the Recall
		// free function in this package.
		Recall(key any) (data any, release Releaser, found bool)
	}

	// ThreadsafeCaretaker is a Caretaker implementation that is threadsafe for
	// any number of readers and writers. Readers can be concurrent with other
	// readers but writers are serialized with both readers and other writers.
	// The zero-value of this type is a valid and functional state. A
	// ThreadsafeCaretaker must not be copied after first use.
	ThreadsafeCaretaker struct {
		mu   sync.RWMutex
		data map[any]mementoEntry
		ver  uint32
	}

	// Releaser removes a memento from a Caretaker.
	Releaser func()

	mementoEntry struct {
		ver      uint32
		memento  any
		releaser Releaser
	}
)

// Hold adds or overwrites a memento value under the provided key in the
// referenced Caretaker. The data will be kept alive for later recall by the
// originator until the Releaser is called or the Caretaker's lifetime
// completes.
func Hold[K comparable, V any](p Caretaker, key K, v V) Releaser {
	return p.Hold(key, v)
}

// Recall loads a memento previously stored with the provided Caretaker under
// the provided key. If no such value is found, the third return parameter will
// be `false`.
func Recall[K comparable, V any](p Caretaker, key K) (V, Releaser, bool) {
	var zeroV V
	if untypedV, unpin, ok := p.Recall(key); ok {
		if v, ok := untypedV.(V); ok {
			return v, unpin, ok
		}
	}
	return zeroV, nil, false
}

// Hold implements Caretaker.Hold with thread safety.
func (tc *ThreadsafeCaretaker) Hold(k any, v any) Releaser {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	if tc.data == nil {
		tc.data = make(map[any]mementoEntry)
	}
	tc.ver++
	ver := tc.ver
	releaser := func() {
		tc.mu.Lock()
		defer tc.mu.Unlock()
		if entry, ok := tc.data[k]; ok && entry.ver == ver {
			delete(tc.data, k)
		}
	}
	tc.data[k] = mementoEntry{memento: v, releaser: releaser, ver: ver}
	return releaser
}

// Recall implements Caretaker.Recall with thread safety.
func (tc *ThreadsafeCaretaker) Recall(k any) (any, Releaser, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	if tc.data == nil {
		return nil, nil, false
	}
	if v, ok := tc.data[k]; ok {
		return v.memento, v.releaser, true
	}
	return nil, nil, false
}
