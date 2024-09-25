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

package collection

import (
	"maps"
	"sync"
)

// SyncMap implements a simple mutex-wrapped map. SyncMap is copyable like a normal map[K]V.
type SyncMap[K comparable, V any] struct {
	// Use a pointer to RWMutex instead of embedding so that the contents of this struct itself
	// are immutable and copyable, and copies refer to the same RWMutex and map.
	*sync.RWMutex
	// For the same reason, contents (the pointer) should not be changed.
	contents map[K]V
}

func NewSyncMap[K comparable, V any]() SyncMap[K, V] {
	return SyncMap[K, V]{
		RWMutex:  &sync.RWMutex{},
		contents: make(map[K]V),
	}
}

func (m *SyncMap[K, V]) Get(key K) (value V, ok bool) {
	m.RLock()
	defer m.RUnlock()
	value, ok = m.contents[key]
	return
}

func (m *SyncMap[K, V]) GetOrSet(key K, value V) (v V, exist bool) {
	m.RLock()
	currentValue, ok := m.contents[key]
	m.RUnlock()
	if ok {
		return currentValue, ok
	}

	m.Lock()
	defer m.Unlock()
	currentValue, ok = m.contents[key]
	if ok {
		return currentValue, ok
	}
	m.contents[key] = value
	return value, false
}

func (m *SyncMap[K, V]) Set(key K, value V) {
	m.Lock()
	defer m.Unlock()
	m.contents[key] = value
}

func (m *SyncMap[K, V]) Delete(key K) {
	m.Lock()
	defer m.Unlock()
	delete(m.contents, key)
}

func (m *SyncMap[K, V]) Pop(key K) (value V, ok bool) {
	m.Lock()
	defer m.Unlock()
	value, ok = m.contents[key]
	if ok {
		delete(m.contents, key)
	}
	return value, ok
}

func (m *SyncMap[K, V]) PopAll() map[K]V {
	m.Lock()
	defer m.Unlock()
	contents := maps.Clone(m.contents)
	clear(m.contents)
	return contents
}
