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

package collection

import "sync"

// OnceMap is a concurrent map which lazily constructs its values. Map values are initialized on-the-fly, using a
// provided construction function only when a key is accessed for the first time.
type OnceMap[K comparable, T any] struct {
	mu        sync.RWMutex
	inner     map[K]T
	construct func(K) T
}

// NewOnceMap creates a [OnceMap] from a given construct function.
// construct should be kept light as it is called while holding a lock on the entire map.
func NewOnceMap[K comparable, T any](construct func(K) T) *OnceMap[K, T] {
	return &OnceMap[K, T]{
		construct: construct,
		inner:     make(map[K]T, 0),
	}
}

func (p *OnceMap[K, T]) Get(key K) T {
	p.mu.RLock()
	value, ok := p.inner[key]
	p.mu.RUnlock()
	if !ok {
		p.mu.Lock()
		defer p.mu.Unlock()
		if value, ok = p.inner[key]; !ok {
			value = p.construct(key)
			p.inner[key] = value
		}
	}

	return value
}
