// The MIT License
//
// Copyright (c) 2024 Temporal Technologies, Inc.
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

package testhook

import (
	"sync"
)

type Hook[T any] interface {
	Invoke(args ...any) T
	Set(func(args ...any) T)
	Reset()
}

type hook[T any] struct {
	mu sync.RWMutex
	fn func(args ...any) T
}

func NewHook[T any]() Hook[T] {
	return &hook[T]{}
}

func (h *hook[T]) Invoke(args ...any) T {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var result T
	if h.fn != nil {
		result = h.fn(args...)
	}
	return result
}

// TODO: allow to set maximum invocations
func (h *hook[T]) Set(fn func(args ...any) T) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.fn = fn
}

func (h *hook[T]) Reset() {
	h.Set(nil)
}
