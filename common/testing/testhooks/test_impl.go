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

//go:build test_dep

package testhooks

import (
	"sync"

	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewTestHooksImpl),
)

type (
	// TestHooks holds a registry of active test hooks. It should be obtained through fx and
	// used with Get and Set.
	TestHooks interface {
		// private accessors; access must go through package-level Get/Set
		get(string) (any, bool)
		set(string, any)
		del(string)
	}

	// testHooksImpl is an implementation of TestHooks.
	testHooksImpl struct {
		m sync.Map
	}
)

// Get gets the value of a test hook from the registry.
func Get[T any](th TestHooks, key string) (T, bool) {
	if val, ok := th.get(key); ok {
		// this is only used in test so we want to panic on type mismatch:
		return val.(T), ok // nolint:revive
	}
	var zero T
	return zero, false
}

// Call calls a func() hook if present.
func Call(th TestHooks, key string) {
	if hook, ok := Get[func()](th, key); ok {
		hook()
	}
}

// Set sets a test hook to a value and returns a cleanup function to unset it.
// Calls to Set and the cleanup functions should form a stack.
func Set[T any](th TestHooks, key string, val T) func() {
	th.set(key, val)
	return func() { th.del(key) }
}

// NewTestHooksImpl returns a new instance of a test hook registry. This is provided and used
// in the main "resource" module as a default, but in functional tests, it's overridden by an
// explicitly constructed instance.
func NewTestHooksImpl() TestHooks {
	return &testHooksImpl{}
}

func (th *testHooksImpl) get(key string) (any, bool) {
	return th.m.Load(key)
}

func (th *testHooksImpl) set(key string, val any) {
	th.m.Store(key, val)
}

func (th *testHooksImpl) del(key string) {
	th.m.Delete(key)
}
