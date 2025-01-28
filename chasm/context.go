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

package chasm

import (
	"context"
	"time"
)

type Context interface {
	// Context is not bound to any component,
	// so all methods needs to take in component as a parameter

	// NOTE: component created in the current transaction won't have a ref
	// this is a Ref to the component state at the start of the transition
	Ref(Component) (ComponentRef, bool)
	Now(Component) time.Time

	// Intent() OperationIntent
	// ComponentOptions(Component) []ComponentOption

	getContext() context.Context
}

type MutableContext interface {
	Context

	AddTask(Component, TaskAttributes, any) error

	// Add more methods here for other storage commands/primitives.
	// e.g. HistoryEvent

	// Get a Ref for the component
	// This ref to the component state at the end of the transition
	// Same as Ref(Component) method in Context,
	// this only works for components that already exists at the start of the transition
	//
	// If we provide this method, then the method on the engine doesn't need to
	// return a Ref
	// NewRef(Component) (ComponentRef, bool)
}
