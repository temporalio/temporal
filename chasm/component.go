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

//go:generate mockgen -copyright_file ../LICENSE -package $GOPACKAGE -source $GOFILE -destination component_mock.go

package chasm

import "context"

type Component interface {
	LifecycleState() LifecycleState

	// TBD: the framework can just put the component in terminated state
	// component lifecycle state can still be running when getting terminated
	// but framework will use some rule to block incoming operations
	// Terminate()

	// we may not need this in the beginning
	mustEmbedUnimplementedComponent()
}

// Embed UnimplementedComponent to get forward compatibility
type UnimplementedComponent struct{}

func (UnimplementedComponent) LifecycleState() LifecycleState {
	return LifecycleStateUnspecified
}

// func (UnimplementedComponent) Terminate() {}

func (UnimplementedComponent) mustEmbedUnimplementedComponent() {}

// Shall it be named ComponentLifecycleState?
type LifecycleState int

const (
	LifecycleStateCreated LifecycleState = 1 << iota
	LifecycleStateRunning
	// LifecycleStatePaused // <- this can also be a method of the engine: PauseComponent
	LifecycleStateCompleted
	LifecycleStateFailed
	// LifecycleStateTerminated
	// LifecycleStateReset

	LifecycleStateUnspecified = LifecycleState(0)
)

type OperationIntent int

const (
	OperationIntentProgress OperationIntent = 1 << iota
	OperationIntentObserve

	OperationIntentUnspecified = OperationIntent(0)
)

// The operation intent must come from the context
// as the handler may not pass the endpoint request as Input to,
// say, the chasm.UpdateComponent method.
// So similar to the chasm engine, handler needs to add the intent
// to the context.
type operationIntentCtxKeyType string

const operationIntentCtxKey engineCtxKeyType = "chasmOperationIntent"

func newContextWithOperationIntent(
	ctx context.Context,
	intent OperationIntent,
) context.Context {
	return context.WithValue(ctx, operationIntentCtxKey, intent)
}

func operationIntentFromContext(
	ctx context.Context,
) OperationIntent {
	intent, ok := ctx.Value(engineCtxKey).(OperationIntent)
	if !ok {
		return OperationIntentUnspecified
	}
	return intent
}
