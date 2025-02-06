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
)

type engine interface {
	newInstance(
		context.Context,
		EntityKey,
		func(MutableContext) (Component, error),
		...TransitionOption,
	) (ComponentRef, error)
	updateWithNewInstance(
		context.Context,
		EntityKey,
		func(MutableContext) (Component, error),
		func(MutableContext, Component) error,
		...TransitionOption,
	) (ComponentRef, error)

	updateComponent(
		context.Context,
		ComponentRef,
		func(MutableContext, Component) error,
		...TransitionOption,
	) (ComponentRef, error)
	readComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) error,
		...TransitionOption,
	) error

	pollComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) (any, bool, error),
		func(MutableContext, Component, any) error,
		...TransitionOption,
	) (ComponentRef, error)
}

type BusinessIDReusePolicy int

const (
	BusinessIDReusePolicyAllowDuplicate BusinessIDReusePolicy = iota
	BusinessIDReusePolicyRejectDuplicate
)

type BusinessIDConflictPolicy int

const (
	BusinessIDConflictPolicyFail BusinessIDConflictPolicy = iota
	BusinessIDConflictPolicyTermiateExisting
	BusinessIDConflictPolicyUseExisting
)

type transitionOptions struct {
}

type TransitionOption func(*transitionOptions)

// (only) this transition will not be persisted
// The next non-speculative transition will persist this transition as well.
// Compared to the EntityEphemeral() operation on RegistrableComponent,
// the scope of this operation is limited to a certain transition,
// while the EntityEphemeral() applies to all transitions.
// TODO: we need to figure out a way to run the tasks
// generated in a speculative transition
func WithSpeculative() TransitionOption {
	panic("not implemented")
}

// this only applies to NewEntity and UpdateWithNewEntity
func WithBusinessIDPolicy(
	reusePolicy BusinessIDReusePolicy,
	conflictPolicy BusinessIDConflictPolicy,
) TransitionOption {
	panic("not implemented")
}

// Not needed for V1
// func WithEagerLoading(
// 	paths []ComponentPath,
// ) OperationOption {
// 	panic("not implemented")
// }

func NewEntity[C Component, I any, O any](
	ctx context.Context,
	key EntityKey,
	newFn func(MutableContext, I) (C, O, error),
	input I,
	opts ...TransitionOption,
) (O, []byte, error) {
	var output O
	ref, err := engineFromContext(ctx).newInstance(
		ctx,
		key,
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output, err = newFn(ctx, input)
			return c, err
		},
		opts...,
	)
	return output, ref.Serialize(), err
}

func UpdateWithNewEntity[C Component, I any, O1 any, O2 any](
	ctx context.Context,
	key EntityKey,
	newFn func(MutableContext, I) (C, O1, error),
	updateFn func(C, MutableContext, I) (O2, error),
	input I,
	opts ...TransitionOption,
) (O1, O2, []byte, error) {
	var output1 O1
	var output2 O2
	ref, err := engineFromContext(ctx).updateWithNewInstance(
		ctx,
		key,
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output1, err = newFn(ctx, input)
			return c, err
		},
		func(ctx MutableContext, c Component) error {
			var err error
			output2, err = updateFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)
	return output1, output2, ref.Serialize(), err
}

// TODO:
//   - consider merge with ReadComponent
//   - consider remove ComponentRef from the return value and allow components to get
//     the ref in the transition function. There are some caveats there, check the
//     comment of the NewRef method in MutableContext.
func UpdateComponent[C Component, R []byte | ComponentRef, I any, O any](
	ctx context.Context,
	r R,
	updateFn func(C, MutableContext, I) (O, error),
	input I,
	opts ...TransitionOption,
) (O, []byte, error) {
	var output O

	ref, err := convertComponentRef(r)
	if err != nil {
		return output, nil, err
	}

	newRef, err := engineFromContext(ctx).updateComponent(
		ctx,
		ref,
		func(ctx MutableContext, c Component) error {
			var err error
			output, err = updateFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)

	return output, newRef.Serialize(), err
}

func ReadComponent[C Component, R []byte | ComponentRef, I any, O any](
	ctx context.Context,
	r R,
	readFn func(C, Context, I) (O, error),
	input I,
	opts ...TransitionOption,
) (O, error) {
	var output O

	ref, err := convertComponentRef(r)
	if err != nil {
		return output, err
	}

	err = engineFromContext(ctx).readComponent(
		ctx,
		ref,
		func(ctx Context, c Component) error {
			var err error
			output, err = readFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)
	return output, err
}

type PollComponentRequest[C Component, I any, O any] struct {
	Ref         ComponentRef
	PredicateFn func(C, Context, I) bool
	OperationFn func(C, MutableContext, I) (O, error)
	Input       I
}

func PollComponent[C Component, R []byte | ComponentRef, I any, O any, T any](
	ctx context.Context,
	r R,
	predicateFn func(C, Context, I) (T, bool, error),
	operationFn func(C, MutableContext, I, T) (O, error),
	input I,
	opts ...TransitionOption,
) (O, []byte, error) {
	var output O

	ref, err := convertComponentRef(r)
	if err != nil {
		return output, nil, err
	}

	newRef, err := engineFromContext(ctx).pollComponent(
		ctx,
		ref,
		func(ctx Context, c Component) (any, bool, error) {
			return predicateFn(c.(C), ctx, input)
		},
		func(ctx MutableContext, c Component, t any) error {
			var err error
			output, err = operationFn(c.(C), ctx, input, t.(T))
			return err
		},
		opts...,
	)
	return output, newRef.Serialize(), err
}

func convertComponentRef[R []byte | ComponentRef](
	r R,
) (ComponentRef, error) {
	if refToken, ok := any(r).([]byte); ok {
		return DeserializeComponentRef(refToken)
	}

	//revive:disable-next-line:unchecked-type-assertion
	return any(r).(ComponentRef), nil
}

type engineCtxKeyType string

const engineCtxKey engineCtxKeyType = "chasmEngine"

// this will be done by the nexus handler?
// alternatively the engine can be a global variable,
// but not a good practice in fx.
func newEngineContext(
	ctx context.Context,
	engine engine,
) context.Context {
	return context.WithValue(ctx, engineCtxKey, engine)
}

func engineFromContext(
	ctx context.Context,
) engine {
	e, ok := ctx.Value(engineCtxKey).(engine)
	if !ok {
		return nil
	}
	return e
}
