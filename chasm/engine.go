//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination engine_mock.go

package chasm

import (
	"context"
)

// NoValue is a sentinel type representing no value.
// Useful for accessing components using the engine methods (e.g., [GetComponent]) with a function that does not need to
// return any information.
type NoValue = *struct{}

type Engine interface {
	NewEntity(
		context.Context,
		ComponentRef,
		func(MutableContext) (Component, error),
		...TransitionOption,
	) (EntityKey, []byte, error)
	UpdateWithNewEntity(
		context.Context,
		ComponentRef,
		func(MutableContext) (Component, error),
		func(MutableContext, Component) error,
		...TransitionOption,
	) (EntityKey, []byte, error)

	UpdateComponent(
		context.Context,
		ComponentRef,
		func(MutableContext, Component) error,
		...TransitionOption,
	) ([]byte, error)
	ReadComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) error,
		...TransitionOption,
	) error

	PollComponent(
		context.Context,
		ComponentRef,
		func(Context, Component) (any, bool, error),
		func(MutableContext, Component, any) error,
		...TransitionOption,
	) ([]byte, error)
}

type BusinessIDReusePolicy int

const (
	BusinessIDReusePolicyAllowDuplicate BusinessIDReusePolicy = iota
	BusinessIDReusePolicyAllowDuplicateFailedOnly
	BusinessIDReusePolicyRejectDuplicate
)

type BusinessIDConflictPolicy int

const (
	BusinessIDConflictPolicyFail BusinessIDConflictPolicy = iota
	BusinessIDConflictPolicyTermiateExisting
	// TODO: Do we want to support UseExisting conflict policy?
	// BusinessIDConflictPolicyUseExisting
)

type TransitionOptions struct {
	ReusePolicy    BusinessIDReusePolicy
	ConflictPolicy BusinessIDConflictPolicy
	RequestID      string
	Speculative    bool
}

type TransitionOption func(*TransitionOptions)

// (only) this transition will not be persisted
// The next non-speculative transition will persist this transition as well.
// Compared to the EntityEphemeral() operation on RegistrableComponent,
// the scope of this operation is limited to a certain transition,
// while the EntityEphemeral() applies to all transitions.
// TODO: we need to figure out a way to run the tasks
// generated in a speculative transition
func WithSpeculative() TransitionOption {
	return func(opts *TransitionOptions) {
		opts.Speculative = true
	}
}

// this only applies to NewEntity and UpdateWithNewEntity
func WithBusinessIDPolicy(
	reusePolicy BusinessIDReusePolicy,
	conflictPolicy BusinessIDConflictPolicy,
) TransitionOption {
	return func(opts *TransitionOptions) {
		opts.ReusePolicy = reusePolicy
		opts.ConflictPolicy = conflictPolicy
	}
}

// this only applies to NewEntity and UpdateWithNewEntity
func WithRequestID(
	requestID string,
) TransitionOption {
	return func(opts *TransitionOptions) {
		opts.RequestID = requestID
	}
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
) (O, EntityKey, []byte, error) {
	var output O
	entityKey, serializedRef, err := engineFromContext(ctx).NewEntity(
		ctx,
		NewComponentRef[C](key),
		func(ctx MutableContext) (Component, error) {
			var c C
			var err error
			c, output, err = newFn(ctx, input)
			return c, err
		},
		opts...,
	)
	if err != nil {
		return output, EntityKey{}, nil, err
	}
	return output, entityKey, serializedRef, err
}

func UpdateWithNewEntity[C Component, I any, O1 any, O2 any](
	ctx context.Context,
	key EntityKey,
	newFn func(MutableContext, I) (C, O1, error),
	updateFn func(C, MutableContext, I) (O2, error),
	input I,
	opts ...TransitionOption,
) (O1, O2, EntityKey, []byte, error) {
	var output1 O1
	var output2 O2
	entityKey, serializedRef, err := engineFromContext(ctx).UpdateWithNewEntity(
		ctx,
		NewComponentRef[C](key),
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
	if err != nil {
		return output1, output2, EntityKey{}, nil, err
	}
	return output1, output2, entityKey, serializedRef, err
}

// TODO:
//   - consider merge with ReadComponent
//   - consider remove ComponentRef from the return value and allow components to get
//     the ref in the transition function. There are some caveats there, check the
//     comment of the NewRef method in MutableContext.
func UpdateComponent[C any, R []byte | ComponentRef, I any, O any](
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

	newSerializedRef, err := engineFromContext(ctx).UpdateComponent(
		ctx,
		ref,
		func(ctx MutableContext, c Component) error {
			var err error
			output, err = updateFn(c.(C), ctx, input)
			return err
		},
		opts...,
	)

	if err != nil {
		return output, nil, err
	}
	return output, newSerializedRef, err
}

func ReadComponent[C any, R []byte | ComponentRef, I any, O any](
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

	err = engineFromContext(ctx).ReadComponent(
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

func PollComponent[C any, R []byte | ComponentRef, I any, O any, T any](
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

	newSerializedRef, err := engineFromContext(ctx).PollComponent(
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
	if err != nil {
		return output, nil, err
	}
	return output, newSerializedRef, err
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
func NewEngineContext(
	ctx context.Context,
	engine Engine,
) context.Context {
	return context.WithValue(ctx, engineCtxKey, engine)
}

func engineFromContext(
	ctx context.Context,
) Engine {
	e, ok := ctx.Value(engineCtxKey).(Engine)
	if !ok {
		return nil
	}
	return e
}
