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
	NewExecution(
		context.Context,
		ComponentRef,
		func(MutableContext) (Component, error),
		...TransitionOption,
	) (ExecutionKey, []byte, error)
	UpdateWithNewExecution(
		context.Context,
		ComponentRef,
		func(MutableContext) (Component, error),
		func(MutableContext, Component) error,
		...TransitionOption,
	) (ExecutionKey, []byte, error)

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
		func(Context, Component) (bool, error),
		...TransitionOption,
	) ([]byte, error)

	// NotifyExecution notifies any PollComponent callers waiting on the execution.
	NotifyExecution(ExecutionKey)
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
	BusinessIDConflictPolicyTerminateExisting
	BusinessIDConflictPolicyUseExisting
)

type TransitionOptions struct {
	ReusePolicy    BusinessIDReusePolicy
	ConflictPolicy BusinessIDConflictPolicy
	RequestID      string
	Speculative    bool
	TaskQueue      string
}

type TransitionOption func(*TransitionOptions)

// (only) this transition will not be persisted
// The next non-speculative transition will persist this transition as well.
// Compared to the ExecutionEphemeral() operation on RegistrableComponent,
// the scope of this operation is limited to a certain transition,
// while the ExecutionEphemeral() applies to all transitions.
// TODO: we need to figure out a way to run the tasks
// generated in a speculative transition
func WithSpeculative() TransitionOption {
	return func(opts *TransitionOptions) {
		opts.Speculative = true
	}
}

// WithBusinessIDPolicy sets the businessID reuse and conflict policy
// used in the transition when creating a new execution.
// This option only applies to NewExecution() and UpdateWithNewExecution().
func WithBusinessIDPolicy(
	reusePolicy BusinessIDReusePolicy,
	conflictPolicy BusinessIDConflictPolicy,
) TransitionOption {
	return func(opts *TransitionOptions) {
		opts.ReusePolicy = reusePolicy
		opts.ConflictPolicy = conflictPolicy
	}
}

// WithRequestID sets the requestID used when creating a new execution.
// This option only applies to NewExecution() and UpdateWithNewExecution().
func WithRequestID(
	requestID string,
) TransitionOption {
	return func(opts *TransitionOptions) {
		opts.RequestID = requestID
	}
}

// WithTaskQueue sets the task queue on the execution.
func WithTaskQueue(
	taskQueue string,
) TransitionOption {
	return func(opts *TransitionOptions) {
		opts.TaskQueue = taskQueue
	}
}

// Not needed for V1
// func WithEagerLoading(
// 	paths []ComponentPath,
// ) OperationOption {
// 	panic("not implemented")
// }

func NewExecution[C Component, I any, O any](
	ctx context.Context,
	key ExecutionKey,
	newFn func(MutableContext, I) (C, O, error),
	input I,
	opts ...TransitionOption,
) (O, ExecutionKey, []byte, error) {
	var output O
	executionKey, serializedRef, err := engineFromContext(ctx).NewExecution(
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
		return output, ExecutionKey{}, nil, err
	}
	return output, executionKey, serializedRef, err
}

func UpdateWithNewExecution[C Component, I any, O1 any, O2 any](
	ctx context.Context,
	key ExecutionKey,
	newFn func(MutableContext, I) (C, O1, error),
	updateFn func(C, MutableContext, I) (O2, error),
	input I,
	opts ...TransitionOption,
) (O1, O2, ExecutionKey, []byte, error) {
	var output1 O1
	var output2 O2
	executionKey, serializedRef, err := engineFromContext(ctx).UpdateWithNewExecution(
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
		return output1, output2, ExecutionKey{}, nil, err
	}
	return output1, output2, executionKey, serializedRef, err
}

// TODO:
//   - consider merge with ReadComponent
//   - consider remove ComponentRef from the return value and allow components to get
//     the ref in the transition function. There are some caveats there, check the
//     comment of the NewRef method in MutableContext.
//
// UpdateComponent applies updateFn to the component identified by the supplied component reference.
// It returns the result, along with the new component reference. opts are currently ignored.
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

// ReadComponent returns the result of evaluating readFn against the component identified by the
// component reference. opts are currently ignored.
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

// PollComponent waits until the predicate is true when evaluated against the component identified
// by the supplied component reference. If this times out due to a server-imposed long-poll timeout
// then it returns (nil, nil, nil), as an indication that the caller should continue long-polling.
// Otherwise it returns (output, ref, err), where output is the output of the predicate function,
// and ref is a component reference identifying the state at which the predicate was satisfied. The
// predicate must be monotonic: if it returns true at execution state transition s then it must
// return true at all transitions t > s. If the predicate is true at the outset then PollComponent
// returns immediately. opts are currently ignored.
func PollComponent[C any, R []byte | ComponentRef, I any, O any](
	ctx context.Context,
	r R,
	monotonicPredicate func(C, Context, I) (O, bool, error),
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
		func(ctx Context, c Component) (bool, error) {
			out, satisfied, err := monotonicPredicate(c.(C), ctx, input)
			if satisfied {
				output = out
			}
			return satisfied, err
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
