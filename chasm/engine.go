//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination engine_mock.go

package chasm

import (
	"context"

	"go.temporal.io/server/common/log"
)

// NoValue is a sentinel type representing no value.
// Useful for accessing components using the engine methods (e.g., [GetComponent]) with a function that does not need to
// return any information.
type NoValue = *struct{}

type Engine interface {
	StartExecution(
		context.Context,
		ComponentRef,
		func(MutableContext, ArchetypeID, *Registry) (RootComponent, error),
		...TransitionOption,
	) (StartExecutionResult, error)
	UpdateWithStartExecution(
		context.Context,
		ComponentRef,
		func(MutableContext, ArchetypeID, *Registry) (RootComponent, error),
		func(MutableContext, Component, *Registry) error,
		...TransitionOption,
	) (EngineUpdateWithStartExecutionResult, error)

	UpdateComponent(
		context.Context,
		ComponentRef,
		func(MutableContext, Component, *Registry) error,
		...TransitionOption,
	) ([]byte, error)
	ReadComponent(
		context.Context,
		ComponentRef,
		func(Context, Component, *Registry) error,
		...TransitionOption,
	) error

	PollComponent(
		context.Context,
		ComponentRef,
		func(Context, Component, *Registry) (bool, error),
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
}

type TransitionOption func(*TransitionOptions)

// StartExecutionResult contains the outcome of creating a new execution via [StartExecution].
//
// This struct provides information about whether a new execution was actually created,
// along with identifiers needed to reference the execution in subsequent operations.
//
// Fields:
//   - ExecutionKey: The unique identifier for the execution. This key can be used to
//     look up or reference the execution in future operations.
//   - ExecutionRef: A serialized reference to the newly created root component.
//     This can be passed to [UpdateComponent], [ReadComponent], or [PollComponent]
//     to interact with the component. Use [DeserializeComponentRef] to convert this
//     back to a [ComponentRef] if needed.
//   - Created: Indicates whether a new execution was actually created. When false,
//     the execution already existed (based on the [BusinessIDReusePolicy] and
//     [BusinessIDConflictPolicy] configured via [WithBusinessIDPolicy]), and the
//     existing execution was returned instead.
type StartExecutionResult struct {
	ExecutionKey ExecutionKey
	ExecutionRef []byte
	Created      bool
}

// UpdateWithStartExecutionResult is the result of a UpdateWithStartExecution operation.
//
// Fields:
//   - ExecutionKey: The unique identifier for the execution. This key can be used to
//     look up or reference the execution in future operations.
//   - ExecutionRef: A serialized reference to the newly created root component.
//     This can be passed to [UpdateComponent], [ReadComponent], or [PollComponent]
//     to interact with the component. Use [DeserializeComponentRef] to convert this
//     back to a [ComponentRef] if needed.
//   - Created: Indicates whether a new execution was actually created. When false,
//     the execution already existed (based on the [BusinessIDReusePolicy] and
//     [BusinessIDConflictPolicy] configured via [WithBusinessIDPolicy]), and the
//     existing execution was returned instead.
//   - UpdateOutput: The output value returned by the update function.
type UpdateWithStartExecutionResult[O any] struct {
	ExecutionKey ExecutionKey
	ExecutionRef []byte
	Created      bool
	UpdateOutput O
}

// EngineUpdateWithStartExecutionResult is a type alias for the result type returned by the UpdateWithStart Engine implementation.
type EngineUpdateWithStartExecutionResult = UpdateWithStartExecutionResult[struct{}]

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
// This option only applies to StartExecution() and UpdateWithStartExecution().
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
// This option only applies to StartExecution() and UpdateWithStartExecution().
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

// StartExecution creates a new execution with a component initialized by the provided factory function.
//
// This is the primary entry point for starting a new execution in the CHASM engine. It handles
// the lifecycle of creating and persisting a new component within an execution context.
//
// Type Parameters:
//   - C: The component type to create, must implement [RootComponent]
//   - I: The input type passed to the factory function
//   - O: The output type returned by the factory function
//
// Parameters:
//   - ctx: Context containing the CHASM engine (must be created via [NewEngineContext])
//   - key: Unique identifier for the execution, used for deduplication and lookup
//   - startFn: Factory function that creates the component and produces output.
//     Receives a [MutableContext] for accessing engine capabilities and the input value.
//   - input: Application-specific data passed to startFn
//   - opts: Optional [TransitionOption] functions to configure creation behavior:
//   - [WithBusinessIDPolicy]: Controls duplicate handling and conflict resolution
//   - [WithRequestID]: Sets a request ID for idempotency
//   - [WithSpeculative]: Defers persistence until the next non-speculative transition
//
// Returns:
//   - O: The output value produced by startFn
//   - [NewExecutionResult]: Contains the execution key, serialized ref, and whether a new execution was created
//   - error: Non-nil if creation failed or policy constraints were violated
func StartExecution[C RootComponent, I any](
	ctx context.Context,
	key ExecutionKey,
	startFn func(MutableContext, I) (C, error),
	input I,
	opts ...TransitionOption,
) (StartExecutionResult, error) {
	result, err := engineFromContext(ctx).StartExecution(
		ctx,
		NewComponentRef[C](key),
		func(ctx MutableContext, archetypeID ArchetypeID, registry *Registry) (_ RootComponent, retErr error) {
			defer log.CapturePanic(ctx.Logger(), &retErr)

			var c C
			var err error
			c, err = startFn(augmentContextForArchetypeID(ctx, archetypeID, registry), input)
			return c, err
		},
		opts...,
	)
	if err != nil {
		return StartExecutionResult{}, err
	}

	return StartExecutionResult{
		ExecutionKey: result.ExecutionKey,
		ExecutionRef: result.ExecutionRef,
		Created:      result.Created,
	}, nil
}

func UpdateWithStartExecution[C RootComponent, I any, O any](
	ctx context.Context,
	key ExecutionKey,
	startFn func(MutableContext, I) (C, error),
	updateFn func(C, MutableContext, I) (O, error),
	input I,
	opts ...TransitionOption,
) (UpdateWithStartExecutionResult[O], error) {
	var output O
	result, err := engineFromContext(ctx).UpdateWithStartExecution(
		ctx,
		NewComponentRef[C](key),
		func(ctx MutableContext, archetypeID ArchetypeID, registry *Registry) (_ RootComponent, retErr error) {
			defer log.CapturePanic(ctx.Logger(), &retErr)

			var c C
			var err error
			c, err = startFn(augmentContextForArchetypeID(ctx, archetypeID, registry), input)
			return c, err
		},
		func(ctx MutableContext, c Component, registry *Registry) (retErr error) {
			defer log.CapturePanic(ctx.Logger(), &retErr)

			var err error
			output, err = updateFn(
				c.(C),
				AugmentContextForComponent(ctx, c, registry),
				input,
			)
			return err
		},
		opts...,
	)
	if err != nil {
		return UpdateWithStartExecutionResult[O]{
			UpdateOutput: output,
		}, err
	}
	return UpdateWithStartExecutionResult[O]{
		ExecutionKey: result.ExecutionKey,
		ExecutionRef: result.ExecutionRef,
		Created:      result.Created,
		UpdateOutput: output,
	}, nil
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
		func(ctx MutableContext, c Component, registry *Registry) (retErr error) {
			defer log.CapturePanic(ctx.Logger(), &retErr)

			var err error
			output, err = updateFn(
				c.(C),
				AugmentContextForComponent(ctx, c, registry),
				input,
			)
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
		func(ctx Context, c Component, registry *Registry) (retErr error) {
			defer log.CapturePanic(ctx.Logger(), &retErr)

			var err error
			output, err = readFn(
				c.(C),
				AugmentContextForComponent(ctx, c, registry),
				input,
			)
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
		func(ctx Context, c Component, registry *Registry) (_ bool, retErr error) {
			defer log.CapturePanic(ctx.Logger(), &retErr)

			out, satisfied, err := monotonicPredicate(
				c.(C),
				AugmentContextForComponent(ctx, c, registry),
				input,
			)
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
