//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination engine_mock.go

package chasm

import (
	"context"
	"reflect"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/payload"
	"google.golang.org/protobuf/proto"
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
		func(Context, Component) (any, bool, error),
		func(MutableContext, Component, any) error,
		...TransitionOption,
	) ([]byte, error)

	ListExecutions(
		context.Context,
		reflect.Type,
		*ListExecutionsRequest,
	) (*ListExecutionsResponse[*commonpb.Payload], error)

	CountExecutions(
		context.Context,
		reflect.Type,
		*CountExecutionsRequest,
	) (*CountExecutionsResponse, error)
}

type ListExecutionsResponse[M proto.Message] struct {
	Executions    []*ExecutionInfo[M]
	NextPageToken []byte
}
type ExecutionInfo[M proto.Message] struct {
	BusinessID             string
	RunID                  string
	StartTime              time.Time
	CloseTime              time.Time
	HistoryLength          int64
	HistorySizeBytes       int64
	StateTransitionCount   int64
	ChasmSearchAttributes  SearchAttributesMap
	CustomSearchAttributes map[string]*commonpb.Payload
	Memo                   *commonpb.Memo
	ChasmMemo              M
}

type ListExecutionsRequest struct {
	NamespaceID   string
	NamespaceName string
	Query         string
	PageSize      int
	NextPageToken []byte
}

type CountExecutionsRequest struct {
	NamespaceID   string
	NamespaceName string
	Query         string
}

type CountExecutionsResponse struct {
	Count int64
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

// ListExecutions lists the executions of a CHASM archetype given an initial query.
// The query string can specify any combination of CHASM, custom, and predefined/system search attributes.
// The generic parameter C is the CHASM component type used for executions and search attribute filtering.
// The generic parameter M is the type of the memo payload to be unmarshaled from the execution.
// PageSize is required, must be greater than 0.
// NextPageToken is optional, set on subsequent requests to continue listing the next page of executions.
// Note: For CHASM executions, TemporalNamespaceDivision is the predefined search attribute
// that is used to identify the archetype of the execution.
// If the query string does not specify TemporalNamespaceDivision, the archetype C of the request will be used to filter the executions.
// If the initial query already specifies TemporalNamespaceDivision, the archetype C of the request will
// only be used to get the registered SearchAttributes.
func ListExecutions[C Component, M proto.Message](
	ctx context.Context,
	request *ListExecutionsRequest,
) (*ListExecutionsResponse[M], error) {
	archetypeType := reflect.TypeFor[C]()
	response, err := engineFromContext(ctx).ListExecutions(ctx, archetypeType, request)
	if err != nil {
		return nil, err
	}

	// Convert response, unmarshaling ChasmMemo to type M
	executions := make([]*ExecutionInfo[M], len(response.Executions))
	for i, execution := range response.Executions {
		chasmMemoInterface := reflect.New(reflect.TypeFor[M]().Elem()).Interface()
		chasmMemo, ok := chasmMemoInterface.(M)
		if !ok {
			return nil, serviceerror.NewInternalf("failed to cast chasm memo to type %s", reflect.TypeFor[M]().String())
		}
		err := payload.Decode(execution.ChasmMemo, chasmMemo)
		if err != nil {
			return nil, serviceerror.NewInternalf("failed to decode chasm memo: %v", err)
		}
		executions[i] = &ExecutionInfo[M]{
			BusinessID:             execution.BusinessID,
			RunID:                  execution.RunID,
			StartTime:              execution.StartTime,
			CloseTime:              execution.CloseTime,
			HistoryLength:          execution.HistoryLength,
			HistorySizeBytes:       execution.HistorySizeBytes,
			StateTransitionCount:   execution.StateTransitionCount,
			ChasmSearchAttributes:  execution.ChasmSearchAttributes,
			CustomSearchAttributes: execution.CustomSearchAttributes,
			Memo:                   execution.Memo,
			ChasmMemo:              chasmMemo,
		}
	}

	return &ListExecutionsResponse[M]{
		Executions:    executions,
		NextPageToken: response.NextPageToken,
	}, nil
}

// CountExecutions counts the executions of a CHASM archetype given an initial query.
// The generic parameter C is the CHASM component type used for executions and search attribute filtering.
// The query string can specify any combination of CHASM, custom, and predefined/system search attributes.
// Note: For CHASM executions, TemporalNamespaceDivision is the predefined search attribute
// that is used to identify the archetype of the execution.
// If the query string does not specify TemporalNamespaceDivision, the archetype C of the request will be used to count the executions.
// If the initial query already specifies TemporalNamespaceDivision, the archetype C of the request will
// only be used to get the registered SearchAttributes.
func CountExecutions[C Component](
	ctx context.Context,
	request *CountExecutionsRequest,
) (*CountExecutionsResponse, error) {
	archetypeType := reflect.TypeFor[C]()
	return engineFromContext(ctx).CountExecutions(ctx, archetypeType, request)
}
