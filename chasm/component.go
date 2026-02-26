//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination component_mock.go

package chasm

import (
	"context"
	"reflect"
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
)

type Component interface {
	LifecycleState(Context) LifecycleState

	// we may not need this in the beginning
	mustEmbedUnimplementedComponent()
}

type TerminableComponent interface {
	Component

	// Terminate method is invoked by the chasm framework on an execution's root component when the execution
	// needs to be forcefully terminated.
	// Some examples include:
	// - Execution state becomes too large.
	// - Two running executions with the same businessID when namespace performs a force failover.
	Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error)
}

type TerminateComponentRequest struct {
	Identity  string
	Reason    string
	Details   *commonpb.Payloads
	RequestID string
}

type TerminateComponentResponse struct{}

// RootComponent is the interface that must be implemented by the top level component of a chasm execution.
// When the RootComponent's LifecycleState transitions to a closed state, the entire execution is considered closed,
// and will be cleaned up by the chasm framework after namespace's retention period. The BusinessID is also available for reuse.
//
// TODO: (not yet true) Visibility record will no longer be updated after RootComponent is closed.
type RootComponent interface {
	TerminableComponent
}

// Embed UnimplementedComponent to get forward compatibility
type UnimplementedComponent struct{}

func (UnimplementedComponent) mustEmbedUnimplementedComponent() {}

var UnimplementedComponentT = reflect.TypeFor[UnimplementedComponent]()

// Shall it be named ComponentLifecycleState?
type LifecycleState int

const (
	// Lifecycle states that are considered OPEN
	//
	// LifecycleStateCreated LifecycleState = 1 << iota
	LifecycleStateRunning LifecycleState = 2 << iota
	// LifecycleStatePaused

	// Lifecycle states that are considered CLOSED
	//
	LifecycleStateCompleted
	LifecycleStateFailed
	// LifecycleStateTerminated
	// LifecycleStateTimedout
	// LifecycleStateReset
)

func (s LifecycleState) IsClosed() bool {
	return s >= LifecycleStateCompleted
}

func (s LifecycleState) String() string {
	switch s {
	case LifecycleStateRunning:
		return "Running"
	case LifecycleStateCompleted:
		return "Completed"
	case LifecycleStateFailed:
		return "Failed"
	default:
		return strconv.Itoa(int(s))
	}
}

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
type operationIntentCtxKeyType struct{}

var operationIntentCtxKey = operationIntentCtxKeyType{}

func newContextWithOperationIntent(
	ctx context.Context,
	intent OperationIntent,
) context.Context {
	return context.WithValue(ctx, operationIntentCtxKey, intent)
}

func operationIntentFromContext(
	ctx context.Context,
) OperationIntent {
	intent, ok := ctx.Value(operationIntentCtxKey).(OperationIntent)
	if !ok {
		return OperationIntentUnspecified
	}
	return intent
}
