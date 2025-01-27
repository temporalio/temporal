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
	return ctx.Value(engineCtxKey).(OperationIntent)
}
