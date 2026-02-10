package chasm

import (
	"context"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
)

type Context interface {
	// Context is not bound to any component,
	// so all methods needs to take in component as a parameter

	// NOTE: component created in the current transaction won't have a ref
	// this is a Ref to the component state at the start of the transition
	Ref(Component) ([]byte, error)
	// Now returns the current time in the context of the given component.
	// In a context of a transaction, this time must be used to allow for framework support of pause and time skipping.
	Now(Component) time.Time
	// ExecutionKey returns the execution key for the execution the context is operating on.
	ExecutionKey() ExecutionKey
	// StateTransitionCount returns the number of create/update transactions in the history of this execution.
	StateTransitionCount() int64
	// ExecutionCloseTime returns the time when the execution was closed. An execution is closed when its root component reaches a terminal
	// state in its lifecycle. If the component is still running (not yet closed), it returns a zero time.Time value.
	ExecutionCloseTime() time.Time
	// Logger returns a logger tagged with execution key and other chasm framework internal information.
	Logger() log.Logger
	// GetNamespaceEntry returns the namespace entry for the execution.
	GetNamespaceEntry() *namespace.Namespace

	// Intent() OperationIntent
	// ComponentOptions(Component) []ComponentOption

	// GetContext returns the underlying context.Context for use in I/O operations (e.g., RPC calls).
	GetContext() context.Context

	structuredRef(Component) (ComponentRef, error)
}

type MutableContext interface {
	Context

	// AddTask adds a task to be emitted as part of the current transaction.
	// The task is associated with the given component and will be invoked via the registered executor for the given task
	// referencing the component.
	AddTask(Component, TaskAttributes, any)

	// AddHistoryEvent adds a history event for the execution.
	AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent

	// Get a Ref for the component
	// This ref to the component state at the end of the transition
	// Same as Ref(Component) method in Context,
	// this only works for components that already exists at the start of the transition
	//
	// If we provide this method, then the method on the engine doesn't need to
	// return a Ref
	// NewRef(Component) (ComponentRef, bool)
}

type immutableCtx struct {
	// The context here is not really used today.
	// But it will be when we support partial loading later,
	// and the framework potentially needs to go to persistence to load some fields.
	ctx context.Context

	executionKey ExecutionKey

	// Not embedding the Node here to avoid exposing AddTask() method on Node,
	// so that ContextImpl won't implement MutableContext interface.
	root *Node
}

type mutableCtx struct {
	*immutableCtx
}

// NewContext creates a new Context from an existing Context and root Node.
//
// NOTE: Library authors should not invoke this constructor directly, and instead use [ReadComponent].
func NewContext(
	ctx context.Context,
	node *Node,
) Context {
	return newContext(ctx, node)
}

// newContext creates a new immutableCtx from an existing Context and root Node.
// This is similar to NewContext, but returns *immutableCtx instead of Context interface.
func newContext(
	ctx context.Context,
	node *Node,
) *immutableCtx {
	workflowKey := node.backend.GetWorkflowKey()
	return &immutableCtx{
		ctx:  ctx,
		root: node.root(),
		executionKey: ExecutionKey{
			NamespaceID: workflowKey.NamespaceID,
			BusinessID:  workflowKey.WorkflowID,
			RunID:       workflowKey.RunID,
		},
	}
}

func (c *immutableCtx) Ref(component Component) ([]byte, error) {
	return c.root.Ref(component)
}

func (c *immutableCtx) Now(component Component) time.Time {
	return c.root.Now(component)
}

func (c *immutableCtx) ExecutionKey() ExecutionKey {
	return c.executionKey
}

func (c *immutableCtx) StateTransitionCount() int64 {
	return c.root.backend.GetExecutionInfo().GetStateTransitionCount()
}

func (c *immutableCtx) ExecutionCloseTime() time.Time {
	closeTime := c.root.backend.GetExecutionInfo().GetCloseTime()
	if closeTime == nil {
		return time.Time{}
	}
	return closeTime.AsTime()
}

func (c *immutableCtx) Logger() log.Logger {
	return c.root.logger
}

func (c *immutableCtx) structuredRef(component Component) (ComponentRef, error) {
	return c.root.structuredRef(component)
}

func (c *immutableCtx) GetNamespaceEntry() *namespace.Namespace {
	return c.root.backend.GetNamespaceEntry()
}

func (c *immutableCtx) GetContext() context.Context {
	return c.ctx
}

// NewMutableContext creates a new MutableContext from an existing Context and root Node.
//
// NOTE: Library authors should not invoke this constructor directly, and instead use the [UpdateComponent],
// [UpdateWithStartExecution], or [StartExecution] APIs.
func NewMutableContext(
	ctx context.Context,
	root *Node,
) MutableContext {
	return &mutableCtx{
		immutableCtx: newContext(ctx, root),
	}
}

func (c *mutableCtx) AddTask(
	component Component,
	attributes TaskAttributes,
	payload any,
) {
	c.root.AddTask(component, attributes, payload)
}

func (c *mutableCtx) AddHistoryEvent(
	t enumspb.EventType,
	setAttributes func(*historypb.HistoryEvent),
) *historypb.HistoryEvent {
	return c.root.backend.AddHistoryEvent(t, setAttributes)
}
