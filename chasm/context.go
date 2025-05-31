package chasm

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
)

type Context interface {
	// Context is not bound to any component,
	// so all methods needs to take in component as a parameter

	// NOTE: component created in the current transaction won't have a ref
	// this is a Ref to the component state at the start of the transition
	RefC(Component) (ComponentRef, bool)
	RefD(proto.Message) (ComponentRef, bool)
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

	// Get a RefC for the component
	// This ref to the component state at the end of the transition
	// Same as RefC(Component) method in Context,
	// this only works for components that already exists at the start of the transition
	//
	// If we provide this method, then the method on the engine doesn't need to
	// return a RefC
	// NewRef(Component) (ComponentRef, bool)
}

type ContextImpl struct {
	ctx context.Context

	// Not embedding the Node here to avoid exposing AddTask() method on Node,
	// so that ContextImpl won't implement MutableContext interface.
	root *Node
}

type MutableContextImpl struct {
	*ContextImpl
}

func NewContext(
	ctx context.Context,
	root *Node,
) *ContextImpl {
	return &ContextImpl{
		ctx:  ctx,
		root: root,
	}
}

func (c *ContextImpl) RefC(component Component) (ComponentRef, bool) {
	return c.root.RefC(component)
}

func (c *ContextImpl) RefD(data proto.Message) (ComponentRef, bool) {
	return c.root.RefD(data)
}

func (c *ContextImpl) Now(component Component) time.Time {
	return c.root.Now(component)
}

func (c *ContextImpl) getContext() context.Context {
	return c.ctx
}

func NewMutableContext(
	ctx context.Context,
	root *Node,
) *MutableContextImpl {
	return &MutableContextImpl{
		ContextImpl: NewContext(ctx, root),
	}
}

func (c *MutableContextImpl) AddTask(
	component Component,
	attributes TaskAttributes,
	payload any,
) error {
	return c.root.AddTask(component, attributes, payload)
}
