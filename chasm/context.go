//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination context_mock.go

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
	Ref(Component) ([]byte, error)
	Now(Component) time.Time

	// Intent() OperationIntent
	// ComponentOptions(Component) []ComponentOption

	getContext() context.Context
}

type MutableContext interface {
	Context

	AddTask(Component, TaskAttributes, any)

	// Add more methods here for other storage commands/primitives.
	// e.g. HistoryEvent

	// Get a Ref for the component
	// This ref to the component state at the end of the transition
	// Same as Ref(Component) method in Context,
	// this only works for components that already exists at the start of the transition
	//
	// If we provide this method, then the method on the engine doesn't need to
	// return a Ref
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
	node *Node,
) *ContextImpl {
	return &ContextImpl{
		ctx:  ctx,
		root: node.root(),
	}
}

func (c *ContextImpl) Ref(component Component) ([]byte, error) {
	return c.root.Ref(component)
}

func (c *ContextImpl) componentNodePath(component Component) ([]string, error) {
	return c.root.componentNodePath(component)
}

func (c *ContextImpl) dataNodePath(data proto.Message) ([]string, error) {
	return c.root.dataNodePath(data)
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
) {
	c.root.AddTask(component, attributes, payload)
}
