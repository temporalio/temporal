package chasm

import "context"

// SideEffectTaskHandlerBase provides a default Discard implementation that returns ErrTaskDiscarded.
// Embed this in side-effect task handler structs to satisfy the SideEffectTaskHandler interface.
type SideEffectTaskHandlerBase[T any] struct{}

func (SideEffectTaskHandlerBase[T]) Discard(_ context.Context, _ ComponentRef, _ TaskAttributes, _ T) error {
	return ErrTaskDiscarded
}

func (SideEffectTaskHandlerBase[T]) TaskGroup() string { return "" }

func (SideEffectTaskHandlerBase[T]) sideEffectTaskHandler() {}

// PureTaskHandlerBase must be embedded in all pure task handler implementations.
type PureTaskHandlerBase struct{}

func (PureTaskHandlerBase) pureTaskHandler() {}
