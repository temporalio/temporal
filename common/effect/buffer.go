package effect

import "context"

// Buffer holds a set of effect and rollback functions that can be invoked as a
// batch with a defined order. Once either Apply or Cancel is called, all
// buffered effects are cleared. This type is not threadsafe. The zero-value of
// a Buffer is a valid state.
type Buffer struct {
	effects []func(context.Context)
	cancels []func(context.Context)
}

// OnAfterCommit adds the provided effect function to set of such functions to
// be invoked when Buffer.Apply is called.
func (b *Buffer) OnAfterCommit(effect func(context.Context)) {
	b.effects = append(b.effects, effect)
}

// OnAfterRollback adds the provided effect function to set of such functions to
// be invoked when Buffer.Cancel is called.
func (b *Buffer) OnAfterRollback(effect func(context.Context)) {
	b.cancels = append(b.cancels, effect)
}

// Apply invokes the buffered effect functions in the order that they were added
// to this Buffer.
// It returns true if any effects were applied.
func (b *Buffer) Apply(ctx context.Context) bool {
	applied := false
	b.cancels = nil
	for _, effect := range b.effects {
		effect(ctx)
		applied = true
	}
	b.effects = nil
	return applied
}

// Cancel invokes the buffered rollback functions in the reverse of the order
// that they were added to this Buffer.
// It returns true if any effects were canceled.
func (b *Buffer) Cancel(ctx context.Context) bool {
	canceled := false
	b.effects = nil
	for i := len(b.cancels) - 1; i >= 0; i-- {
		b.cancels[i](ctx)
		canceled = true
	}
	b.cancels = nil
	return canceled
}
