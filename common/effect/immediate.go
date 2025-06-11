package effect

import "context"

type immediate struct{ context.Context }

// Immediate returns an effects.Controller that executes effects immediately upon
// insertion. Useful in contexts where you don't actually want to delay effect
// application and in tests.
func Immediate(ctx context.Context) Controller { return immediate{ctx} }

func (i immediate) OnAfterCommit(effect func(context.Context)) { effect(i) }
func (i immediate) OnAfterRollback(func(context.Context))      {}
