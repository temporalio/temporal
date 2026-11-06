package workflow

// HasCachedState reports whether the context retains reusable execution state.
// The caller must hold the workflow lock.
func (c *ContextImpl) HasCachedState() bool {
	return c.MutableState != nil || c.updateRegistry != nil || c.taskCompletionBuffer != nil
}
