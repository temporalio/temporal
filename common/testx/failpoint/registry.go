package failpoint

import (
	"context"
)

// Registry manages failpoint instances.
// It provides thread-safe operations to register and query failpoints.
type Registry interface {
	// Get retrieves a failpoint by key. Returns nil if not found.
	Get(key Key) any

	// Set stores a failpoint instance for the given key.
	Set(key Key, fp any)

	// Delete removes a failpoint from the registry.
	Delete(key Key)

	// Keys returns the keys of all registered failpoints.
	Keys() []Key

	// Subscribe calls fn for every execution of an enabled failpoint in this registry, until the
	// returned function is called. fn must not call into the failpoint that is executing.
	Subscribe(fn func(Event)) (unsubscribe func())

	// emit reports an execution to the subscribers.
	emit(Event)
}

// Source provides the registry that Execute uses. Servers use their testhooks.TestHooks, so each
// server, and each test cluster, has its own registry.
type Source interface {
	Failpoints() Registry
}

type registryCtxKey struct{}

// InjectRegistry adds a failpoint registry to the context.
func InjectRegistry(ctx context.Context, registry Registry) context.Context {
	return context.WithValue(ctx, registryCtxKey{}, registry)
}

// GetRegistry retrieves a failpoint registry from the context.
func GetRegistry(ctx context.Context) (Registry, bool) {
	registry, ok := ctx.Value(registryCtxKey{}).(Registry)
	return registry, ok
}
