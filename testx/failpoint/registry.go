package failpoint

import (
	"context"

	"go.temporal.io/server/api/failpointservice/v1"
)

// Registry manages failpoint instances for remote configuration.
// It provides thread-safe operations to register, configure, and query failpoints.
// Registry embeds the gRPC service interface and adds low-level access methods.
type Registry interface {
	failpointservice.FailpointServiceServer

	// Get retrieves a failpoint by key. Returns nil if not found.
	Get(key Key) any

	// Set stores a failpoint instance for the given key.
	Set(key Key, fp any)

	// Delete removes a failpoint from the registry.
	Delete(key Key)
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
