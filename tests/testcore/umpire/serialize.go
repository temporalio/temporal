package umpire

import (
	"context"
	"sync"

	"google.golang.org/grpc"
)

// SerializeStrategy returns a Strategy that serializes client calls sharing the
// same key, so concurrent actions on the same logical entity cannot interleave
// at the RPC boundary while unrelated entities still run in parallel. keyFn
// derives the key from the method and request; an empty key disables
// serialization for that call.
//
// This is the L2.5 layer from plans.deterministic.md — a per-(entity) mutex
// that kills cross-action races without collapsing all concurrency. It only
// has an effect once the property test issues client calls concurrently (e.g.
// parallel namespaces, goal 11); under today's sequential rapid actions it is
// a no-op, so it is intentionally left unwired until parallelism lands.
func SerializeStrategy(keyFn func(method string, req any) string) Strategy {
	var registryMu sync.Mutex
	locks := make(map[string]*sync.Mutex)
	lockFor := func(key string) *sync.Mutex {
		registryMu.Lock()
		defer registryMu.Unlock()
		l, ok := locks[key]
		if !ok {
			l = &sync.Mutex{}
			locks[key] = l
		}
		return l
	}
	return Strategy{
		Name: "serialize",
		Client: func(
			ctx context.Context,
			method string,
			req, reply any,
			cc *grpc.ClientConn,
			invoker grpc.UnaryInvoker,
			opts ...grpc.CallOption,
		) error {
			key := keyFn(method, req)
			if key == "" {
				return invoker(ctx, method, req, reply, cc, opts...)
			}
			l := lockFor(key)
			l.Lock()
			defer l.Unlock()
			return invoker(ctx, method, req, reply, cc, opts...)
		},
	}
}
