package faultinjection

import (
	"context"
	"sync"
	"sync/atomic"
)

// RPCCallback is a callback function for RPC fault injection.
// It receives context, method name, request, and response (nil for pre-handler calls).
//
// Parameters:
//   - ctx: the request context
//   - fullMethod: the full gRPC method name
//   - req: the request proto message
//   - resp: the response proto message (nil when called before handler)
//   - err: the error from handler (nil when called before handler)
//
// Returns:
//   - bool: if true, the callback matched and the returned values should be used.
//     If false, the callback did not match and other callbacks should be checked.
//   - any: replacement response (only used if matched is true)
//   - error: replacement error (only used if matched is true)
type RPCCallback func(ctx context.Context, fullMethod string, req, resp any, err error) (matched bool, newResp any, newErr error)

// rpcCallbackEntry represents a registered RPC callback with its ID.
type rpcCallbackEntry struct {
	id       uint64
	callback RPCCallback
}

// RPCFaultGenerator handles fault injection for RPC requests and responses.
type RPCFaultGenerator struct {
	mu        sync.RWMutex
	callbacks []rpcCallbackEntry
	nextID    atomic.Uint64
}

// NewRPCFaultGenerator creates a new RPCFaultGenerator instance.
func NewRPCFaultGenerator() *RPCFaultGenerator {
	return &RPCFaultGenerator{
		callbacks: make([]rpcCallbackEntry, 0),
	}
}

// RegisterCallback registers an RPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *RPCFaultGenerator) RegisterCallback(cb RPCCallback) func() {
	if r == nil {
		return func() {}
	}
	id := r.nextID.Add(1)
	entry := rpcCallbackEntry{id: id, callback: cb}

	r.mu.Lock()
	r.callbacks = append(r.callbacks, entry)
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		for i, e := range r.callbacks {
			if e.id == id {
				r.callbacks = append(r.callbacks[:i], r.callbacks[i+1:]...)
				return
			}
		}
	}
}

// Generate checks all registered RPC callbacks for the given request/response.
// Returns (true, resp, err) if a callback matched, or (false, nil, nil) if no callbacks matched.
func (r *RPCFaultGenerator) Generate(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
	if r == nil {
		return false, nil, nil
	}
	r.mu.RLock()
	numCallbacks := len(r.callbacks)
	if numCallbacks == 0 {
		r.mu.RUnlock()
		return false, nil, nil
	}
	callbacks := make([]rpcCallbackEntry, numCallbacks)
	copy(callbacks, r.callbacks)
	r.mu.RUnlock()

	for _, entry := range callbacks {
		if matched, newResp, newErr := entry.callback(ctx, fullMethod, req, resp, err); matched {
			return true, newResp, newErr
		}
	}
	return false, nil, nil
}
