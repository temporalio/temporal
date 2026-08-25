package faultinjection

import (
	"context"
	"sync"
	"sync/atomic"
)

type rpcFaultStage int

const (
	rpcFaultStageRequest rpcFaultStage = iota
	rpcFaultStageResponse
)

// RPCRequestCallback is a callback function for pre-handler RPC fault injection.
type RPCRequestCallback func(ctx context.Context, fullMethod string, req any) (matched bool, newResp any, newErr error)

// RPCResponseCallback is a callback function for post-handler RPC fault injection.
type RPCResponseCallback func(ctx context.Context, fullMethod string, req, resp any, err error) (matched bool, newResp any, newErr error)

type rpcCallback func(ctx context.Context, fullMethod string, stage rpcFaultStage, req, resp any, err error) (matched bool, newResp any, newErr error)

// rpcCallbackEntry represents a registered RPC callback with its ID.
type rpcCallbackEntry struct {
	id       uint64
	callback rpcCallback
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

// RegisterRequestCallback registers a pre-handler RPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *RPCFaultGenerator) RegisterRequestCallback(cb RPCRequestCallback) func() {
	return r.registerCallback(func(ctx context.Context, fullMethod string, stage rpcFaultStage, req, _ any, _ error) (bool, any, error) {
		if stage != rpcFaultStageRequest {
			return false, nil, nil
		}
		return cb(ctx, fullMethod, req)
	})
}

// RegisterResponseCallback registers a post-handler RPC fault injection callback and returns a
// cleanup function that removes the callback when called.
func (r *RPCFaultGenerator) RegisterResponseCallback(cb RPCResponseCallback) func() {
	return r.registerCallback(func(ctx context.Context, fullMethod string, stage rpcFaultStage, req, resp any, err error) (bool, any, error) {
		if stage != rpcFaultStageResponse {
			return false, nil, nil
		}
		return cb(ctx, fullMethod, req, resp, err)
	})
}

func (r *RPCFaultGenerator) registerCallback(cb rpcCallback) func() {
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

// GenerateRequest checks all registered RPC callbacks before the handler runs.
// Returns (true, resp, err) if a callback matched, or (false, nil, nil) if no callbacks matched.
func (r *RPCFaultGenerator) GenerateRequest(ctx context.Context, fullMethod string, req any) (bool, any, error) {
	return r.generate(ctx, fullMethod, rpcFaultStageRequest, req, nil, nil)
}

// GenerateResponse checks all registered RPC callbacks after the handler runs.
// Returns (true, resp, err) if a callback matched, or (false, nil, nil) if no callbacks matched.
func (r *RPCFaultGenerator) GenerateResponse(ctx context.Context, fullMethod string, req, resp any, err error) (bool, any, error) {
	return r.generate(ctx, fullMethod, rpcFaultStageResponse, req, resp, err)
}

func (r *RPCFaultGenerator) generate(ctx context.Context, fullMethod string, stage rpcFaultStage, req, resp any, err error) (bool, any, error) {
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
		if matched, newResp, newErr := entry.callback(ctx, fullMethod, stage, req, resp, err); matched {
			return true, newResp, newErr
		}
	}
	return false, nil, nil
}
