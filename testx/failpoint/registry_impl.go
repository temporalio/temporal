//go:build test_dep

package failpoint

import (
	"context"
	"errors"
	"sync"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/failpointservice/v1"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const RecurrenceUnspecified = enumsspb.FAILPOINT_RECURRENCE_UNSPECIFIED

type registryImpl struct {
	failpointservice.UnimplementedFailpointServiceServer
	mu         sync.RWMutex
	failpoints map[Key]any
}

// NewRegistry creates a new failpoint registry for testing.
func NewRegistry() Registry {
	return &registryImpl{
		failpoints: make(map[Key]any),
	}
}

func (r *registryImpl) Get(key Key) any {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.failpoints[key]
}

func (r *registryImpl) Set(key Key, fp any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failpoints[key] = fp
}

func (r *registryImpl) Delete(key Key) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failpoints, key)
}

// gRPC service implementation

// requestKey returns the key named by a request. Requests address failpoints by name, so the key
// is not declared with NewKey.
func requestKey(name, namespaceID string) Key {
	return Key{name: name, namespaceID: namespace.ID(namespaceID)}
}

// validateKey rejects an empty key name. Without this check, a request with no key set would
// silently create a failpoint that no ExecuteCtx call site can ever match.
func validateKey(key Key) error {
	if key.name == "" {
		return status.Error(codes.InvalidArgument, "failpoint key must be set")
	}
	return nil
}

func (r *registryImpl) DisableFailpoint(ctx context.Context, req *failpointservice.DisableFailpointRequest) (*failpointservice.DisableFailpointResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	r.mu.Lock()
	defer r.mu.Unlock()

	fp := r.failpoints[key]
	if fp == nil {
		// Disable is idempotent: a missing key means there's nothing to do, not an error.
		// Tests that disable in cleanup hooks would otherwise see flaky NotFound errors.
		return &failpointservice.DisableFailpointResponse{}, nil
	}

	switch f := fp.(type) {
	case *Failpoint[error]:
		f.Disable()
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}

	delete(r.failpoints, key)
	return &failpointservice.DisableFailpointResponse{}, nil
}

func (r *registryImpl) DisableAllFailpoints(ctx context.Context, req *failpointservice.DisableAllFailpointsRequest) (*failpointservice.DisableAllFailpointsResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Snapshot keys before deleting to avoid relying on Go's delete-during-range
	// behavior — well-defined but easy to get wrong if the loop body ever grows.
	keys := make([]Key, 0, len(r.failpoints))
	for key := range r.failpoints {
		keys = append(keys, key)
	}
	for _, key := range keys {
		switch f := r.failpoints[key].(type) {
		case *Failpoint[error]:
			f.Disable()
		default:
			// Unknown type — skip Disable; we still drop the entry below.
		}
		delete(r.failpoints, key)
	}

	return &failpointservice.DisableAllFailpointsResponse{}, nil
}

func (r *registryImpl) InjectPanic(ctx context.Context, req *failpointservice.InjectPanicRequest) (*failpointservice.InjectPanicResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	if err := validateKey(key); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	fp := r.failpoints[key]
	if fp == nil {
		fp = NewFailpoint[error](key)
		r.failpoints[key] = fp
	}

	recurrence := req.Recurrence
	if recurrence == RecurrenceUnspecified {
		recurrence = Always
	}

	switch f := fp.(type) {
	case *Failpoint[error]:
		f.Panic(errors.New(req.PanicValue), recurrence)
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}

	return &failpointservice.InjectPanicResponse{}, nil
}

func (r *registryImpl) InjectBlock(ctx context.Context, req *failpointservice.InjectBlockRequest) (*failpointservice.InjectBlockResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	if err := validateKey(key); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	fp := r.failpoints[key]
	if fp == nil {
		fp = NewFailpoint[error](key)
		r.failpoints[key] = fp
	}

	recurrence := req.Recurrence
	if recurrence == RecurrenceUnspecified {
		recurrence = Always
	}

	switch f := fp.(type) {
	case *Failpoint[error]:
		f.Block(recurrence)
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}

	return &failpointservice.InjectBlockResponse{}, nil
}

func (r *registryImpl) InjectReturn(ctx context.Context, req *failpointservice.InjectReturnRequest) (*failpointservice.InjectReturnResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	if err := validateKey(key); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	fp := r.failpoints[key]
	if fp == nil {
		fp = NewFailpoint[error](key)
		r.failpoints[key] = fp
	}

	recurrence := req.Recurrence
	if recurrence == RecurrenceUnspecified {
		recurrence = Always
	}

	switch f := fp.(type) {
	case *Failpoint[error]:
		f.Return(errors.New(req.ErrorMessage), recurrence)
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}

	return &failpointservice.InjectReturnResponse{}, nil
}

func (r *registryImpl) WaitForBlock(ctx context.Context, req *failpointservice.WaitForBlockRequest) (*failpointservice.WaitForBlockResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	r.mu.RLock()
	fp := r.failpoints[key]
	r.mu.RUnlock()

	if fp == nil {
		return nil, status.Errorf(codes.NotFound, "failpoint %s not found", key)
	}

	var waitCh chan struct{}
	switch f := fp.(type) {
	case *Failpoint[error]:
		waitCh = f.WaitCh()
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}

	select {
	case <-waitCh:
		return &failpointservice.WaitForBlockResponse{}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (r *registryImpl) ListFailpoints(ctx context.Context, req *failpointservice.ListFailpointsRequest) (*failpointservice.ListFailpointsResponse, error) {
	r.mu.RLock()
	keys := make([]Key, 0, len(r.failpoints))
	for k := range r.failpoints {
		keys = append(keys, k)
	}
	r.mu.RUnlock()

	failpoints := make([]*failpointservice.FailpointInfo, 0, len(keys))
	for _, key := range keys {
		if info := r.getInfo(key); info != nil {
			failpoints = append(failpoints, info)
		}
	}

	return &failpointservice.ListFailpointsResponse{
		Failpoints: failpoints,
	}, nil
}

// infoProvider is implemented by Failpoint[T].
type infoProvider interface {
	Info() *failpointservice.FailpointInfo
}

func (r *registryImpl) getInfo(key Key) *failpointservice.FailpointInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	fp := r.failpoints[key]
	if fp == nil {
		return nil
	}

	if ip, ok := fp.(infoProvider); ok {
		return ip.Info()
	}
	return &failpointservice.FailpointInfo{
		Key:         key.name,
		NamespaceId: key.namespaceID.String(),
		Action:      Off,
	}
}
