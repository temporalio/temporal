//go:build test_dep

package failpoint

import (
	"context"
	"errors"
	"sync"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/failpointservice/v1"
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

// validateKey rejects the proto zero value, which is the FailpointKey enum's
// "unknown" sentinel. Without this check, a request with no key set would
// silently create a failpoint that no ExecuteCtx call site can ever match.
func validateKey(key Key) error {
	if key == enumsspb.FAILPOINT_KEY_UNSPECIFIED {
		return status.Error(codes.InvalidArgument, "failpoint key must be set")
	}
	return nil
}

func (r *registryImpl) DisableFailpoint(ctx context.Context, req *failpointservice.DisableFailpointRequest) (*failpointservice.DisableFailpointResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fp := r.failpoints[req.Key]
	if fp == nil {
		// Disable is idempotent: a missing key means there's nothing to do, not an error.
		// Tests that disable in cleanup hooks would otherwise see flaky NotFound errors.
		return &failpointservice.DisableFailpointResponse{}, nil
	}

	switch f := fp.(type) {
	case *Failpoint[error]:
		f.Disable()
	case *Failpoint2[error, error]:
		f.Disable()
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %d: %T", req.Key, fp)
	}

	delete(r.failpoints, req.Key)
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
		case *Failpoint2[error, error]:
			f.Disable()
		default:
			// Unknown type — skip Disable; we still drop the entry below.
		}
		delete(r.failpoints, key)
	}

	return &failpointservice.DisableAllFailpointsResponse{}, nil
}

func (r *registryImpl) InjectPanic(ctx context.Context, req *failpointservice.InjectPanicRequest) (*failpointservice.InjectPanicResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	fp := r.failpoints[req.Key]
	if fp == nil {
		fp = NewFailpoint[error](req.Key)
		r.failpoints[req.Key] = fp
	}

	recurrence := req.Recurrence
	if recurrence == RecurrenceUnspecified {
		recurrence = Always
	}

	switch f := fp.(type) {
	case *Failpoint[error]:
		f.Panic(errors.New(req.PanicValue), recurrence)
	case *Failpoint2[error, error]:
		f.Panic(errors.New(req.PanicValue), recurrence)
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %d: %T", req.Key, fp)
	}

	return &failpointservice.InjectPanicResponse{}, nil
}

func (r *registryImpl) InjectBlock(ctx context.Context, req *failpointservice.InjectBlockRequest) (*failpointservice.InjectBlockResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	fp := r.failpoints[req.Key]
	if fp == nil {
		fp = NewFailpoint[error](req.Key)
		r.failpoints[req.Key] = fp
	}

	recurrence := req.Recurrence
	if recurrence == RecurrenceUnspecified {
		recurrence = Always
	}

	switch f := fp.(type) {
	case *Failpoint[error]:
		f.Block(recurrence)
	case *Failpoint2[error, error]:
		f.Block(recurrence)
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %d: %T", req.Key, fp)
	}

	return &failpointservice.InjectBlockResponse{}, nil
}

func (r *registryImpl) InjectReturn(ctx context.Context, req *failpointservice.InjectReturnRequest) (*failpointservice.InjectReturnResponse, error) {
	if err := validateKey(req.Key); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	fp := r.failpoints[req.Key]
	if fp == nil {
		fp = NewFailpoint[error](req.Key)
		r.failpoints[req.Key] = fp
	}

	recurrence := req.Recurrence
	if recurrence == RecurrenceUnspecified {
		recurrence = Always
	}

	switch f := fp.(type) {
	case *Failpoint[error]:
		f.Return(errors.New(req.ErrorMessage), recurrence)
	case *Failpoint2[error, error]:
		f.Return(errors.New(req.ErrorMessage), errors.New(req.ErrorMessage), recurrence)
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %d: %T", req.Key, fp)
	}

	return &failpointservice.InjectReturnResponse{}, nil
}

func (r *registryImpl) WaitForBlock(ctx context.Context, req *failpointservice.WaitForBlockRequest) (*failpointservice.WaitForBlockResponse, error) {
	r.mu.RLock()
	fp := r.failpoints[req.Key]
	r.mu.RUnlock()

	if fp == nil {
		return nil, status.Errorf(codes.NotFound, "failpoint %d not found", req.Key)
	}

	var waitCh chan struct{}
	switch f := fp.(type) {
	case *Failpoint[error]:
		waitCh = f.WaitCh()
	case *Failpoint2[error, error]:
		waitCh = f.WaitCh()
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %d: %T", req.Key, fp)
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

// infoProvider is implemented by Failpoint[T] and Failpoint2[T1, T2].
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
		Key:    key,
		Action: Off,
	}
}
