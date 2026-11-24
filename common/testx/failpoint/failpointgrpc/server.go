//go:build test_dep

// Package failpointgrpc serves the FailpointService gRPC API for a failpoint registry, so tests
// can configure failpoints in another process.
package failpointgrpc

import (
	"context"
	"errors"
	"sync"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	failpointpb.UnimplementedFailpointServiceServer
	registry failpoint.Registry
	// mu makes the lookup and update of the registry within one request atomic.
	mu sync.Mutex
}

// NewServer returns a FailpointService that configures the failpoints in registry.
func NewServer(registry failpoint.Registry) failpointpb.FailpointServiceServer {
	return &server{registry: registry}
}

// requestKey returns the key named by a request. Requests address failpoints by name, so the key
// is not declared with NewKey.
func requestKey(name, namespaceID string) failpoint.Key {
	return failpoint.KeyFromName(name, namespaceID)
}

// validateKey rejects an empty key name. Without this check, a request with no key set would
// silently create a failpoint that no ExecuteCtx call site can ever match.
func validateKey(key failpoint.Key) error {
	if key.Name() == "" {
		return status.Error(codes.InvalidArgument, "failpoint key must be set")
	}
	return nil
}

func requestRecurrence(recurrence failpointpb.Recurrence) failpoint.Recurrence {
	if recurrence == failpointpb.Recurrence_RECURRENCE_UNSPECIFIED {
		return failpoint.Always
	}
	return failpoint.Recurrence(recurrence)
}

// errorFailpoint returns the error failpoint for key, creating and registering it if needed.
// The caller must hold s.mu.
func (s *server) errorFailpoint(key failpoint.Key) (*failpoint.Failpoint[error], error) {
	fp := s.registry.Get(key)
	if fp == nil {
		f := failpoint.NewFailpoint[error](key)
		s.registry.Set(key, f)
		return f, nil
	}
	f, ok := fp.(*failpoint.Failpoint[error])
	if !ok {
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}
	return f, nil
}

func (s *server) DisableFailpoint(ctx context.Context, req *failpointpb.DisableFailpointRequest) (*failpointpb.DisableFailpointResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	s.mu.Lock()
	defer s.mu.Unlock()

	fp := s.registry.Get(key)
	if fp == nil {
		// Disable is idempotent: a missing key means there's nothing to do, not an error.
		// Tests that disable in cleanup hooks would otherwise see flaky NotFound errors.
		return &failpointpb.DisableFailpointResponse{}, nil
	}

	switch f := fp.(type) {
	case *failpoint.Failpoint[error]:
		f.Disable()
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}

	s.registry.Delete(key)
	return &failpointpb.DisableFailpointResponse{}, nil
}

func (s *server) DisableAllFailpoints(ctx context.Context, req *failpointpb.DisableAllFailpointsRequest) (*failpointpb.DisableAllFailpointsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, key := range s.registry.Keys() {
		switch f := s.registry.Get(key).(type) {
		case *failpoint.Failpoint[error]:
			f.Disable()
		default:
			// Unknown type — skip Disable; we still drop the entry below.
		}
		s.registry.Delete(key)
	}

	return &failpointpb.DisableAllFailpointsResponse{}, nil
}

func (s *server) InjectPanic(ctx context.Context, req *failpointpb.InjectPanicRequest) (*failpointpb.InjectPanicResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	if err := validateKey(key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.errorFailpoint(key)
	if err != nil {
		return nil, err
	}
	f.Panic(errors.New(req.PanicValue), requestRecurrence(req.Recurrence))
	return &failpointpb.InjectPanicResponse{}, nil
}

func (s *server) InjectBlock(ctx context.Context, req *failpointpb.InjectBlockRequest) (*failpointpb.InjectBlockResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	if err := validateKey(key); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.errorFailpoint(key)
	if err != nil {
		return nil, err
	}
	f.Block(requestRecurrence(req.Recurrence))
	return &failpointpb.InjectBlockResponse{}, nil
}

func (s *server) InjectReturn(ctx context.Context, req *failpointpb.InjectReturnRequest) (*failpointpb.InjectReturnResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	if err := validateKey(key); err != nil {
		return nil, err
	}
	injected := errors.New(req.ErrorMessage)
	if req.ErrorName != "" {
		newError, ok := failpoint.LookupError(req.ErrorName)
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "unknown failpoint error %q", req.ErrorName)
		}
		injected = newError(req.ErrorMessage)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.errorFailpoint(key)
	if err != nil {
		return nil, err
	}
	f.Return(injected, requestRecurrence(req.Recurrence))
	return &failpointpb.InjectReturnResponse{}, nil
}

func (s *server) WaitForBlock(ctx context.Context, req *failpointpb.WaitForBlockRequest) (*failpointpb.WaitForBlockResponse, error) {
	key := requestKey(req.Key, req.NamespaceId)
	s.mu.Lock()
	fp := s.registry.Get(key)
	s.mu.Unlock()

	if fp == nil {
		return nil, status.Errorf(codes.NotFound, "failpoint %s not found", key)
	}

	var waitCh chan struct{}
	switch f := fp.(type) {
	case *failpoint.Failpoint[error]:
		waitCh = f.WaitCh()
	default:
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}

	select {
	case <-waitCh:
		return &failpointpb.WaitForBlockResponse{}, nil
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (s *server) ListFailpoints(ctx context.Context, req *failpointpb.ListFailpointsRequest) (*failpointpb.ListFailpointsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := s.registry.Keys()
	failpoints := make([]*failpointpb.FailpointInfo, 0, len(keys))
	for _, key := range keys {
		fp := s.registry.Get(key)
		if fp == nil {
			continue
		}
		failpoints = append(failpoints, infoProto(key, fp))
	}

	return &failpointpb.ListFailpointsResponse{
		Failpoints: failpoints,
	}, nil
}

// infoProvider is implemented by Failpoint[T].
type infoProvider interface {
	Info() failpoint.Info
}

func infoProto(key failpoint.Key, fp any) *failpointpb.FailpointInfo {
	info := failpoint.Info{Key: key, Action: failpoint.Off}
	if ip, ok := fp.(infoProvider); ok {
		info = ip.Info()
	}
	return &failpointpb.FailpointInfo{
		Key:         info.Key.Name(),
		NamespaceId: info.Key.NamespaceID(),
		Enabled:     info.Enabled,
		Action:      failpointpb.Action(info.Action),
		Recurrence:  failpointpb.Recurrence(info.Recurrence),
	}
}
