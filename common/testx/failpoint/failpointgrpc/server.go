// Package failpointgrpc serves the FailpointService gRPC API for a failpoint registry, so tests
// can configure failpoints in another process.
package failpointgrpc

import (
	"context"
	"errors"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server synchronizes through the registry, not a lock of its own: several servers can serve the
// same registry, such as the services of one process.
type server struct {
	failpointpb.UnimplementedFailpointServiceServer
	registry failpoint.Registry
}

// NewServer returns a FailpointService that configures the failpoints in registry. Without a
// registry, every request fails with FailedPrecondition.
func NewServer(registry failpoint.Registry) failpointpb.FailpointServiceServer {
	return &server{registry: registry}
}

// errNoRegistry is returned by a server without a registry.
var errNoRegistry = status.Error(codes.FailedPrecondition, "the process has no failpoint registry")

// disabler is implemented by Failpoint[T] for every T.
type disabler interface {
	Disable()
}

// requestKey returns the key named by a request. Requests address failpoints by name, so the key
// is not declared with NewKey.
func requestKey(name, scope string) failpoint.Key {
	return failpoint.KeyFromName(name, scope)
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

// configure applies configure to the error failpoint for key, creating and registering it if
// needed.
func (s *server) configure(key failpoint.Key, configure func(*failpoint.Failpoint[error])) error {
	var err error
	// Configure serializes this with the removals, also those of other servers of the registry,
	// so a failpoint is never configured after a concurrent disable removed it.
	s.registry.Configure(func() {
		fp := s.registry.GetOrSet(key, failpoint.NewFailpoint[error](key))
		f, ok := fp.(*failpoint.Failpoint[error])
		if !ok {
			err = status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
			return
		}
		configure(f)
	})
	return err
}

// remove disables the failpoint for key and removes it from the registry, serialized with
// configure. An entry that is not a failpoint is left in place.
func (s *server) remove(key failpoint.Key) error {
	var err error
	s.registry.Configure(func() {
		fp := s.registry.Get(key)
		if fp == nil {
			return
		}
		f, ok := fp.(disabler)
		if !ok {
			err = status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
			return
		}
		s.registry.CompareAndDelete(key, fp)
		f.Disable()
	})
	return err
}

func (s *server) DisableFailpoint(ctx context.Context, req *failpointpb.DisableFailpointRequest) (*failpointpb.DisableFailpointResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	key := requestKey(req.Key, req.Scope)
	// Disable is idempotent: a missing key means there's nothing to do, not an error.
	// Tests that disable in cleanup hooks would otherwise see flaky NotFound errors.
	if err := s.remove(key); err != nil {
		return nil, err
	}
	return &failpointpb.DisableFailpointResponse{}, nil
}

func (s *server) DisableAllFailpoints(ctx context.Context, req *failpointpb.DisableAllFailpointsRequest) (*failpointpb.DisableAllFailpointsResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	// Detach all entries at once, so an injection either happened before and is disabled, or
	// happens after and is kept.
	s.registry.Configure(func() {
		for _, fp := range s.registry.DeleteAll() {
			if f, ok := fp.(disabler); ok {
				f.Disable()
			}
			// Unknown type — skip Disable; the entry is dropped above.
		}
	})

	return &failpointpb.DisableAllFailpointsResponse{}, nil
}

func (s *server) InjectPanic(ctx context.Context, req *failpointpb.InjectPanicRequest) (*failpointpb.InjectPanicResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	key := requestKey(req.Key, req.Scope)
	if err := validateKey(key); err != nil {
		return nil, err
	}
	err := s.configure(key, func(f *failpoint.Failpoint[error]) {
		f.Panic(errors.New(req.PanicValue), requestRecurrence(req.Recurrence))
	})
	if err != nil {
		return nil, err
	}
	return &failpointpb.InjectPanicResponse{}, nil
}

func (s *server) InjectBlock(ctx context.Context, req *failpointpb.InjectBlockRequest) (*failpointpb.InjectBlockResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	key := requestKey(req.Key, req.Scope)
	if err := validateKey(key); err != nil {
		return nil, err
	}
	err := s.configure(key, func(f *failpoint.Failpoint[error]) {
		f.Block(requestRecurrence(req.Recurrence))
	})
	if err != nil {
		return nil, err
	}
	return &failpointpb.InjectBlockResponse{}, nil
}

func (s *server) InjectReturn(ctx context.Context, req *failpointpb.InjectReturnRequest) (*failpointpb.InjectReturnResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	key := requestKey(req.Key, req.Scope)
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
	err := s.configure(key, func(f *failpoint.Failpoint[error]) {
		f.Return(injected, requestRecurrence(req.Recurrence))
	})
	if err != nil {
		return nil, err
	}
	return &failpointpb.InjectReturnResponse{}, nil
}

func (s *server) WaitForBlock(ctx context.Context, req *failpointpb.WaitForBlockRequest) (*failpointpb.WaitForBlockResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	key := requestKey(req.Key, req.Scope)
	fp := s.registry.Get(key)
	if fp == nil {
		return nil, status.Errorf(codes.NotFound, "failpoint %s not found", key)
	}

	f, ok := fp.(*failpoint.Failpoint[error])
	if !ok {
		return nil, status.Errorf(codes.Internal, "unsupported failpoint type for key %s: %T", key, fp)
	}
	err := f.AwaitBlock(ctx)
	switch {
	case err == nil:
		return &failpointpb.WaitForBlockResponse{}, nil
	case errors.Is(err, failpoint.ErrNotBlocking):
		return nil, status.Errorf(codes.FailedPrecondition, "failpoint %s: %v", key, err)
	default:
		return nil, status.FromContextError(err).Err()
	}
}

func (s *server) DescribeRegistry(ctx context.Context, req *failpointpb.DescribeRegistryRequest) (*failpointpb.DescribeRegistryResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	return &failpointpb.DescribeRegistryResponse{RegistryId: s.registry.ID()}, nil
}

func (s *server) ListFailpoints(ctx context.Context, req *failpointpb.ListFailpointsRequest) (*failpointpb.ListFailpointsResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
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
		Key:        info.Key.Name(),
		Scope:      info.Key.Scope(),
		Enabled:    info.Enabled,
		Action:     failpointpb.Action(info.Action),
		Recurrence: failpointpb.Recurrence(info.Recurrence),
	}
}
