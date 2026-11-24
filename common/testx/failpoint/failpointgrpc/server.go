// Package failpointgrpc serves the FailpointService gRPC API for a failpoint registry, so tests
// can configure failpoints in another process.
package failpointgrpc

import (
	"context"
	"errors"
	"slices"
	"strings"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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

// owned is implemented by Failpoint[T] for every T.
type owned interface {
	Owner() string
}

// checkNotOwned rejects a change to a failpoint that other code manages, such as a rule set, so
// the two don't undo each other's changes.
func checkNotOwned(key failpoint.Key, fp any) error {
	if o, ok := fp.(owned); ok && o.Owner() != "" {
		return status.Errorf(codes.FailedPrecondition, "failpoint %s is managed by %s", key, o.Owner())
	}
	return nil
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
		if err = checkNotOwned(key, fp); err != nil {
			return
		}
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
		if err = checkNotOwned(key, fp); err != nil {
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
	// happens after and is kept. Failpoints that other code manages, such as a rule set, are kept.
	s.registry.Configure(func() {
		removed := s.registry.DeleteFunc(func(key failpoint.Key, fp any) bool {
			return checkNotOwned(key, fp) != nil
		})
		for _, fp := range removed {
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

	stats := s.registry.BuggifyStats()
	buggifyKeys := make([]*failpointpb.BuggifyKeyInfo, 0, len(stats))
	for _, st := range stats {
		buggifyKeys = append(buggifyKeys, &failpointpb.BuggifyKeyInfo{
			Key:       protoString(st.Name),
			Activated: st.Activated,
			Hits:      st.Hits,
			Fires:     st.Fires,
		})
	}
	resp := &failpointpb.ListFailpointsResponse{Failpoints: failpoints}
	resp.BuggifyKeys, resp.BuggifyKeysOmitted = limitBuggifyKeys(buggifyKeys, maxListFailpointsSize-proto.Size(resp)-11)
	return resp, nil
}

// maxListFailpointsSize bounds the encoded size of a ListFailpoints response with BUGGIFY counters,
// well below gRPC's default limit of 4 MiB, since key names, such as those of HTTP operations,
// contain text chosen by callers. The counters only take the room that the registered failpoints
// leave, so they can't make a response too large that would fit without them.
const maxListFailpointsSize = 2 << 20

// limitBuggifyKeys sorts keys, activated ones first and then by name, and keeps those that fit in
// budget bytes. It returns them and the number it left out.
func limitBuggifyKeys(keys []*failpointpb.BuggifyKeyInfo, budget int) ([]*failpointpb.BuggifyKeyInfo, int64) {
	slices.SortStableFunc(keys, func(a, b *failpointpb.BuggifyKeyInfo) int {
		if a.Activated != b.Activated {
			if a.Activated {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Key, b.Key)
	})
	size := 0
	for i, key := range keys {
		// Each entry of a repeated field also takes a tag and a length of up to protowire.SizeVarint
		// bytes; 1 + 10 is an upper bound.
		size += proto.Size(key) + 11
		if size > budget {
			return keys[:i], int64(len(keys) - i)
		}
	}
	return keys, 0
}

// protoString returns s with invalid UTF-8 replaced, since proto strings must be valid UTF-8 and
// key names can contain bytes chosen by callers, such as an escaped HTTP path.
func protoString(s string) string {
	return strings.ToValidUTF8(s, "\uFFFD")
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
		Key:        protoString(info.Key.Name()),
		Scope:      protoString(info.Key.Scope()),
		Enabled:    info.Enabled,
		Action:     failpointpb.Action(info.Action),
		Recurrence: failpointpb.Recurrence(info.Recurrence),
	}
}
