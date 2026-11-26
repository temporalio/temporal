// Package failpointgrpc serves the FailpointService gRPC API for a failpoint registry, so tests
// can configure failpoints in another process.
package failpointgrpc

import (
	"context"
	"errors"
	"strings"

	"go.temporal.io/testx/failpoint"
	"go.temporal.io/testx/failpoint/buggify"
	"go.temporal.io/testx/failpoint/failpointpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// server synchronizes through the registry, not a lock of its own: several servers can serve the
// same registry, such as the services of one process.
type server struct {
	failpointpb.UnimplementedFailpointServiceServer
	registry *failpoint.Registry
	buggify  *buggify.Buggify
	// listLimit is the size in bytes that the response of ListFailpoints is kept within.
	listLimit int
}

// ServerOption configures NewServer.
type ServerOption func(*server)

// WithBuggify reports the execution counters of b in ListFailpoints and starts counting executions
// on b immediately.
func WithBuggify(b *buggify.Buggify) ServerOption {
	return func(s *server) { s.buggify = b }
}

// NewServer returns a FailpointService that configures the failpoints in registry. Without a
// registry, every request fails with FailedPrecondition.
//
// Pass WithBuggify to report BUGGIFY execution counters in ListFailpoints.
func NewServer(registry *failpoint.Registry, opts ...ServerOption) failpointpb.FailpointServiceServer {
	s := &server{registry: registry, listLimit: defaultListLimit}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.buggify != nil {
		s.buggify.CountExecutions()
	}
	return s
}

// errNoRegistry is returned by a server without a registry.
var errNoRegistry = status.Error(codes.FailedPrecondition, "the process has no failpoint registry")

// checkNotManaged rejects a change to a failpoint that a rule set manages, so the two don't undo
// each other's changes.
func checkNotManaged(fp interface{ Info() failpoint.Info }) error {
	if info := fp.Info(); info.Managed {
		return status.Errorf(codes.FailedPrecondition, "failpoint %s is managed by failpoint rules", info.Key)
	}
	return nil
}

// requestKey returns the key named by a request. Requests address the failpoints of error keys,
// which code declared with NewKey or under a reserved prefix.
func requestKey(name, scope string) (failpoint.Key[error], error) {
	if name == "" {
		return failpoint.Key[error]{}, status.Error(codes.InvalidArgument, "failpoint key must be set")
	}
	key, err := failpoint.ErrorKey(name, scope)
	if err != nil {
		return failpoint.Key[error]{}, status.Error(codes.InvalidArgument, err.Error())
	}
	return key, nil
}

// requestTrigger returns the trigger that the recurrence of a request stands for: once fires once,
// Trigger{MaxFires: 1}, and always has no limit. The protocol has no other triggers.
func requestTrigger(recurrence failpointpb.Recurrence) (failpoint.Trigger, error) {
	switch recurrence {
	case failpointpb.Recurrence_RECURRENCE_UNSPECIFIED, failpointpb.Recurrence_RECURRENCE_ALWAYS:
		return failpoint.Trigger{}, nil
	case failpointpb.Recurrence_RECURRENCE_ONCE:
		return failpoint.Trigger{MaxFires: 1}, nil
	default:
		return failpoint.Trigger{}, status.Errorf(codes.InvalidArgument, "unknown recurrence %d", recurrence)
	}
}

// recurrenceOf returns the recurrence of the protocol that stands for trigger, see requestTrigger.
// A trigger that is neither unlimited nor Trigger{MaxFires: 1} has none.
func recurrenceOf(trigger failpoint.Trigger) (failpointpb.Recurrence, bool) {
	switch trigger {
	case failpoint.Trigger{}:
		return failpointpb.Recurrence_RECURRENCE_ALWAYS, true
	case failpoint.Trigger{MaxFires: 1}:
		return failpointpb.Recurrence_RECURRENCE_ONCE, true
	default:
		return failpointpb.Recurrence_RECURRENCE_UNSPECIFIED, false
	}
}

// configure applies configure to the error failpoint for key, creating and registering it if
// needed. The action setters replace the trigger and the counts of a failpoint that exists, so
// the next execution fires what the request says.
func (s *server) configure(key failpoint.Key[error], configure func(*failpoint.Failpoint[error])) error {
	var err error
	// Atomically serializes this with the removals, also those of other servers of the registry,
	// so a failpoint is never configured after a concurrent disable removed it.
	s.registry.Atomically(func() {
		fp := s.registry.GetOrSet(failpoint.NewFailpoint(key))
		if err = checkNotManaged(fp); err != nil {
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
// configure.
func (s *server) remove(key failpoint.Key[error]) error {
	var err error
	s.registry.Atomically(func() {
		fp := s.registry.Get(key)
		if fp == nil {
			return
		}
		if err = checkNotManaged(fp); err != nil {
			return
		}
		s.registry.CompareAndDelete(fp)
		fp.Disable()
	})
	return err
}

func (s *server) DisableFailpoint(ctx context.Context, req *failpointpb.DisableFailpointRequest) (*failpointpb.DisableFailpointResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	key, err := requestKey(req.Name, req.Scope)
	if err != nil {
		return nil, err
	}
	// A missing key is not an error.
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
	s.registry.Atomically(func() {
		s.registry.DeleteFunc(func(info failpoint.Info) bool { return !info.Managed })
	})

	return &failpointpb.DisableAllFailpointsResponse{}, nil
}

func (s *server) InjectPanic(ctx context.Context, req *failpointpb.InjectPanicRequest) (*failpointpb.InjectPanicResponse, error) {
	if s.registry == nil {
		return nil, errNoRegistry
	}
	key, err := requestKey(req.Name, req.Scope)
	if err != nil {
		return nil, err
	}
	trigger, err := requestTrigger(req.Recurrence)
	if err != nil {
		return nil, err
	}
	err = s.configure(key, func(f *failpoint.Failpoint[error]) {
		f.Panic(errors.New(req.PanicValue), trigger)
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
	key, err := requestKey(req.Name, req.Scope)
	if err != nil {
		return nil, err
	}
	trigger, err := requestTrigger(req.Recurrence)
	if err != nil {
		return nil, err
	}
	err = s.configure(key, func(f *failpoint.Failpoint[error]) {
		f.Block(trigger)
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
	key, err := requestKey(req.Name, req.Scope)
	if err != nil {
		return nil, err
	}
	trigger, err := requestTrigger(req.Recurrence)
	if err != nil {
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
	err = s.configure(key, func(f *failpoint.Failpoint[error]) {
		f.Return(injected, trigger)
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
	key, err := requestKey(req.Name, req.Scope)
	if err != nil {
		return nil, err
	}
	fp := s.registry.Get(key)
	if fp == nil {
		return nil, status.Errorf(codes.NotFound, "failpoint %s not found", key)
	}
	err = fp.Wait(ctx)
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
	infos := s.registry.Infos()
	failpoints := make([]*failpointpb.FailpointInfo, 0, len(infos))
	for _, info := range infos {
		failpoints = append(failpoints, infoProto(info))
	}

	var stats []buggify.NameStats
	if s.buggify != nil {
		stats = s.buggify.Stats()
	}
	fallbackKeys := make([]*failpointpb.FallbackKeyInfo, 0, len(stats))
	for _, st := range stats {
		fallbackKeys = append(fallbackKeys, &failpointpb.FallbackKeyInfo{
			Name:       protoString(st.Name),
			Activated:  st.Activated,
			Executions: st.Executions,
			Fires:      st.Fires,
		})
	}
	resp := &failpointpb.ListFailpointsResponse{Failpoints: failpoints, FallbackKeys: fallbackKeys}
	// A registry can hold many failpoints or counters with long names, which would make the response
	// larger than a client accepts, so the response reports what it leaves out.
	limitList(resp, s.listLimit)
	return resp, nil
}

// protoString returns s with invalid UTF-8 replaced, since proto strings must be valid UTF-8 and
// key names can contain bytes chosen by callers, such as an escaped HTTP path.
func protoString(s string) string {
	return strings.ToValidUTF8(s, "\uFFFD")
}

// infoProto describes info in the protocol. A failpoint is enabled if it has an action, also after
// a trigger of one fire is spent, as the failpoint keeps its action. Its recurrence is once if the
// trigger limits it to one fire, and always otherwise: the listing leaves out the other limits of a
// trigger (probability, skipping the first executions, a maximum of more than one fire), as the
// protocol has no fields for them.
func infoProto(info failpoint.Info) *failpointpb.FailpointInfo {
	recurrence := failpointpb.Recurrence_RECURRENCE_ALWAYS
	if info.Trigger.MaxFires == 1 {
		recurrence = failpointpb.Recurrence_RECURRENCE_ONCE
	}
	return &failpointpb.FailpointInfo{
		Name:       protoString(info.Key.Name()),
		Scope:      protoString(info.Key.Scope()),
		Enabled:    info.Action > failpoint.ActionOff,
		Action:     actionProto(info.Action),
		Recurrence: recurrence,
	}
}

func actionProto(a failpoint.Action) failpointpb.Action {
	switch a {
	case failpoint.ActionOff:
		return failpointpb.Action_ACTION_OFF
	case failpoint.ActionReturn:
		return failpointpb.Action_ACTION_RETURN
	case failpoint.ActionPanic:
		return failpointpb.Action_ACTION_PANIC
	case failpoint.ActionBlock:
		return failpointpb.Action_ACTION_BLOCK
	case failpoint.ActionPause:
		return failpointpb.Action_ACTION_PAUSE
	case failpoint.ActionCall:
		return failpointpb.Action_ACTION_CALL
	case failpoint.ActionOn:
		return failpointpb.Action_ACTION_ON
	default:
		return failpointpb.Action_ACTION_UNSPECIFIED
	}
}
