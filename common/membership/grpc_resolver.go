package membership

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"

	"go.temporal.io/server/common/primitives"
	"go.uber.org/fx"
	"google.golang.org/grpc/resolver"
)

const (
	grpcResolverScheme = "membership"

	// Go's URL parser allows ~ in hostnames
	delim = "~"
)

type (
	// grpcBuilder implements grpc/resolver.Builder. This is a singleton that's registered with grpc.
	grpcBuilder struct {
		resolvers sync.Map // pointer as string -> *GRPCResolver
	}

	// GRPCResolver and the resolvers map in grpcBuilder is used to pass a Monitor through a string url.
	GRPCResolver struct {
		monitor Monitor
	}

	// grpcResolver is a single instance of a resolver.
	grpcResolver struct {
		cc       resolver.ClientConn
		r        ServiceResolver
		notifyCh chan *ChangedEvent
		wg       sync.WaitGroup
	}
)

var (
	GRPCResolverModule = fx.Options(
		fx.Provide(newGRPCResolver),
	)

	_ resolver.Builder = (*grpcBuilder)(nil)

	globalGrpcBuilder grpcBuilder

	errInvalidUrl     = errors.New("invalid grpc resolver url")
	errNotInitialized = errors.New("grpc resolver has not been initialized yet")
)

func init() {
	// This must be called in init to avoid race conditions.
	resolver.Register(&globalGrpcBuilder)
}

// Most code should not use this, this is only exposed for code that has to recognize and use a
// grpc membership url outside of grpc.
func GetServiceResolverFromURL(u *url.URL) (ServiceResolver, error) {
	return globalGrpcBuilder.getServiceResolver(u)
}

// This should only be used in unit tests. For normal code, use the *GRPCResolver provided by fx.
// Monitor may be nil if it's not needed, but then note that GetServiceResolverFromURL will panic.
func GRPCResolverURLForTesting(monitor Monitor, service primitives.ServiceName) string {
	return newGRPCResolver(monitor).MakeURL(service)
}

func newGRPCResolver(monitor Monitor) *GRPCResolver {
	res := &GRPCResolver{monitor: monitor}
	globalGrpcBuilder.resolvers.Store(fmt.Sprintf("%p", res), res)
	return res
}

func (g *GRPCResolver) MakeURL(service primitives.ServiceName) string {
	return fmt.Sprintf("%s://%s%s%p", grpcResolverScheme, string(service), delim, g)
}

func (m *grpcBuilder) Scheme() string {
	return grpcResolverScheme
}

func (m *grpcBuilder) getServiceResolver(u *url.URL) (ServiceResolver, error) {
	if u.Scheme != grpcResolverScheme {
		return nil, errInvalidUrl
	}
	service, ptr, found := strings.Cut(u.Host, delim)
	if !found {
		return nil, errInvalidUrl
	}
	v, ok := m.resolvers.Load(ptr)
	if !ok {
		return nil, errNotInitialized
	}
	return v.(*GRPCResolver).monitor.GetResolver(primitives.ServiceName(service))
}

func (m *grpcBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	serviceResolver, err := m.getServiceResolver(&target.URL)
	if err != nil {
		return nil, err
	}
	grpcResolver := &grpcResolver{
		cc:       cc,
		r:        serviceResolver,
		notifyCh: make(chan *ChangedEvent, 1),
	}
	if err := grpcResolver.start(); err != nil {
		return nil, err
	}
	return grpcResolver, nil
}

func (m *grpcResolver) start() error {
	if err := m.r.AddListener(fmt.Sprintf("%p", m), m.notifyCh); err != nil {
		return err
	}
	m.wg.Add(1)
	go m.listen()

	// Try once to get address synchronously. If this fails, it's okay, we'll listen for
	// changes and update the resolver later.
	m.resolve()
	return nil
}

func (m *grpcResolver) listen() {
	for range m.notifyCh {
		m.resolve()
	}
	m.wg.Done()
}

func (m *grpcResolver) resolve() {
	members := m.r.AvailableMembers()
	if len(members) == 0 {
		// grpc considers it an error if we report no addresses, and fails the connection eagerly.
		// Instead, just poke membership and then wait until it notifies us.
		m.r.RequestRefresh()
		return
	}
	addresses := make([]resolver.Address, 0, len(members))
	for _, hostInfo := range members {
		addresses = append(addresses, resolver.Address{
			Addr: hostInfo.GetAddress(),
		})
	}
	if err := m.cc.UpdateState(resolver.State{Addresses: addresses}); err != nil {
		fmt.Printf("error updating state in gRPC resolver: %v", err)
	}
}

func (m *grpcResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	select {
	case m.notifyCh <- nil:
	default:
	}
}

func (m *grpcResolver) Close() {
	if err := m.r.RemoveListener(fmt.Sprintf("%p", m)); err != nil {
		fmt.Printf("error removing listener from gRPC resolver: %v", err)
	}
	close(m.notifyCh)
	m.wg.Wait() // wait until listen() exits
}
