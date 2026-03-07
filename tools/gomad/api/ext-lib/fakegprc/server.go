package fakegrpc

import (
	"context"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/stats"

	SIMAPI "go.temporal.io/server/tools/gomad/api/lang"
	SIMLIB "go.temporal.io/server/tools/gomad/api/lib"
)

var serversByAddr = map[string]*Server{}

var _ server = &Server{}

type (
	server interface {
		grpc.ServiceRegistrar
		reflection.ServiceInfoProvider
	}
	Server struct {
		doneCh      chan struct{}
		impls       map[string]any
		services    map[string]*grpc.ServiceDesc
		methods     map[string]map[string]grpc.MethodDesc
		interceptor grpc.UnaryServerInterceptor
	}
	ServerOption = func(*Server)
)

func NewServer(opts ...ServerOption) *Server {
	s := &Server{
		doneCh:   make(chan struct{}),
		impls:    make(map[string]any),
		services: make(map[string]*grpc.ServiceDesc),
		methods:  make(map[string]map[string]grpc.MethodDesc),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func ChainUnaryInterceptor(interceptors ...grpc.UnaryServerInterceptor) ServerOption {
	return func(s *Server) {
		var interceptor grpc.UnaryServerInterceptor
		if len(interceptors) == 0 {
			interceptor = nil
		} else if len(interceptors) == 1 {
			interceptor = interceptors[0]
		} else {
			interceptor = chainUnaryInterceptors(interceptors)
		}
		s.interceptor = interceptor
	}
}

func ChainStreamInterceptor(interceptors ...grpc.StreamServerInterceptor) ServerOption {
	return func(s *Server) {
		// ignore
	}
}

func (g *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	g.impls[desc.ServiceName] = impl
	g.services[desc.ServiceName] = desc
	g.methods[desc.ServiceName] = make(map[string]grpc.MethodDesc, len(desc.Methods))
	for _, method := range desc.Methods {
		g.methods[desc.ServiceName][method.MethodName] = method
	}
}

func (g *Server) Serve(l net.Listener) error {
	serversByAddr[l.(*SIMLIB.Listener).Address] = g
	SIMAPI.ChanRcv(g.doneCh)
	return nil
}

func (g *Server) Stop() {
	SIMAPI.ChanClose(g.doneCh)
}

func (g *Server) GracefulStop() {
	// TODO?
	g.Stop()
}

func (g *Server) GetServiceInfo() map[string]grpc.ServiceInfo {
	res := make(map[string]grpc.ServiceInfo, len(g.services))
	for name, desc := range g.services {
		res[name] = grpc.ServiceInfo{
			Methods:  make([]grpc.MethodInfo, len(desc.Methods)),
			Metadata: desc.Metadata,
		}
		for i, method := range desc.Methods {
			res[name].Methods[i] = grpc.MethodInfo{
				Name: method.MethodName,
			}
		}
	}
	return res
}

func chainUnaryInterceptors(interceptors []grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return interceptors[0](ctx, req, info, getChainUnaryHandler(interceptors, 0, info, handler))
	}
}

func getChainUnaryHandler(interceptors []grpc.UnaryServerInterceptor, curr int, info *grpc.UnaryServerInfo, finalHandler grpc.UnaryHandler) grpc.UnaryHandler {
	if curr == len(interceptors)-1 {
		return finalHandler
	}
	return func(ctx context.Context, req any) (any, error) {
		return interceptors[curr+1](ctx, req, info, getChainUnaryHandler(interceptors, curr+1, info, finalHandler))
	}
}

func Creds(c credentials.TransportCredentials) ServerOption {
	return func(s *Server) {
		// ignore
	}
}

func KeepaliveParams(kp keepalive.ServerParameters) ServerOption {
	return func(s *Server) {
		// ignore
	}
}

func KeepaliveEnforcementPolicy(kep keepalive.EnforcementPolicy) ServerOption {
	return func(s *Server) {
		// ignore
	}
}

func StatsHandler(h stats.Handler) ServerOption {
	return func(s *Server) {
		// ignore
	}
}

func SetTrailer(ctx context.Context, md metadata.MD) error {
	return nil
}
