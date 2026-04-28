package grpcinject

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"
)

// MetadataFunc is a function type that modifies the outgoing context with metadata
type MetadataFunc func(ctx context.Context) context.Context

type Interceptor struct {
	current            atomic.Value // type MetadataFunc
	currentUnary       atomic.Value // type grpc.UnaryClientInterceptor
	currentUnaryServer atomic.Value // type grpc.UnaryServerInterceptor
}

func NewInterceptor() *Interceptor {
	return &Interceptor{}
}

func (i *Interceptor) Set(fn MetadataFunc) {
	i.current.Store(fn)
}

func (i *Interceptor) SetUnary(fn grpc.UnaryClientInterceptor) {
	i.currentUnary.Store(fn)
}

// SetUnaryServer installs a server-side unary interceptor. It is shared
// across every Temporal service in the cluster (frontend, history, matching,
// worker), so a single Set call covers inbound RPCs to all services.
func (i *Interceptor) SetUnaryServer(fn grpc.UnaryServerInterceptor) {
	i.currentUnaryServer.Store(fn)
}

// UnaryServer returns the server-side counterpart of Unary. The returned
// interceptor delegates to whatever was set most recently with SetUnaryServer
// (or passes through if nothing is set), letting tests swap behavior at
// runtime.
func (i *Interceptor) UnaryServer() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		if fn, ok := i.currentUnaryServer.Load().(grpc.UnaryServerInterceptor); ok && fn != nil {
			return fn(ctx, req, info, handler)
		}
		return handler(ctx, req)
	}
}

func (i *Interceptor) Unary() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply any,
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if fn, ok := i.current.Load().(MetadataFunc); ok && fn != nil {
			ctx = fn(ctx)
		}
		if fn, ok := i.currentUnary.Load().(grpc.UnaryClientInterceptor); ok && fn != nil {
			return fn(ctx, method, req, reply, cc, invoker, opts...)
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func (i *Interceptor) Stream() grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption,
	) (grpc.ClientStream, error) {
		if fn, ok := i.current.Load().(MetadataFunc); ok && fn != nil {
			ctx = fn(ctx)
		}
		return streamer(ctx, desc, cc, method, opts...)
	}
}
