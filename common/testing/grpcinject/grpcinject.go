package grpcinject

import (
	"context"
	"sync/atomic"

	"google.golang.org/grpc"
)

// MetadataFunc is a function type that modifies the outgoing context with metadata
type MetadataFunc func(ctx context.Context) context.Context

type Interceptor struct {
	current atomic.Value // type MetadataFunc
}

func NewInterceptor() *Interceptor {
	return &Interceptor{}
}

func (i *Interceptor) Set(fn MetadataFunc) {
	i.current.Store(fn)
}

func (i *Interceptor) currentFunc() MetadataFunc {
	if v, ok := i.current.Load().(MetadataFunc); ok && v != nil {
		return v
	}
	return nil
}

func (i *Interceptor) Unary() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if fn, ok := i.current.Load().(MetadataFunc); ok && fn != nil {
			ctx = fn(ctx)
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
