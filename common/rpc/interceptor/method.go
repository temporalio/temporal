package interceptor

import (
	"context"
	"slices"

	"go.temporal.io/server/common/faultinjection"
	"google.golang.org/grpc"
)

// MethodInfo describes a service method independently of its entry transport.
type MethodInfo struct {
	Server     any
	FullMethod string
}

type Handler func(context.Context, any) (any, error)

// Interceptor wraps a service method reached through gRPC or the HTTP gateway.
type Interceptor func(context.Context, any, *MethodInfo, Handler) (any, error)

// FromGRPC bridges existing application middleware to the neutral method contract.
func FromGRPC(middleware ...grpc.UnaryServerInterceptor) []Interceptor {
	result := make([]Interceptor, 0, len(middleware))
	for _, intercept := range middleware {
		result = append(result, func(ctx context.Context, req any, info *MethodInfo, next Handler) (any, error) {
			return intercept(ctx, req, (*grpc.UnaryServerInfo)(info), grpc.UnaryHandler(next))
		})
	}
	return result
}

// ToGRPC adapts method middleware at a gRPC boundary.
func ToGRPC(middleware ...Interceptor) []grpc.UnaryServerInterceptor {
	result := make([]grpc.UnaryServerInterceptor, 0, len(middleware))
	for _, intercept := range middleware {
		result = append(result, func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
			return intercept(ctx, req, (*MethodInfo)(info), Handler(next))
		})
	}
	return result
}

// Chain executes middleware in declaration order around the final handler.
func Chain(middleware ...Interceptor) Interceptor {
	return func(ctx context.Context, req any, info *MethodInfo, handler Handler) (any, error) {
		for _, m := range slices.Backward(middleware) {
			next, intercept := handler, m
			handler = func(ctx context.Context, req any) (any, error) { return intercept(ctx, req, info, next) }
		}
		return handler(ctx, req)
	}
}

// InjectFaults applies service-method test hooks without assuming a transport.
func InjectFaults(generator faultinjection.Generator[any, any]) Interceptor {
	if generator == nil {
		return nil
	}
	return func(ctx context.Context, req any, info *MethodInfo, handler Handler) (any, error) {
		return faultinjection.Invoke(ctx, generator, info.FullMethod, req, func() (any, error) { return handler(ctx, req) })
	}
}
