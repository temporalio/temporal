package interceptor_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/rpc/interceptor"
	"google.golang.org/grpc"
)

func TestMethodMiddlewareAcrossTransportAdapters(t *testing.T) {
	t.Parallel()
	var calls []string
	middleware := func(name string) interceptor.Interceptor {
		return func(ctx context.Context, request any, info *interceptor.MethodInfo, next interceptor.Handler) (any, error) {
			calls = append(calls, name+" before")
			require.Equal(t, "/service/Method", info.FullMethod)
			response, err := next(ctx, request)
			calls = append(calls, name+" after")
			return response, err
		}
	}
	// Cross both boundaries to verify order, method metadata, and response handling.
	grpcMiddleware := interceptor.ToGRPC(middleware("first"), middleware("second"))
	chain := interceptor.Chain(interceptor.FromGRPC(grpcMiddleware...)...)
	response, err := chain(t.Context(), "request", &interceptor.MethodInfo{FullMethod: "/service/Method"}, func(_ context.Context, request any) (any, error) {
		require.Equal(t, "request", request)
		calls = append(calls, "handler")
		return "response", nil
	})
	require.NoError(t, err)
	require.Equal(t, "response", response)
	require.Equal(t, []string{"first before", "second before", "handler", "second after", "first after"}, calls)
}

func TestMethodAdapterPreservesMetadataIdentity(t *testing.T) {
	t.Parallel()
	info := &grpc.UnaryServerInfo{FullMethod: "/before"}
	middleware := interceptor.ToGRPC(func(ctx context.Context, request any, method *interceptor.MethodInfo, next interceptor.Handler) (any, error) {
		method.FullMethod = "/after"
		return next(ctx, request)
	})[0]
	_, err := middleware(t.Context(), nil, info, func(context.Context, any) (any, error) { return nil, nil })
	require.NoError(t, err)
	require.Equal(t, "/after", info.FullMethod)
}
