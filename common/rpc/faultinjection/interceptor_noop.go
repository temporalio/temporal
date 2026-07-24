//go:build !test_dep

package faultinjection

import (
	"context"

	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

// GRPCUnaryServerInterceptor returns a pass-through interceptor when test hooks are disabled.
func GRPCUnaryServerInterceptor(testhooks.TestHooks) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return handler(ctx, req)
	}
}
