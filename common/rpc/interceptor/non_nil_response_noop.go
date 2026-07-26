//go:build !test_dep

package interceptor

import (
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

// AddNonNilResponseInterceptor is a no-op in production builds.
func AddNonNilResponseInterceptor(
	interceptors []grpc.UnaryServerInterceptor,
	_ log.Logger,
) []grpc.UnaryServerInterceptor {
	return interceptors
}
