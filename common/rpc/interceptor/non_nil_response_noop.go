//go:build !test_dep

package interceptor

import (
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

// NewNonNilResponseInterceptor returns nil in production builds.
func NewNonNilResponseInterceptor(
	_ log.Logger,
) grpc.UnaryServerInterceptor {
	return nil
}
