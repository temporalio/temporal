//go:build !test_dep

package faultinjection

import (
	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

// GRPCUnaryServerInterceptor returns nil when test hooks are disabled.
func GRPCUnaryServerInterceptor(testhooks.TestHooks) grpc.UnaryServerInterceptor {
	return nil
}
