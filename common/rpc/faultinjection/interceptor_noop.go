//go:build !test_dep

package faultinjection

import (
	"net/http"

	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

// GRPCUnaryServerInterceptor returns nil when test hooks are disabled.
func GRPCUnaryServerInterceptor(testhooks.TestHooks) grpc.UnaryServerInterceptor {
	return nil
}

// HTTPRoundTripper returns base unchanged when test hooks are disabled.
func HTTPRoundTripper(base http.RoundTripper, _ string, _ testhooks.TestHooks) http.RoundTripper {
	return base
}
