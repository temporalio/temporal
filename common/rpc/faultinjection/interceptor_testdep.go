//go:build test_dep

package faultinjection

import (
	"context"
	"net/http"

	"go.temporal.io/server/common/testing/testhooks"
	"google.golang.org/grpc"
)

// GRPCUnaryServerInterceptor returns a unary server interceptor that checks for
// dynamically registered fault injection callbacks before and after the handler.
//
// This is primarily used for testing, allowing tests to register callbacks that
// can inspect requests/responses and inject faults on demand.
//
// Behavior:
// - If no generator is registered, the handler proceeds normally.
// - Callbacks are checked before handler (resp=nil, err=nil). If matched, handler is skipped.
// - Callbacks are checked after handler with actual resp/err. If matched, returned values are used.
// - If no callbacks match, the handler's response/error is returned.
func GRPCUnaryServerInterceptor(testHooks testhooks.TestHooks) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		generate, ok := testhooks.Get(testHooks, testhooks.RPCFaultGenerator, testhooks.GlobalScope)
		if !ok {
			return handler(ctx, req)
		}

		// Check before handler (can short-circuit)
		if matched, resp, err := generate(ctx, info.FullMethod, req, nil, nil); matched {
			return resp, err
		}

		// Call handler
		resp, err := handler(ctx, req)

		// Check after handler (can modify response/error)
		if matched, newResp, newErr := generate(ctx, info.FullMethod, req, resp, err); matched {
			return newResp, newErr
		}

		return resp, err
	}
}

// HTTPRoundTripper wraps base so outbound HTTP calls (e.g. Nexus operation invocations to a
// handler) pass through the same RPCFaultGenerator seam as gRPC: a registered callback can
// hold (block, then decline), fail, or pass each call. The synthetic method handed to the
// generator is "HTTP <METHOD> <path>". namespaceID scopes the call to its owning namespace.
func HTTPRoundTripper(base http.RoundTripper, namespaceID string, testHooks testhooks.TestHooks) http.RoundTripper {
	return &faultRoundTripper{base: base, namespaceID: namespaceID, testHooks: testHooks}
}

type faultRoundTripper struct {
	base        http.RoundTripper
	namespaceID string
	testHooks   testhooks.TestHooks
}

func (f *faultRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	generate, ok := testhooks.Get(f.testHooks, testhooks.RPCFaultGenerator, testhooks.GlobalScope)
	if !ok {
		return f.base.RoundTrip(r)
	}
	method := "HTTP " + r.Method + " " + r.URL.Path
	req := &HTTPFaultRequest{NamespaceID: f.namespaceID, Request: r}

	// Before the call: a matching callback can fail it (return an error) or hold it (block,
	// then decline the match so the call proceeds). A match without an error is ignored here
	// (RoundTrip must not return a nil response and nil error).
	if matched, _, err := generate(r.Context(), method, req, nil, nil); matched && err != nil {
		return nil, err
	}

	resp, callErr := f.base.RoundTrip(r)

	// After the call: a matching callback may replace the outcome with an error.
	if matched, _, err := generate(r.Context(), method, req, resp, callErr); matched && err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return nil, err
	}
	return resp, callErr
}
