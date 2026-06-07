package frontend

import (
	"context"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/nexus/principaltoken"
	"google.golang.org/grpc"
)

// PrincipalTokenInterceptor verifies a forwarded principal token on an inbound
// gRPC request and promotes the end-user onto the context — the gRPC analogue of
// promotePrincipalToken — so a workflow started from a Nexus operation inherits
// the original end-user as RootCallerPrincipal. Must run after the auth
// interceptor (which strips spoofed principal headers); trust is the token's
// (signature, or trusted peer), so honoring it on the external edge is safe.
type PrincipalTokenInterceptor struct {
	verifier principaltoken.Verifier
}

// NewPrincipalTokenInterceptor constructs the interceptor. verifier may be nil
// (feature off), in which case Intercept is a pass-through.
func NewPrincipalTokenInterceptor(verifier principaltoken.Verifier) *PrincipalTokenInterceptor {
	return &PrincipalTokenInterceptor{verifier: verifier}
}

func (i *PrincipalTokenInterceptor) Intercept(
	ctx context.Context,
	req any,
	_ *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	return handler(i.promote(ctx), req)
}

// promote verifies the token (if any) and, on success, sets the end-user
// principal on the context. It deliberately does NOT touch the immediate-caller
// Principal: that was set by the auth interceptor from THIS RPC's authenticated
// identity (e.g. the worker), which is the correct immediate caller — the
// token's service caller describes the upstream Nexus hop, not this RPC.
func (i *PrincipalTokenInterceptor) promote(ctx context.Context) context.Context {
	if i.verifier == nil {
		return ctx
	}
	token := headers.GetValues(ctx, principaltoken.Header)[0]
	if token == "" {
		return ctx
	}
	verified, err := i.verifier.Verify(ctx, token)
	if err != nil {
		// An invalid/forged token is simply dropped; the request proceeds with
		// no propagated end-user (equivalent to the feature being off).
		return ctx
	}
	if verified.EndUser != nil {
		ctx = headers.SetEndUserPrincipal(ctx, principalForDisplay(verified.EndUser, verified.EndUserResolvedName))
	}
	return ctx
}
