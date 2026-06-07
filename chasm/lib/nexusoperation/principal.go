package nexusoperation

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/nexus/principaltoken"
)

// namespacePrincipalType is the Principal.Type for a namespace caller. The
// Name is the caller's namespace; it is only meaningful scoped to the token's
// verified issuer (namespace names are not globally unique across clusters).
const namespacePrincipalType = "namespace"

// newAttributedPrincipal wraps an authenticated principal as the at-rest
// AttributedPrincipal, with the display-name snapshot captured at write time
// (empty when the principal's Name is already human-readable). Returns nil for a
// nil principal so absence stays absence.
func newAttributedPrincipal(p *commonpb.Principal, resolvedName string) *commonspb.AttributedPrincipal {
	if p == nil {
		return nil
	}
	return &commonspb.AttributedPrincipal{Principal: p, ResolvedName: resolvedName}
}

// resolvedNameFromChasmContext reads the immediate-caller display-name snapshot
// set by the auth interceptor (empty when the Name is already human-readable).
func resolvedNameFromChasmContext(ctx chasm.Context) string {
	return ctx.RequestHeader(headers.PrincipalResolvedNameHeaderName)
}

// PrincipalFromContext reads the immediate-caller principal from the chasm
// context's incoming metadata. Server-trusted: the auth interceptor wrote it
// after authentication and stripped any spoofed inbound values. Nil when no
// principal header is present (OSS without principal derivation, etc.).
func PrincipalFromContext(ctx chasm.Context) *commonpb.Principal {
	typ := ctx.RequestHeader(headers.PrincipalTypeHeaderName)
	name := ctx.RequestHeader(headers.PrincipalNameHeaderName)
	if typ == "" && name == "" {
		return nil
	}
	return &commonpb.Principal{Type: typ, Name: name}
}

// SetCallerPrincipals records the caller identity on a workflow-initiated
// operation so the outbound dispatch can propagate it. serviceCaller is the
// worker that issued the schedule command; endUserCaller is the chain's
// originating identity (RootCallerPrincipal). No-op when both are nil.
func SetCallerPrincipals(ctx chasm.MutableContext, op *Operation, serviceCaller, endUserCaller *commonpb.Principal) {
	if serviceCaller == nil && endUserCaller == nil {
		return
	}
	op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{
		// The resolved name pairs with the immediate caller; the end-user
		// (RootCallerPrincipal) carries no snapshot.
		ServiceCallerPrincipal: newAttributedPrincipal(serviceCaller, resolvedNameFromChasmContext(ctx)),
		EndUserCallerPrincipal: newAttributedPrincipal(endUserCaller, ""),
	})
}

// attachPrincipalIdentity mints a signed principal token and attaches it as a
// single header on the outbound Nexus request. The token is the sole carrier of
// caller identity across the Nexus hop — there is no raw-header fallback, so
// when no signer is configured nothing is propagated (the feature is off).
//
// The handler verifies the token (signature, or trusted transport peer) and
// promotes the principals; it strips any spoofed inbound principal headers, so
// the signed token is the only path by which identity survives ingress.
func attachPrincipalIdentity(
	ctx context.Context,
	h nexus.Header,
	signer principaltoken.Signer,
	resolver principaltoken.PrincipalResolver,
	serviceCaller, endUserCaller *commonspb.AttributedPrincipal,
	namespaceCaller *commonpb.Principal,
) error {
	if signer == nil ||
		(serviceCaller.GetPrincipal() == nil && endUserCaller.GetPrincipal() == nil) {
		return nil
	}
	svcName, err := resolveDisplayName(ctx, resolver, serviceCaller)
	if err != nil {
		return err
	}
	euName, err := resolveDisplayName(ctx, resolver, endUserCaller)
	if err != nil {
		return err
	}
	token, err := signer.Sign(ctx, principaltoken.Content{
		ServiceCaller:             serviceCaller.GetPrincipal(),
		EndUser:                   endUserCaller.GetPrincipal(),
		NamespaceCaller:           namespaceCaller,
		ServiceCallerResolvedName: svcName,
		EndUserResolvedName:       euName,
	})
	if err != nil {
		return err
	}
	h.Set(principaltoken.Header, token)
	return nil
}

// resolveDisplayName returns the human-readable name snapshot for a stored
// principal: the one captured at write time if present, otherwise resolved now
// (a noop in OSS, returning ""). Resolver errors are surfaced to the caller,
// which logs and proceeds without propagated identity.
func resolveDisplayName(
	ctx context.Context,
	resolver principaltoken.PrincipalResolver,
	sp *commonspb.AttributedPrincipal,
) (string, error) {
	if sp.GetResolvedName() != "" {
		return sp.GetResolvedName(), nil
	}
	if resolver == nil || sp.GetPrincipal() == nil {
		return "", nil
	}
	return resolver.Resolve(ctx, sp.GetPrincipal())
}
