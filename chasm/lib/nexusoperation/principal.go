package nexusoperation

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/headers"
)

// namespacePrincipalType is the Principal.Type for a namespace. The Name is the
// caller's namespace; it is only meaningful scoped to the token's verified
// issuer (namespace names are not globally unique across clusters).
const namespacePrincipalType = "namespaces"

// principalEmpty reports whether a principal carries no identity.
func principalEmpty(p *commonpb.Principal) bool {
	return p.GetType() == "" && p.GetName() == "" && p.GetId() == ""
}

// buildCaller assembles the at-rest/propagated caller: the chain's root
// end-user, plus the immediate (service) Nexus caller and its namespace as the
// single actor hop (Phase 1). Returns nil when neither a root nor an actor is
// present so absence stays absence.
func buildCaller(serviceCaller, namespaceCaller, endUserCaller *commonpb.Principal) *commonpb.Caller {
	caller := &commonpb.Caller{}
	if !principalEmpty(endUserCaller) {
		caller.Root = endUserCaller
	}
	if !principalEmpty(serviceCaller) || !principalEmpty(namespaceCaller) {
		actor := &commonpb.Actor{}
		if !principalEmpty(serviceCaller) {
			actor.Principal = serviceCaller
		}
		if !principalEmpty(namespaceCaller) {
			actor.Namespace = namespaceCaller
		}
		caller.Actors = []*commonpb.Actor{actor}
	}
	if caller.Root == nil && len(caller.Actors) == 0 {
		return nil
	}
	return caller
}

// serviceCallerOf returns the immediate (service) caller — the principal of the
// most recent actor hop — or nil.
func serviceCallerOf(caller *commonpb.Caller) *commonpb.Principal {
	actors := caller.GetActors()
	if len(actors) == 0 {
		return nil
	}
	return actors[len(actors)-1].GetPrincipal()
}

// PrincipalFromContext reads the immediate-caller principal from the chasm
// context's incoming metadata. Server-trusted: the auth interceptor wrote it
// after authentication and stripped any spoofed inbound values. Nil when no
// principal header is present (OSS without principal derivation, etc.).
func PrincipalFromContext(ctx chasm.Context) *commonpb.Principal {
	typ := ctx.RequestHeader(headers.PrincipalTypeHeaderName)
	name := ctx.RequestHeader(headers.PrincipalNameHeaderName)
	id := ctx.RequestHeader(headers.PrincipalIdHeaderName)
	if typ == "" && name == "" && id == "" {
		return nil
	}
	return &commonpb.Principal{Type: typ, Name: name, Id: id}
}

// SetCallerPrincipals records the callers on a workflow-initiated operation so
// the outbound dispatch can propagate them. serviceCaller is the worker that
// issued the schedule command; endUserCaller is the chain's originating
// identity (the workflow's stored root caller). No-op when both are nil.
func SetCallerPrincipals(ctx chasm.MutableContext, op *Operation, serviceCaller, endUserCaller *commonpb.Principal) {
	caller := buildCaller(serviceCaller, nil, endUserCaller)
	if caller == nil {
		return
	}
	op.RequestData = chasm.NewDataField(ctx, &nexusoperationpb.OperationRequestData{Caller: caller})
}
