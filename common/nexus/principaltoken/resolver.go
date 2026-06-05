package principaltoken

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
)

// PrincipalResolver snapshots the human-readable name for a principal at the
// time identity is captured/propagated. In OSS principal.Name is already
// human-readable, so the default NoopResolver returns "" and readers fall back
// to principal.Name. Cloud injects a resolver that maps an opaque identity ID
// to its current display name, captured at write time so audit records survive
// later renames/deletes.
type PrincipalResolver interface {
	// Resolve returns the human-readable display name for p, or "" if there is
	// nothing to resolve (the name is already human-readable). It must not fail
	// the caller's operation on lookup error — implementations should return
	// ("", nil) to degrade to the opaque name rather than block propagation.
	Resolve(ctx context.Context, p *commonpb.Principal) (resolvedName string, err error)
}

// NoopResolver is the OSS default: principal.Name is already human-readable, so
// there is nothing to resolve.
type NoopResolver struct{}

func (NoopResolver) Resolve(context.Context, *commonpb.Principal) (string, error) {
	return "", nil
}
