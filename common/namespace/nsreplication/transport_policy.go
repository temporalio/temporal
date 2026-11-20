package nsreplication

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// TransportPolicy reports whether a namespace's outgoing replication stays on
// the legacy replication queue while the cell runs the shadow or CHASM
// transport. A held namespace is neither shadowed nor sent through CHASM. A
// deployment uses it to hold back namespaces whose replication is not ready for
// CHASM delivery yet; it cannot move a namespace off the legacy queue. The
// source asks it on every namespace registration and update, so a namespace
// moves between transports as the answer changes, and receivers order the
// writes by version either way. The default holds nothing.
type TransportPolicy func(ctx context.Context, detail *persistencespb.NamespaceDetail) (holdOnLegacy bool)

// NewDefaultTransportPolicy creates the default TransportPolicy.
func NewDefaultTransportPolicy() TransportPolicy {
	return func(context.Context, *persistencespb.NamespaceDetail) bool { return false }
}
