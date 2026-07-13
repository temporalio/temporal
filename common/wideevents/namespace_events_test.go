package wideevents

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceLifecycleEventName(t *testing.T) {
	require.Equal(t, "namespace_lifecycle", NamespaceLifecyclePayload{}.EventName())
}

// TestNamespaceLifecycleFieldSetLocked pins the complete set of field names NamespaceLifecycle can
// emit. This set is the event's published wire contract that downstream consumers depend on. If
// this test fails you have added, removed, or renamed an emitted field: do so deliberately, get
// the change reviewed, and then update `want` to match.
func TestNamespaceLifecycleFieldSetLocked(t *testing.T) {
	// want pins both the field set and the emitted values (the published wire contract).
	// Composite fields (details) are emitted as a compact JSON string.
	want := map[string]any{
		"phase":        "route_computed",
		"namespace":    "ns",
		"namespace_id": "ns-id",
		"details":      `{"k":"v"}`,
	}

	got := valueMap(NamespaceLifecyclePayload{
		Phase:       "route_computed",
		Namespace:   "ns",
		NamespaceID: "ns-id",
		Details:     map[string]any{"k": "v"},
	}.Attributes())

	require.Equal(t, want, got,
		"NamespaceLifecycle emitted field set or values changed; this alters the event's published "+
			"wire contract. Make the change deliberately, get it reviewed, then update `want`.")
}
