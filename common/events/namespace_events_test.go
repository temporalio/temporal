package events

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNamespaceLifecycleRegisteredName(t *testing.T) {
	require.Equal(t, "namespace_lifecycle", NamespaceLifecycle.Name())
}

// TestNamespaceLifecycleFieldSetLocked pins the complete set of field names NamespaceLifecycle can
// emit. This set is the event's published wire contract that downstream consumers depend on. If
// this test fails you have added, removed, or renamed an emitted field: do so deliberately, get
// the change reviewed, and then update `want` to match.
func TestNamespaceLifecycleFieldSetLocked(t *testing.T) {
	want := []string{"phase", "namespace", "namespace_id", "details"}

	enc := newCaptureEncoder()
	NamespaceLifecyclePayload{
		Phase:       "route_computed",
		Namespace:   "ns",
		NamespaceID: "ns-id",
		Details:     map[string]any{"k": "v"},
	}.Encode(enc)

	gotKeys := make([]string, 0, len(enc.fields))
	for k := range enc.fields {
		gotKeys = append(gotKeys, k)
	}

	require.ElementsMatch(t, want, gotKeys,
		"NamespaceLifecycle emitted-field set changed; this alters the event's published wire "+
			"contract. Make the change deliberately, get it reviewed, then update `want`.")
}
