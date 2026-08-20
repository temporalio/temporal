package verifychildworkflowcompletionrecorded

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
)

func key(workflowID string) definition.WorkflowKey {
	return definition.NewWorkflowKey("ns", workflowID, "run")
}

func TestInFlightResends_ZeroValueIsUsable(t *testing.T) {
	var r InFlightResends // no constructor
	claimed, atCapacity := r.tryClaim(key("a"), 8)
	require.True(t, claimed)
	require.False(t, atCapacity)
}

func TestInFlightResends_DedupesSameParent(t *testing.T) {
	var r InFlightResends

	claimed, atCapacity := r.tryClaim(key("a"), 8)
	require.True(t, claimed)
	require.False(t, atCapacity)

	// Same parent while the first is still held.
	claimed, atCapacity = r.tryClaim(key("a"), 8)
	require.False(t, claimed)
	require.False(t, atCapacity, "a duplicate is not a capacity problem")

	// A different parent is unaffected.
	claimed, _ = r.tryClaim(key("b"), 8)
	require.True(t, claimed)

	// Releasing lets the parent be claimed again.
	r.release(key("a"))
	claimed, _ = r.tryClaim(key("a"), 8)
	require.True(t, claimed)
}

func TestInFlightResends_EnforcesMaxInFlight(t *testing.T) {
	var r InFlightResends

	claimed, _ := r.tryClaim(key("a"), 2)
	require.True(t, claimed)
	claimed, _ = r.tryClaim(key("b"), 2)
	require.True(t, claimed)

	// Third distinct parent exceeds the cap.
	claimed, atCapacity := r.tryClaim(key("c"), 2)
	require.False(t, claimed)
	require.True(t, atCapacity)

	// A duplicate of an already-held parent still reports dedup, not capacity.
	claimed, atCapacity = r.tryClaim(key("a"), 2)
	require.False(t, claimed)
	require.False(t, atCapacity)

	// Freeing a slot admits the previously rejected parent.
	r.release(key("b"))
	claimed, atCapacity = r.tryClaim(key("c"), 2)
	require.True(t, claimed)
	require.False(t, atCapacity)
}

func TestInFlightResends_ZeroMaxRejectsEverything(t *testing.T) {
	var r InFlightResends
	claimed, atCapacity := r.tryClaim(key("a"), 0)
	require.False(t, claimed)
	require.True(t, atCapacity)
}
