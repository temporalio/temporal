package workflowresend

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
	claimed, atCapacity := r.TryClaim(key("a"), 8)
	require.True(t, claimed)
	require.False(t, atCapacity)
}

func TestInFlightResends_DedupesSameWorkflow(t *testing.T) {
	var r InFlightResends

	claimed, atCapacity := r.TryClaim(key("a"), 8)
	require.True(t, claimed)
	require.False(t, atCapacity)

	// Same workflow while the first is still held.
	claimed, atCapacity = r.TryClaim(key("a"), 8)
	require.False(t, claimed)
	require.False(t, atCapacity, "a duplicate is not a capacity problem")

	// A different workflow is unaffected.
	claimed, _ = r.TryClaim(key("b"), 8)
	require.True(t, claimed)

	// Releasing lets the workflow be claimed again.
	r.Release(key("a"))
	claimed, _ = r.TryClaim(key("a"), 8)
	require.True(t, claimed)
}

func TestInFlightResends_EnforcesMaxInFlight(t *testing.T) {
	var r InFlightResends

	claimed, _ := r.TryClaim(key("a"), 2)
	require.True(t, claimed)
	claimed, _ = r.TryClaim(key("b"), 2)
	require.True(t, claimed)

	// Third distinct workflow exceeds the cap.
	claimed, atCapacity := r.TryClaim(key("c"), 2)
	require.False(t, claimed)
	require.True(t, atCapacity)

	// A duplicate of an already-held workflow still reports dedup, not capacity.
	claimed, atCapacity = r.TryClaim(key("a"), 2)
	require.False(t, claimed)
	require.False(t, atCapacity)

	// Freeing a slot admits the previously rejected workflow.
	r.Release(key("b"))
	claimed, atCapacity = r.TryClaim(key("c"), 2)
	require.True(t, claimed)
	require.False(t, atCapacity)
}

func TestInFlightResends_ZeroMaxRejectsEverything(t *testing.T) {
	var r InFlightResends
	claimed, atCapacity := r.TryClaim(key("a"), 0)
	require.False(t, claimed)
	require.True(t, atCapacity)
}
