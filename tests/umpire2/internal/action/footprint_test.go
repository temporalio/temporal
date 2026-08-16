package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/action"
)

// TestReconcileFootprint pins the wire-level drift check: a declared internal call that never fired
// is drift, an observed non-ambient call outside the plan's Entry ∪ Footprint is drift, ambient
// traffic is ignored, and a plan that declares no Footprint reconciles to nothing (opt-in).
func TestReconcileFootprint(t *testing.T) {
	plan := []umpire.Action{{Name: "x", Entry: []string{"EntryRPC"}, Footprint: []string{"InternalRPC"}}}

	// Clean: entry + declared internal both observed; ambient long-poll ignored.
	require.Empty(t, action.ReconcileFootprint(plan,
		[]string{"/svc/EntryRPC", "/svc/InternalRPC", "/svc/PollWorkflowTaskQueue"}))

	// Missing: the declared internal call was not observed.
	missing := action.ReconcileFootprint(plan, []string{"/svc/EntryRPC"})
	require.Len(t, missing, 1)
	require.Equal(t, "InternalRPC", missing[0].Call)
	require.Contains(t, missing[0].Reason, "not observed")

	// Undeclared: an observed non-ambient call outside Entry ∪ Footprint.
	extra := action.ReconcileFootprint(plan,
		[]string{"/svc/EntryRPC", "/svc/InternalRPC", "/svc/SurpriseRPC"})
	require.Len(t, extra, 1)
	require.Contains(t, extra[0].Call, "SurpriseRPC")
	require.Contains(t, extra[0].Reason, "not declared")

	// Opt-in: a plan whose actions declare no Footprint is not reconciled.
	require.Empty(t, action.ReconcileFootprint(
		[]umpire.Action{{Name: "y", Entry: []string{"E"}}}, []string{"/svc/anything"}))
}
