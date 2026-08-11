package namespacereplication

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
)

// TestNewNamespaceMutationComponent verifies the component starts RUNNING with a
// PENDING local apply and a PENDING per-peer entry for every peer cell.
func TestNewNamespaceMutationComponent(t *testing.T) {
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{
		Operation: namespacereplicationpb.NAMESPACE_OPERATION_UPDATE,
		PeerCells: []string{"cellB", "cellC"},
	})

	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_RUNNING, c.GetStatus())
	require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_PENDING, c.GetLocalApply().GetOutcome())
	require.Len(t, c.GetPeerApply(), 2)
	for _, cell := range []string{"cellB", "cellC"} {
		require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING, c.GetPeerApply()[cell].GetOutcome(), cell)
	}
}

// TestLifecycleState maps each component status onto the CHASM lifecycle state
// the framework uses for retention / completion.
func TestLifecycleState(t *testing.T) {
	testCases := []struct {
		status namespacereplicationpb.ComponentStatus
		want   chasm.LifecycleState
	}{
		{namespacereplicationpb.COMPONENT_STATUS_RUNNING, chasm.LifecycleStateRunning},
		{namespacereplicationpb.COMPONENT_STATUS_COMPLETED, chasm.LifecycleStateCompleted},
		{namespacereplicationpb.COMPONENT_STATUS_FAILED, chasm.LifecycleStateFailed},
		{namespacereplicationpb.COMPONENT_STATUS_UNSPECIFIED, chasm.LifecycleStateRunning},
	}
	for _, tc := range testCases {
		c := &NamespaceMutationComponent{NamespaceMutationState: &namespacereplicationpb.NamespaceMutationState{Status: tc.status}}
		require.Equal(t, tc.want, c.LifecycleState(nil))
	}
}

// TestAllPeersTerminal verifies the completion predicate: true only when every
// peer reached a terminal outcome (Applied / NoOpStale / FailedTerminal), false
// while any peer is still Pending or FailedRetriable.
func TestAllPeersTerminal(t *testing.T) {
	testCases := []struct {
		name  string
		peers map[string]namespacereplicationpb.PeerApplyOutcome
		want  bool
	}{
		{name: "no peers is terminal", peers: nil, want: true},
		{
			name:  "all applied",
			peers: map[string]namespacereplicationpb.PeerApplyOutcome{"a": namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED, "b": namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED},
			want:  true,
		},
		{
			name: "mixed terminal outcomes",
			peers: map[string]namespacereplicationpb.PeerApplyOutcome{
				"a": namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED,
				"b": namespacereplicationpb.PEER_APPLY_OUTCOME_NO_OP_STALE,
				"c": namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL,
				"d": namespacereplicationpb.PEER_APPLY_OUTCOME_NOT_ADMITTED,
			},
			want: true,
		},
		{
			name:  "one pending is not terminal",
			peers: map[string]namespacereplicationpb.PeerApplyOutcome{"a": namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED, "b": namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING},
			want:  false,
		},
		{
			name:  "one failed-retriable is not terminal",
			peers: map[string]namespacereplicationpb.PeerApplyOutcome{"a": namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE},
			want:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			peerApply := make(map[string]*namespacereplicationpb.PeerApplyStatus, len(tc.peers))
			for cell, outcome := range tc.peers {
				peerApply[cell] = &namespacereplicationpb.PeerApplyStatus{Outcome: outcome}
			}
			c := &NamespaceMutationComponent{NamespaceMutationState: &namespacereplicationpb.NamespaceMutationState{PeerApply: peerApply}}
			require.Equal(t, tc.want, c.allPeersTerminal())
		})
	}
}
