package nsrepl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	nsreplpb "go.temporal.io/server/chasm/lib/nsrepl/gen/nsreplpb/v1"
)

// TestNewNamespaceMutationComponent verifies the component starts RUNNING with a
// PENDING local apply and a PENDING per-peer entry for every peer cell.
func TestNewNamespaceMutationComponent(t *testing.T) {
	c := NewNamespaceMutationComponent(&nsreplpb.NamespaceMutation{
		Operation: nsreplpb.NAMESPACE_OPERATION_UPDATE,
		PeerCells: []string{"cellB", "cellC"},
	})

	require.Equal(t, nsreplpb.COMPONENT_STATUS_RUNNING, c.GetStatus())
	require.Equal(t, nsreplpb.LOCAL_APPLY_OUTCOME_PENDING, c.GetLocalApply().GetOutcome())
	require.Len(t, c.GetPeerApply(), 2)
	for _, cell := range []string{"cellB", "cellC"} {
		require.Equal(t, nsreplpb.PEER_APPLY_OUTCOME_PENDING, c.GetPeerApply()[cell].GetOutcome(), cell)
	}
}

// TestLifecycleState maps each component status onto the CHASM lifecycle state
// the framework uses for retention / completion.
func TestLifecycleState(t *testing.T) {
	testCases := []struct {
		status nsreplpb.ComponentStatus
		want   chasm.LifecycleState
	}{
		{nsreplpb.COMPONENT_STATUS_RUNNING, chasm.LifecycleStateRunning},
		{nsreplpb.COMPONENT_STATUS_COMPLETED, chasm.LifecycleStateCompleted},
		{nsreplpb.COMPONENT_STATUS_FAILED, chasm.LifecycleStateFailed},
		{nsreplpb.COMPONENT_STATUS_UNSPECIFIED, chasm.LifecycleStateRunning},
	}
	for _, tc := range testCases {
		c := &NamespaceMutationComponent{NamespaceMutationState: &nsreplpb.NamespaceMutationState{Status: tc.status}}
		require.Equal(t, tc.want, c.LifecycleState(nil))
	}
}

// TestAllPeersTerminal verifies the completion predicate: true only when every
// peer reached a terminal outcome (Applied / NoOpStale / FailedTerminal), false
// while any peer is still Pending or FailedRetriable.
func TestAllPeersTerminal(t *testing.T) {
	testCases := []struct {
		name  string
		peers map[string]nsreplpb.PeerApplyOutcome
		want  bool
	}{
		{name: "no peers is terminal", peers: nil, want: true},
		{
			name:  "all applied",
			peers: map[string]nsreplpb.PeerApplyOutcome{"a": nsreplpb.PEER_APPLY_OUTCOME_APPLIED, "b": nsreplpb.PEER_APPLY_OUTCOME_APPLIED},
			want:  true,
		},
		{
			name: "mixed terminal outcomes",
			peers: map[string]nsreplpb.PeerApplyOutcome{
				"a": nsreplpb.PEER_APPLY_OUTCOME_APPLIED,
				"b": nsreplpb.PEER_APPLY_OUTCOME_NO_OP_STALE,
				"c": nsreplpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL,
			},
			want: true,
		},
		{
			name:  "one pending is not terminal",
			peers: map[string]nsreplpb.PeerApplyOutcome{"a": nsreplpb.PEER_APPLY_OUTCOME_APPLIED, "b": nsreplpb.PEER_APPLY_OUTCOME_PENDING},
			want:  false,
		},
		{
			name:  "one failed-retriable is not terminal",
			peers: map[string]nsreplpb.PeerApplyOutcome{"a": nsreplpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE},
			want:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			peerApply := make(map[string]*nsreplpb.PeerApplyStatus, len(tc.peers))
			for cell, outcome := range tc.peers {
				peerApply[cell] = &nsreplpb.PeerApplyStatus{Outcome: outcome}
			}
			c := &NamespaceMutationComponent{NamespaceMutationState: &nsreplpb.NamespaceMutationState{PeerApply: peerApply}}
			require.Equal(t, tc.want, c.allPeersTerminal())
		})
	}
}
