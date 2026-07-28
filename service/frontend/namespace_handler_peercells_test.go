package frontend

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPeerCellsFromClusters pins the peer fan-out set for namespace mutations.
// The load-bearing case is cluster removal (deglobalization / peer-set shrink):
// a cluster dropped from the new list must still be fanned out to — via the
// previous list — so it receives the final mutation and learns it is no longer a
// participant. Computing peers from the new list alone silently strands it, a
// regression from the legacy queue's broadcast fan-out.
func TestPeerCellsFromClusters(t *testing.T) {
	const current = "A"
	testCases := []struct {
		name     string
		newList  []string
		prevList []string
		want     []string
	}{
		{
			name:     "add cluster: new peer included",
			newList:  []string{"A", "B", "C"},
			prevList: []string{"A", "B"},
			want:     []string{"B", "C"},
		},
		{
			name:     "remove cluster (shrink to multi): removed cluster still targeted",
			newList:  []string{"A", "B"},
			prevList: []string{"A", "B", "C"},
			want:     []string{"B", "C"},
		},
		{
			name:     "remove cluster (shrink to single): removed cluster still targeted",
			newList:  []string{"A"},
			prevList: []string{"A", "B"},
			want:     []string{"B"},
		},
		{
			name:     "no cluster-list change",
			newList:  []string{"A", "B"},
			prevList: []string{"A", "B"},
			want:     []string{"B"},
		},
		{
			name:     "create (nil previous), multi cluster",
			newList:  []string{"A", "B"},
			prevList: nil,
			want:     []string{"B"},
		},
		{
			name:     "create (nil previous), single cluster: no peers",
			newList:  []string{"A"},
			prevList: nil,
			want:     nil,
		},
		{
			name:     "duplicate within new list deduped",
			newList:  []string{"A", "B", "B"},
			prevList: nil,
			want:     []string{"B"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := peerCellsFromClusters(current, tc.newList, tc.prevList)
			require.Equal(t, tc.want, got)
		})
	}
}
