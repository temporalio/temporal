package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	matching "go.temporal.io/server/client/matching"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

func TestValidatePartitionCounts(t *testing.T) {
	t.Parallel()

	var stale *serviceerrors.StalePartitionCounts
	var internal *serviceerror.Internal
	type allowed struct {
		delta int32
		ratio float32
	}

	tests := []struct {
		name        string
		partitionID int
		scaleInfo   *taskqueuespb.PartitionScaleInfo
		clientPC    matching.PartitionCounts
		forWrite    bool
		allowed     allowed
		expected    any
	}{
		// missing/invalid scale info -> accept
		{
			name:      "nil scale info",
			scaleInfo: nil,
			forWrite:  true,
			allowed:   allowed{delta: 2, ratio: 1.5},
			expected:  nil,
		},
		{
			name:      "zero scale info",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 0, Write: 0},
			forWrite:  true,
			allowed:   allowed{delta: 2, ratio: 1.5},
			expected:  nil,
		},
		{
			name:      "invalid scale info (write > read)",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 2, Write: 4},
			forWrite:  true,
			allowed:   allowed{delta: 2, ratio: 1.5},
			expected:  nil,
		},

		// partition id validity
		{
			name:        "negative partition id",
			partitionID: -1,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    &internal,
		},
		{
			name:        "partition id == read",
			partitionID: 8,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    &stale,
		},
		{
			name:        "partition id > read",
			partitionID: 10,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    false,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    &stale,
		},
		{
			name:        "draining partition, write",
			partitionID: 5,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    &stale,
		},
		{
			name:        "draining partition, read",
			partitionID: 5,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    false,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    nil,
		},
		{
			name:        "active partition, write",
			partitionID: 2,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    nil,
		},
		{
			name:        "active partition, read",
			partitionID: 2,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    false,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    nil,
		},

		// client counts comparison
		{
			name:        "no client counts",
			partitionID: 3,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 8},
			clientPC:    matching.PartitionCounts{},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    nil,
		},
		{
			name:        "client counts match",
			partitionID: 3,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 8},
			clientPC:    matching.PartitionCounts{Read: 8, Write: 8},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    nil,
		},
		{
			name:        "client counts too far off",
			partitionID: 3,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 8},
			clientPC:    matching.PartitionCounts{Read: 20, Write: 20},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    &stale,
		},
		{
			name:        "within delta",
			partitionID: 3,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 8},
			clientPC:    matching.PartitionCounts{Read: 10, Write: 10},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    nil,
		},
		{
			name:        "within ratio (delta exceeds)",
			partitionID: 3,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 100, Write: 100},
			clientPC:    matching.PartitionCounts{Read: 110, Write: 110},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    nil,
		},
		{
			name:        "both delta and ratio exceed",
			partitionID: 0,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 4, Write: 4},
			clientPC:    matching.PartitionCounts{Read: 10, Write: 10},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    &stale,
		},
		{
			name:        "read path compares read counts",
			partitionID: 0,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			clientPC:    matching.PartitionCounts{Read: 20, Write: 4},
			forWrite:    false,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    &stale,
		},
		{
			name:        "write path compares write counts",
			partitionID: 0,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			clientPC:    matching.PartitionCounts{Read: 20, Write: 4},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    nil,
		},
		{
			name:        "client counts below server",
			partitionID: 0,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 20, Write: 20},
			clientPC:    matching.PartitionCounts{Read: 4, Write: 4},
			forWrite:    true,
			allowed:     allowed{delta: 2, ratio: 1.5},
			expected:    &stale,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePartitionCounts(tc.partitionID, tc.scaleInfo, tc.clientPC, tc.forWrite, tc.allowed.delta, tc.allowed.ratio)
			if tc.expected == nil {
				require.NoError(t, err)
			} else {
				require.ErrorAs(t, err, tc.expected)
			}
		})
	}
}
