package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	matching "go.temporal.io/server/client/matching"
	"go.temporal.io/server/common/dynamicconfig"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

func TestValidatePartitionCounts(t *testing.T) {
	t.Parallel()

	var stale *serviceerrors.StalePartitionCounts
	var internal *serviceerror.Internal

	tests := []struct {
		name        string
		partitionID int
		scaleInfo   *taskqueuespb.PartitionScaleInfo
		forWrite    bool
		expected    any
	}{
		// partition id validity
		{
			name:        "negative partition id",
			partitionID: -1,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    true,
			expected:    &internal,
		},
		{
			name:        "partition id == read",
			partitionID: 8,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    true,
			expected:    &stale,
		},
		{
			name:        "partition id > read",
			partitionID: 10,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    false,
			expected:    &stale,
		},
		{
			name:        "draining partition, write",
			partitionID: 5,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    true,
			expected:    &stale,
		},
		{
			name:        "draining partition, read",
			partitionID: 5,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    false,
			expected:    nil,
		},
		{
			name:        "active partition, write",
			partitionID: 2,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    true,
			expected:    nil,
		},
		{
			name:        "active partition, read",
			partitionID: 2,
			scaleInfo:   &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			forWrite:    false,
			expected:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePartitionCounts(tc.partitionID, tc.scaleInfo, tc.forWrite)
			if tc.expected == nil {
				require.NoError(t, err)
			} else {
				require.ErrorAs(t, err, tc.expected)
			}
		})
	}
}

func TestValidatePartitionCountDifference(t *testing.T) {
	t.Parallel()

	var stale *serviceerrors.StalePartitionCounts

	difference := dynamicconfig.PartitionScaleDifference{
		AllowedDelta: 2,
		AllowedRatio: 1.5,
	}

	tests := []struct {
		name      string
		scaleInfo *taskqueuespb.PartitionScaleInfo
		clientPC  matching.PartitionCounts
		forWrite  bool
		expected  any
	}{
		{
			name:      "client counts match",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 8},
			clientPC:  matching.PartitionCounts{Read: 8, Write: 8},
			forWrite:  true,
			expected:  nil,
		},
		{
			name:      "client counts too far off",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 8},
			clientPC:  matching.PartitionCounts{Read: 20, Write: 20},
			forWrite:  true,
			expected:  &stale,
		},
		{
			name:      "within delta",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 8},
			clientPC:  matching.PartitionCounts{Read: 10, Write: 10},
			forWrite:  true,
			expected:  nil,
		},
		{
			name:      "within ratio (delta exceeds)",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 100, Write: 100},
			clientPC:  matching.PartitionCounts{Read: 110, Write: 110},
			forWrite:  true,
			expected:  nil,
		},
		{
			name:      "both delta and ratio exceed",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 4, Write: 4},
			clientPC:  matching.PartitionCounts{Read: 10, Write: 10},
			forWrite:  true,
			expected:  &stale,
		},
		{
			name:      "read path compares read counts",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			clientPC:  matching.PartitionCounts{Read: 20, Write: 4},
			forWrite:  false,
			expected:  &stale,
		},
		{
			name:      "write path compares write counts",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 8, Write: 4},
			clientPC:  matching.PartitionCounts{Read: 20, Write: 4},
			forWrite:  true,
			expected:  nil,
		},
		{
			name:      "client counts below server",
			scaleInfo: &taskqueuespb.PartitionScaleInfo{Read: 20, Write: 20},
			clientPC:  matching.PartitionCounts{Read: 4, Write: 4},
			forWrite:  true,
			expected:  &stale,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePartitionCountDifference(tc.scaleInfo, tc.forWrite, tc.clientPC, difference)
			if tc.expected == nil {
				require.NoError(t, err)
			} else {
				require.ErrorAs(t, err, tc.expected)
			}
		})
	}
}
