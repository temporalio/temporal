package priorities

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

func TestMerge(t *testing.T) {
	defaultPriority := &commonpb.Priority{}

	testcases := []struct {
		name     string
		base     *commonpb.Priority
		override *commonpb.Priority
		expected *commonpb.Priority
	}{
		{
			name:     "all nil",
			base:     nil,
			override: nil,
			expected: nil,
		},
		{
			name:     "base is nil",
			base:     defaultPriority,
			override: nil,
			expected: defaultPriority,
		},
		{
			name:     "override is nil",
			base:     nil,
			override: defaultPriority,
			expected: defaultPriority,
		},
		{
			name:     "priority key is overriden",
			base:     defaultPriority,
			override: &commonpb.Priority{PriorityKey: 5},
			expected: &commonpb.Priority{PriorityKey: 5},
		},
		{
			name:     "priority key is not overriden by default value",
			base:     &commonpb.Priority{PriorityKey: 1},
			override: defaultPriority,
			expected: &commonpb.Priority{PriorityKey: 1},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := Merge(tc.base, tc.override)
			require.EqualExportedValues(t, tc.expected, actual)
		})
	}
}
