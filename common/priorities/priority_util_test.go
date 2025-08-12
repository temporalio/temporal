package priorities

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
		{
			name:     "fairness key is overriden",
			base:     defaultPriority,
			override: &commonpb.Priority{FairnessKey: "one"},
			expected: &commonpb.Priority{FairnessKey: "one"},
		},
		{
			name:     "fairness key is not overriden by default value",
			base:     &commonpb.Priority{FairnessKey: "two"},
			override: defaultPriority,
			expected: &commonpb.Priority{FairnessKey: "two"},
		},
		{
			name:     "fairness weight is overriden",
			base:     defaultPriority,
			override: &commonpb.Priority{FairnessWeight: 3.0},
			expected: &commonpb.Priority{FairnessWeight: 3.0},
		},
		{
			name:     "fairness weight is not overriden by default value",
			base:     &commonpb.Priority{FairnessWeight: 3.0},
			override: defaultPriority,
			expected: &commonpb.Priority{FairnessWeight: 3.0},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			actual := Merge(tc.base, tc.override)
			require.EqualExportedValues(t, tc.expected, actual)
		})
	}
}

func TestValidate(t *testing.T) {
	testcases := []struct {
		p   *commonpb.Priority
		err bool
	}{
		{p: &commonpb.Priority{}},
		{p: &commonpb.Priority{PriorityKey: 5}},
		{p: &commonpb.Priority{PriorityKey: -5}, err: true},
		{p: &commonpb.Priority{FairnessKey: "abcdefg"}},
		{p: &commonpb.Priority{FairnessKey: strings.Repeat("abcdefg", 10)}, err: true},
		{p: &commonpb.Priority{FairnessWeight: 0.1}},
		{p: &commonpb.Priority{FairnessWeight: 1e10}},
		{p: &commonpb.Priority{FairnessWeight: -3}, err: true},
	}

	for _, tc := range testcases {
		t.Run("test", func(t *testing.T) {
			err := Validate(tc.p)
			if tc.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
