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
			override: commonpb.Priority_builder{PriorityKey: 5}.Build(),
			expected: commonpb.Priority_builder{PriorityKey: 5}.Build(),
		},
		{
			name:     "priority key is not overriden by default value",
			base:     commonpb.Priority_builder{PriorityKey: 1}.Build(),
			override: defaultPriority,
			expected: commonpb.Priority_builder{PriorityKey: 1}.Build(),
		},
		{
			name:     "fairness key is overriden",
			base:     defaultPriority,
			override: commonpb.Priority_builder{FairnessKey: "one"}.Build(),
			expected: commonpb.Priority_builder{FairnessKey: "one"}.Build(),
		},
		{
			name:     "fairness key is not overriden by default value",
			base:     commonpb.Priority_builder{FairnessKey: "two"}.Build(),
			override: defaultPriority,
			expected: commonpb.Priority_builder{FairnessKey: "two"}.Build(),
		},
		{
			name:     "fairness weight is overriden",
			base:     defaultPriority,
			override: commonpb.Priority_builder{FairnessWeight: 3.0}.Build(),
			expected: commonpb.Priority_builder{FairnessWeight: 3.0}.Build(),
		},
		{
			name:     "fairness weight is not overriden by default value",
			base:     commonpb.Priority_builder{FairnessWeight: 3.0}.Build(),
			override: defaultPriority,
			expected: commonpb.Priority_builder{FairnessWeight: 3.0}.Build(),
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
		{p: commonpb.Priority_builder{PriorityKey: 5}.Build()},
		{p: commonpb.Priority_builder{PriorityKey: -5}.Build(), err: true},
		{p: commonpb.Priority_builder{FairnessKey: "abcdefg"}.Build()},
		{p: commonpb.Priority_builder{FairnessKey: strings.Repeat("abcdefg", 10)}.Build(), err: true},
		{p: commonpb.Priority_builder{FairnessWeight: 0.1}.Build()},
		{p: commonpb.Priority_builder{FairnessWeight: 1e10}.Build()},
		{p: commonpb.Priority_builder{FairnessWeight: -3}.Build(), err: true},
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
