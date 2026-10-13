package sqlplugin

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/searchattribute/sadefs"
)

func TestDbFields_LastField_Version(t *testing.T) {
	lastField := DbFields[len(DbFields)-1]
	require.Equal(t, VersionColumnName, lastField)
}

func TestParseCountGroupByGroupValue(t *testing.T) {
	testCases := []struct {
		name      string
		fieldName string
		value     any
		expected  any
	}{
		{
			name:      "namespace division value",
			fieldName: sadefs.TemporalNamespaceDivision,
			value:     []byte("divisionA"),
			expected:  "divisionA",
		},
		{
			// Default-division workflows have a NULL division and must be
			// normalized to the empty string to match Elasticsearch's
			// Missing("") behavior.
			name:      "namespace division null",
			fieldName: sadefs.TemporalNamespaceDivision,
			value:     nil,
			expected:  "",
		},
		{
			// NULL for other fields is left untouched (out of scope).
			name:      "other field null passes through",
			fieldName: "SomeKeyword",
			value:     nil,
			expected:  nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseCountGroupByGroupValue(tc.fieldName, tc.value)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}
