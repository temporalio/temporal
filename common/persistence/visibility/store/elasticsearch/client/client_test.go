package client

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
)

func Test_BuildPutMappingBody(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		input    map[string]enumspb.IndexedValueType
		expected string
	}{
		{
			input:    map[string]enumspb.IndexedValueType{"Field": enumspb.INDEXED_VALUE_TYPE_TEXT},
			expected: "map[properties:map[Field:map[type:text]]]",
		},
		{
			input:    map[string]enumspb.IndexedValueType{"Field": enumspb.INDEXED_VALUE_TYPE_KEYWORD},
			expected: "map[properties:map[Field:map[type:keyword]]]",
		},
		{
			input:    map[string]enumspb.IndexedValueType{"Field": enumspb.INDEXED_VALUE_TYPE_INT},
			expected: "map[properties:map[Field:map[type:long]]]",
		},
		{
			input:    map[string]enumspb.IndexedValueType{"Field": enumspb.INDEXED_VALUE_TYPE_DOUBLE},
			expected: "map[properties:map[Field:map[scaling_factor:10000 type:scaled_float]]]",
		},
		{
			input:    map[string]enumspb.IndexedValueType{"Field": enumspb.INDEXED_VALUE_TYPE_BOOL},
			expected: "map[properties:map[Field:map[type:boolean]]]",
		},
		{
			input:    map[string]enumspb.IndexedValueType{"Field": enumspb.INDEXED_VALUE_TYPE_DATETIME},
			expected: "map[properties:map[Field:map[type:date_nanos]]]",
		},
		{
			input:    map[string]enumspb.IndexedValueType{"Field": enumspb.IndexedValueType(-1)},
			expected: "map[properties:map[]]",
		},
	}

	for _, test := range tests {
		assert.Equal(test.expected, fmt.Sprintf("%v", buildMappingBody(test.input)))
	}
}
