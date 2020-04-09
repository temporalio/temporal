package cli

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestAdminAddSearchAttribute_isValueTypeValid(t *testing.T) {
	testCases := []struct {
		name     string
		input    int
		expected bool
	}{
		{
			name:     "negative",
			input:    -1,
			expected: false,
		},
		{
			name:     "valid",
			input:    0,
			expected: true,
		},
		{
			name:     "valid",
			input:    5,
			expected: true,
		},
		{
			name:     "unknown",
			input:    6,
			expected: false,
		},
	}

	for _, testCase := range testCases {
		assert.Equal(t, testCase.expected, isValueTypeValid(testCase.input))
	}
}
