package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertPathToCamel(t *testing.T) {
	testCases := []struct {
		input          string
		expectedOutput []string
	}{
		{
			input:          "underscore",
			expectedOutput: []string{"underscore"},
		},
		{
			input:          "CamelCase",
			expectedOutput: []string{"camelCase"},
		},
		{
			input:          "UPPERCASE",
			expectedOutput: []string{"uPPERCASE"},
		},
		{
			input:          "Dot.Separated",
			expectedOutput: []string{"dot", "separated"},
		},
		{
			input:          "dash_separated",
			expectedOutput: []string{"dashSeparated"},
		},
		{
			input:          "dash_separated.and_another",
			expectedOutput: []string{"dashSeparated", "andAnother"},
		},
		{
			input:          "Already.CamelCase",
			expectedOutput: []string{"already", "camelCase"},
		},
		{
			input:          "Mix.of.Snake_case.and.CamelCase",
			expectedOutput: []string{"mix", "of", "snakeCase", "and", "camelCase"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			actualOutput := ConvertPathToCamel(tc.input)
			assert.Equal(t, tc.expectedOutput, actualOutput)
		})
	}
}

func TestFieldMaskHasSubPath(t *testing.T) {
	fieldMaskPaths := map[string]struct{}{
		"retryPolicy":                 {},
		"retryPolicy.initialInterval": {},
		"retryPolicy.maximumAttempts": {},
		"retryPolicyExtra.field":      {},
	}

	require.True(t, FieldMaskHasSubPath(fieldMaskPaths, "retryPolicy"))
	require.False(t, FieldMaskHasSubPath(map[string]struct{}{"retryPolicy": {}}, "retryPolicy"))
	require.False(t, FieldMaskHasSubPath(fieldMaskPaths, "retryPolicy.initialInterval"))
	require.False(t, FieldMaskHasSubPath(map[string]struct{}{"retryPolicyExtra.field": {}}, "retryPolicy"))
}

func TestFieldMaskHasPathOrSubPath(t *testing.T) {
	fieldMaskPaths := map[string]struct{}{
		"retryPolicy":          {},
		"priority.fairnessKey": {},
		"taskQueue.name":       {},
	}

	require.True(t, FieldMaskHasPathOrSubPath(fieldMaskPaths, "retryPolicy"))
	require.True(t, FieldMaskHasPathOrSubPath(fieldMaskPaths, "priority"))
	require.False(t, FieldMaskHasPathOrSubPath(fieldMaskPaths, "retryPolicy.initialInterval"))
}
