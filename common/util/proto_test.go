package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
