// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
