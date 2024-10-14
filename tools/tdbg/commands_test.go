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

package tdbg

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/tasks"
)

func TestGetCategory(t *testing.T) {
	cat101 := tasks.NewCategory(101, tasks.CategoryTypeImmediate, "CategoryName")

	registry := tasks.NewDefaultTaskCategoryRegistry()
	registry.AddCategory(cat101)

	testCases := []struct {
		name        string
		input       string
		expectedCat tasks.Category
		expectedErr string
	}{
		{
			name:        "same case",
			input:       "CategoryName",
			expectedCat: cat101,
		},
		{
			name:        "different case",
			input:       "cAtEgOrYnAmE",
			expectedCat: cat101,
		},
		{
			name:        "not found",
			input:       "random",
			expectedErr: "unknown task category \"random\"",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := require.New(t)
			cat, err := getCategory(registry, tc.input)
			if tc.expectedErr == "" {
				s.NoError(err)
				s.Equal(tc.expectedCat, cat)
			} else {
				s.ErrorContains(err, tc.expectedErr)
			}
		})
	}
}
