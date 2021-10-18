// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package client

import (
	"fmt"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func Test_ConvertV7Sorters(t *testing.T) {
	sortersV7 := make([]elastic.Sorter, 2)
	sortersV7[0] = elastic.NewFieldSort("test")
	sortersV7[1] = elastic.NewFieldSort("test2")

	sortersV6 := convertV7SortersToV6(sortersV7)
	require.NotNil(t, sortersV6[0])
	source0, err0 := sortersV6[0].Source()
	require.NoError(t, err0)
	require.NotNil(t, source0)
}

func TestIsResponseRetryable(t *testing.T) {
	status := []int{408, 429, 500, 503, 507}
	for _, code := range status {
		require.True(t, IsRetryableStatus(code))
	}
}
