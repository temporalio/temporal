// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package executions

import "github.com/uber/cadence/common"

func (s *workflowsSuite) TestFlattenShards() {
	testCases := []struct {
		input    Shards
		expected []int
	}{
		{
			input: Shards{
				List: []int{1, 2, 3},
			},
			expected: []int{1, 2, 3},
		},
		{
			input: Shards{
				Range: &ShardRange{
					Min: 5,
					Max: 10,
				},
			},
			expected: []int{5, 6, 7, 8, 9},
		},
	}
	for _, tc := range testCases {
		s.Equal(tc.expected, flattenShards(tc.input))
	}
}

func (s *workflowsSuite) TestResolveFixerConfig() {
	result := resolveFixerConfig(FixerWorkflowConfigOverwrites{
		Concurrency: common.IntPtr(1000),
	})
	s.Equal(ResolvedFixerWorkflowConfig{
		Concurrency:             1000,
		BlobstoreFlushThreshold: 1000,
		InvariantCollections: InvariantCollections{
			InvariantCollectionMutableState: true,
			InvariantCollectionHistory:      true,
		},
	}, result)
}
