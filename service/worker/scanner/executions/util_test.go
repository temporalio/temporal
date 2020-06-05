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
		input        Shards
		expectedList []int
		expectedMin  int
		expectedMax  int
	}{
		{
			input: Shards{
				List: []int{1, 2, 3},
			},
			expectedList: []int{1, 2, 3},
			expectedMin:  1,
			expectedMax:  3,
		},
		{
			input: Shards{
				Range: &ShardRange{
					Min: 5,
					Max: 10,
				},
			},
			expectedList: []int{5, 6, 7, 8, 9},
			expectedMin:  5,
			expectedMax:  9,
		},
		{
			input: Shards{
				List: []int{90, 1, 2, 3},
			},
			expectedList: []int{90, 1, 2, 3},
			expectedMin:  1,
			expectedMax:  90,
		},
	}
	for _, tc := range testCases {
		shardList, min, max := flattenShards(tc.input)
		s.Equal(tc.expectedList, shardList)
		s.Equal(tc.expectedMin, min)
		s.Equal(tc.expectedMax, max)
	}
}

func (s *workflowsSuite) TestResolveFixerConfig() {
	result := resolveFixerConfig(FixerWorkflowConfigOverwrites{
		Concurrency: common.IntPtr(1000),
	})
	s.Equal(ResolvedFixerWorkflowConfig{
		Concurrency:             1000,
		BlobstoreFlushThreshold: 1000,
		ActivityBatchSize:       200,
		InvariantCollections: InvariantCollections{
			InvariantCollectionMutableState: true,
			InvariantCollectionHistory:      true,
		},
	}, result)
}

func (s *workflowsSuite) TestValidateShards() {
	testCases := []struct {
		shards    Shards
		expectErr bool
	}{
		{
			shards:    Shards{},
			expectErr: true,
		},
		{
			shards: Shards{
				List:  []int{},
				Range: &ShardRange{},
			},
			expectErr: true,
		},
		{
			shards: Shards{
				List: []int{},
			},
			expectErr: true,
		},
		{
			shards: Shards{
				Range: &ShardRange{
					Min: 0,
					Max: 0,
				},
			},
			expectErr: true,
		},
		{
			shards: Shards{
				Range: &ShardRange{
					Min: 0,
					Max: 1,
				},
			},
			expectErr: false,
		},
	}

	for _, tc := range testCases {
		err := validateShards(tc.shards)
		if tc.expectErr {
			s.Error(err)
		} else {
			s.NoError(err)
		}
	}
}

func (s *workflowsSuite) TestGetBatchIndices() {
	testCases := []struct {
		batchSize   int
		concurrency int
		sliceLength int
		workerIdx   int
		batches     [][]int
	}{
		{
			batchSize:   1,
			concurrency: 1,
			sliceLength: 20,
			workerIdx:   0,
			batches:     [][]int{{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}, {12}, {13}, {14}, {15}, {16}, {17}, {18}, {19}},
		},
		{
			batchSize:   2,
			concurrency: 1,
			sliceLength: 20,
			workerIdx:   0,
			batches:     [][]int{{0, 1}, {2, 3}, {4, 5}, {6, 7}, {8, 9}, {10, 11}, {12, 13}, {14, 15}, {16, 17}, {18, 19}},
		},
		{
			batchSize:   7,
			concurrency: 1,
			sliceLength: 20,
			workerIdx:   0,
			batches:     [][]int{{0, 1, 2, 3, 4, 5, 6}, {7, 8, 9, 10, 11, 12, 13}, {14, 15, 16, 17, 18, 19}},
		},
		{
			batchSize:   5,
			concurrency: 3,
			sliceLength: 20,
			workerIdx:   0,
			batches:     [][]int{{0, 3, 6, 9, 12}, {15, 18}},
		},
		{
			batchSize:   5,
			concurrency: 3,
			sliceLength: 20,
			workerIdx:   1,
			batches:     [][]int{{1, 4, 7, 10, 13}, {16, 19}},
		},
		{
			batchSize:   5,
			concurrency: 3,
			sliceLength: 20,
			workerIdx:   2,
			batches:     [][]int{{2, 5, 8, 11, 14}, {17}},
		},
	}

	for _, tc := range testCases {
		s.Equal(tc.batches, getBatchIndices(tc.batchSize, tc.concurrency, tc.sliceLength, tc.workerIdx))
	}
}

func (s *workflowsSuite) TestGetShardBatches() {
	var shards []int
	for i := 5; i < 50; i += 2 {
		shards = append(shards, i)
	}
	batches := getShardBatches(5, 3, shards, 1)
	s.Equal([][]int{
		{7, 13, 19, 25, 31},
		{37, 43, 49},
	}, batches)
}

func (s *workflowsSuite) TestGetCorruptedKeysBatches() {
	var keys []CorruptedKeysEntry
	for i := 5; i < 50; i += 2 {
		keys = append(keys, CorruptedKeysEntry{
			ShardID: i,
		})
	}
	batches := getCorruptedKeysBatches(5, 3, keys, 1)
	s.Equal([][]CorruptedKeysEntry{
		{
			{ShardID: 7},
			{ShardID: 13},
			{ShardID: 19},
			{ShardID: 25},
			{ShardID: 31},
		},
		{
			{ShardID: 37},
			{ShardID: 43},
			{ShardID: 49},
		},
	}, batches)
}
