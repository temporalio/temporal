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

package replication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetPollingShardIds(t *testing.T) {
	testCases := []struct {
		shardID          int32
		remoteShardCount int32
		localShardCount  int32
		expectedShardIDs []int32
	}{
		{
			1,
			4,
			4,
			[]int32{1},
		},
		{
			1,
			2,
			4,
			[]int32{1},
		},
		{
			3,
			2,
			4,
			nil,
		},
		{
			1,
			16,
			4,
			[]int32{1, 5, 9, 13},
		},
		{
			4,
			16,
			4,
			[]int32{4, 8, 12, 16},
		},
		{
			4,
			17,
			4,
			[]int32{4, 8, 12, 16},
		},
		{
			1,
			17,
			4,
			[]int32{1, 5, 9, 13, 17},
		},
	}
	for idx, tt := range testCases {
		t.Run(fmt.Sprintf("Testcase %d", idx), func(t *testing.T) {
			shardIDs := generateShardIDs(tt.shardID, tt.localShardCount, tt.remoteShardCount)
			assert.Equal(t, tt.expectedShardIDs, shardIDs)
		})
	}
}
