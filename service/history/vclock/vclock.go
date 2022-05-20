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

package vclock

import (
	"fmt"

	"go.temporal.io/api/serviceerror"

	clockspb "go.temporal.io/server/api/clock/v1"
)

func NewVectorClock(
	clusterID int64,
	shardID int32,
	clock int64,
) *clockspb.VectorClock {
	return &clockspb.VectorClock{
		ClusterId: clusterID,
		ShardId:   shardID,
		Clock:     clock,
	}
}

func Comparable(
	clock1 *clockspb.VectorClock,
	clock2 *clockspb.VectorClock,
) bool {
	if clock1 == nil || clock2 == nil {
		return false
	}
	return clock1.GetClusterId() == clock2.GetClusterId() &&
		clock1.GetShardId() == clock2.GetShardId()
}

func Compare(
	clock1 *clockspb.VectorClock,
	clock2 *clockspb.VectorClock,
) (int, error) {
	if !Comparable(clock1, clock2) {
		return 0, serviceerror.NewInternal(fmt.Sprintf(
			"Encountered shard ID mismatch: %v:%v vs %v:%v",
			clock1.GetClusterId(),
			clock1.GetShardId(),
			clock2.GetClusterId(),
			clock2.GetShardId(),
		))
	}

	vClock1 := clock1.GetClock()
	vClock2 := clock2.GetClock()
	if vClock1 < vClock2 {
		return -1, nil
	} else if vClock1 > vClock2 {
		return 1, nil
	} else {
		return 0, nil
	}
}
