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

func NewShardClock(
	shardID int32,
	clock int64,
) *clockspb.ShardClock {
	return &clockspb.ShardClock{
		Id:    shardID,
		Clock: clock,
	}
}

func HappensBefore(
	clock1 *clockspb.ShardClock,
	clock2 *clockspb.ShardClock,
) (bool, error) {
	compare, err := Compare(clock1, clock2)
	if err != nil {
		return false, err
	}
	return compare < 0, nil
}

func Compare(
	clock1 *clockspb.ShardClock,
	clock2 *clockspb.ShardClock,
) (int, error) {
	if clock1.GetId() != clock2.GetId() {
		return 0, serviceerror.NewInternal(fmt.Sprintf(
			"Encountered shard ID mismatch: %v vs %v",
			clock1.GetId(),
			clock2.GetId(),
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
