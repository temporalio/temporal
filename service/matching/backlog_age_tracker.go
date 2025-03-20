// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package matching

import (
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	godsutils "github.com/emirpasic/gods/utils"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const emptyBacklogAge time.Duration = -1

// backlogAgeTracker is not safe for concurrent use
type backlogAgeTracker struct {
	tree treemap.Map // unix nano as int64 -> int (count)
}

func newBacklogAgeTracker() backlogAgeTracker {
	return backlogAgeTracker{tree: *treemap.NewWith(godsutils.Int64Comparator)}
}

// record adds or removes a task from the tracker.
func (b backlogAgeTracker) record(ts *timestamppb.Timestamp, delta int) {
	if ts == nil {
		return
	}

	createTime := ts.AsTime().UnixNano()
	count := delta
	if prev, ok := b.tree.Get(createTime); ok {
		count += prev.(int) // nolint:revive
	}
	if count = max(0, count); count == 0 {
		b.tree.Remove(createTime)
	} else {
		b.tree.Put(createTime, count)
	}
}

// oldestTime returns the time of the oldest task in this backlog, or
// the zero Time if empty.
func (b backlogAgeTracker) oldestTime() time.Time {
	if b.tree.Empty() {
		return time.Time{}
	}
	k, _ := b.tree.Min()
	return time.Unix(0, k.(int64)) // nolint:revive
}

// minNonZeroTime returns the minimum time of a and b, ignoring zero times.
// If both a and b are zero, it returns zero.
func minNonZeroTime(a, b time.Time) time.Time {
	if a.IsZero() {
		return b
	} else if b.IsZero() {
		return a
	}
	return util.MinTime(a, b)
}
