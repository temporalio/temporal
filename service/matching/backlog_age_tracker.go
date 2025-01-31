package matching

import (
	"time"

	"github.com/emirpasic/gods/maps/treemap"
	godsutils "github.com/emirpasic/gods/utils"
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
		count += prev.(int)
	}
	if count = max(0, count); count == 0 {
		b.tree.Remove(createTime)
	} else {
		b.tree.Put(createTime, count)
	}
}

// getBacklogAge returns the largest age in this backlog (age of oldest task),
// or emptyBacklogAge if empty.
func (b backlogAgeTracker) getAge() time.Duration {
	if b.tree.Empty() {
		return emptyBacklogAge
	}
	k, _ := b.tree.Min()
	oldest := k.(int64)
	return time.Since(time.Unix(0, oldest))
}
