package matching

import (
	"fmt"

	"github.com/emirpasic/gods/maps/treemap"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
)

// fairLevel is a pair of "pass" and "id". Pass is a number that can be assigned to cause
// levels to be ordered to preserve fairness. Id is a unique task id. fairLevels are ordered
// lexicographically.
type fairLevel struct {
	pass int64
	id   int64
}

func (a fairLevel) String() string {
	return fmt.Sprintf("<%d,%d>", a.pass, a.id)
}

// Returns true if a < b lexicographically.
func (a fairLevel) less(b fairLevel) bool {
	return a.pass < b.pass || a.pass == b.pass && a.id < b.id
}

func newFairLevelTreeMap() *treemap.Map {
	return treemap.NewWith(func(aany, bany any) int {
		a, b := aany.(fairLevel), bany.(fairLevel) // nolint:revive
		if a.less(b) {
			return -1
		} else if b.less(a) {
			return 1
		}
		return 0
	})
}

// Returns the max of a and b.
func (a fairLevel) max(b fairLevel) fairLevel {
	if a.less(b) {
		return b
	}
	return a
}

// Returns the next highest fair level.
func (a fairLevel) inc() fairLevel {
	return fairLevel{pass: a.pass, id: a.id + 1}
}

func fairLevelFromAllocatedTask(t *persistencespb.AllocatedTaskInfo) fairLevel {
	return fairLevel{pass: t.TaskPass, id: t.TaskId}
}

func fairLevelFromProto(l *taskqueuespb.FairLevel) fairLevel {
	if l == nil {
		return fairLevel{}
	}
	return fairLevel{pass: l.TaskPass, id: l.TaskId}
}

func (a fairLevel) toProto() *taskqueuespb.FairLevel {
	if (a == fairLevel{}) {
		return nil
	}
	return &taskqueuespb.FairLevel{TaskPass: a.pass, TaskId: a.id}
}
