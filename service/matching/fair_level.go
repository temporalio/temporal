package matching

import (
	"fmt"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// fairLevel is a pair of "pass" and "id". Pass is a number that can be assigned to cause
// levels to be ordered to preserve fairness. Id is a unique task id. fairLevels are ordered
// lexicographically.
type fairLevel struct {
	pass int64
	id   int64
}

func (l fairLevel) String() string {
	return fmt.Sprintf("<%d,%d>", l.pass, l.id)
}

func fairLevelLess(a, b fairLevel) bool {
	return a.pass < b.pass || a.pass == b.pass && a.id < b.id
}

func fairLevelComparator(aany, bany any) int {
	a := aany.(fairLevel) // nolint:revive
	b := bany.(fairLevel) // nolint:revive
	if fairLevelLess(a, b) {
		return -1
	} else if fairLevelLess(b, a) {
		return 1
	}
	return 0
}

func fairLevelMax(a, b fairLevel) fairLevel {
	if fairLevelLess(a, b) {
		return b
	}
	return a
}

func fairLevelPlusOne(a fairLevel) fairLevel {
	return fairLevel{pass: a.pass, id: a.id + 1}
}

func allocatedTaskFairLevel(t *persistencespb.AllocatedTaskInfo) fairLevel {
	return fairLevel{pass: t.PassNumber, id: t.TaskId}
}
