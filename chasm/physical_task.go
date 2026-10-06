package chasm

import (
	"math"
	"reflect"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
)

// TaskCategory is the history task queue that a physical side effect task is written to.
type TaskCategory int

const (
	TaskCategoryTransfer TaskCategory = iota
	TaskCategoryTimer
	TaskCategoryOutbound
	TaskCategoryVisibility
)

// PhysicalSideEffectTask is the physical task that executes one side effect task of a
// component. The backend persists it in the history task queue for its [TaskCategory].
type PhysicalSideEffectTask struct {
	VisibilityTimestamp time.Time
	Destination         string // Set for outbound tasks.
	Info                *persistencespb.ChasmTaskInfo

	// In-memory only
	DeserializedTask reflect.Value
	// Attempt is the current processing attempt for this physical task, starting at 1. It is copied
	// from the task executable before execution or validation and is not persisted. Surfaced to
	// CHASM handlers via TaskAttributes.Attempt.
	Attempt int
}

// PhysicalPureTask is the physical task that executes the pure tasks of an execution that are
// due at VisibilityTimestamp. The backend persists it in the timer queue.
type PhysicalPureTask struct {
	VisibilityTimestamp time.Time
	ArchetypeID         ArchetypeID
}

// maxPureTaskScheduledTime is later than the scheduled time of any physical pure task.
var maxPureTaskScheduledTime = time.Unix(0, math.MaxInt64)
