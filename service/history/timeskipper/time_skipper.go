package timeskipper

import (
	"errors"
	"sync"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/service/history/tasks"
)

type (

	// TimeSkipperType defines the type of time skipping behavior
	TimeSkipperType int32

	// TimeSkipper holds information about time skipping for a workflow execution.
	// This allows workflows to skip forward in time while maintaining correct timer behavior.
	TimeSkipper struct {
		NamespaceID string
		SkipperID   string
		SkipperType TimeSkipperType
		Enabled     bool
		Unlocked    bool

		SkippedDurations   []time.Duration
		BaseTimeSource     clock.TimeSource
		AdjustedTimeSource clock.TimeSource

		ExecutionKey chasm.ExecutionKey
		Mutex        sync.Mutex
	}
)

// When this is an independent time skipper,
// the execution key is used to find the shard of the time skipper
func NewIndependentTimeSkipper(
	executionKey chasm.ExecutionKey,
	baseTimeSource clock.TimeSource,
) *TimeSkipper {
	adjustedTimeSource := clock.NewTimeSkippingTimeSource(baseTimeSource)
	return &TimeSkipper{
		NamespaceID:        executionKey.NamespaceID,
		ExecutionKey:       executionKey,
		SkipperType:        TimeSkipperTypeIndependent,
		Enabled:            false,
		Unlocked:           false,
		BaseTimeSource:     baseTimeSource,
		AdjustedTimeSource: adjustedTimeSource,
	}
}

func NewTimeSkipper() *TimeSkipper {
	return &TimeSkipper{
		Enabled:          false,
		Unlocked:         false,
		SkippedDurations: make([]time.Duration, 0),
	}
}

// PumpOnce is the main entry point for the time skipper
// when time skipper should be triggered, this method will be called
func (ts *TimeSkipper) PumpOnce() {
	// simplified process just for demo
	if !ts.Enabled {
		return
	}
	ts.lockAllExecutions()
	defer ts.unlockAllExecutions()

	if !ts.isAutoSkippable() {
		return
	}
	nextTimerTask, err := ts.findNextTimerTaskToSkip()
	if nextTimerTask == nil {
		return
	}
	if err != nil {
		return
	}
	ts.addTimeSkippedEvents(nextTimerTask)
	ts.updateMutableState(nextTimerTask)
}

// KEY-STEP-0: acquire per-execution lock
func (ts *TimeSkipper) lockAllExecutions() error {
	// acquire execution lock of all executions (will this make deadlock with other operations?)
	return errors.New("not implemented")
}

func (ts *TimeSkipper) unlockAllExecutions() error {
	return errors.New("not implemented")
}

// KEY-STEP-1: check if time skipping is auto-skippable
func (ts *TimeSkipper) isAutoSkippable() bool {
	if ts.isTimeSkipperUnlocked() {
		return true
	}
	// todo:
	// check IsAutoSkippable for each execution
	return false
}

// KEY-STEP-2: get the next timer task to skip
func (ts *TimeSkipper) findNextTimerTaskToSkip() (tasks.Task, error) {
	return nil, errors.New("not implemented")
}

// KEY-STEP-2: add events to all these executions
func (ts *TimeSkipper) addTimeSkippedEvents(nextTimerTask tasks.Task) error {
	return errors.New("not implemented")
}

// KEY-STEP-3: update mutable state of all these executions
func (ts *TimeSkipper) updateMutableState(nextTimerTask tasks.Task) error {
	// KEY-STEP-4: the most important step
	// in-memory change:
	// -update the time source of all these executions
	// -update the skipped durations of all these executions
	// -update the pending timer tasks of all these executions
	// persistence change:
	// -update the change into persistence
	return errors.New("not implemented")
}

func (ts *TimeSkipper) isTimeSkipperUnlocked() bool {
	return ts.Unlocked
}

// SetExecutionUnlocked sets the unlocked state for a specific execution
func (ts *TimeSkipper) Unlock(key chasm.ExecutionKey) {
	ts.Unlocked = true
}

func (ts *TimeSkipper) Lock(key chasm.ExecutionKey) {
	ts.Unlocked = false
}

const (
	// TimeSkipperTypeIndependent means this workflow skips time independently
	TimeSkipperTypeIndependent TimeSkipperType = iota

	// TimeSkipperTypeGroup means this workflow is part of a group that skips time together
	TimeSkipperTypeGroup
)

// String returns a string representation of TimeSkipperType
func (t TimeSkipperType) String() string {
	switch t {
	case TimeSkipperTypeIndependent:
		return "Independent"
	case TimeSkipperTypeGroup:
		return "Group"
	default:
		return "Unknown"
	}
}
