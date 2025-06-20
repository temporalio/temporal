package tasks

// State represents the current state of a task
type State int

const (
	// TaskStatePending is the state for a task when it's waiting to be processed or currently being processed
	TaskStatePending State = iota + 1
	// TaskStateAborted is the state for a task when its executor shuts down
	TaskStateAborted
	// TaskStateCancelled is the state for a task when its execution has request to be cancelled
	TaskStateCancelled
	// TaskStateAcked is the state for a task if it has been successfully completed
	TaskStateAcked
	// TaskStateNacked is the state for a task if it can not be processed
	TaskStateNacked
)
