package callback

import (
	"time"
)

const (
	TaskTypeInvocation = "chasm.callbacks.Invocation"
	TaskTypeBackoff    = "chasm.callbacks.Backoff"
)

type InvocationTask struct {
	// The base URL for nexus callbacks.
	// Will have other meanings as more callback use cases are added.
	destination string
	attempt     int32
}

func NewInvocationTask(destination string) InvocationTask {
	return InvocationTask{destination: destination}
}

type BackoffTask struct {
	deadline time.Time
}

func NewBackoffTask(deadline time.Time) BackoffTask {
	return BackoffTask{deadline: deadline}
}
