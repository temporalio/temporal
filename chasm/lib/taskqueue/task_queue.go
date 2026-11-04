package taskqueue

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/taskqueue/gen/taskqueuepb/v1"
)

// TaskQueue is the CHASM root component for one task queue family.
type TaskQueue struct {
	chasm.UnimplementedComponent
	*taskqueuepb.TaskQueueState
	UserData chasm.Field[*UserData]
}

// NewTaskQueue creates a task queue family without user data.
func NewTaskQueue() *TaskQueue {
	return &TaskQueue{TaskQueueState: &taskqueuepb.TaskQueueState{}}
}

func (t *TaskQueue) LifecycleState(chasm.Context) chasm.LifecycleState {
	if t.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (*TaskQueue) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

func (t *TaskQueue) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	if t.Closed {
		return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("task queue is closed")
	}
	t.Closed = true
	return chasm.TerminateComponentResponse{}, nil
}
