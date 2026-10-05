package tquserdata

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
)

// TaskQueueUserData is the CHASM mirror of one root task queue's persisted user data.
type TaskQueueUserData struct {
	chasm.UnimplementedComponent
	*tquserdatapb.UserDataState
	Data chasm.Field[*persistencespb.TaskQueueUserData]
}

func (u *TaskQueueUserData) LifecycleState(chasm.Context) chasm.LifecycleState {
	if u.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (*TaskQueueUserData) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

func (u *TaskQueueUserData) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	u.Closed = true
	return chasm.TerminateComponentResponse{}, nil
}
