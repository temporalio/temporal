package tquserdata

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
)

// TaskQueueUserData is the CHASM component for one task queue family's user data.
type TaskQueueUserData struct {
	chasm.UnimplementedComponent
	*tquserdatapb.TaskQueueUserDataState
	Data chasm.Field[*tquserdatapb.TaskQueueUserData]
}

func (*TaskQueueUserData) LifecycleState(chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

func (*TaskQueueUserData) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

func (*TaskQueueUserData) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	// TODO: Terminate task queue user data when task queue termination is implemented, since its lifecycle is tied to the task queue.
	return chasm.TerminateComponentResponse{}, serviceerror.NewUnimplemented("task queue user data termination is not implemented")
}
