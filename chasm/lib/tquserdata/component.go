package tquserdata

import (
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.temporal.io/server/common"
)

// TaskQueueUserData is the CHASM component for one task queue family's user data.
type TaskQueueUserData struct {
	chasm.UnimplementedComponent
	*tquserdatapb.TaskQueueUserDataState
	PerType chasm.Map[int32, *persistencespb.TaskQueueTypeUserData]
}

func (t *TaskQueueUserData) LifecycleState(chasm.Context) chasm.LifecycleState {
	if t.Closed {
		return chasm.LifecycleStateCompleted
	}
	return chasm.LifecycleStateRunning
}

func (*TaskQueueUserData) ContextMetadata(chasm.Context) map[string]string {
	return nil
}

func (t *TaskQueueUserData) Terminate(chasm.MutableContext, chasm.TerminateComponentRequest) (chasm.TerminateComponentResponse, error) {
	if t.Closed {
		return chasm.TerminateComponentResponse{}, serviceerror.NewFailedPrecondition("task queue user data is closed")
	}
	t.Closed = true
	return chasm.TerminateComponentResponse{}, nil
}

func (t *TaskQueueUserData) getTaskQueueUserData(ctx chasm.Context) *tquserdatapb.TaskQueueUserData {
	data := &tquserdatapb.TaskQueueUserData{
		Clock:   t.Clock,
		PerType: make(map[int32]*persistencespb.TaskQueueTypeUserData, len(t.PerType)),
	}
	for taskQueueType, field := range t.PerType {
		data.PerType[taskQueueType] = field.Get(ctx)
	}
	return common.CloneProto(data)
}

func (t *TaskQueueUserData) setTaskQueueUserData(ctx chasm.MutableContext, data *tquserdatapb.TaskQueueUserData) {
	data = common.CloneProto(data)
	t.Clock = data.Clock
	t.PerType = make(chasm.Map[int32, *persistencespb.TaskQueueTypeUserData], len(data.PerType))
	for taskQueueType, typeData := range data.PerType {
		t.PerType[taskQueueType] = chasm.NewDataField(ctx, typeData)
	}
}
