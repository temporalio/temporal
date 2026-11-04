package taskqueue

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/taskqueue/gen/taskqueuepb/v1"
	"go.temporal.io/server/common"
	"google.golang.org/protobuf/proto"
)

// UserData is the CHASM component for one task queue family's user data.
type UserData struct {
	chasm.UnimplementedComponent
	*taskqueuepb.UserDataState
	PerType chasm.Map[int32, *persistencespb.TaskQueueTypeUserData]
}

// NewUserData creates a task queue family's user data at CHASM CAS version 1.
func NewUserData(ctx chasm.MutableContext, data *taskqueuepb.UpsertTaskQueueUserDataRequest) *UserData {
	userData := &UserData{
		UserDataState: &taskqueuepb.UserDataState{Version: 1},
	}
	userData.setTaskQueueUserData(ctx, data)
	return userData
}

func (*UserData) LifecycleState(chasm.Context) chasm.LifecycleState {
	// CHASM uses the root's lifecycle for execution closure; this method must return an enum, so it returns Running.
	return chasm.LifecycleStateRunning
}

func (t *UserData) getTaskQueueUserData(ctx chasm.Context) *taskqueuepb.GetTaskQueueUserDataResponse {
	data := &taskqueuepb.GetTaskQueueUserDataResponse{
		Version: t.Version,
		Clock:   t.Clock,
		PerType: make(map[int32]*persistencespb.TaskQueueTypeUserData, len(t.PerType)),
	}
	for taskQueueType, field := range t.PerType {
		data.PerType[taskQueueType] = field.Get(ctx)
	}
	return common.CloneProto(data)
}

func (t *UserData) setTaskQueueUserData(ctx chasm.MutableContext, data *taskqueuepb.UpsertTaskQueueUserDataRequest) {
	t.Clock = common.CloneProto(data.Clock)
	if t.PerType == nil {
		t.PerType = make(chasm.Map[int32, *persistencespb.TaskQueueTypeUserData], len(data.PerType))
	}
	for taskQueueType, typeData := range data.PerType {
		if proto.Size(typeData) == 0 {
			continue
		}
		if field, ok := t.PerType[taskQueueType]; ok {
			storedData := field.Get(ctx)
			proto.Reset(storedData)
			proto.Merge(storedData, typeData)
		} else {
			t.PerType[taskQueueType] = chasm.NewDataField(ctx, common.CloneProto(typeData))
		}
	}
}
