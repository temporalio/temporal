package tquserdata

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"google.golang.org/protobuf/proto"
)

var errUnchanged = errors.New("task queue user data CHASM mirror is unchanged")

type handler struct {
	tquserdatapb.UnimplementedTaskQueueUserDataServiceServer
}

func newHandler() *handler {
	return &handler{}
}

func (*handler) UpsertTaskQueueUserData(
	ctx context.Context,
	req *tquserdatapb.UpsertTaskQueueUserDataRequest,
) (*tquserdatapb.UpsertTaskQueueUserDataResponse, error) {
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" || req.GetUserData() == nil || req.GetLegacyVersion() <= 0 ||
		req.GetBusinessId() != BusinessID(req.GetTaskQueue()) {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data mirror request")
	}

	_, err := chasm.UpdateWithStartExecution(
		ctx,
		chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetBusinessId()},
		func(mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (*UserData, error) {
			return &UserData{
				UserDataState: &tquserdatapb.UserDataState{},
				Data:          chasm.NewDataField(mutableContext, &persistencespb.TaskQueueUserData{}),
			}, nil
		},
		func(userData *UserData, mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (struct{}, error) {
			if req.GetLegacyVersion() < userData.LegacyVersion {
				return struct{}{}, errUnchanged
			}
			if req.GetLegacyVersion() == userData.LegacyVersion {
				if proto.Equal(userData.Data.Get(mutableContext), req.GetUserData()) {
					return struct{}{}, errUnchanged
				}
				return struct{}{}, serviceerror.NewFailedPrecondition("same legacy user data version has a different payload")
			}
			userData.LegacyVersion = req.GetLegacyVersion()
			userData.Data = chasm.NewDataField(mutableContext, proto.Clone(req.GetUserData()).(*persistencespb.TaskQueueUserData))
			return struct{}{}, nil
		},
		req,
	)
	if errors.Is(err, errUnchanged) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return &tquserdatapb.UpsertTaskQueueUserDataResponse{}, nil
}
