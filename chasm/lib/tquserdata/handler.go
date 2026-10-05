package tquserdata

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
)

type handler struct {
	tquserdatapb.UnimplementedTaskQueueUserDataServiceServer
	metricsHandler metrics.Handler
}

func newHandler(metricsHandler metrics.Handler) *handler {
	return &handler{metricsHandler: metricsHandler}
}

func (h *handler) GetTaskQueueUserData(
	ctx context.Context,
	req *tquserdatapb.GetTaskQueueUserDataRequest,
) (_ *tquserdatapb.GetTaskQueueUserDataResponse, retErr error) {
	defer func() {
		outcome := "success"
		if retErr != nil {
			outcome = "error"
			if _, ok := errors.AsType[*serviceerror.NotFound](retErr); ok {
				outcome = "missing"
			}
		}
		metrics.TaskQueueUserDataChasmRead.With(h.metricsHandler).Record(1, metrics.OutcomeTag(outcome))
	}()
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" || req.GetBusinessId() != BusinessID(req.GetTaskQueue()) {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data read request")
	}
	return chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*UserData](chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetBusinessId()}),
		func(userData *UserData, chasmContext chasm.Context, _ *tquserdatapb.GetTaskQueueUserDataRequest) (*tquserdatapb.GetTaskQueueUserDataResponse, error) {
			return &tquserdatapb.GetTaskQueueUserDataResponse{
				UserData: proto.Clone(userData.Data.Get(chasmContext)).(*persistencespb.TaskQueueUserData),
				Version:  userData.Version,
			}, nil
		},
		req,
	)
}

func (h *handler) UpsertTaskQueueUserData(
	ctx context.Context,
	req *tquserdatapb.UpsertTaskQueueUserDataRequest,
) (_ *tquserdatapb.UpsertTaskQueueUserDataResponse, retErr error) {
	defer func() {
		reason := metrics.ReasonString("update")
		if req.GetExpectMissing() {
			reason = "missing"
		}
		outcome := "success"
		if retErr != nil {
			outcome = "error"
			if _, ok := errors.AsType[*serviceerror.FailedPrecondition](retErr); ok {
				outcome = "conflict"
			}
		}
		metrics.TaskQueueUserDataChasmWrite.With(h.metricsHandler).Record(1, metrics.ReasonTag(reason), metrics.OutcomeTag(outcome))
	}()
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" || req.GetUserData() == nil ||
		req.GetBusinessId() != BusinessID(req.GetTaskQueue()) {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data write request")
	}
	return h.writeUserData(ctx, req)
}

func (*handler) writeUserData(ctx context.Context, req *tquserdatapb.UpsertTaskQueueUserDataRequest) (*tquserdatapb.UpsertTaskQueueUserDataResponse, error) {
	key := chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetBusinessId()}
	if req.GetExpectMissing() {
		_, err := chasm.StartExecution(
			ctx,
			key,
			func(mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (*UserData, error) {
				return &UserData{
					UserDataState: &tquserdatapb.UserDataState{Version: 1},
					Data:          chasm.NewDataField(mutableContext, proto.Clone(req.GetUserData()).(*persistencespb.TaskQueueUserData)),
				}, nil
			},
			req,
			chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyFail),
		)
		if _, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
			return nil, serviceerror.NewFailedPrecondition("task queue user data already exists")
		}
		if err != nil {
			return nil, err
		}
		return &tquserdatapb.UpsertTaskQueueUserDataResponse{Version: 1}, nil
	}
	condition, ok := req.GetPrecondition().(*tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion)
	if !ok {
		return nil, serviceerror.NewInvalidArgument("missing task queue user data write precondition")
	}
	if condition.ExpectedVersion < 0 {
		return nil, serviceerror.NewInvalidArgument("expected task queue user data version must not be negative")
	}
	response, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*UserData](key),
		func(userData *UserData, mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (*tquserdatapb.UpsertTaskQueueUserDataResponse, error) {
			if condition.ExpectedVersion != userData.Version {
				return nil, serviceerror.NewFailedPrecondition("task queue user data version changed")
			}
			userData.Version++
			userData.Data = chasm.NewDataField(mutableContext, proto.Clone(req.GetUserData()).(*persistencespb.TaskQueueUserData))
			return &tquserdatapb.UpsertTaskQueueUserDataResponse{Version: userData.Version}, nil
		},
		req,
	)
	return response, err
}
