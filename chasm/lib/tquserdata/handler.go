package tquserdata

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/proto"
)

var errUnchanged = errors.New("task queue user data CHASM mirror is unchanged")

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
		} else if req.GetExpectedClock() != nil {
			reason = "db_newer"
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
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" || req.GetUserData() == nil || req.GetLegacyVersion() <= 0 ||
		req.GetBusinessId() != BusinessID(req.GetTaskQueue()) {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data mirror request")
	}
	if req.GetPrecondition() != nil {
		if err := h.seedUserData(ctx, req); err != nil {
			return nil, err
		}
		return &tquserdatapb.UpsertTaskQueueUserDataResponse{}, nil
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

func (*handler) seedUserData(ctx context.Context, req *tquserdatapb.UpsertTaskQueueUserDataRequest) error {
	key := chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetBusinessId()}
	if req.GetExpectMissing() {
		_, err := chasm.StartExecution(
			ctx,
			key,
			func(mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (*UserData, error) {
				return &UserData{
					UserDataState: &tquserdatapb.UserDataState{LegacyVersion: req.GetLegacyVersion()},
					Data:          chasm.NewDataField(mutableContext, proto.Clone(req.GetUserData()).(*persistencespb.TaskQueueUserData)),
				}, nil
			},
			req,
			chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyRejectDuplicate, chasm.BusinessIDConflictPolicyFail),
		)
		if _, ok := errors.AsType[*chasm.ExecutionAlreadyStartedError](err); ok {
			return serviceerror.NewFailedPrecondition("task queue user data already exists")
		}
		return err
	}
	condition := req.GetExpectedClock()
	if condition == nil {
		return serviceerror.NewInvalidArgument("missing task queue user data clock precondition")
	}
	_, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*UserData](key),
		func(userData *UserData, mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (struct{}, error) {
			currentClock := userData.Data.Get(mutableContext).GetClock()
			if !proto.Equal(currentClock, condition.GetClock()) {
				return struct{}{}, serviceerror.NewFailedPrecondition("task queue user data clock changed")
			}
			incomingClock := req.GetUserData().GetClock()
			if incomingClock == nil || (currentClock != nil && !hybrid_logical_clock.Greater(incomingClock, currentClock)) {
				return struct{}{}, serviceerror.NewFailedPrecondition("task queue user data clock must advance")
			}
			userData.LegacyVersion = req.GetLegacyVersion()
			userData.Data = chasm.NewDataField(mutableContext, proto.Clone(req.GetUserData()).(*persistencespb.TaskQueueUserData))
			return struct{}{}, nil
		},
		req,
	)
	return err
}
