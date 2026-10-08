package tquserdata

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
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

func (h *handler) GetTaskQueueUserDataSnapshot(
	ctx context.Context,
	req *tquserdatapb.GetTaskQueueUserDataSnapshotRequest,
) (_ *tquserdatapb.GetTaskQueueUserDataSnapshotResponse, retErr error) {
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
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data read request")
	}
	return chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueueUserData](chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetTaskQueue()}),
		func(taskQueueUserData *TaskQueueUserData, chasmContext chasm.Context, _ *tquserdatapb.GetTaskQueueUserDataSnapshotRequest) (*tquserdatapb.GetTaskQueueUserDataSnapshotResponse, error) {
			return &tquserdatapb.GetTaskQueueUserDataSnapshotResponse{
				TaskQueueUserData: proto.Clone(taskQueueUserData.Data.Get(chasmContext)).(*tquserdatapb.TaskQueueUserData),
				Version:           taskQueueUserData.Version,
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
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" || req.GetTaskQueueUserData() == nil {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data write request")
	}
	switch condition := req.GetPrecondition().(type) {
	case *tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectMissing:
		if !condition.ExpectMissing {
			return nil, serviceerror.NewInvalidArgument("missing task queue user data write precondition")
		}
	case *tquserdatapb.UpsertTaskQueueUserDataRequest_ExpectedVersion:
		if condition.ExpectedVersion < 0 {
			return nil, serviceerror.NewInvalidArgument("expected task queue user data version must not be negative")
		}
	default:
		return nil, serviceerror.NewInvalidArgument("missing task queue user data write precondition")
	}
	key := chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetTaskQueue()}
	if req.GetExpectMissing() {
		_, err := chasm.StartExecution(
			ctx,
			key,
			func(mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (*TaskQueueUserData, error) {
				return &TaskQueueUserData{
					TaskQueueUserDataState: &tquserdatapb.TaskQueueUserDataState{Version: 1},
					Data:                   chasm.NewDataField(mutableContext, proto.Clone(req.GetTaskQueueUserData()).(*tquserdatapb.TaskQueueUserData)),
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
	response, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueueUserData](key),
		func(taskQueueUserData *TaskQueueUserData, mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (*tquserdatapb.UpsertTaskQueueUserDataResponse, error) {
			if req.GetExpectedVersion() != taskQueueUserData.Version {
				return nil, serviceerror.NewFailedPrecondition("task queue user data version changed")
			}
			taskQueueUserData.Version++
			taskQueueUserData.Data = chasm.NewDataField(mutableContext, proto.Clone(req.GetTaskQueueUserData()).(*tquserdatapb.TaskQueueUserData))
			return &tquserdatapb.UpsertTaskQueueUserDataResponse{Version: taskQueueUserData.Version}, nil
		},
		req,
	)
	return response, err
}
