package tquserdata

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/tquserdata/gen/tquserdatapb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type handler struct {
	tquserdatapb.UnimplementedTaskQueueUserDataServiceServer
	logger log.Logger
}

func newHandler(logger log.Logger) *handler {
	return &handler{logger: logger}
}

func (h *handler) GetTaskQueueUserDataSnapshot(
	ctx context.Context,
	req *tquserdatapb.GetTaskQueueUserDataSnapshotRequest,
) (_ *tquserdatapb.GetTaskQueueUserDataSnapshotResponse, retErr error) {
	defer func() {
		if retErr != nil {
			if _, ok := errors.AsType[*serviceerror.NotFound](retErr); !ok {
				h.logger.Error("Failed to read task queue user data from CHASM",
					tag.Error(retErr),
					tag.WorkflowNamespaceID(req.GetNamespaceId()),
					tag.WorkflowTaskQueueName(req.GetTaskQueue()),
				)
			}
		}
	}()
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data read request")
	}
	return chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueueUserData](chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetTaskQueue()}),
		func(taskQueueUserData *TaskQueueUserData, chasmContext chasm.Context, _ *tquserdatapb.GetTaskQueueUserDataSnapshotRequest) (*tquserdatapb.GetTaskQueueUserDataSnapshotResponse, error) {
			return &tquserdatapb.GetTaskQueueUserDataSnapshotResponse{
				TaskQueueUserData: taskQueueUserData.getTaskQueueUserData(chasmContext),
				Version:           taskQueueUserData.Version,
			}, nil
		},
		req,
	)
}

func (h *handler) UpsertTaskQueueUserData(
	ctx context.Context,
	req *tquserdatapb.UpsertTaskQueueUserDataRequest,
) (*tquserdatapb.UpsertTaskQueueUserDataResponse, error) {
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" || req.GetTaskQueueUserData() == nil {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data write request")
	}
	if req.GetExpectedVersion() < 0 {
		return nil, serviceerror.NewInvalidArgument("expected task queue user data version must not be negative")
	}
	key := chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetTaskQueue()}
	if req.GetExpectedVersion() == 0 {
		_, err := chasm.StartExecution(
			ctx,
			key,
			func(mutableContext chasm.MutableContext, _ *tquserdatapb.UpsertTaskQueueUserDataRequest) (*TaskQueueUserData, error) {
				return NewTaskQueueUserData(mutableContext, req.GetTaskQueueUserData()), nil
			},
			req,
			chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyFail),
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
			if taskQueueUserData.Closed {
				return nil, serviceerror.NewFailedPrecondition("task queue user data is closed")
			}
			if req.GetExpectedVersion() != taskQueueUserData.Version {
				return nil, serviceerror.NewFailedPrecondition("task queue user data version changed")
			}
			taskQueueUserData.Version++
			taskQueueUserData.setTaskQueueUserData(mutableContext, req.GetTaskQueueUserData())
			return &tquserdatapb.UpsertTaskQueueUserDataResponse{Version: taskQueueUserData.Version}, nil
		},
		req,
	)
	return response, err
}
