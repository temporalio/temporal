package taskqueue

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/taskqueue/gen/taskqueuepb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type handler struct {
	taskqueuepb.UnimplementedTaskQueueUserDataServiceServer
	logger log.Logger
}

func newHandler(logger log.Logger) *handler {
	return &handler{logger: logger}
}

func (h *handler) GetTaskQueueUserData(
	ctx context.Context,
	req *taskqueuepb.GetTaskQueueUserDataRequest,
) (_ *taskqueuepb.GetTaskQueueUserDataResponse, retErr error) {
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
		chasm.NewComponentRef[*TaskQueue](chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetTaskQueue()}),
		func(taskQueue *TaskQueue, chasmContext chasm.Context, _ *taskqueuepb.GetTaskQueueUserDataRequest) (*taskqueuepb.GetTaskQueueUserDataResponse, error) {
			if taskQueue.Closed {
				return nil, serviceerror.NewNotFound("task queue user data is terminated")
			}
			userData, ok := taskQueue.UserData.TryGet(chasmContext)
			if !ok {
				return nil, serviceerror.NewNotFound("task queue user data does not exist")
			}
			return userData.getTaskQueueUserData(chasmContext), nil
		},
		req,
	)
}

func (h *handler) UpsertTaskQueueUserData(
	ctx context.Context,
	req *taskqueuepb.UpsertTaskQueueUserDataRequest,
) (*taskqueuepb.UpsertTaskQueueUserDataResponse, error) {
	if req.GetNamespaceId() == "" || req.GetTaskQueue() == "" {
		return nil, serviceerror.NewInvalidArgument("invalid task queue user data write request")
	}
	if len(req.GetPerType()) == 0 {
		return nil, serviceerror.NewInvalidArgument("task queue user data per-type map must not be empty")
	}
	if req.GetExpectedVersion() < 0 {
		return nil, serviceerror.NewInvalidArgument("expected task queue user data version must not be negative")
	}
	key := chasm.ExecutionKey{NamespaceID: req.GetNamespaceId(), BusinessID: req.GetTaskQueue()}
	if req.GetExpectedVersion() == 0 {
		result, err := chasm.StartExecution(
			ctx,
			key,
			func(mutableContext chasm.MutableContext, _ *taskqueuepb.UpsertTaskQueueUserDataRequest) (*TaskQueue, error) {
				taskQueue := NewTaskQueue()
				taskQueue.UserData = chasm.NewComponentField(mutableContext, NewUserData(mutableContext, req))
				return taskQueue, nil
			},
			req,
			chasm.WithBusinessIDPolicy(chasm.BusinessIDReusePolicyAllowDuplicate, chasm.BusinessIDConflictPolicyUseExisting),
		)
		if err != nil {
			return nil, err
		}
		if result.Created {
			return &taskqueuepb.UpsertTaskQueueUserDataResponse{Version: 1}, nil
		}
	}
	response, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*TaskQueue](key),
		func(taskQueue *TaskQueue, mutableContext chasm.MutableContext, _ *taskqueuepb.UpsertTaskQueueUserDataRequest) (*taskqueuepb.UpsertTaskQueueUserDataResponse, error) {
			if taskQueue.Closed {
				return nil, serviceerror.NewFailedPrecondition("task queue user data is closed")
			}
			userData, ok := taskQueue.UserData.TryGet(mutableContext)
			if req.GetExpectedVersion() == 0 {
				if ok {
					return nil, serviceerror.NewFailedPrecondition("task queue user data already exists")
				}
				taskQueue.UserData = chasm.NewComponentField(mutableContext, NewUserData(mutableContext, req))
				return &taskqueuepb.UpsertTaskQueueUserDataResponse{Version: 1}, nil
			}
			if !ok {
				return nil, serviceerror.NewNotFound("task queue user data does not exist")
			}
			if req.GetExpectedVersion() != userData.Version {
				return nil, serviceerror.NewFailedPrecondition("task queue user data version changed")
			}
			userData.Version++
			userData.setTaskQueueUserData(mutableContext, req)
			return &taskqueuepb.UpsertTaskQueueUserDataResponse{Version: userData.Version}, nil
		},
		req,
	)
	return response, err
}
