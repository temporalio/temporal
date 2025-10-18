package scheduler

import (
	"context"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

type handler struct {
	schedulerpb.UnimplementedSchedulerServiceServer
}

func newHandler() *handler {
	return &handler{}
}

func (h *handler) CreateSchedule(ctx context.Context, req *schedulerpb.CreateScheduleRequest) (*schedulerpb.CreateScheduleResponse, error) {
	resp, _, _, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  req.Request.ScheduleId,
		},
		Create,
		req,
		chasm.WithRequestID(req.Request.RequestId),
	)
	return resp, err
}

func (h *handler) UpdateSchedule(ctx context.Context, req *schedulerpb.UpdateScheduleRequest) (*schedulerpb.UpdateScheduleResponse, error) {
	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.EntityKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.Request.ScheduleId,
			},
		),
		(*Scheduler).Update,
		req,
	)
	return resp, err
}

func (h *handler) PatchSchedule(ctx context.Context, req *schedulerpb.PatchScheduleRequest) (*schedulerpb.PatchScheduleResponse, error) {
	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.EntityKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.Request.ScheduleId,
			},
		),
		(*Scheduler).Patch,
		req,
	)
	return resp, err
}

func (h *handler) DeleteSchedule(ctx context.Context, req *schedulerpb.DeleteScheduleRequest) (*schedulerpb.DeleteScheduleResponse, error) {
	resp, _, err := chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.EntityKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.Request.ScheduleId,
			},
		),
		(*Scheduler).Delete,
		req,
	)
	return resp, err
}

func (h *handler) DescribeSchedule(ctx context.Context, req *schedulerpb.DescribeScheduleRequest) (*schedulerpb.DescribeScheduleResponse, error) {
	return chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.EntityKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.Request.ScheduleId,
			},
		),
		(*Scheduler).Describe,
		req,
	)
}
