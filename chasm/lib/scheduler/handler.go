package scheduler

import (
	"context"
	"errors"
	"fmt"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
)

type handler struct {
	schedulerpb.UnimplementedSchedulerServiceServer

	logger      log.Logger
	specBuilder *legacyscheduler.SpecBuilder
}

func newHandler(logger log.Logger, specBuilder *legacyscheduler.SpecBuilder) *handler {
	return &handler{
		logger:      logger,
		specBuilder: specBuilder,
	}
}

// convertError converts CHASM engine errors to appropriate serviceerrors.
func convertError(err error, scheduleID string) error {
	if err == nil {
		return nil
	}

	var alreadyStartedErr *chasm.ExecutionAlreadyStartedError
	if errors.As(err, &alreadyStartedErr) {
		return serviceerror.NewAlreadyExists(
			fmt.Sprintf("schedule %q is already registered", scheduleID),
		)
	}

	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return serviceerror.NewNotFound("schedule not found")
	}

	return err
}

func (h *handler) CreateSchedule(ctx context.Context, req *schedulerpb.CreateScheduleRequest) (resp *schedulerpb.CreateScheduleResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	result, err := chasm.NewExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  req.FrontendRequest.ScheduleId,
		},
		CreateScheduler,
		req,
		chasm.WithRequestID(req.FrontendRequest.RequestId),
	)
	return result.Output, convertError(err, req.FrontendRequest.ScheduleId)
}

func (h *handler) UpdateSchedule(ctx context.Context, req *schedulerpb.UpdateScheduleRequest) (resp *schedulerpb.UpdateScheduleResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	resp, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.ExecutionKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.FrontendRequest.ScheduleId,
			},
		),
		(*Scheduler).Update,
		req,
	)
	return resp, convertError(err, req.FrontendRequest.ScheduleId)
}

func (h *handler) PatchSchedule(ctx context.Context, req *schedulerpb.PatchScheduleRequest) (resp *schedulerpb.PatchScheduleResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	resp, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.ExecutionKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.FrontendRequest.ScheduleId,
			},
		),
		(*Scheduler).Patch,
		req,
	)
	return resp, convertError(err, req.FrontendRequest.ScheduleId)
}

func (h *handler) DeleteSchedule(ctx context.Context, req *schedulerpb.DeleteScheduleRequest) (resp *schedulerpb.DeleteScheduleResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	resp, _, err = chasm.UpdateComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.ExecutionKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.FrontendRequest.ScheduleId,
			},
		),
		(*Scheduler).Delete,
		req,
	)
	return resp, convertError(err, req.FrontendRequest.ScheduleId)
}

func (h *handler) DescribeSchedule(ctx context.Context, req *schedulerpb.DescribeScheduleRequest) (resp *schedulerpb.DescribeScheduleResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	resp, err = chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.ExecutionKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.FrontendRequest.ScheduleId,
			},
		),
		func(s *Scheduler, ctx chasm.Context, req *schedulerpb.DescribeScheduleRequest) (*schedulerpb.DescribeScheduleResponse, error) {
			return s.Describe(ctx, req, h.specBuilder)
		},
		req,
	)
	return resp, convertError(err, req.FrontendRequest.ScheduleId)
}

func (h *handler) ListScheduleMatchingTimes(ctx context.Context, req *schedulerpb.ListScheduleMatchingTimesRequest) (resp *schedulerpb.ListScheduleMatchingTimesResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	resp, err = chasm.ReadComponent(
		ctx,
		chasm.NewComponentRef[*Scheduler](
			chasm.ExecutionKey{
				NamespaceID: req.NamespaceId,
				BusinessID:  req.FrontendRequest.ScheduleId,
			},
		),
		func(s *Scheduler, ctx chasm.Context, req *schedulerpb.ListScheduleMatchingTimesRequest) (*schedulerpb.ListScheduleMatchingTimesResponse, error) {
			return s.ListMatchingTimes(ctx, req, h.specBuilder)
		},
		req,
	)
	return resp, convertError(err, req.FrontendRequest.ScheduleId)
}
