package scheduler

import (
	"context"
	"errors"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
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

func (h *handler) CreateSchedule(ctx context.Context, req *schedulerpb.CreateScheduleRequest) (resp *schedulerpb.CreateScheduleResponse, err error) {
	defer log.CapturePanic(h.logger, &err)

	_, err = chasm.StartExecution(
		ctx,
		chasm.ExecutionKey{
			NamespaceID: req.NamespaceId,
			BusinessID:  req.FrontendRequest.ScheduleId,
		},
		CreateScheduler,
		req,
		chasm.WithRequestID(req.FrontendRequest.RequestId),
	)

	var alreadyStartedErr *chasm.ExecutionAlreadyStartedError
	if errors.As(err, &alreadyStartedErr) {
		// Check if the existing schedule is a sentinel.
		//
		// TODO lina@ - this can be removed (as well as all other sentinel business)
		// after fully migrated to CHASM schedulers.
		_, readErr := chasm.ReadComponent(
			ctx,
			chasm.NewComponentRef[*Scheduler](
				chasm.ExecutionKey{
					NamespaceID: req.NamespaceId,
					BusinessID:  req.FrontendRequest.ScheduleId,
				},
			),
			func(s *Scheduler, ctx chasm.Context, _ *struct{}) (*struct{}, error) {
				if s.IsSentinel() {
					return nil, ErrSentinel
				}
				return nil, nil
			},
			(*struct{})(nil),
		)
		if readErr != nil {
			return nil, readErr // Returns ErrSentinel (404) if sentinel
		}
		return nil, serviceerror.NewAlreadyExistsf("schedule %q is already registered", req.FrontendRequest.ScheduleId)
	}

	return &schedulerpb.CreateScheduleResponse{
		FrontendResponse: &workflowservice.CreateScheduleResponse{
			ConflictToken: initialSerializedConflictToken,
		},
	}, err
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
	return resp, err
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
	return resp, err
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
	return resp, err
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
	return resp, err
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
	return resp, err
}
