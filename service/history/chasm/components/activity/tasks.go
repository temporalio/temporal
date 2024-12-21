package activity

import (
	"context"

	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/service/history/chasm"
	"go.temporal.io/server/service/history/consts"
)

// The intent of a Task is always Progress

type DispatchTask struct{}

type DispatchTaskHandler struct {
	matchingClient matchingservice.MatchingServiceClient
}

func (h *DispatchTaskHandler) Validate(
	chasmContext chasm.Context,
	activity *Activity,
	task *DispatchTask,
) error {
	if !activity.State.StartedTime.AsTime().IsZero() {
		return consts.ErrStaleReference
	}

	return nil
}

func (h *DispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	t *DispatchTask,
) error {
	addTaskRequest, _, err := chasm.UpdateComponent(
		ctx,
		activityRef,
		(*Activity).GetDispatchInfo,
		t,
	)
	if err != nil {
		return err
	}

	_, err = h.matchingClient.AddActivityTask(ctx, addTaskRequest)
	return err
}

const (
	TimeoutTypeScheduleToStart = iota
	TimeoutTypeStartToClose
)

type TimeoutTask struct {
	// similar to component state,
	// use a proto message if possible
	TimeoutType int
}

type TimeoutTaskHandler struct{}

func (h *TimeoutTaskHandler) Validate(
	chasmContext chasm.Context,
	activity *Activity,
	task *TimeoutTask,
) error {
	panic("not implemented")
}

func (h *TimeoutTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	t *TimeoutTask,
) error {
	panic("not implemented")
}
