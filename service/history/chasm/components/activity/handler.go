package activity

import (
	"context"
	"time"

	"go.temporal.io/server/service/history/chasm"
)

// This will be nexus
type ActivityHandler struct {
}

type NewActivityRequest struct {
	Input []byte

	notifier EventNotifier
}

type NewActivityResponse struct {
	RefToken []byte
}

type ScheduleRequest struct {
	Input []byte
}
type ScheduleResponse struct{}

type RecordStartedRequest struct {
	RefToken []byte
}

type RecordStartedResponse struct {
	RefToken []byte
	Input    []byte
}

type RecordCompletedRequest struct {
	RefToken []byte
	Output   []byte
}

type RecordCompletedResponse struct{}

type DescribeActivityRequest struct{}

type DescribeActivityResponse struct {
	IsAbandonded  bool
	StartedTime   time.Time
	CompletedTime time.Time
}

func (h *ActivityHandler) NewActivity(
	ctx context.Context,
	request *NewActivityRequest,
) (*NewActivityResponse, error) {
	resp, activityRef, err := chasm.NewEntity(
		ctx,
		chasm.EntityKey{
			NamespaceID: "default",
			BusinessID:  "memo",
			// in V1 we probably don't support specifying instanceID,
			// need to change persistence implementation for supporting that.
			// InstanceID:  uuid.New().String(),
		},
		NewScheduledActivity,
		request,
		chasm.WithBusinessIDPolicy(
			chasm.BusinessIDReusePolicyAllowDuplicate,
			chasm.BusinessIDConflictPolicyFail,
		),
	)
	if err != nil {
		return nil, err
	}

	resp.RefToken = activityRef
	return resp, err
}

func (h *ActivityHandler) RecordStarted(
	ctx context.Context,
	request *RecordStartedRequest,
) (*RecordStartedResponse, error) {
	// resp := &RecordStartedResponse{}

	resp, startedActivityRefToken, err := chasm.UpdateComponent(
		ctx,
		request.RefToken,
		(*Activity).RecordStarted,
		request,
		// chasm.EngineEagerLoadOption([]chasm.ComponentPath{
		// 	{"Input"},
		// }),
	)

	resp.RefToken = startedActivityRefToken
	return resp, err
}

func (h *ActivityHandler) RecordCompleted(
	ctx context.Context,
	request *RecordCompletedRequest,
) (*RecordCompletedResponse, error) {
	resp, _, err := chasm.UpdateComponent(
		ctx,
		request.RefToken,
		(*Activity).RecordCompleted,
		request,
	)

	return resp, err
}

type GetActivityResultRequest struct {
	RefToken []byte
}

type GetActivityResultResponse struct {
	Output []byte
}

func (h *ActivityHandler) GetActivityResult(
	ctx context.Context,
	request *GetActivityResultRequest,
) (*GetActivityResultResponse, error) {

	var resp *GetActivityResultResponse
	resp, _, err := chasm.PollComponent(
		ctx,
		request.RefToken,
		func(a *Activity, ctx chasm.Context, _ *GetActivityResultRequest) bool {
			return a.LifecycleState() == chasm.LifecycleStateCompleted
		},
		func(a *Activity, ctx chasm.MutableContext, _ *GetActivityResultRequest) (*GetActivityResultResponse, error) {
			outputPayload, err := a.Output.Get(ctx)
			resp.Output = outputPayload.Data
			return resp, err
		},
		request,
	)
	return resp, err
}
