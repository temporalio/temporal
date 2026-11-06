package activity

import (
	"context"
	"errors"
	"sync"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
)

type releaseLimiterTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*activitypb.ReleaseLimiterTask]
	concurrencyServiceClient fcpb.ConcurrencyServiceClient
}

func newReleaseLimiterTaskHandler(
	concurrencyServiceClient fcpb.ConcurrencyServiceClient,
) *releaseLimiterTaskHandler {
	return &releaseLimiterTaskHandler{
		concurrencyServiceClient: concurrencyServiceClient,
	}
}

func (h *releaseLimiterTaskHandler) Validate(
	chasm.Context,
	*Activity,
	chasm.TaskInvocation,
	*activitypb.ReleaseLimiterTask,
) (bool, error) {
	return true, nil
}

func (h *releaseLimiterTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *activitypb.ReleaseLimiterTask,
) error {
	limiters := task.GetLimiters()
	errs := make([]error, len(limiters))
	var wg sync.WaitGroup
	for i, limiter := range limiters {
		release := func() {
			switch limiter.GetLimiterType() {
			case enumsspb.LIMITER_TYPE_CONCURRENCY:
				_, errs[i] = h.concurrencyServiceClient.Batch(ctx, &fcpb.ConcurrencyServiceBatchRequest{
					NamespaceId:  activityRef.NamespaceID,
					Key:          limiter.GetKey(),
					ReleaseSlots: []string{limiter.GetSlotId()},
				})
			default:
				errs[i] = serviceerror.NewInternalf("unknown limiter type %v", limiter.GetLimiterType())
			}
		}
		if i == len(limiters)-1 {
			release()
		} else {
			wg.Go(release)
		}
	}
	wg.Wait()
	return errors.Join(errs...)
}
