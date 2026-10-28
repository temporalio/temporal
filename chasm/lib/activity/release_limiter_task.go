package activity

import (
	"context"
	"errors"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/flowcontrol/concurrency"
	fcpb "go.temporal.io/server/chasm/lib/flowcontrol/gen/flowcontrolpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
)

type releaseLimiterTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*activitypb.ReleaseLimiterTask]
	concurrencyServiceClient fcpb.ConcurrencyServiceClient
	metricsHandler           metrics.Handler
	timeSource               clock.TimeSource
}

func newReleaseLimiterTaskHandler(
	concurrencyServiceClient fcpb.ConcurrencyServiceClient,
	config *Config,
	metricsHandler metrics.Handler,
) *releaseLimiterTaskHandler {
	return &releaseLimiterTaskHandler{
		metricsHandler: metricsHandler,
		timeSource:     clock.NewRealTimeSource(),
		concurrencyServiceClient: concurrency.NewBatchingClient(
			concurrencyServiceClient,
			config.FlowControlClientBatcherOptions(),
			clock.NewRealTimeSource(),
			nil, // history only releases, it doesn't cache limiter state
		),
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
	var releaseErrors []error
	for _, limiter := range task.GetLimiters() {
		switch limiter.GetLimiterType() {
		case enumsspb.LIMITER_TYPE_CONCURRENCY:
			_, err := h.concurrencyServiceClient.Batch(ctx, &fcpb.ConcurrencyServiceBatchRequest{
				NamespaceId:  activityRef.NamespaceID,
				Key:          limiter.GetKey(),
				ReleaseSlots: []string{limiter.GetSlotId()},
			})
			if err != nil {
				releaseErrors = append(releaseErrors, err)
			} else if task.GetScheduledAt() != nil {
				concurrency.RecordReleaseLatency(h.metricsHandler, activityRef.NamespaceID, limiter.GetKey(), h.timeSource.Now().Sub(task.GetScheduledAt().AsTime()))
			}
		default:
		}
	}
	return errors.Join(releaseErrors...)
}
