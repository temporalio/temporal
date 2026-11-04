package wci

import (
	"slices"
	"strings"

	wcimetrics "go.temporal.io/auto-scaled-workers/wci/metrics"
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
)

// taskDestination keys a group's compute tasks for the outbound queue's circuit breaker: per provider, and per
// region set for region-scoped groups, so one region's failing provider doesn't hold up another's.
func taskDestination(spec *wcipb.ScalingGroupSpec) string {
	provider := spec.GetCompute().GetProviderType()
	if len(spec.GetRegionIds()) == 0 {
		return provider
	}
	regions := slices.Clone(spec.GetRegionIds())
	slices.Sort(regions)
	return provider + "/" + strings.Join(regions, "+")
}

// requestInvoke starts an invoke, or queues it behind the one in flight (at most one per group).
func (w *WorkerControllerInstance) requestInvoke(ctx chasm.MutableContext, g *ScalingGroup, destination string) {
	if g.InflightInvokeId != 0 {
		g.PendingInvokes++
		return
	}
	g.startInvoke(ctx, destination)
}

// startInvoke keys the invoke by this transaction (+1 so it is never 0, which means none in flight).
func (g *ScalingGroup) startInvoke(ctx chasm.MutableContext, destination string) {
	g.InflightInvokeId = ctx.ExecutionInfo().StateTransitionCount + 1
	g.InflightInvokeAttempt = 1
	g.scheduleInvoke(ctx, destination)
}

func (g *ScalingGroup) scheduleInvoke(ctx chasm.MutableContext, destination string) {
	ctx.AddTask(g, chasm.TaskAttributes{Destination: destination}, &wcipb.InvokeTask{
		RequestId: g.InflightInvokeId,
		Attempt:   g.InflightInvokeAttempt,
	})
}

// finishInvoke records an invoke result. Every outcome changes (inflight_invoke_id, inflight_invoke_attempt),
// so the running task is invalid at close and singleton Ignore never drops its successor.
func (w *WorkerControllerInstance) finishInvoke(ctx chasm.MutableContext, g *ScalingGroup, providerType, destination string, err error) {
	// One emission per invoke outcome; retried attempts are not counted, as with the workflow's activity.
	if err == nil || !retryable(err) || g.InflightInvokeAttempt >= maxInvokeAttempts {
		recordOperation(metricsFor(ctx, w, providerType), wcimetrics.OperationTypeInvokeWorker, computeErrorType(err))
	}
	switch {
	case err == nil:
		if g.PendingInvokes > 0 {
			g.PendingInvokes--
			g.startInvoke(ctx, destination)
			return
		}
		g.InflightInvokeId = 0
		g.InflightInvokeAttempt = 0
	case retryable(err) && g.InflightInvokeAttempt < maxInvokeAttempts:
		g.InflightInvokeAttempt++
		g.scheduleInvoke(ctx, destination)
	default:
		g.PendingInvokes = 0
		g.InflightInvokeId = 0
		g.InflightInvokeAttempt = 0
		w.recordValidation(ctx, err)
	}
}
