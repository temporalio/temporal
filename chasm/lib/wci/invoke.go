package wci

import (
	"go.temporal.io/server/chasm"
	wcipb "go.temporal.io/server/chasm/lib/wci/gen/wcipb/v1"
)

// requestInvoke starts an invoke, or queues it behind the one in flight (at most one per group).
func (w *WorkerControllerInstance) requestInvoke(ctx chasm.MutableContext, g *ScalingGroup, providerType string) {
	if g.InflightInvokeId != 0 {
		g.PendingInvokes++
		return
	}
	g.startInvoke(ctx, providerType)
}

// startInvoke keys the invoke by this transaction (+1 so it is never 0, which means none in flight).
func (g *ScalingGroup) startInvoke(ctx chasm.MutableContext, providerType string) {
	g.InflightInvokeId = ctx.ExecutionInfo().StateTransitionCount + 1
	g.InflightInvokeAttempt = 1
	g.scheduleInvoke(ctx, providerType)
}

func (g *ScalingGroup) scheduleInvoke(ctx chasm.MutableContext, providerType string) {
	ctx.AddTask(g, chasm.TaskAttributes{Destination: providerType}, &wcipb.InvokeTask{
		RequestId: g.InflightInvokeId,
		Attempt:   g.InflightInvokeAttempt,
	})
}

// finishInvoke records an invoke result. Every outcome changes (inflight_invoke_id, inflight_invoke_attempt),
// so the running task is invalid at close and singleton Ignore never drops its successor.
func (w *WorkerControllerInstance) finishInvoke(ctx chasm.MutableContext, g *ScalingGroup, providerType string, err error) {
	switch {
	case err == nil:
		if g.PendingInvokes > 0 {
			g.PendingInvokes--
			g.startInvoke(ctx, providerType)
			return
		}
		g.InflightInvokeId = 0
		g.InflightInvokeAttempt = 0
	case retryable(err) && g.InflightInvokeAttempt < maxInvokeAttempts:
		g.InflightInvokeAttempt++
		g.scheduleInvoke(ctx, providerType)
	default:
		g.PendingInvokes = 0
		g.InflightInvokeId = 0
		g.InflightInvokeAttempt = 0
		w.recordValidation(ctx, err)
	}
}
