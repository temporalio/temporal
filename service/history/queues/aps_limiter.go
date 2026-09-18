package queues

import (
	"context"
	"sync"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/quotas"
)

// APSLimiter is benchmark scaffolding, not a production feature. It stands in for the SaaS
// action-rate limiter so a live cluster can be driven into namespace APS throttling on demand,
// which is what gives the throttle controller something to pace.
//
// The bucket is per (host, namespace): this is an fx singleton per history process, so on N
// history pods the aggregate ceiling is N times the configured RPS. Production APS is
// distributed and has one global budget, so never read a multi-pod result here as equivalent.
type (
	APSLimiterOptions struct {
		Enabled           dynamicconfig.BoolPropertyFn
		RPS               dynamicconfig.IntPropertyFn
		ActivityTasksOnly dynamicconfig.BoolPropertyFn
	}

	apsExecutorWrapper struct {
		options APSLimiterOptions

		mu       sync.Mutex
		limiters map[string]*quotas.RateLimiterImpl
	}

	apsExecutor struct {
		delegate Executor
		wrapper  *apsExecutorWrapper
	}
)

func NewAPSExecutorWrapper(options APSLimiterOptions) ExecutorWrapper {
	return &apsExecutorWrapper{
		options:  options,
		limiters: make(map[string]*quotas.RateLimiterImpl),
	}
}

func (w *apsExecutorWrapper) Wrap(delegate Executor) Executor {
	return &apsExecutor{delegate: delegate, wrapper: w}
}

// limiterFor hands out one bucket per namespace, shared by every shard on this host.
func (w *apsExecutorWrapper) limiterFor(namespaceID string) *quotas.RateLimiterImpl {
	rps := float64(w.options.RPS())

	w.mu.Lock()
	defer w.mu.Unlock()

	limiter, ok := w.limiters[namespaceID]
	if !ok {
		limiter = quotas.NewRateLimiter(rps, int(rps))
		w.limiters[namespaceID] = limiter
	}
	limiter.SetRateBurst(rps, int(rps))
	return limiter
}

func (e *apsExecutor) Execute(ctx context.Context, executable Executable) ExecuteResponse {
	if !e.shouldMeter(executable) {
		return e.delegate.Execute(ctx, executable)
	}
	if !e.wrapper.limiterFor(executable.GetNamespaceID()).Allow() {
		// Shaped exactly like the SaaS limiter's refusal, because that shape is what the
		// controller keys on: namespace scope, APS cause.
		return ExecuteResponse{
			ExecutionErr: &serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
				Message: "namespace APS limit reached",
			},
		}
	}
	return e.delegate.Execute(ctx, executable)
}

// shouldMeter keeps workflow tasks out of the limiter when asked. A throttled workflow task
// regenerates forever, which couples the size of the backlog to how long the run takes;
// metering only activity dispatches makes the backlog a real instantaneous queue depth.
func (e *apsExecutor) shouldMeter(executable Executable) bool {
	if !e.wrapper.options.Enabled() {
		return false
	}
	if !e.wrapper.options.ActivityTasksOnly() {
		return true
	}
	return executable.GetType() == enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK
}
