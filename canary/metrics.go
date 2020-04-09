package canary

import (
	"time"

	"github.com/uber-go/tally"
	"go.temporal.io/temporal/workflow"
)

// counters go here
const (
	startedCount                         = "started"
	failedCount                          = "failed"
	successCount                         = "succeeded"
	startWorkflowCount                   = "startworkflow"
	startWorkflowSuccessCount            = "startworkflow.success"
	startWorkflowFailureCount            = "startworkflow.failures"
	startWorkflowAlreadyStartedCount     = "startworkflow.failures.alreadystarted"
	startWorkflowNamespaceNotActiveCount = "startworkflow.failures.namespacenotactive"
	listOpenWorkflowsCount               = "list-open-workflows"
	listOpenWorkflowsFailureCount        = "list-open-workflows.failures"
	listWorkflowsCount                   = "list-workflows"
	listWorkflowsFailureCount            = "list-workflows.failures"
	listArchivedWorkflowCount            = "list-archived-workflows"
	listArchivedWorkflowFailureCount     = "list-archived-workflows.failures"
	getWorkflowHistoryCount              = "get-workflow-history"
	getWorkflowHistoryFailureCount       = "get-workflow-history.failures"
	errTimeoutCount                      = "errors.timeout"
	errIncompatibleVersion               = "errors.incompatibleversion"
)

// latency metrics go here
const (
	latency                      = "latency"
	startLatency                 = "latency.schedule-to-start"
	startWorkflowLatency         = "latency.startworkflow"
	listOpenWorkflowsLatency     = "latency.list-open-workflows"
	listWorkflowsLatency         = "latency.list-workflows"
	listArchivedWorkflowsLatency = "latency.list-archived-workflows"
	getWorkflowHistoryLatency    = "latency.get-workflow-history"
	timerDriftLatency            = "latency.timer-drift"
)

// workflowMetricsProfile is the state that's needed to
// record success/failed and latency metrics at the end
// of a workflow
type workflowMetricsProfile struct {
	ctx            workflow.Context
	startTimestamp int64
	scope          tally.Scope
}

// workflowMetricScope creates and returns a child metric scope with tags
// that identify the current workflow type
func workflowMetricScope(ctx workflow.Context, wfType string) tally.Scope {
	parent := workflow.GetMetricsScope(ctx)
	return parent.Tagged(map[string]string{"operation": wfType})
}

// recordWorkflowStart emits metrics at the beginning of a workflow function
func recordWorkflowStart(ctx workflow.Context, wfType string, scheduledTimeNanos int64) *workflowMetricsProfile {
	now := workflow.Now(ctx).UnixNano()
	scope := workflowMetricScope(ctx, wfType)
	elapsed := maxInt64(0, now-scheduledTimeNanos)
	scope.Timer(startLatency).Record(time.Duration(elapsed))
	scope.Counter(startedCount).Inc(1)
	return &workflowMetricsProfile{
		ctx:            ctx,
		startTimestamp: now,
		scope:          scope,
	}
}

// recordWorkflowEnd emits metrics at the end of a workflow function
func recordWorkflowEnd(scope tally.Scope, elapsed time.Duration, err error) error {
	scope.Timer(latency).Record(elapsed)
	if err == nil {
		scope.Counter(successCount).Inc(1)
		return err
	}
	scope.Counter(failedCount).Inc(1)
	if _, ok := err.(*workflow.TimeoutError); ok {
		scope.Counter(errTimeoutCount).Inc(1)
	}
	return err
}

// end records the elapsed time and reports the latency,
// success, failed counts to m3
func (profile *workflowMetricsProfile) end(err error) error {
	now := workflow.Now(profile.ctx).UnixNano()
	elapsed := time.Duration(now - profile.startTimestamp)
	return recordWorkflowEnd(profile.scope, elapsed, err)
}

// recordActivityStart emits metrics at the beginning of an activity function
func recordActivityStart(
	scope tally.Scope, activityType string, scheduledTimeNanos int64) (tally.Scope, tally.Stopwatch) {
	scope = scope.Tagged(map[string]string{"operation": activityType})
	elapsed := maxInt64(0, time.Now().UnixNano()-scheduledTimeNanos)
	scope.Timer(startLatency).Record(time.Duration(elapsed))
	scope.Counter(startedCount).Inc(1)
	sw := scope.Timer(latency).Start()
	return scope, sw
}

// recordActivityEnd emits metrics at the end of an activity function
func recordActivityEnd(scope tally.Scope, sw tally.Stopwatch, err error) {
	sw.Stop()
	if err != nil {
		scope.Counter(failedCount).Inc(1)
		return
	}
	scope.Counter(successCount).Inc(1)
}
