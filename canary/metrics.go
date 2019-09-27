// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package canary

import (
	"time"

	"github.com/uber-go/tally"
	"go.uber.org/cadence/workflow"
)

// counters go here
const (
	startedCount                      = "started"
	failedCount                       = "failed"
	successCount                      = "succeeded"
	startWorkflowCount                = "startworkflow"
	startWorkflowSuccessCount         = "startworkflow.success"
	startWorkflowFailureCount         = "startworkflow.failures"
	startWorkflowAlreadyStartedCount  = "startworkflow.failures.alreadystarted"
	startWorkflowDomainNotActiveCount = "startworkflow.failures.domainnotactive"
	listOpenWorkflowsCount            = "list-open-workflows"
	listOpenWorkflowsFailureCount     = "list-open-workflows.failures"
	listWorkflowsCount                = "list-workflows"
	listWorkflowsFailureCount         = "list-workflows.failures"
	getWorkflowHistoryCount           = "get-workflow-history"
	getWorkflowHistoryFailureCount    = "get-workflow-history.failures"
	errTimeoutCount                   = "errors.timeout"
	errIncompatibleVersion            = "errors.incompatibleversion"
)

// latency metrics go here
const (
	latency                   = "latency"
	startLatency              = "latency.schedule-to-start"
	startWorkflowLatency      = "latency.startworkflow"
	listOpenWorkflowsLatency  = "latency.list-open-workflows"
	listWorkflowsLatency      = "latency.list-workflows"
	getWorkflowHistoryLatency = "latency.get-workflow-history"
	timerDriftLatency         = "latency.timer-drift"
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
