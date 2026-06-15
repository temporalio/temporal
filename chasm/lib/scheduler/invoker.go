package scheduler

import (
	"fmt"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The Invoker component is responsible for executing buffered actions.
type Invoker struct {
	chasm.UnimplementedComponent

	*schedulerpb.InvokerState

	Scheduler chasm.ParentPtr[*Scheduler]

	EventLog chasm.Field[*EventLog]
}

func (i *Invoker) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// NewInvoker returns an initialized Invoker component, which should
// be parented under a Scheduler root component.
func NewInvoker(ctx chasm.MutableContext) *Invoker {
	return newInvokerWithState(ctx, &schedulerpb.InvokerState{
		BufferedStarts: []*schedulespb.BufferedStart{},
	})
}

func newInvokerWithState(ctx chasm.MutableContext, state *schedulerpb.InvokerState) *Invoker {
	i := &Invoker{
		InvokerState: state,
		EventLog:     chasm.NewComponentField(ctx, NewEventLog(ctx)),
	}
	return i
}

// EnqueueBufferedStarts adds new BufferedStarts to the invocation queue,
// immediately kicking off a processing task.
func (i *Invoker) EnqueueBufferedStarts(ctx chasm.MutableContext, starts []*schedulespb.BufferedStart) {
	i.BufferedStarts = append(i.BufferedStarts, starts...)
	if len(starts) > 0 {
		i.EventLog.Get(ctx).LogEvent(ctx, fmt.Sprintf("enqueued %d buffered start(s)", len(starts)))
	}
	i.addTasks(ctx)
}

type processBufferResult struct {
	startWorkflows     []*schedulespb.BufferedStart
	cancelWorkflows    []*commonpb.WorkflowExecution
	terminateWorkflows []*commonpb.WorkflowExecution

	// discardStarts will be dropped from the Invoker's BufferedStarts without execution.
	discardStarts []*schedulespb.BufferedStart

	// Number of buffered starts dropped due to overlap policy during processing.
	overlapSkipped         int64
	overlapSkippedByPolicy map[enumspb.ScheduleOverlapPolicy]int64

	// Number of buffered starts dropped from missing the catchup window,
	// bucketed by whether a running action contributed to the miss.
	missedCatchupByActionRunning map[bool]int64
}

// recordProcessBufferResult updates the Invoker's internal state based on result, as well as the
// LastProcessedTime watermark. Tasks to continue execution are added, if needed.
func (i *Invoker) recordProcessBufferResult(ctx chasm.MutableContext, result *processBufferResult) {
	discards := make(map[string]bool) // request ID -> is present
	ready := make(map[string]bool)
	for _, start := range result.discardStarts {
		discards[start.RequestId] = true
	}
	for _, start := range result.startWorkflows {
		ready[start.RequestId] = true
	}

	// Drop discarded starts, and update requested starts for execution.
	var starts []*schedulespb.BufferedStart
	readiedStarts := 0
	deferredStarts := 0
	for _, start := range i.GetBufferedStarts() {
		if discards[start.RequestId] {
			continue
		}

		// Starts ready for execution are set to their first attempt.
		if ready[start.RequestId] && start.Attempt < 1 {
			start.Attempt = 1
			readiedStarts++
		} else if start.Attempt == 0 {
			// Start was processed but deferred (e.g., BUFFER_ONE policy with running workflow).
			// Mark as deferred (-1) to distinguish from newly-enqueued starts so addTasks
			// won't schedule an immediate ProcessBuffer task for them - they wait on
			// recordCompletedAction to re-enable.
			start.Attempt = -1
			deferredStarts++
		}

		starts = append(starts, start)
	}

	if readiedStarts > 0 || deferredStarts > 0 {
		i.EventLog.Get(ctx).LogEvent(ctx,
			fmt.Sprintf("recordProcessBufferResult readied %d starts, deferred %d starts", readiedStarts, deferredStarts))
	}

	// Update internal state.
	i.BufferedStarts = starts
	i.CancelWorkflows = append(i.GetCancelWorkflows(), result.cancelWorkflows...)
	i.TerminateWorkflows = append(i.GetTerminateWorkflows(), result.terminateWorkflows...)
	i.LastProcessedTime = timestamppb.New(ctx.Now(i))

	// Re-arm tasks if this call changed state, or if the LastProcessedTime advance
	// just unblocked backed-off starts.
	i.addTasks(ctx)
}

type executeResult struct {
	// Starts that executed successfully. Their RunId and StartTime should be
	// copied to the corresponding BufferedStart in the buffer.
	CompletedStarts []*schedulespb.BufferedStart

	// Starts that failed with a retryable error should be updated and kept in the buffer.
	RetryableStarts []*schedulespb.BufferedStart

	// Starts that failed with a non-retryable error can be removed from the buffer.
	FailedStarts []*schedulespb.BufferedStart

	CompletedCancels    []*commonpb.WorkflowExecution
	CompletedTerminates []*commonpb.WorkflowExecution
}

// Append combines two executeResults (no deduplication is done).
func (e *executeResult) Append(o executeResult) executeResult {
	return executeResult{
		CompletedStarts:     append(e.CompletedStarts, o.CompletedStarts...),
		RetryableStarts:     append(e.RetryableStarts, o.RetryableStarts...),
		FailedStarts:        append(e.FailedStarts, o.FailedStarts...),
		CompletedCancels:    append(e.CompletedCancels, o.CompletedCancels...),
		CompletedTerminates: append(e.CompletedTerminates, o.CompletedTerminates...),
	}
}

// recordExecuteResult updates the Invoker's internal state with the results of a
// completed InvokerExecuteTask. It returns the number of *new* actions recorded
// (starts that transitioned from "no RunId" to "has RunId" in this call) and
// the number of completed results that were dropped because they were previously
// recorded.
func (i *Invoker) recordExecuteResult(ctx chasm.MutableContext, result *executeResult) (newlyStarted, droppedDuplicates int) {
	completed := make(map[string]*schedulespb.BufferedStart) // request ID -> BufferedStart with RunId/StartTime
	failed := make(map[string]bool)                          // request ID -> is present
	retryable := make(map[string]*schedulespb.BufferedStart) // request ID -> *BufferedStart
	canceled := make(map[string]bool)                        // run ID -> is present
	terminated := make(map[string]bool)                      // run ID -> is present

	for _, start := range result.CompletedStarts {
		completed[start.RequestId] = start
	}
	for _, start := range result.FailedStarts {
		failed[start.RequestId] = true
	}
	for _, start := range result.RetryableStarts {
		retryable[start.RequestId] = start
	}
	for _, wf := range result.CompletedCancels {
		canceled[wf.RunId] = true
	}
	for _, wf := range result.CompletedTerminates {
		terminated[wf.RunId] = true
	}

	// Remove failed (non-retryable) starts from the buffer.
	removedStarts := 0
	retriedStarts := 0
	i.BufferedStarts = slices.DeleteFunc(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		removedStarts++
		return failed[start.RequestId]
	})
	i.CancelWorkflows = slices.DeleteFunc(i.GetCancelWorkflows(), func(we *commonpb.WorkflowExecution) bool {
		removedStarts++
		return canceled[we.RunId]
	})
	i.TerminateWorkflows = slices.DeleteFunc(i.GetTerminateWorkflows(), func(we *commonpb.WorkflowExecution) bool {
		removedStarts++
		return terminated[we.RunId]
	})

	// Update BufferedStarts with results, dropping duplicates.
	for _, start := range i.GetBufferedStarts() {
		if start.RunId != "" {
			if _, isDuplicate := completed[start.RequestId]; isDuplicate {
				droppedDuplicates++
			}
			continue
		}
		if completedStart, ok := completed[start.RequestId]; ok {
			start.RunId = completedStart.GetRunId()
			start.StartTime = completedStart.GetStartTime()
			start.HasCallback = true
			newlyStarted++
		}
		if retry, ok := retryable[start.RequestId]; ok {
			start.Attempt++
			start.BackoffTime = retry.GetBackoffTime()
			retriedStarts++
		}
	}

	i.EventLog.Get(ctx).LogEvent(ctx,
		fmt.Sprintf("recordExecuteResult kicked off %d starts, removed %d starts, retried %d starts",
			newlyStarted,
			removedStarts,
			retriedStarts))

	i.addTasks(ctx)
	return newlyStarted, droppedDuplicates
}

// runningWorkflowID returns the workflow ID associated with the given
// outstanding request.
func (i *Invoker) runningWorkflowID(requestID string) string {
	for _, start := range i.GetBufferedStarts() {
		if start.GetRequestId() == requestID && start.GetCompleted() == nil {
			return start.GetWorkflowId()
		}
	}
	return ""
}

// recordCompletedAction updates Invoker metadata and kicks off tasks after
// an action completes. It marks the BufferedStart as completed by setting
// the Completed field.
//
// Returns the schedule time of the completed action for metrics.
func (i *Invoker) recordCompletedAction(
	ctx chasm.MutableContext,
	completed *schedulespb.CompletedResult,
	requestID string,
) (scheduleTime time.Time) {
	i.EventLog.Get(ctx).LogEvent(ctx, fmt.Sprintf("recording completed action: %s", requestID))

	// Find the BufferedStart and mark it as completed.
	for _, start := range i.BufferedStarts {
		if start.GetRequestId() == requestID {
			scheduleTime = start.DesiredTime.AsTime()
			start.Completed = completed
			break
		}
	}

	// Re-enable deferred starts (Attempt == -1) so they can be re-processed by
	// ProcessBuffer now that a workflow has completed. This allows the overlap
	// policy to be re-evaluated.
	for _, start := range i.BufferedStarts {
		if start.Attempt == -1 {
			start.Attempt = 0
		}
	}

	// Update DesiredTime on the first pending start for metrics. DesiredTime is used
	// to drive action latency between buffered starts (the time it takes between
	// completing one start and kicking off the next). It also signals in processBuffer
	// that this start was blocked behind a running action: if DesiredTime (the previous
	// action's CloseTime) is past the start's catchup deadline, the previous action's
	// duration caused the miss.
	idx := slices.IndexFunc(i.BufferedStarts, func(start *schedulespb.BufferedStart) bool {
		return start.Attempt == 0
	})
	if idx >= 0 {
		i.BufferedStarts[idx].DesiredTime = timestamppb.New(completed.GetCloseTime().AsTime())
	}

	// Apply retention to keep only the last N completed actions.
	i.applyCompletedRetention()

	// addTasks will add an immediate ProcessBufferTask if we have any starts pending
	// kick-off.
	i.addTasks(ctx)

	return
}

// addTasks adds both ProcessBuffer and Execute tasks as needed. It should be
// called when completing processing/executing tasks, to drive backoff/retry.
func (i *Invoker) addTasks(ctx chasm.MutableContext) {
	// If we have Attempt = 0 starts, generate a ProcessBufferTask immediately. If we
	// have starts that are backing off, add a timer task for the earliest backoff time.
	if i.hasUnprocessedStarts() {
		i.EventLog.Get(ctx).LogEvent(ctx, "scheduled processBufferTask immediately")
		ctx.AddTask(i, chasm.TaskAttributes{
			ScheduledTime: chasm.TaskScheduledTimeImmediate,
		}, &schedulerpb.InvokerProcessBufferTask{})
	} else if deadline := i.nextBackoffDeadline(); !deadline.IsZero() {
		i.EventLog.Get(ctx).LogEvent(ctx,
			fmt.Sprintf("scheduled processBufferTask for %s", deadline.Format(time.RFC3339)))
		ctx.AddTask(i, chasm.TaskAttributes{
			ScheduledTime: deadline,
		}, &schedulerpb.InvokerProcessBufferTask{})
	}

	// Execute drains work that's ready now: pending cancels/terminates, and
	// starts that are past their backoff.
	if len(i.GetCancelWorkflows()) > 0 ||
		len(i.GetTerminateWorkflows()) > 0 ||
		len(i.getEligibleBufferedStarts()) > 0 {
		i.EventLog.Get(ctx).LogEvent(ctx, "scheduled executeTask")
		ctx.AddTask(i, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	}
}

// hasUnprocessedStarts reports whether any BufferedStart is still awaiting its
// initial ProcessBuffer pass (Attempt == 0).
func (i *Invoker) hasUnprocessedStarts() bool {
	for _, start := range i.GetBufferedStarts() {
		if start.GetAttempt() == 0 {
			return true
		}
	}
	return false
}

// nextBackoffDeadline returns the earliest BackoffTime among starts that are
// retrying, or the zero time if none are.
func (i *Invoker) nextBackoffDeadline() time.Time {
	var deadline time.Time
	lastProcessedTime := i.LastProcessedTime.AsTime()
	for _, start := range i.GetBufferedStarts() {
		backoff := start.GetBackoffTime().AsTime()
		// We only care about starts that are retrying.
		if start.GetAttempt() <= 0 ||
			start.GetRunId() != "" ||
			start.GetCompleted() != nil ||
			// Backed-off starts will be selected by getEligibleBufferedStarts and kick off
			// an Execute task, instead.
			start.BackoffTime.AsTime().Before(lastProcessedTime) {
			continue
		}
		if deadline.IsZero() || backoff.Before(deadline) {
			deadline = backoff
		}
	}

	return deadline
}

// getEligibleBufferedStarts returns all BufferedStarts that are marked for
// execution (Attempt > 0), haven't been started yet (no RunId), and aren't
// presently backing off, based on last processed time.
func (i *Invoker) getEligibleBufferedStarts() []*schedulespb.BufferedStart {
	lastProcessed := i.GetLastProcessedTime().AsTime()
	return util.FilterSlice(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return start.Attempt > 0 &&
			start.GetRunId() == "" &&
			!start.GetBackoffTime().AsTime().After(lastProcessed)
	})
}

// isWorkflowStarted returns true if a workflow with the given ID has already
// been started (has a RunId set).
func (i *Invoker) isWorkflowStarted(workflowID string) bool {
	for _, start := range i.GetBufferedStarts() {
		if start.GetWorkflowId() == workflowID && start.GetRunId() != "" {
			return true
		}
	}
	return false
}

// runningWorkflowExecutions returns the list of workflow executions that
// have been started but not yet completed.
func (i *Invoker) runningWorkflowExecutions() []*commonpb.WorkflowExecution {
	var running []*commonpb.WorkflowExecution
	for _, start := range i.GetBufferedStarts() {
		if start.GetRunId() != "" && start.GetCompleted() == nil {
			running = append(running, &commonpb.WorkflowExecution{
				WorkflowId: start.GetWorkflowId(),
				RunId:      start.GetRunId(),
			})
		}
	}
	return running
}

// recentActions returns started/completed actions as ScheduleActionResults.
// This includes both running workflows (with status RUNNING) and completed
// workflows (with their final status).
func (i *Invoker) recentActions() []*schedulepb.ScheduleActionResult {
	var results []*schedulepb.ScheduleActionResult
	for _, start := range i.GetBufferedStarts() {
		// Only include workflows that have been started (have a RunId).
		if start.GetRunId() == "" {
			continue
		}
		status := enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		if start.GetCompleted() != nil {
			status = start.GetCompleted().GetStatus()
		}
		results = append(results, &schedulepb.ScheduleActionResult{
			ScheduleTime: start.GetActualTime(),
			ActualTime:   start.GetStartTime(),
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: start.GetWorkflowId(),
				RunId:      start.GetRunId(),
			},
			StartWorkflowStatus: status,
		})
	}
	return results
}

// applyCompletedRetention removes the oldest completed BufferedStarts beyond
// the retention limit.
func (i *Invoker) applyCompletedRetention() {
	var completed []*schedulespb.BufferedStart
	var nonCompleted []*schedulespb.BufferedStart

	for _, start := range i.BufferedStarts {
		if start.GetCompleted() != nil {
			completed = append(completed, start)
		} else {
			nonCompleted = append(nonCompleted, start)
		}
	}

	// Sort by oldest first.
	slices.SortFunc(completed, func(a, b *schedulespb.BufferedStart) int {
		return a.GetCompleted().GetCloseTime().AsTime().Compare(b.GetCompleted().GetCloseTime().AsTime())
	})

	keepFrom := max(0, len(completed)-recentActionCount)
	completed = completed[keepFrom:]

	i.BufferedStarts = append(nonCompleted, completed...)
}
