package scheduler

import (
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
}

func (i *Invoker) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// NewInvoker returns an intialized Invoker component, which should
// be parented under a Scheduler root component.
func NewInvoker(ctx chasm.MutableContext) *Invoker {
	return &Invoker{
		InvokerState: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{},
		},
	}
}

// EnqueueBufferedStarts adds new BufferedStarts to the invocation queue,
// immediately kicking off a processing task.
func (i *Invoker) EnqueueBufferedStarts(ctx chasm.MutableContext, starts []*schedulespb.BufferedStart) {
	i.BufferedStarts = append(i.BufferedStarts, starts...)
	i.addTasks(ctx)
}

type processBufferResult struct {
	startWorkflows     []*schedulespb.BufferedStart
	cancelWorkflows    []*commonpb.WorkflowExecution
	terminateWorkflows []*commonpb.WorkflowExecution

	// discardStarts will be dropped from the Invoker's BufferedStarts without execution.
	discardStarts []*schedulespb.BufferedStart

	// Number of buffered starts dropped due to overlap policy during processing.
	overlapSkipped int64

	// Nunmber of buffered starts dropped from missing the catchup window.
	missedCatchupWindow int64
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
	for _, start := range i.GetBufferedStarts() {
		if discards[start.RequestId] {
			continue
		}

		// Starts ready for execution are set to their first attempt.
		if ready[start.RequestId] && start.Attempt < 1 {
			start.Attempt = 1
		} else if start.Attempt == 0 {
			// Start was processed but deferred (e.g., BUFFER_ONE policy with running workflow).
			// Mark as deferred (-1) to distinguish from newly-enqueued starts. This prevents
			// processingDeadline() from scheduling an immediate task for deferred starts.
			start.Attempt = -1
		}

		starts = append(starts, start)
	}

	// Update internal state.
	i.BufferedStarts = starts
	i.CancelWorkflows = append(i.GetCancelWorkflows(), result.cancelWorkflows...)
	i.TerminateWorkflows = append(i.GetTerminateWorkflows(), result.terminateWorkflows...)
	i.LastProcessedTime = timestamppb.New(ctx.Now(i))

	// Only schedule new tasks if this processBuffer call actually did something.
	// This prevents duplicate task scheduling when multiple ProcessBuffer tasks
	// run in the same transaction (e.g., from multiple backfillers).
	if len(result.startWorkflows) > 0 ||
		len(result.discardStarts) > 0 ||
		len(result.cancelWorkflows) > 0 ||
		len(result.terminateWorkflows) > 0 {
		i.addTasks(ctx)
	}
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
// completed InvokerExecuteTask. Tasks to continue execution are added, if needed.
func (i *Invoker) recordExecuteResult(ctx chasm.MutableContext, result *executeResult) {
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
	i.BufferedStarts = slices.DeleteFunc(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return failed[start.RequestId]
	})
	i.CancelWorkflows = slices.DeleteFunc(i.GetCancelWorkflows(), func(we *commonpb.WorkflowExecution) bool {
		return canceled[we.RunId]
	})
	i.TerminateWorkflows = slices.DeleteFunc(i.GetTerminateWorkflows(), func(we *commonpb.WorkflowExecution) bool {
		return terminated[we.RunId]
	})

	// Update BufferedStarts with results.
	for _, start := range i.GetBufferedStarts() {
		if completedStart, ok := completed[start.RequestId]; ok {
			start.RunId = completedStart.GetRunId()
			start.StartTime = completedStart.GetStartTime()
		}
		if retry, ok := retryable[start.RequestId]; ok {
			start.Attempt++
			start.BackoffTime = retry.GetBackoffTime()
		}
	}

	// Add tasks if other actions are backing off or still pending execution.
	i.addTasks(ctx)
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
	// completing one start and kicking off the next). We set that on the first start
	// pending execution.
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
	totalStarts := len(i.GetBufferedStarts())
	eligibleStarts := len(i.getEligibleBufferedStarts())

	// Add a ProcessBuffer pure task whenever there are BufferedStarts that are
	// backing off, or are still pending initial processing.
	if (totalStarts - eligibleStarts) > 0 {
		ctx.AddTask(i, chasm.TaskAttributes{
			ScheduledTime: i.processingDeadline(),
		}, &schedulerpb.InvokerProcessBufferTask{})
	}

	// Add an Execute side effect task whenever there are any eligible actions
	// pending execution.
	if len(i.GetCancelWorkflows()) > 0 || len(i.GetTerminateWorkflows()) > 0 || eligibleStarts > 0 {
		ctx.AddTask(i, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	}
}

// processingDeadline returns the earliest possible time that the BufferedStarts
// queue should be processed, taking into account starts that have not yet been
// attempted, as well as those that are pending backoff to retry. If the buffer
// is empty, the return value will be Time's zero value.
func (i *Invoker) processingDeadline() time.Time {
	var deadline time.Time
	for _, start := range i.GetBufferedStarts() {
		if start.GetAttempt() == 0 {
			// Return zero time to schedule an immediate task for unprocessed starts.
			return chasm.TaskScheduledTimeImmediate
		}
		backoff := start.GetBackoffTime().AsTime()
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
	return util.FilterSlice(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return start.Attempt > 0 &&
			start.GetRunId() == "" &&
			start.BackoffTime.AsTime().Before(i.GetLastProcessedTime().AsTime())
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
