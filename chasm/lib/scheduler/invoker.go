package scheduler

import (
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
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

	Scheduler chasm.Field[*Scheduler]
}

func (i *Invoker) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// NewInvoker returns an intialized Invoker component, which should
// be parented under a Scheduler root component.
func NewInvoker(ctx chasm.MutableContext, scheduler *Scheduler) *Invoker {
	return &Invoker{
		InvokerState: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{},
		},
		Scheduler: chasm.ComponentPointerTo(ctx, scheduler),
	}
}

// EnqueueBufferedStarts adds new BufferedStarts to the invocation queue,
// immediately kicking off a processing task.
func (i *Invoker) EnqueueBufferedStarts(ctx chasm.MutableContext, starts []*schedulespb.BufferedStart) {
	i.BufferedStarts = append(i.BufferedStarts, starts...)

	// Immediately begin processing the new starts.
	ctx.AddTask(i, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{})
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
		}

		starts = append(starts, start)
	}

	// Update internal state.
	i.BufferedStarts = starts
	i.CancelWorkflows = append(i.GetCancelWorkflows(), result.cancelWorkflows...)
	i.TerminateWorkflows = append(i.GetTerminateWorkflows(), result.terminateWorkflows...)
	i.LastProcessedTime = timestamppb.New(ctx.Now(i))

	i.addTasks(ctx)
}

type executeResult struct {
	// Starts that executed successfully can be removed from the buffer.
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
	completed := make(map[string]bool)                       // request ID -> is present
	failed := make(map[string]bool)                          // request ID -> is present
	retryable := make(map[string]*schedulespb.BufferedStart) // request ID -> *BufferedStart
	canceled := make(map[string]bool)                        // run ID -> is present
	terminated := make(map[string]bool)                      // run ID -> is present

	for _, start := range result.CompletedStarts {
		completed[start.RequestId] = true
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

	// Update Invoker state to remove completed items from their buffers.
	i.BufferedStarts = slices.DeleteFunc(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return completed[start.RequestId] || failed[start.RequestId]
	})
	i.CancelWorkflows = slices.DeleteFunc(i.GetCancelWorkflows(), func(we *commonpb.WorkflowExecution) bool {
		return canceled[we.RunId]
	})
	i.TerminateWorkflows = slices.DeleteFunc(i.GetTerminateWorkflows(), func(we *commonpb.WorkflowExecution) bool {
		return terminated[we.RunId]
	})

	// Update attempt counts and backoffs for failed/retrying starts.
	for _, start := range i.GetBufferedStarts() {
		if retry, ok := retryable[start.RequestId]; ok {
			start.Attempt++
			start.BackoffTime = retry.GetBackoffTime()
		}
	}

	// Add tasks if other actions are backing off or still pending execution.
	i.addTasks(ctx)
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
	if len(i.CancelWorkflows) > 0 || len(i.TerminateWorkflows) > 0 || eligibleStarts > 0 {
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
// execution (Attempt > 0), and aren't presently backing off, based on last
// processed time.
func (i *Invoker) getEligibleBufferedStarts() []*schedulespb.BufferedStart {
	return util.FilterSlice(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return start.Attempt > 0 && start.BackoffTime.AsTime().Before(i.GetLastProcessedTime().AsTime())
	})
}
