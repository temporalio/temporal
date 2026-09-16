package scheduler

import (
	"fmt"
	"strings"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/internal"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

const (
	callbackIgnoredUnrecognizedRequest metrics.ReasonString = "unrecognized_request_id"
	callbackIgnoredAlreadyCompleted    metrics.ReasonString = "already_completed"
)

var _ chasm.NexusCompletionHandler = &Scheduler{}

// completedResultFromNexusCompletion restores the workflow status represented by
// a Nexus completion's success/failure union. This preserves the status model used
// by the V1 watcher: success is COMPLETED, while failure details distinguish
// CANCELED, TIMED_OUT, and TERMINATED from a generic FAILED result.
func completedResultFromNexusCompletion(
	info *persistencespb.ChasmNexusCompletion,
) (*schedulespb.CompletedResult, error) {
	var status enumspb.WorkflowExecutionStatus
	switch outcome := info.GetOutcome().(type) {
	case *persistencespb.ChasmNexusCompletion_Failure:
		if outcome.Failure == nil {
			return nil, serviceerror.NewInvalidArgument("invalid completion failure")
		}
		switch outcome.Failure.FailureInfo.(type) {
		case *failurepb.Failure_CanceledFailureInfo:
			status = enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
		case *failurepb.Failure_TimeoutFailureInfo:
			status = enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
		case *failurepb.Failure_TerminatedFailureInfo:
			status = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		default:
			status = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
		}
	case *persistencespb.ChasmNexusCompletion_Success:
		status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	default:
		return nil, serviceerror.NewInvalidArgument("invalid completion outcome")
	}
	return &schedulespb.CompletedResult{
		Status:    status,
		CloseTime: info.GetCloseTime(),
	}, nil
}

// completedResultFromWorkflowInfo applies the V1 watcher semantics to a status
// returned by Describe. RUNNING and PAUSED still occupy the schedule's overlap
// slot, and CONTINUED_AS_NEW is followed to the latest run when the callback is
// attached, so none are action completions. A non-nil result represents one of
// the five final workflow statuses handled by the V1 watcher.
func completedResultFromWorkflowInfo(
	wfInfo *workflowpb.WorkflowExecutionInfo,
) (*schedulespb.CompletedResult, error) {
	status := wfInfo.GetStatus()
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		return nil, nil
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return &schedulespb.CompletedResult{
			Status:    status,
			CloseTime: wfInfo.GetCloseTime(),
		}, nil
	default:
		return nil, fmt.Errorf("unexpected workflow execution status: %s", status)
	}
}

func countsAsFailureForPause(status enumspb.WorkflowExecutionStatus) bool {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return true
	default:
		return false
	}
}

func (s *Scheduler) recordIgnoredCallback(
	ctx chasm.MutableContext,
	metricsHandler metrics.Handler,
	requestID string,
	reason metrics.ReasonString,
	message string,
) {
	s.getOrCreateEventLog(ctx).LogEvent(ctx, fmt.Sprintf("%s: %s", message, requestID))
	ctx.Logger().Warn(message, tag.RequestID(requestID), tag.ScheduleID(s.ScheduleId))
	metricsHandler.Counter(metrics.ScheduleCallbackIgnored.Name()).Record(1, metrics.ReasonTag(reason))
}

// HandleNexusCompletion allows Scheduler to record workflow completions from
// workflows started by the same scheduler tree's Invoker.
func (s *Scheduler) HandleNexusCompletion(
	ctx chasm.MutableContext,
	info *persistencespb.ChasmNexusCompletion,
) error {
	invoker := s.Invoker.Get(ctx)
	metricsHandler := newTaggedMetricsHandler(ctx.MetricsHandler(), s)

	var start *schedulespb.BufferedStart
	for _, bufferedStart := range invoker.GetBufferedStarts() {
		if bufferedStart.GetRequestId() == info.RequestId {
			start = bufferedStart
			break
		}
	}
	if start == nil {
		// Missing request IDs are expected for start-only ALLOW_ALL actions because
		// their callbacks remain attached for rolling-upgrade compatibility.
		// TODO: Restore warning and event logging once those callbacks can be safely omitted.
		metricsHandler.Counter(metrics.ScheduleCallbackIgnored.Name()).Record(
			1,
			metrics.ReasonTag(callbackIgnoredUnrecognizedRequest),
		)
		return nil
	}
	if start.GetCompleted() != nil {
		// Completion callbacks may be validly redelivered, for example after a workflow reset.
		// Preserve state but keep the duplicate observable through the log and metric.
		s.recordIgnoredCallback(
			ctx,
			metricsHandler,
			info.RequestId,
			callbackIgnoredAlreadyCompleted,
			"handled Nexus completion for an already-completed buffered start",
		)
		return nil
	}
	// Record how long it took for the callback to arrive after the action completed.
	// Use ctx.Now instead of time.Since to use a consistent time source across nodes,
	// and clamp to zero in case of clock skew.
	if closeTime := info.GetCloseTime().AsTime(); !closeTime.IsZero() {
		latency := max(0, ctx.Now(s).Sub(closeTime))
		metricsHandler.Timer(metrics.ScheduleCallbackLatency.Name()).Record(latency)
	}

	// TODO - also record payload sizes once we have metrics wired into CHASM context.
	completed, err := completedResultFromNexusCompletion(info)
	if err != nil {
		return err
	}
	s.completeAction(ctx, info.RequestId, completed, info)
	s.Generator.Get(ctx).Generate(ctx)

	return nil
}

// completeAction records a buffered start's completion and reports whether a
// matching incomplete start was found. When outcome is nil, completion
// metadata came from Describe and payload-derived last-completion state is
// deliberately left unchanged.
func (s *Scheduler) completeAction(
	ctx chasm.MutableContext,
	requestID string,
	completed *schedulespb.CompletedResult,
	outcome *persistencespb.ChasmNexusCompletion,
) bool {
	invoker := s.Invoker.Get(ctx)
	var start *schedulespb.BufferedStart
	for _, bufferedStart := range invoker.BufferedStarts {
		if bufferedStart.RequestId == requestID && bufferedStart.Completed == nil {
			start = bufferedStart
			break
		}
	}
	if start == nil {
		ctx.Logger().Error(
			"failed to complete action because its buffered start was not found or was already completed",
			tag.RequestID(requestID),
			tag.ScheduleID(s.ScheduleId),
		)
		return false
	}
	workflowID := start.GetWorkflowId()
	tracksCompletionResult := internal.TracksCompletionResult(start.GetOverlapPolicy())

	// TODO - also record payload sizes once we have metrics wired into CHASM context.
	if outcome != nil && tracksCompletionResult {
		switch outcome := outcome.Outcome.(type) {
		case *persistencespb.ChasmNexusCompletion_Failure:
			previousResult := s.LastCompletionResult.Get(ctx)
			s.LastCompletionResult = chasm.NewDataField(ctx, &schedulerpb.LastCompletionResult{Failure: outcome.Failure, Success: previousResult.Success})
		case *persistencespb.ChasmNexusCompletion_Success:
			s.LastCompletionResult = chasm.NewDataField(ctx, &schedulerpb.LastCompletionResult{Success: outcome.Success})
		}
	}
	if tracksCompletionResult && countsAsFailureForPause(completed.Status) && s.Schedule.Policies.PauseOnFailure && !s.Schedule.State.Paused {
		s.Schedule.State.Paused = true
		s.Schedule.State.Notes = fmt.Sprintf("paused, workflow %s: %s", strings.ToLower(completed.Status.String()), workflowID)
		s.updateConflictToken()
	}
	start.HasCallback = true
	invoker.recordCompletedAction(ctx, completed, requestID)
	return true
}
