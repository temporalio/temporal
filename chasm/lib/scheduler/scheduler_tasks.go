package scheduler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SchedulerIdleTaskHandlerOptions struct {
	fx.In

	Config         *Config
	MetricsHandler metrics.Handler
	BaseLogger     log.Logger
}

type SchedulerIdleTaskHandler struct {
	chasm.PureTaskHandlerBase
	config         *Config
	metricsHandler metrics.Handler
	baseLogger     log.Logger
}

func NewSchedulerIdleTaskHandler(opts SchedulerIdleTaskHandlerOptions) *SchedulerIdleTaskHandler {
	return &SchedulerIdleTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
	}
}

func (r *SchedulerIdleTaskHandler) Execute(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerIdleTask,
) error {
	scheduler.getOrCreateEventLog(ctx).LogEvent(ctx, "schedule closed from idle timer")
	scheduler.Closed = true
	newTaggedMetricsHandler(r.metricsHandler, scheduler).
		Counter(metrics.ScheduleIdleTask.Name()).
		Record(1, metrics.OutcomeTag(outcomeFired), metrics.ReasonTag(reasonNone))
	return nil
}

// Idle-task invalidation reasons. Limited cardinality for ReasonTag.
const (
	idleInvalidatedHeldOpen        metrics.ReasonString = "held_open"
	idleInvalidatedExpirationShift metrics.ReasonString = "expiration_shift"
	idleInvalidatedClosed          metrics.ReasonString = "closed"
	idleAlreadyArmed               metrics.ReasonString = "already_armed"
)

func (r *SchedulerIdleTaskHandler) Validate(
	ctx chasm.Context,
	scheduler *Scheduler,
	taskAttrs chasm.TaskInvocation,
	task *schedulerpb.SchedulerIdleTask,
) (bool, error) {
	if scheduler.Closed {
		r.recordInvalidated(scheduler, idleInvalidatedClosed, taskAttrs.ScheduledTime, time.Time{})
		return false, nil
	}
	if scheduler.isHeldOpen() {
		r.recordInvalidated(scheduler, idleInvalidatedHeldOpen, taskAttrs.ScheduledTime, time.Time{})
		return false, nil
	}

	// Use After (not strict equality) so sub-precision drift doesn't drop tasks
	// that should still fire.
	idleExpiration := scheduler.idleDeadline(ctx, task.IdleTimeTotal.AsDuration())
	if idleExpiration.After(taskAttrs.ScheduledTime) {
		r.recordInvalidated(scheduler, idleInvalidatedExpirationShift, taskAttrs.ScheduledTime, idleExpiration)
		return false, nil
	}

	// Deadline moved earlier. getLastEventTime floors the recomputed value at the
	// persisted LastEventTime mark, so this is now only reachable on a schedule
	// whose mark predates that field (nil) - i.e. one that has not ticked since
	// the upgrade. Fire anyway (closing the schedule is the safe call) but log,
	// so a mark that is failing to hold the line still surfaces.
	if idleExpiration.Before(taskAttrs.ScheduledTime) {
		newTaggedLogger(r.baseLogger, scheduler).Warn("idle deadline regressed",
			tag.Timestamp(idleExpiration),
			tag.NewTimeTag("scheduled-time", taskAttrs.ScheduledTime))
	}
	return true, nil
}

func (r *SchedulerIdleTaskHandler) recordInvalidated(
	scheduler *Scheduler,
	reason metrics.ReasonString,
	scheduledTime time.Time,
	recomputedDeadline time.Time,
) {
	newTaggedMetricsHandler(r.metricsHandler, scheduler).
		Counter(metrics.ScheduleIdleTask.Name()).
		Record(1, metrics.OutcomeTag(outcomeInvalidated), metrics.ReasonTag(reason))
	newTaggedLogger(r.baseLogger, scheduler).Debug("idle task invalidated",
		tag.NewStringTag("reason", string(reason)),
		tag.NewTimeTag("scheduled-time", scheduledTime),
		tag.NewTimeTag("recomputed-deadline", recomputedDeadline))
}

type SchedulerCallbacksTaskHandlerOptions struct {
	fx.In

	Config         *Config
	HistoryClient  resource.HistoryClient
	FrontendClient workflowservice.WorkflowServiceClient
	MetricsHandler metrics.Handler
}

type SchedulerCallbacksTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*schedulerpb.SchedulerCallbacksTask]
	config         *Config
	historyClient  resource.HistoryClient
	frontendClient workflowservice.WorkflowServiceClient
	metricsHandler metrics.Handler
}

func NewSchedulerCallbacksTaskHandler(opts SchedulerCallbacksTaskHandlerOptions) *SchedulerCallbacksTaskHandler {
	return &SchedulerCallbacksTaskHandler{
		config:         opts.Config,
		historyClient:  opts.HistoryClient,
		frontendClient: opts.FrontendClient,
		metricsHandler: opts.MetricsHandler,
	}
}

// watchResult holds the outcome of watchRunningStart for a single BufferedStart.
// A nil completed field means the callback was successfully attached and the
// workflow is still running.
type watchResult struct {
	completed *schedulespb.CompletedResult

	// reason attributes how completed was arrived at, for ScheduleCallbackReattach.
	reason metrics.ReasonString
}

func (r *SchedulerCallbacksTaskHandler) Execute(
	ctx context.Context,
	schedulerRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerCallbacksTask,
) error {
	var scheduler *Scheduler
	var starts []*schedulespb.BufferedStart
	var componentRef []byte

	// Read scheduler state and capture the scheduler's component ref.
	_, err := chasm.ReadComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.Context, _ any) (struct{}, error) {
			scheduler = &Scheduler{
				SchedulerState: common.CloneProto(s.SchedulerState),
			}

			invoker := s.Invoker.Get(ctx)
			for _, start := range invoker.BufferedStarts {
				if needsCallback(start) {
					starts = append(starts, common.CloneProto(start))
				}
			}

			ref, err := ctx.Ref(s)
			if err != nil {
				return struct{}{}, err
			}
			componentRef = ref

			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read component: %w", err)
	}
	if scheduler == nil {
		return errors.New("scheduler component was nil after read")
	}

	// Attach callbacks and check workflow status. One start's failure must not
	// block the others.
	results := make(map[string]*watchResult, len(starts))
	var errs []error
	for _, start := range starts {
		result, err := r.watchRunningStart(ctx, scheduler, start, componentRef)
		if err != nil {
			errs = append(errs, fmt.Errorf("buffered start %s: %w", start.RequestId, err))
			continue
		}
		results[start.RequestId] = result
	}

	// Apply results to the invoker's BufferedStarts and fire tasks.
	_, _, err = chasm.UpdateComponent(
		ctx,
		schedulerRef,
		func(s *Scheduler, ctx chasm.MutableContext, _ any) (chasm.NoValue, error) {
			generator := s.Generator.Get(ctx)
			invoker := s.Invoker.Get(ctx)
			var completedRequestIDs []string

			for _, start := range invoker.BufferedStarts {
				if result, ok := results[start.RequestId]; ok {
					if result.completed != nil {
						completedRequestIDs = append(completedRequestIDs, start.RequestId)
					} else {
						start.HasCallback = true
					}
				}
			}
			for _, requestID := range completedRequestIDs {
				s.completeAction(ctx, requestID, results[requestID].completed, nil)
			}

			s.getOrCreateEventLog(ctx).LogEvent(ctx,
				fmt.Sprintf("attached callbacks to %d already-running workflow(s)", len(results)))
			if len(errs) > 0 {
				s.getOrCreateEventLog(ctx).LogEvent(ctx,
					fmt.Sprintf("failed to resolve %d migrated workflow(s), will retry: %v", len(errs), errors.Join(errs...)))
			}

			// Fire scheduler tasks even if some starts are unresolved: they still occupy
			// their overlap slot (as a failing V1 watcher leaves RunningWorkflows), and
			// withholding generation risks a permanently stuck schedule once this task
			// exhausts its retries.
			invoker.addTasks(ctx)
			generator.Generate(ctx)

			return nil, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to update component state: %w", err)
	}

	// Only after the update commits, so a rolled-back re-attach isn't counted.
	metricsHandler := newTaggedMetricsHandler(r.metricsHandler, scheduler)
	for _, result := range results {
		outcome := outcomeReattachAttached
		if result.completed != nil {
			outcome = outcomeReattachCompleted
		}
		metrics.ScheduleCallbackReattach.With(metricsHandler).Record(1,
			metrics.OutcomeTag(outcome),
			metrics.ReasonTag(result.reason))
	}
	if len(errs) > 0 {
		metrics.ScheduleCallbackReattach.With(metricsHandler).Record(int64(len(errs)),
			metrics.OutcomeTag(outcomeReattachFailed),
			metrics.ReasonTag(reasonNone))
	}

	return errors.Join(errs...)
}

const (
	// maxReattachAttempts bounds re-resolution when the chain's tail closes or
	// continues-as-new between Describe and attach.
	maxReattachAttempts = 3
	// maxChainFollowHops bounds walking close events of a chain that is no
	// longer current.
	maxChainFollowHops = 100
)

// chainTail is the latest execution in a buffered start's execution chain.
type chainTail struct {
	runID string
	// completed is nil while the tail is still open.
	completed *schedulespb.CompletedResult
	reason    metrics.ReasonString
}

// watchRunningStart will attach a Nexus completion callback to the open tail of
// a BufferedStart's execution chain. If the chain has already closed, the start
// is updated to indicate it has completed. Intended for migration/anti-entropy
// cases.
func (r *SchedulerCallbacksTaskHandler) watchRunningStart(
	ctx context.Context,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
	schedulerRef []byte,
) (*watchResult, error) {
	requestSpec := scheduler.GetSchedule().GetAction().GetStartWorkflow()

	// Pack this start's request ID into the callback token so completions are matched from the token
	// (which survives continue-as-new) rather than the started workflow's callback state.
	callback, err := chasm.GenerateNexusCallback(schedulerRef, start.RequestId, r.config.EncodeInternalTokenWithEnvelope(scheduler.Namespace))
	if err != nil {
		return nil, err
	}

	for range maxReattachAttempts {
		tail, err := r.resolveChainTail(ctx, scheduler, start)
		if err != nil {
			return nil, err
		}
		if tail.completed != nil {
			return &watchResult{completed: tail.completed, reason: tail.reason}, nil
		}

		// USE_EXISTING attaches to whatever run is current for the workflow ID;
		// REJECT_DUPLICATE prevents starting a new workflow if it has closed.
		resp, err := r.frontendClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                scheduler.Namespace,
			WorkflowId:               start.WorkflowId,
			RequestId:                start.RequestId,
			Identity:                 scheduler.identity(),
			WorkflowType:             requestSpec.GetWorkflowType(),
			TaskQueue:                requestSpec.GetTaskQueue(),
			WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			CompletionCallbacks:      []*commonpb.Callback{callback},
			OnConflictOptions: &workflowpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
		})
		if _, ok := errors.AsType[*serviceerror.WorkflowExecutionAlreadyStarted](err); ok {
			// The tail closed before attach; re-resolve to observe its outcome.
			continue
		}
		if err != nil {
			return nil, err
		}
		if runID := resp.GetRunId(); runID != "" && runID != tail.runID {
			// Attached to a different run: a CAN successor (re-resolving confirms it,
			// and the re-attach dedupes on request ID) or, if ours closed, an unrelated
			// run (re-resolving records our outcome; the stray callback is ignored).
			continue
		}
		return &watchResult{reason: reasonNone}, nil
	}
	return nil, fmt.Errorf("execution chain kept changing across %d callback attach attempts", maxReattachAttempts)
}

// resolveChainTail mirrors the V1 watcher: check the current run for the
// workflow ID, accepting it only if it belongs to start's chain. Otherwise the
// chain is closed and is followed from start.RunId by NewExecutionRunId.
func (r *SchedulerCallbacksTaskHandler) resolveChainTail(
	ctx context.Context,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
) (*chainTail, error) {
	info, err := r.describeExecution(ctx, scheduler, start.WorkflowId, "")
	if _, ok := errors.AsType[*serviceerror.NotFound](err); !ok && err != nil {
		return nil, err
	}
	if err == nil && (info.GetFirstRunId() == start.RunId || info.GetExecution().GetRunId() == start.RunId) {
		if info.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
			return nil, fmt.Errorf("current run %s is continued-as-new", info.GetExecution().GetRunId())
		}
		completed, err := completedResultFromWorkflowInfo(info)
		if err != nil {
			return nil, err
		}
		return &chainTail{runID: info.GetExecution().GetRunId(), completed: completed, reason: reasonReattachAlreadyClosed}, nil
	}
	return r.followClosedChain(ctx, scheduler, start)
}

// followClosedChain walks start's chain by close events. An open run is always
// the current one, so reaching here means the chain has closed and only its
// final status is missing.
func (r *SchedulerCallbacksTaskHandler) followClosedChain(
	ctx context.Context,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
) (*chainTail, error) {
	runID := start.RunId
	for range maxChainFollowHops {
		info, err := r.describeExecution(ctx, scheduler, start.WorkflowId, runID)
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return unobservedChainTail(runID, reasonReattachNotFound), nil
		}
		if err != nil {
			return nil, err
		}
		status := info.GetStatus()
		if status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || status == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
			// Raced with the current-run check; let attach sort it out.
			return &chainTail{runID: runID}, nil
		}

		histResp, err := r.frontendClient.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:              scheduler.Namespace,
			Execution:              &commonpb.WorkflowExecution{WorkflowId: start.WorkflowId, RunId: runID},
			MaximumPageSize:        1,
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			SkipArchival:           true,
		})
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return unobservedChainTail(runID, reasonReattachNotFound), nil
		}
		if err != nil {
			return nil, err
		}
		events := histResp.GetHistory().GetEvents()
		if len(events) == 0 {
			return nil, fmt.Errorf("no close event for run %s", runID)
		}
		if next := newExecutionRunID(events[len(events)-1]); next != "" {
			runID = next
			continue
		}
		if status == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
			return nil, fmt.Errorf("continued-as-new run %s has no successor", runID)
		}
		completed, err := completedResultFromWorkflowInfo(info)
		if err != nil {
			return nil, err
		}
		return &chainTail{runID: runID, completed: completed, reason: reasonReattachAlreadyClosed}, nil
	}
	// The chain isn't current, so it's closed; give up on its exact status
	// rather than leave the start occupying its overlap slot forever.
	return unobservedChainTail(runID, reasonReattachChainUnresolved), nil
}

// unobservedChainTail synthesizes TERMINATED for a closed chain whose outcome
// can't be observed. TERMINATED doesn't trigger pause-on-failure, matching V1,
// which treats a vanished workflow as closed with no status.
func unobservedChainTail(runID string, reason metrics.ReasonString) *chainTail {
	return &chainTail{
		runID: runID,
		completed: &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			CloseTime: timestamppb.Now(),
		},
		reason: reason,
	}
}

func (r *SchedulerCallbacksTaskHandler) describeExecution(
	ctx context.Context,
	scheduler *Scheduler,
	workflowID string,
	runID string,
) (*workflowpb.WorkflowExecutionInfo, error) {
	resp, err := r.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId,
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: scheduler.Namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID},
		},
	})
	if err != nil {
		return nil, err
	}
	return resp.GetWorkflowExecutionInfo(), nil
}

// newExecutionRunID returns the run a close event hands off to (continue-as-new,
// retry, or cron), or "" if the chain ends here.
func newExecutionRunID(event *historypb.HistoryEvent) string {
	if attrs := event.GetWorkflowExecutionContinuedAsNewEventAttributes(); attrs != nil {
		return attrs.GetNewExecutionRunId()
	}
	if attrs := event.GetWorkflowExecutionCompletedEventAttributes(); attrs != nil {
		return attrs.GetNewExecutionRunId()
	}
	if attrs := event.GetWorkflowExecutionFailedEventAttributes(); attrs != nil {
		return attrs.GetNewExecutionRunId()
	}
	if attrs := event.GetWorkflowExecutionTimedOutEventAttributes(); attrs != nil {
		return attrs.GetNewExecutionRunId()
	}
	return ""
}

func (r *SchedulerCallbacksTaskHandler) Validate(
	ctx chasm.Context,
	scheduler *Scheduler,
	taskAttrs chasm.TaskInvocation,
	task *schedulerpb.SchedulerCallbacksTask,
) (bool, error) {
	invoker := scheduler.Invoker.Get(ctx)
	if slices.ContainsFunc(invoker.BufferedStarts, needsCallback) {
		return true, nil
	}
	return false, nil
}

func needsCallback(start *schedulespb.BufferedStart) bool {
	return !start.HasCallback && start.GetRunId() != "" && start.GetCompleted() == nil
}
