package scheduler

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/testing/testhooks"
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

	// Deadline moved earlier - shouldn't happen if getLastEventTime is monotonic.
	// Fire (closing the schedule is the safe call) but log so a real regression
	// surfaces.
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
	TestHooks      testhooks.TestHooks
}

type SchedulerCallbacksTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*schedulerpb.SchedulerCallbacksTask]
	config         *Config
	historyClient  resource.HistoryClient
	frontendClient workflowservice.WorkflowServiceClient
	testHooks      testhooks.TestHooks
}

func NewSchedulerCallbacksTaskHandler(opts SchedulerCallbacksTaskHandlerOptions) *SchedulerCallbacksTaskHandler {
	return &SchedulerCallbacksTaskHandler{
		config:         opts.Config,
		historyClient:  opts.HistoryClient,
		frontendClient: opts.FrontendClient,
		testHooks:      opts.TestHooks,
	}
}

// watchResult holds the outcome of watchRunningStart for a single BufferedStart.
// A nil completed field means the callback was successfully attached and the
// workflow is still running.
type watchResult struct {
	completed           *schedulespb.CompletedResult
	firstExecutionRunID string
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

	// Attach callbacks and check workflow status.
	results := make(map[string]*watchResult, len(starts))
	for _, start := range starts {
		result, err := r.watchRunningStart(ctx, scheduler, start, componentRef)
		if err != nil {
			return err
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

			for _, start := range invoker.BufferedStarts {
				if result, ok := results[start.RequestId]; ok {
					start.HasCallback = true
					start.FirstExecutionRunId = result.firstExecutionRunID
					if result.completed != nil {
						start.Completed = result.completed
					}
				}
			}

			s.getOrCreateEventLog(ctx).LogEvent(ctx,
				fmt.Sprintf("reconciled callbacks for %d already-running workflow(s)", len(results)))

			// Now that running workflow state has been refreshed, scheduler tasks can be
			// fired.
			invoker.addTasks(ctx)
			generator.Generate(ctx)

			return nil, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to update component state: %w", err)
	}

	return nil
}

// watchRunningStart will attach a Nexus completion callback to a running
// BufferedStart. If the start's workflow has already closed, the start is updated
// to indicate it has completed. Intended for migration/anti-entropy cases.
func (r *SchedulerCallbacksTaskHandler) watchRunningStart(
	ctx context.Context,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
	schedulerRef []byte,
) (*watchResult, error) {
	// Describe the workflow to ensure it exists and is still running.
	descResp, err := r.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId,
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: scheduler.Namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: start.WorkflowId,
				RunId:      start.RunId,
			},
		},
	})
	if err != nil {
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) {
			return &watchResult{
				completed: &schedulespb.CompletedResult{
					Status:    enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
					CloseTime: timestamppb.Now(),
				},
			}, nil
		}
		return nil, err
	}

	wfInfo := descResp.GetWorkflowExecutionInfo()
	firstExecutionRunID := wfInfo.GetFirstRunId()
	wfProgressing := wfInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING ||
		wfInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED

	if !wfProgressing {
		return &watchResult{
			firstExecutionRunID: firstExecutionRunID,
			completed: &schedulespb.CompletedResult{
				Status:    wfInfo.GetStatus(),
				CloseTime: wfInfo.GetCloseTime(),
			},
		}, nil
	}
	testhooks.Call(r.testHooks, testhooks.SchedulerCallbackAttachmentAfterDescribe, namespace.ID(scheduler.NamespaceId))

	// Workflow is still running. Attach a Nexus completion callback by issuing
	// a StartWorkflowExecution with USE_EXISTING conflict policy. REJECT_DUPLICATE
	// reuse policy prevents accidentally starting a new workflow if the original
	// completes between the describe and this call.
	requestSpec := scheduler.GetSchedule().GetAction().GetStartWorkflow()

	// Pack this start's request ID into the callback token so completions are matched from the token
	// (which survives continue-as-new) rather than the started workflow's callback state.
	callback, err := chasm.GenerateNexusCallback(schedulerRef, start.RequestId, r.config.EncodeInternalTokenWithEnvelope(scheduler.Namespace))
	if err != nil {
		return nil, err
	}

	attachResp, err := r.frontendClient.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
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
	if err != nil {
		// The workflow can close or be replaced after Describe and before this
		// attach. Re-read the observed run to preserve its actual
		// terminal status instead of synthesizing COMPLETED.
		return r.describeCompletedStart(ctx, scheduler, start, firstExecutionRunID)
	}
	if !attachedToDescribedChain(wfInfo, attachResp) {
		return r.describeCompletedStart(ctx, scheduler, start, firstExecutionRunID)
	}

	// Callback attached successfully.
	return &watchResult{firstExecutionRunID: firstExecutionRunID}, nil
}

func attachedToDescribedChain(
	wfInfo *workflowpb.WorkflowExecutionInfo,
	attachResp *workflowservice.StartWorkflowExecutionResponse,
) bool {
	if describedFirstRunID, attachedFirstRunID := wfInfo.GetFirstRunId(), attachResp.GetFirstExecutionRunId(); describedFirstRunID != "" && attachedFirstRunID != "" {
		return describedFirstRunID == attachedFirstRunID
	}
	return wfInfo.GetExecution().GetRunId() == attachResp.GetRunId()
}

func (r *SchedulerCallbacksTaskHandler) describeCompletedStart(
	ctx context.Context,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
	firstExecutionRunID string,
) (*watchResult, error) {
	descResp, err := r.historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId,
		Request: &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: scheduler.Namespace,
			Execution: &commonpb.WorkflowExecution{WorkflowId: start.WorkflowId, RunId: start.RunId},
		},
	})
	if err != nil {
		return nil, err
	}
	wfInfo := descResp.GetWorkflowExecutionInfo()
	if wfInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING || wfInfo.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED {
		return nil, serviceerror.NewUnavailable("workflow changed while attaching scheduler callback")
	}
	return &watchResult{
		firstExecutionRunID: firstExecutionRunID,
		completed: &schedulespb.CompletedResult{
			Status:    wfInfo.GetStatus(),
			CloseTime: wfInfo.GetCloseTime(),
		},
	}, nil
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
