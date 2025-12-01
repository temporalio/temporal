package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
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
	"go.temporal.io/server/common/util"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	legacyscheduler "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	InvokerTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		SpecProcessor  SpecProcessor

		HistoryClient resource.HistoryClient

		// FrontendClient is used for specifically StartWorkflow calls, to ensure that
		// the request makes it through metering's interceptor. Because we don't change for
		// terminate/cancels, we can go directly to history for other service calls.
		FrontendClient workflowservice.WorkflowServiceClient
	}

	InvokerExecuteTaskExecutor struct {
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		historyClient  resource.HistoryClient
		frontendClient workflowservice.WorkflowServiceClient
	}

	InvokerProcessBufferTaskExecutor struct {
		config         *Config
		metricsHandler metrics.Handler
		baseLogger     log.Logger
		historyClient  resource.HistoryClient
		frontendClient workflowservice.WorkflowServiceClient
	}

	// Per-task context.
	invokerTaskExecutorContext struct {
		context.Context

		actionsTaken int
		maxActions   int
	}

	rateLimitedError struct {
		// The requested interval to delay processing by rescheduilng.
		delay time.Duration
	}
)

const (
	// Lower bound for the deadline in which buffered actions are dropped.
	startWorkflowMinDeadline = 5 * time.Second

	// Because the catchup window doesn't apply to a manual start, pick a custom
	// execution deadline before timing out a start.
	manualStartExecutionDeadline = 1 * time.Hour

	// Upper bound on how many times starting an individual buffered action should be retried.
	InvokerMaxStartAttempts = 10 // TODO - dial this up/remove it
)

var (
	errRetryLimitExceeded       = queueerrors.NewUnprocessableTaskError("retry limit exceeded")
	_                     error = &rateLimitedError{}
)

func NewInvokerExecuteTaskExecutor(opts InvokerTaskExecutorOptions) *InvokerExecuteTaskExecutor {
	return &InvokerExecuteTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		historyClient:  opts.HistoryClient,
		frontendClient: opts.FrontendClient,
	}
}

func NewInvokerProcessBufferTaskExecutor(opts InvokerTaskExecutorOptions) *InvokerProcessBufferTaskExecutor {
	return &InvokerProcessBufferTaskExecutor{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		baseLogger:     opts.BaseLogger,
		historyClient:  opts.HistoryClient,
		frontendClient: opts.FrontendClient,
	}
}

func (e *InvokerExecuteTaskExecutor) Validate(
	_ chasm.Context,
	invoker *Invoker,
	_ chasm.TaskAttributes,
	_ *schedulerpb.InvokerExecuteTask,
) (bool, error) {
	// If another execute task already happened to kick everything off, we don't need
	// this one.
	eligibleStarts := invoker.getEligibleBufferedStarts()
	valid := len(invoker.GetTerminateWorkflows())+
		len(invoker.GetCancelWorkflows())+
		len(eligibleStarts) > 0
	return valid, nil
}

func (e *InvokerExecuteTaskExecutor) Execute(
	ctx context.Context,
	invokerRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *schedulerpb.InvokerExecuteTask,
) error {
	var result executeResult

	var invoker *Invoker
	var scheduler *Scheduler
	var lastCompletionState *schedulerpb.LastCompletionResult
	var callback *commonpb.Callback

	// Read and deep copy returned components, since we'll continue to access them
	// outside of this function (outside of the MS lock).
	_, err := chasm.ReadComponent(
		ctx,
		invokerRef,
		func(i *Invoker, ctx chasm.Context, _ any) (struct{}, error) {
			invoker = &Invoker{
				InvokerState: common.CloneProto(i.InvokerState),
			}

			s := i.Scheduler.Get(ctx)
			scheduler = &Scheduler{
				SchedulerState:     common.CloneProto(s.SchedulerState),
				cacheConflictToken: s.cacheConflictToken,
				compiledSpec:       s.compiledSpec,
			}

			lcs := s.LastCompletionResult.Get(ctx)
			lastCompletionState = common.CloneProto(lcs)

			// Set up the completion callback to handle workflow results.
			cb, err := chasm.GenerateNexusCallback(ctx, s)
			if err != nil {
				return struct{}{}, err
			}
			callback = common.CloneProto(cb)

			return struct{}{}, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to read component: %w", err)
	}

	logger := newTaggedLogger(e.baseLogger, scheduler)

	// Terminate, cancel, and start workflows. The result struct contains the
	// complete outcome of all requests executed in a single batch.
	//
	// Invoker will never have work pending for more than one of these calls (terminate,
	// cancel, start) at a time, so it isn't sensible to run them in parallel. The
	// structure below is simply for code simplicity.
	ictx := e.newInvokerTaskExecutorContext(ctx, scheduler)
	result = result.Append(e.terminateWorkflows(ictx, logger, scheduler, invoker.GetTerminateWorkflows()))
	result = result.Append(e.cancelWorkflows(ictx, logger, scheduler, invoker.GetCancelWorkflows()))
	sres, startResults := e.startWorkflows(ictx, logger, scheduler, invoker.getEligibleBufferedStarts(), lastCompletionState, callback)
	result = result.Append(sres)

	// Record action results on the Invoker (internal state), as well as the
	// Scheduler (user-facing metrics).
	_, _, err = chasm.UpdateComponent(
		ctx,
		invokerRef,
		func(i *Invoker, ctx chasm.MutableContext, _ any) (chasm.NoValue, error) {
			s := i.Scheduler.Get(ctx)

			i.recordExecuteResult(ctx, &result)
			s.recordActionResult(&schedulerActionResult{starts: startResults})

			return nil, nil
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to update component state: %w", err)
	}

	return nil
}

// takeNextAction increments the context's actionTaken counter, returning true if
// the action should be executed, and false if the task should instead yield.
func (i *invokerTaskExecutorContext) takeNextAction() bool {
	allowed := i.actionsTaken < i.maxActions
	if allowed {
		i.actionsTaken++
	}
	return allowed
}

// cancelWorkflows does a best-effort attempt to cancel all workflow executions provided in targets.
func (e *InvokerExecuteTaskExecutor) cancelWorkflows(
	ctx invokerTaskExecutorContext,
	logger log.Logger,
	scheduler *Scheduler,
	targets []*commonpb.WorkflowExecution,
) (result executeResult) {
	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	for _, wf := range targets {
		if !ctx.takeNextAction() {
			break
		}

		// Run all cancels concurrently.
		newCtx := ctx.Clone()
		wg.Go(func() {
			err := e.cancelWorkflow(newCtx, scheduler, wf)

			resultMutex.Lock()
			defer resultMutex.Unlock()

			if err != nil {
				logger.Error("failed to cancel workflow", tag.Error(err), tag.WorkflowID(wf.WorkflowId))
				e.metricsHandler.Counter(metrics.ScheduleCancelWorkflowErrors.Name()).Record(1)
			}

			// Cancels are only attempted once.
			result.CompletedCancels = append(result.CompletedCancels, wf)
		})
	}

	wg.Wait()
	return
}

// terminateWorkflows does a best-effort attempt to terminate all workflow executions provided in targets.
func (e *InvokerExecuteTaskExecutor) terminateWorkflows(
	ctx invokerTaskExecutorContext,
	logger log.Logger,
	scheduler *Scheduler,
	targets []*commonpb.WorkflowExecution,
) (result executeResult) {
	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	for _, wf := range targets {
		if !ctx.takeNextAction() {
			break
		}

		// Run all terminates concurrently.
		newCtx := ctx.Clone()
		wg.Go(func() {
			err := e.terminateWorkflow(newCtx, scheduler, wf)

			resultMutex.Lock()
			defer resultMutex.Unlock()

			if err != nil {
				logger.Error("failed to terminate workflow", tag.Error(err), tag.WorkflowID(wf.WorkflowId))
				e.metricsHandler.Counter(metrics.ScheduleTerminateWorkflowErrors.Name()).Record(1)
			}

			// Terminates are only attempted once.
			result.CompletedTerminates = append(result.CompletedTerminates, wf)
		})
	}

	wg.Wait()
	return
}

// startWorkflows executes the provided list of starts, returning a result with their outcomes.
func (e *InvokerExecuteTaskExecutor) startWorkflows(
	ctx invokerTaskExecutorContext,
	logger log.Logger,
	scheduler *Scheduler,
	starts []*schedulespb.BufferedStart,
	lastCompletionState *schedulerpb.LastCompletionResult,
	callback *commonpb.Callback,
) (result executeResult, startResults []*schedulepb.ScheduleActionResult) {
	metricsWithTag := e.metricsHandler.WithTags(
		metrics.StringTag(metrics.ScheduleActionTypeTag, metrics.ScheduleActionStartWorkflow))

	var wg sync.WaitGroup
	var resultMutex sync.Mutex

	for _, start := range starts {
		// Starts that haven't been executed yet will remain in `BufferedStarts`,
		// without change, so another ExecuteTask will be immediately created to continue
		// processing in a new task.
		if !ctx.takeNextAction() {
			break
		}

		// Check if this start is already in RecentActions. If so, we crashed
		// after starting a workflow, but before recording the result.
		if scheduler.isActionCompleted(start.WorkflowId) {
			logger.Info("skipping already-completed workflow", tag.WorkflowID(start.WorkflowId))
			continue
		}

		// Run all starts concurrently.
		newCtx := ctx.Clone()
		wg.Go(func() {
			startResult, err := e.startWorkflow(newCtx, scheduler, start, lastCompletionState, callback)

			resultMutex.Lock()
			defer resultMutex.Unlock()

			if err != nil {
				logger.Error("failed to start workflow", tag.Error(err))

				// Don't count "already started" for the error metric or retry, as it is most likely
				// due to misconfiguration.
				if !isAlreadyStartedError(err) {
					metricsWithTag.Counter(metrics.ScheduleActionErrors.Name()).Record(1)
				}

				if isRetryableError(err) {
					// Apply backoff to start and retry.
					e.applyBackoff(start, err)
					result.RetryableStarts = append(result.RetryableStarts, start)
				} else {
					// Drop the start from the buffer.
					result.FailedStarts = append(result.FailedStarts, start)
				}

				return
			}

			metricsWithTag.Counter(metrics.ScheduleActionSuccess.Name()).Record(1)
			result.CompletedStarts = append(result.CompletedStarts, start)
			startResults = append(startResults, startResult)
		})
	}

	wg.Wait()
	return
}

func (e *InvokerProcessBufferTaskExecutor) Validate(
	ctx chasm.Context,
	invoker *Invoker,
	attrs chasm.TaskAttributes,
	_ *schedulerpb.InvokerProcessBufferTask,
) (bool, error) {
	return validateTaskHighWaterMark(invoker.GetLastProcessedTime(), attrs.ScheduledTime)
}

func (e *InvokerProcessBufferTaskExecutor) Execute(
	ctx chasm.MutableContext,
	invoker *Invoker,
	_ chasm.TaskAttributes,
	_ *schedulerpb.InvokerProcessBufferTask,
) error {
	scheduler := invoker.Scheduler.Get(ctx)

	// Make sure we have something to start.
	executionInfo := scheduler.Schedule.GetAction().GetStartWorkflow()
	if executionInfo == nil {
		return queueerrors.NewUnprocessableTaskError("schedules must have an Action set")
	}

	// Compute actions to take from the current buffer.
	result := e.processBuffer(ctx, invoker, scheduler)

	// Update Scheduler metadata.
	scheduler.recordActionResult(&schedulerActionResult{
		overlapSkipped:      result.overlapSkipped,
		missedCatchupWindow: result.missedCatchupWindow,
	})

	// Update internal state and create new tasks.
	invoker.recordProcessBufferResult(ctx, &result)

	return nil
}

// processBuffer resolves the Invoker's buffered starts that haven't yet begun
// execution. This is where the decision is made to drive execution to
// completion, or skip/drop a start.
func (e *InvokerProcessBufferTaskExecutor) processBuffer(
	ctx chasm.MutableContext,
	invoker *Invoker,
	scheduler *Scheduler,
) (result processBufferResult) {
	isRunning := len(scheduler.Info.RunningWorkflows) > 0

	// Processing completely ignores any BufferedStart that's already executing/backing off.
	pendingBufferedStarts := util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return start.Attempt == 0
	})

	// Resolve overlap policies and trim BufferedStarts that are skipped by policy.
	action := legacyscheduler.ProcessBuffer(pendingBufferedStarts, isRunning, scheduler.resolveOverlapPolicy)

	// ProcessBuffer will drop starts by omitting them from NewBuffer. Start with the
	// diff between the input and NewBuffer, and add any executing starts.
	keepStarts := make(map[string]struct{}) // request ID -> is present
	for _, start := range action.NewBuffer {
		keepStarts[start.GetRequestId()] = struct{}{}
	}

	// Combine all available starts.
	readyStarts := action.OverlappingStarts
	if action.NonOverlappingStart != nil {
		readyStarts = append(readyStarts, action.NonOverlappingStart)
	}

	// Update result metrics.
	result.overlapSkipped = action.OverlapSkipped

	// Add starting workflows to result, trim others.
	for _, start := range readyStarts {
		// Ensure we can take more actions. Manual actions are always allowed.
		if !start.Manual && !scheduler.useScheduledAction(true) {
			// Drop buffered automated actions while paused.
			result.discardStarts = append(result.discardStarts, start)
			continue
		}

		if ctx.Now(invoker).After(e.startWorkflowDeadline(scheduler, start)) {
			// Drop expired starts.
			result.missedCatchupWindow++
			result.discardStarts = append(result.discardStarts, start)
			continue
		}

		// Append for immediate execution.
		keepStarts[start.GetRequestId()] = struct{}{}
		result.startWorkflows = append(result.startWorkflows, start)
	}

	result.discardStarts = util.FilterSlice(pendingBufferedStarts, func(start *schedulespb.BufferedStart) bool {
		_, keep := keepStarts[start.GetRequestId()]
		return !keep
	})

	// Terminate overrides cancel if both are requested.
	if action.NeedTerminate {
		result.terminateWorkflows = scheduler.GetInfo().GetRunningWorkflows()
	} else if action.NeedCancel {
		result.cancelWorkflows = scheduler.GetInfo().GetRunningWorkflows()
	}

	return
}

// applyBackoff updates start's BackoffTime based on err and the retry policy.
func (e *InvokerExecuteTaskExecutor) applyBackoff(start *schedulespb.BufferedStart, err error) {
	if err == nil {
		return
	}

	var delay time.Duration
	if rateLimitDelay, ok := isRateLimitedError(err); ok {
		// If we have the rate limiter's delay, use that.
		delay = rateLimitDelay
	} else {
		// Otherwise, use the backoff policy. Elapsed time is left at 0 because we bound
		// on number of attempts.
		delay = e.config.RetryPolicy().ComputeNextDelay(0, int(start.Attempt), nil)
	}

	start.BackoffTime = timestamppb.New(time.Now().Add(delay))
}

// startWorkflowDeadline returns the latest time at which a buffered workflow
// should be started, instead of dropped. The deadline puts an upper bound on
// the number of retry attempts per buffered start.
func (e *InvokerProcessBufferTaskExecutor) startWorkflowDeadline(
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
) time.Time {
	var timeout time.Duration
	if start.Manual {
		// For manual starts, use a default static value, as the catchup window doesn't apply.
		timeout = manualStartExecutionDeadline
	} else {
		// Set request deadline based on the schedule's catchup window, which is the
		// latest time that it's acceptable to start this workflow.
		tweakables := e.config.Tweakables(scheduler.Namespace)
		timeout = catchupWindow(scheduler, tweakables)
	}

	timeout = max(timeout, startWorkflowMinDeadline)

	return start.ActualTime.AsTime().Add(timeout)
}

func (e *InvokerExecuteTaskExecutor) startWorkflow(
	ctx context.Context,
	scheduler *Scheduler,
	start *schedulespb.BufferedStart,
	lastCompletionState *schedulerpb.LastCompletionResult,
	callback *commonpb.Callback,
) (*schedulepb.ScheduleActionResult, error) {
	requestSpec := scheduler.GetSchedule().GetAction().GetStartWorkflow()

	if start.Attempt >= InvokerMaxStartAttempts {
		return nil, errRetryLimitExceeded
	}

	// Get rate limiter permission once per buffered start, on the first attempt only.
	if start.Attempt == 1 {
		delay, err := e.getRateLimiterPermission()
		if err != nil {
			return nil, err
		}
		if delay > 0 {
			return nil, newRateLimitedError(delay)
		}
	}

	reusePolicy := enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE
	if start.Manual {
		reusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}

	var lcr []*commonpb.Payload
	if lastCompletionState.Success != nil {
		lcr = append(lcr, lastCompletionState.Success)
	}
	request := &workflowservice.StartWorkflowExecutionRequest{
		CompletionCallbacks:      []*commonpb.Callback{callback},
		Header:                   requestSpec.Header,
		Identity:                 scheduler.identity(),
		Input:                    requestSpec.Input,
		Memo:                     requestSpec.Memo,
		Namespace:                scheduler.Namespace,
		RequestId:                start.RequestId,
		RetryPolicy:              requestSpec.RetryPolicy,
		SearchAttributes:         scheduler.startWorkflowSearchAttributes(start.NominalTime.AsTime()),
		TaskQueue:                requestSpec.TaskQueue,
		UserMetadata:             requestSpec.UserMetadata,
		WorkflowExecutionTimeout: requestSpec.WorkflowExecutionTimeout,
		WorkflowId:               start.WorkflowId,
		WorkflowIdReusePolicy:    reusePolicy,
		WorkflowRunTimeout:       requestSpec.WorkflowRunTimeout,
		WorkflowTaskTimeout:      requestSpec.WorkflowTaskTimeout,
		WorkflowType:             requestSpec.WorkflowType,
		Priority:                 requestSpec.Priority,
		ContinuedFailure:         lastCompletionState.Failure,
		LastCompletionResult: &commonpb.Payloads{
			Payloads: lcr,
		},
	}

	result, err := e.frontendClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		return nil, err
	}
	actualStartTime := time.Now()

	// Record time taken from action eligible to workflow started.
	if !start.Manual {
		e.metricsHandler.
			Timer(metrics.ScheduleActionDelay.Name()).
			Record(actualStartTime.Sub(start.DesiredTime.AsTime()))
	}

	return &schedulepb.ScheduleActionResult{
		ScheduleTime: start.ActualTime,
		ActualTime:   timestamppb.New(actualStartTime),
		StartWorkflowResult: &commonpb.WorkflowExecution{
			WorkflowId: start.WorkflowId,
			RunId:      result.RunId,
		},
		StartWorkflowStatus: result.Status, // usually should be RUNNING
	}, nil
}

func (e *InvokerExecuteTaskExecutor) terminateWorkflow(
	ctx context.Context,
	scheduler *Scheduler,
	target *commonpb.WorkflowExecution,
) error {
	request := &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId,
		TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:           scheduler.Namespace,
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: target.WorkflowId},
			Reason:              "terminated by schedule overlap policy",
			Identity:            scheduler.identity(),
			FirstExecutionRunId: target.RunId,
		},
	}
	_, err := e.historyClient.TerminateWorkflowExecution(ctx, request)
	return err
}

func (e *InvokerExecuteTaskExecutor) cancelWorkflow(
	ctx context.Context,
	scheduler *Scheduler,
	target *commonpb.WorkflowExecution,
) error {
	request := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: scheduler.NamespaceId,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:           scheduler.Namespace,
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: target.WorkflowId},
			Reason:              "cancelled by schedule overlap policy",
			Identity:            scheduler.identity(),
			FirstExecutionRunId: target.RunId,
		},
	}
	_, err := e.historyClient.RequestCancelWorkflowExecution(ctx, request)
	return err
}

// getRateLimiterPermission returns a delay for which the caller should wait
// before proceeding. If an error is returned, execution should not proceed, and
// reservation should be retried.
func (e *InvokerExecuteTaskExecutor) getRateLimiterPermission() (delay time.Duration, err error) {
	// For now, we're only going to rate limit via APS.
	return
}

func isAlreadyStartedError(err error) bool {
	var expectedErr *serviceerror.WorkflowExecutionAlreadyStarted
	return errors.As(err, &expectedErr)
}

func isRateLimitedError(err error) (time.Duration, bool) {
	var expectedErr *rateLimitedError
	if errors.As(err, &expectedErr) {
		return expectedErr.delay, true
	}
	return 0, false
}

func isRetryableError(err error) bool {
	_, rateLimited := isRateLimitedError(err)
	return !errors.Is(err, errRetryLimitExceeded) &&
		(rateLimited ||
			common.IsServiceTransientError(err) ||
			common.IsContextDeadlineExceededErr(err))
}

func newRateLimitedError(delay time.Duration) error {
	return &rateLimitedError{delay}
}

func (r *rateLimitedError) Error() string {
	return fmt.Sprintf("rate limited for %s", r.delay)
}

func (e *InvokerExecuteTaskExecutor) newInvokerTaskExecutorContext(
	ctx context.Context,
	scheduler *Scheduler,
) invokerTaskExecutorContext {
	tweakables := e.config.Tweakables(scheduler.Namespace)
	maxActions := tweakables.MaxActionsPerExecution

	return invokerTaskExecutorContext{
		Context:      ctx,
		actionsTaken: 0,
		maxActions:   maxActions,
	}
}

func (i invokerTaskExecutorContext) Clone() invokerTaskExecutorContext {
	return invokerTaskExecutorContext{
		Context:      i.Context,
		actionsTaken: i.actionsTaken,
		maxActions:   i.maxActions,
	}
}
