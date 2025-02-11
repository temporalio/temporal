// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/hsm"
	scheduler1 "go.temporal.io/server/service/worker/scheduler"
	"go.uber.org/fx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	InvokerTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		HistoryClient  resource.HistoryClient
		FrontendClient workflowservice.WorkflowServiceClient
	}

	invokerTaskExecutor struct {
		InvokerTaskExecutorOptions
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
	ExecutorMaxStartAttempts = 10 // TODO - dial this up/remove it
)

var (
	errRetryLimitExceeded       = errors.New("retry limit exceeded")
	_                     error = &rateLimitedError{}
)

func RegisterInvokerExecutors(registry *hsm.Registry, options InvokerTaskExecutorOptions) error {
	e := invokerTaskExecutor{
		InvokerTaskExecutorOptions: options,
	}

	err := hsm.RegisterTimerExecutor(registry, e.executeProcessBufferTask)
	if err != nil {
		return err
	}
	return hsm.RegisterImmediateExecutor(registry, e.executeExecuteTask)
}

func (e invokerTaskExecutor) executeExecuteTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task ExecuteTask,
) error {
	var scheduler *Scheduler
	var invoker *Invoker
	var result executeResult

	// Load Scheduler and Invoker's current states.
	err := env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		s, err := loadScheduler(node.Parent)
		if err != nil {
			return err
		}
		scheduler = &s

		i, err := e.loadInvoker(node)
		if err != nil {
			return err
		}
		invoker = &i

		return nil
	})
	if err != nil {
		return err
	}
	logger := newTaggedLogger(e.BaseLogger, *scheduler)

	// If we have nothing to do, we can return without any additional writes.
	eligibleStarts := invoker.getEligibleBufferedStarts()
	if len(invoker.GetTerminateWorkflows())+
		len(invoker.GetCancelWorkflows())+
		len(eligibleStarts) == 0 {
		return nil
	}

	// Terminate, cancel, and start workflows. The result struct contains the
	// complete outcome of all requests executed in a single batch.
	result = result.Append(e.terminateWorkflows(logger, *scheduler, invoker.GetTerminateWorkflows()))
	result = result.Append(e.cancelWorkflows(logger, *scheduler, invoker.GetCancelWorkflows()))
	sres, startResults := e.startWorkflows(logger, env, *scheduler, eligibleStarts)
	result = result.Append(sres)

	// Write results.
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		// Record completed executions on the Invoker.
		err := hsm.MachineTransition(node, func(i Invoker) (hsm.TransitionOutput, error) {
			return TransitionCompleteExecution.Apply(i, EventCompleteExecution{
				Node:          node,
				executeResult: result,
			})
		})
		if err != nil {
			return err
		}

		// Record action results on the Scheduler.
		return hsm.MachineTransition(node.Parent, func(s Scheduler) (hsm.TransitionOutput, error) {
			return TransitionRecordAction.Apply(s, EventRecordAction{
				Node:        node,
				ActionCount: int64(len(startResults)),
				Results:     startResults,
			})
		})
	})
}

// cancelWorkflows does a best-effort attempt to cancel all workflow executions provided in targets.
func (e invokerTaskExecutor) cancelWorkflows(
	logger log.Logger,
	scheduler Scheduler,
	targets []*commonpb.WorkflowExecution,
) (result executeResult) {
	for _, wf := range targets {
		err := e.cancelWorkflow(scheduler, wf)
		if err != nil {
			logger.Error("Failed to cancel workflow", tag.Error(err), tag.WorkflowID(wf.WorkflowId))
			e.MetricsHandler.Counter(metrics.ScheduleCancelWorkflowErrors.Name()).Record(1)
		}

		// Cancels are only attempted once.
		result.CompletedCancels = append(result.CompletedCancels, wf)
	}
	return
}

// terminateWorkflows does a best-effort attempt to cancel all workflow executions provided in targets.
func (e invokerTaskExecutor) terminateWorkflows(
	logger log.Logger,
	scheduler Scheduler,
	targets []*commonpb.WorkflowExecution,
) (result executeResult) {
	for _, wf := range targets {
		err := e.terminateWorkflow(scheduler, wf)
		if err != nil {
			logger.Error("Failed to terminate workflow", tag.Error(err), tag.WorkflowID(wf.WorkflowId))
			e.MetricsHandler.Counter(metrics.ScheduleTerminateWorkflowErrors.Name()).Record(1)
		}

		// Terminates are only attempted once.
		result.CompletedTerminates = append(result.CompletedTerminates, wf)
	}
	return
}

// startWorkflows executes the provided list of starts, returning a result with their outcomes.
func (e invokerTaskExecutor) startWorkflows(
	logger log.Logger,
	env hsm.Environment,
	scheduler Scheduler,
	starts []*schedulespb.BufferedStart,
) (result executeResult, startResults []*schedulepb.ScheduleActionResult) {
	metricsWithTag := e.MetricsHandler.WithTags(
		metrics.StringTag(metrics.ScheduleActionTypeTag, metrics.ScheduleActionStartWorkflow))
	for _, start := range starts {
		startResult, err := e.startWorkflow(env, scheduler, start)
		if err != nil {
			logger.Error("Failed to start workflow", tag.Error(err))

			// Don't count "already started" for the error metric or retry, as it is most likely
			// due to misconfiguration.
			if !isAlreadyStartedError(err) {
				metricsWithTag.Counter(metrics.ScheduleActionErrors.Name()).Record(1)
			}

			if isRetryableError(err) {
				// Apply backoff to start and retry.
				e.applyBackoff(env, start, err)
				result.RetryableStarts = append(result.RetryableStarts, start)
			} else {
				// Drop the start from the buffer.
				result.FailedStarts = append(result.FailedStarts, start)
			}

			continue
		}

		metricsWithTag.Counter(metrics.ScheduleActionSuccess.Name()).Record(1)
		result.CompletedStarts = append(result.CompletedStarts, start)
		startResults = append(startResults, startResult)
	}
	return
}

func (e invokerTaskExecutor) executeProcessBufferTask(env hsm.Environment, node *hsm.Node, task ProcessBufferTask) error {
	schedulerNode := node.Parent
	scheduler, err := loadScheduler(schedulerNode)
	if err != nil {
		return err
	}

	invoker, err := e.loadInvoker(node)
	if err != nil {
		return err
	}

	// Make sure we have something to start. If not, we can clear the buffer and
	// transition to the waiting state.
	executionInfo := scheduler.Schedule.Action.GetStartWorkflow()
	if executionInfo == nil || len(invoker.GetBufferedStarts()) == 0 {
		return hsm.MachineTransition(node, func(e Invoker) (hsm.TransitionOutput, error) {
			return TransitionWait.Apply(e, EventWait{
				Node:              node,
				LastProcessedTime: env.Now(),
			})
		})
	}

	// Compute actions to take from the current buffer.
	result := e.processBuffer(env, invoker, scheduler)

	// Update Scheduler metadata.
	err = hsm.MachineTransition(schedulerNode, func(s Scheduler) (hsm.TransitionOutput, error) {
		return TransitionRecordAction.Apply(s, EventRecordAction{
			Node:           schedulerNode,
			OverlapSkipped: result.OverlapSkipped,
			BufferDropped:  result.BufferDropped,
		})
	})
	if err != nil {
		return err
	}

	// If any BufferedStarts are past their first attempt, we can retry after a backoff.
	backingOff := false
	for _, start := range invoker.GetBufferedStarts() {
		if start.Attempt > 1 {
			backingOff = true
			break
		}
	}

	if backingOff {
		// Processing will be rescheduled for the earliest backing off start.
		return hsm.MachineTransition(node, func(i Invoker) (hsm.TransitionOutput, error) {
			return TransitionRetryProcessing.Apply(i, EventRetryProcessing{
				Node:                node,
				LastProcessedTime:   env.Now(),
				processBufferResult: result,
			})
		})
	}

	// No more buffered starts, or remaining starts are waiting for a workflow to be
	// closed. We can kick off ready starts and transition to waiting.
	return hsm.MachineTransition(node, func(i Invoker) (hsm.TransitionOutput, error) {
		return TransitionFinishProcessing.Apply(i, EventFinishProcessing{
			Node:                node,
			LastProcessedTime:   env.Now(),
			processBufferResult: result,
		})
	})
}

// processBuffer uses ProcessBuffer to resolve the Executor's remaining buffered
// starts, and then carries out the returned action.
func (e invokerTaskExecutor) processBuffer(
	env hsm.Environment,
	invoker Invoker,
	scheduler Scheduler,
) (result processBufferResult) {
	isRunning := len(scheduler.Info.RunningWorkflows) > 0

	// Processing completely ignores any BufferedStart that's already executing/backing off.
	pendingBufferedStarts := util.FilterSlice(invoker.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return start.Attempt == 0
	})

	// Resolve overlap policies and trim BufferedStarts that are skipped by policy.
	action := scheduler1.ProcessBuffer(pendingBufferedStarts, isRunning, scheduler.resolveOverlapPolicy)

	// ProcessBuffer will drop starts by omitting them from NewBuffer. Start with the
	// diff between the input and NewBuffer, and add any executing starts.
	keepStarts := make(map[string]bool) // request ID -> is present
	for _, start := range action.NewBuffer {
		keepStarts[start.GetRequestId()] = true
	}

	// Combine all available starts.
	readyStarts := action.OverlappingStarts
	if action.NonOverlappingStart != nil {
		readyStarts = append(readyStarts, action.NonOverlappingStart)
	}

	// Update result metrics.
	result.OverlapSkipped = action.OverlapSkipped

	// Add starting workflows to result, trim others.
	for _, start := range readyStarts {
		// Ensure we can take more actions. Manual actions are always allowed.
		if !start.Manual && !scheduler.useScheduledAction(true) {
			// Drop buffered automated actions while paused.
			result.DiscardStarts = append(result.DiscardStarts, start)
			continue
		}

		if env.Now().After(e.startWorkflowDeadline(scheduler, start)) {
			// Drop expired starts.
			// TODO - we should increment a user-visible counter on ScheduleInfo when this happens.
			result.DiscardStarts = append(result.DiscardStarts, start)
			continue
		}

		// Append for immediate execution.
		keepStarts[start.GetRequestId()] = true
		result.StartWorkflows = append(result.StartWorkflows, start)
	}

	result.DiscardStarts = util.FilterSlice(pendingBufferedStarts, func(start *schedulespb.BufferedStart) bool {
		return !keepStarts[start.GetRequestId()]
	})

	// Terminate overrides cancel if both are requested.
	if action.NeedTerminate {
		result.TerminateWorkflows = scheduler.GetInfo().GetRunningWorkflows()
	} else if action.NeedCancel {
		result.CancelWorkflows = scheduler.GetInfo().GetRunningWorkflows()
	}

	return
}

// applyBackoff updates start's BackoffTime based on err and the retry policy.
func (e invokerTaskExecutor) applyBackoff(env hsm.Environment, start *schedulespb.BufferedStart, err error) {
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
		delay = e.Config.RetryPolicy().ComputeNextDelay(0, int(start.Attempt), nil)
	}

	start.BackoffTime = timestamppb.New(env.Now().Add(delay))
}

// startWorkflowDeadline returns the latest time at which a buffered workflow
// should be started, instead of dropped. The deadline puts an upper bound on
// the number of retry attempts per buffered start.
func (e invokerTaskExecutor) startWorkflowDeadline(
	scheduler Scheduler,
	start *schedulespb.BufferedStart,
) time.Time {
	var timeout time.Duration
	if start.Manual {
		// For manual starts, use a default static value, as the catchup window doesn't apply.
		timeout = manualStartExecutionDeadline
	} else {
		// Set request deadline based on the schedule's catchup window, which is the
		// latest time that it's acceptable to start this workflow.
		tweakables := e.Config.Tweakables(scheduler.Namespace)
		timeout = catchupWindow(scheduler, tweakables)
	}

	timeout = max(timeout, startWorkflowMinDeadline)

	return start.ActualTime.AsTime().Add(timeout)
}

func (e invokerTaskExecutor) startWorkflow(
	env hsm.Environment,
	scheduler Scheduler,
	start *schedulespb.BufferedStart,
) (*schedulepb.ScheduleActionResult, error) {
	requestSpec := scheduler.GetSchedule().GetAction().GetStartWorkflow()
	nominalTimeSec := start.NominalTime.AsTime().Truncate(time.Second)
	workflowID := fmt.Sprintf("%s-%s", requestSpec.WorkflowId, nominalTimeSec.Format(time.RFC3339))

	if start.Attempt >= ExecutorMaxStartAttempts {
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

	// TODO - set last completion result/continued failure
	// TODO - set search attributes
	request := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                scheduler.Namespace,
		WorkflowId:               workflowID,
		WorkflowType:             requestSpec.WorkflowType,
		TaskQueue:                requestSpec.TaskQueue,
		Input:                    requestSpec.Input,
		WorkflowExecutionTimeout: requestSpec.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       requestSpec.WorkflowRunTimeout,
		WorkflowTaskTimeout:      requestSpec.WorkflowTaskTimeout,
		Identity:                 scheduler.identity(),
		RequestId:                start.RequestId,
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		RetryPolicy:              requestSpec.RetryPolicy,
		Memo:                     requestSpec.Memo,
		SearchAttributes:         nil,
		Header:                   requestSpec.Header,
		LastCompletionResult:     nil,
		ContinuedFailure:         nil,
		UserMetadata:             requestSpec.UserMetadata,
	}
	ctx, cancelFunc := e.newContext(scheduler.Namespace)
	defer cancelFunc()
	result, err := e.FrontendClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		return nil, err
	}

	return &schedulepb.ScheduleActionResult{
		ScheduleTime: start.ActualTime,
		ActualTime:   timestamppb.New(env.Now()),
		StartWorkflowResult: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      result.RunId,
		},
		StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}, nil
}

func (e invokerTaskExecutor) terminateWorkflow(scheduler Scheduler, target *commonpb.WorkflowExecution) error {
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
	ctx, cancelFunc := e.newContext(scheduler.Namespace)
	defer cancelFunc()
	_, err := e.HistoryClient.TerminateWorkflowExecution(ctx, request)
	return err
}

func (e invokerTaskExecutor) cancelWorkflow(scheduler Scheduler, target *commonpb.WorkflowExecution) error {
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
	ctx, cancelFunc := e.newContext(scheduler.Namespace)
	defer cancelFunc()
	_, err := e.HistoryClient.RequestCancelWorkflowExecution(ctx, request)
	return err
}

func (e invokerTaskExecutor) newContext(namespace string) (context.Context, context.CancelFunc) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), e.Config.ServiceCallTimeout())
	ctx = headers.SetCallerInfo(ctx, headers.NewBackgroundCallerInfo(namespace))
	return ctx, cancelFunc
}

// getRateLimiterPermission returns a delay for which the caller should wait
// before proceeding. If an error is returned, execution should not proceed, and
// reservation should be retried.
func (e invokerTaskExecutor) getRateLimiterPermission() (delay time.Duration, err error) {
	// TODO - per-namespace rate limiting. TaskExecutor is more broad than a single namespace.
	return
}

func (e invokerTaskExecutor) loadInvoker(node *hsm.Node) (Invoker, error) {
	prevInvoker, err := hsm.MachineData[Invoker](node)
	if err != nil {
		return Invoker{}, err
	}

	return Invoker{
		InvokerInternal: common.CloneProto(prevInvoker.InvokerInternal),
	}, nil
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
