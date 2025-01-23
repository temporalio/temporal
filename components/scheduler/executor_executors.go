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
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common"
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
	ExecutorTaskExecutorOptions struct {
		fx.In

		Config         *Config
		MetricsHandler metrics.Handler
		BaseLogger     log.Logger
		HistoryClient  resource.HistoryClient
		FrontendClient workflowservice.WorkflowServiceClient
	}

	executorTaskExecutor struct {
		ExecutorTaskExecutorOptions
	}

	rateLimitedDetails struct {
		// The requested interval to delay processing by rescheduilng.
		Delay time.Duration
	}
)

const (
	startWorkflowMinDeadline = 5 * time.Second // Lower bound for the deadline in which buffered actions are dropped.
	startWorkflowMaxDeadline = 1 * time.Hour   // Upper bound for the deadline in which buffered actions are dropped.

	// Because the catchup window doesn't apply to a manual start, pick a custom
	// execution deadline before timing out a start.
	manualStartExecutionDeadline = startWorkflowMaxDeadline

	// Upper bound on how many times starting an individual buffered action should be retried.
	ExecutorMaxStartAttempts = 10

	errTypeRetryLimitExceeded = "RetryLimitExceeded"
	errTypeRateLimited        = "RateLimited"
	errTypeAlreadyStarted     = "serviceerror.WorkflowExecutionAlreadyStarted"
)

func RegisterExecutorExecutors(registry *hsm.Registry, options ExecutorTaskExecutorOptions) error {
	e := executorTaskExecutor{
		ExecutorTaskExecutorOptions: options,
	}
	return hsm.RegisterTimerExecutor(registry, e.executeExecuteTask)
}

func (e executorTaskExecutor) executeExecuteTask(env hsm.Environment, node *hsm.Node, task ExecuteTask) error {
	schedulerNode := node.Parent
	scheduler, err := loadScheduler(schedulerNode)
	if err != nil {
		return err
	}
	logger := newTaggedLogger(e.BaseLogger, scheduler)

	executor, err := e.loadExecutor(node)
	if err != nil {
		return err
	}

	// Make sure we have something to start. If not, we can clear the buffer and
	// transition to the waiting state.
	executionInfo := scheduler.Schedule.Action.GetStartWorkflow()
	if executionInfo == nil || len(executor.BufferedStarts) == 0 {
		return hsm.MachineTransition(node, func(e Executor) (hsm.TransitionOutput, error) {
			e.ExecutorInternal.BufferedStarts = nil
			return TransitionWait.Apply(e, EventWait{
				Node: node,
			})
		})
	}

	// Drain the Executor's BufferedStarts. drainBuffer will update Executor's
	// BufferedStarts, as well as Scheduler metadata.
	e.drainBuffer(logger, env, executor, scheduler, executionInfo)

	// Update SchedulerInfo metadata.
	err = hsm.MachineTransition(schedulerNode, func(s Scheduler) (hsm.TransitionOutput, error) {
		s.Info = scheduler.Info
		return hsm.TransitionOutput{}, nil
	})
	if err != nil {
		return err
	}

	// Find the earliest possible time a BufferedStart can be retried, if any, based
	// on BackoffTime.
	var retryAt time.Time
	for _, start := range executor.BufferedStarts {
		backoffTime := start.BackoffTime.AsTime()

		// Zero-value protobuf timestamps deserialize to the UNIX epoch, skip if the
		// backoffTime looks invalid/unset.
		if backoffTime.Before(start.ActualTime.AsTime()) {
			continue
		}

		if retryAt.IsZero() || backoffTime.Before(retryAt) {
			retryAt = backoffTime
		}
	}

	if !retryAt.IsZero() {
		// When retryAt is set, reschedule.
		return hsm.MachineTransition(node, func(e Executor) (hsm.TransitionOutput, error) {
			e.ExecutorInternal.BufferedStarts = executor.BufferedStarts
			return TransitionExecute.Apply(e, EventExecute{
				Node:     node,
				Deadline: retryAt,
			})
		})
	}

	// No more buffered starts, or remaining starts are waiting for a workflow to be
	// closed. We can transition to waiting.
	return hsm.MachineTransition(node, func(e Executor) (hsm.TransitionOutput, error) {
		e.ExecutorInternal.BufferedStarts = executor.BufferedStarts
		return TransitionWait.Apply(e, EventWait{
			Node: node,
		})
	})
}

// drainBuffer uses ProcessBuffer to resolve the Executor's remaining buffered
// starts, and then carries out the returned action.
func (e executorTaskExecutor) drainBuffer(
	logger log.Logger,
	env hsm.Environment,
	executor Executor,
	scheduler Scheduler,
	request *workflowpb.NewWorkflowExecutionInfo,
) {
	metricsWithTag := e.MetricsHandler.WithTags(
		metrics.StringTag(metrics.ScheduleActionTypeTag, metrics.ScheduleActionStartWorkflow))
	isRunning := len(scheduler.Info.RunningWorkflows) > 0

	// Resolve overlap policies and prepare next workflows to start.
	action := scheduler1.ProcessBuffer(executor.BufferedStarts, isRunning, scheduler.resolveOverlapPolicy)
	newBuffer := action.NewBuffer

	// Combine all available starts.
	allStarts := action.OverlappingStarts
	if action.NonOverlappingStart != nil {
		allStarts = append(allStarts, action.NonOverlappingStart)
	}

	// Start workflows.
	for _, start := range allStarts {
		// Ensure we can take more actions. Manual actions are always allowed.
		if !start.Manual && !scheduler.useScheduledAction(true) {
			// Drop buffered automated actions while paused.
			continue
		}

		if env.Now().Before(start.BackoffTime.AsTime()) {
			// BufferedStart is still backing off, push it back into the buffer.
			newBuffer = append(newBuffer, start)
			continue
		}

		if env.Now().After(e.startWorkflowDeadline(scheduler, start)) {
			// Drop expired starts.
			continue
		}

		result, err := e.startWorkflow(env, scheduler, start, request)
		if err != nil {
			logger.Error("Failed to start workflow: %w", tag.Error(err))

			// Don't count "already started" for the error metric or retry, as it is most likely
			// due to misconfiguration.
			if !isAlreadyStartedError(err) {
				metricsWithTag.Counter(metrics.ScheduleActionErrors.Name()).Record(1)
			}

			if !isRetryableError(err) {
				// Drop non-retryable starts (they've exceeded the retry limit).
				continue
			}

			// When backing off before a retry, a BufferedStart's BackoffTime is updated with the
			// backoff's delay. This is done because an Execute task can be scheduled at any
			// time (such as when generating additional actions), so merely delaying the Execute
			// task's Deadline is insufficient.
			e.applyBackoff(env, start, err)
			newBuffer = append(newBuffer, start)
			continue
		}

		// Record result and success metric.
		metricsWithTag.Counter(metrics.ScheduleActionSuccess.Name()).Record(1)
		e.recordAction(scheduler, result)
	}

	// Apply cancel and terminate requests due to overlap policies.
	e.applyOverlapPolicies(logger, metricsWithTag, scheduler, action.NeedTerminate, action.NeedCancel)

	// Update BufferedStarts and metadata.
	executor.BufferedStarts = newBuffer
	scheduler.Info.OverlapSkipped += action.OverlapSkipped
}

func (e executorTaskExecutor) applyOverlapPolicies(
	logger log.Logger,
	metricsHandler metrics.Handler,
	scheduler Scheduler,
	needTerminate, needCancel bool,
) {
	// Terminate overrides cancel if both are requested.
	if needTerminate {
		for _, target := range scheduler.Info.RunningWorkflows {
			err := e.terminateWorkflow(scheduler, target)
			if err != nil {
				logger.Error("Failed to terminate workflow: %w", tag.Error(err), tag.WorkflowID(target.WorkflowId))
				metricsHandler.Counter(metrics.ScheduleTerminateWorkflowErrors.Name()).Record(1)
			}
		}
	} else if needCancel {
		for _, target := range scheduler.Info.RunningWorkflows {
			err := e.cancelWorkflow(scheduler, target)
			if err != nil {
				logger.Error("Failed to cancel workflow: %w", tag.Error(err), tag.WorkflowID(target.WorkflowId))
				metricsHandler.Counter(metrics.ScheduleCancelWorkflowErrors.Name()).Record(1)
			}
		}
	}
}

// applyBackoff updates start's BackoffTime based on err and the retry policy.
func (e executorTaskExecutor) applyBackoff(env hsm.Environment, start *schedulespb.BufferedStart, err error) {
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
func (e executorTaskExecutor) startWorkflowDeadline(
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
	timeout = min(timeout, startWorkflowMaxDeadline)

	return start.ActualTime.AsTime().UTC().Add(timeout)
}

func (e executorTaskExecutor) startWorkflow(
	env hsm.Environment,
	scheduler Scheduler,
	start *schedulespb.BufferedStart,
	requestSpec *workflowpb.NewWorkflowExecutionInfo,
) (*schedulepb.ScheduleActionResult, error) {
	nominalTimeSec := start.NominalTime.AsTime().UTC().Truncate(time.Second)
	workflowID := fmt.Sprintf("%s-%s", requestSpec.WorkflowId, nominalTimeSec.Format(time.RFC3339))

	if start.Attempt >= ExecutorMaxStartAttempts {
		return nil, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("Retry limit exceeded while attempting to start %s", workflowID),
			errTypeRetryLimitExceeded,
			nil,
		)
	}
	start.Attempt++

	// Get rate limiter permission once per buffered start.
	if start.Attempt == 1 {
		delay, err := e.getRateLimiterPermission()
		if err != nil {
			return nil, err
		}
		if delay > 0 {
			return nil, temporal.NewApplicationError(
				fmt.Sprintf("Rate limited while attempting to start %s", workflowID),
				errTypeRateLimited,
				rateLimitedDetails{Delay: delay},
			)
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
	ctx, cancelFunc := context.WithTimeout(context.Background(), e.Config.ServiceCallTimeout())
	defer cancelFunc()
	result, err := e.FrontendClient.StartWorkflowExecution(ctx, request)
	if err != nil {
		return nil, translateError(err, "StartWorkflowExecution")
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

func (e executorTaskExecutor) terminateWorkflow(scheduler Scheduler, target *commonpb.WorkflowExecution) error {
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
	ctx, cancelFunc := context.WithTimeout(context.Background(), e.Config.ServiceCallTimeout())
	defer cancelFunc()
	_, err := e.HistoryClient.TerminateWorkflowExecution(ctx, request)
	return translateError(err, "TerminateWorkflowExecution")
}

func (e executorTaskExecutor) cancelWorkflow(scheduler Scheduler, target *commonpb.WorkflowExecution) error {
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
	ctx, cancelFunc := context.WithTimeout(context.Background(), e.Config.ServiceCallTimeout())
	defer cancelFunc()
	_, err := e.HistoryClient.RequestCancelWorkflowExecution(ctx, request)
	return translateError(err, "CancelWorkflowExecution")
}

// getRateLimiterPermission returns a delay for which the caller should wait
// before proceeding. If an error is returned, execution should not proceed, and
// reservation should be retried.
func (e executorTaskExecutor) getRateLimiterPermission() (delay time.Duration, err error) {
	// TODO - per-namespace rate limiting. TaskExecutor is more broad than a single namespace.
	return
}

// recordAction updates the SchedulerInfo metadata block with StartWorkflowExecution's result.
func (e executorTaskExecutor) recordAction(s Scheduler, result *schedulepb.ScheduleActionResult) {
	tweakables := e.Config.Tweakables(s.Namespace)
	s.Info.ActionCount++
	s.Info.RecentActions = util.SliceTail(append(s.Info.RecentActions, result), tweakables.RecentActionCount)
	if result.StartWorkflowResult != nil {
		s.Info.RunningWorkflows = append(s.Info.RunningWorkflows, result.StartWorkflowResult)
	}
}

func (e executorTaskExecutor) loadExecutor(node *hsm.Node) (Executor, error) {
	prevExecutor, err := hsm.MachineData[Executor](node)
	if err != nil {
		return Executor{}, err
	}

	return Executor{
		ExecutorInternal: prevExecutor.ExecutorInternal,
	}, nil
}

// translateError converts a dependent service error into an application error.
// Errors are classified between retryable and non-retryable.
func translateError(err error, msgPrefix string) error {
	if err == nil {
		return nil
	}

	message := fmt.Sprintf("%s: %s", msgPrefix, err.Error())
	errorType := util.ErrorType(err)

	if common.IsServiceTransientError(err) || common.IsContextDeadlineExceededErr(err) {
		return temporal.NewApplicationErrorWithCause(message, errorType, err)
	}

	return temporal.NewNonRetryableApplicationError(message, errorType, err)
}

func isAlreadyStartedError(err error) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.Type() == errTypeAlreadyStarted
}

func isRateLimitedError(err error) (time.Duration, bool) {
	var details rateLimitedDetails
	var appErr *temporal.ApplicationError

	if errors.As(err, &appErr) && appErr.Type() == errTypeRateLimited && appErr.Details(&details) == nil {
		return details.Delay, true
	}

	return 0, false
}

func isRetryableError(err error) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && !appErr.NonRetryable()
}
