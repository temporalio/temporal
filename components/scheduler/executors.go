// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"cmp"
	"context"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/util"
	"go.uber.org/fx"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	TaskExecutorOptions struct {
		fx.In
		MetricsHandler metrics.Handler
		Logger         log.Logger
		SpecBuilder    *scheduler.SpecBuilder
		FrontendClient workflowservice.WorkflowServiceClient
		HistoryClient  resource.HistoryClient
	}

	taskExecutor struct {
		options TaskExecutorOptions
		config  *Config
	}

	startWorkflowResult struct {
		result         *schedpb.ScheduleActionResult
		nonOverlapping bool
	}
)

func RegisterExecutor(
	registry *hsm.Registry,
	executorOptions TaskExecutorOptions,
	config *Config,
) error {
	exec := taskExecutor{
		options: executorOptions,
		config:  config,
	}
	if err := hsm.RegisterTimerExecutor(
		registry,
		exec.executeSchedulerWaitTask); err != nil {
		return err
	}
	return hsm.RegisterImmediateExecutor(registry, exec.executeSchedulerRunTask)
}

func (e taskExecutor) executeSchedulerWaitTask(
	env hsm.Environment,
	node *hsm.Node,
	task SchedulerWaitTask,
) error {
	if err := node.CheckRunning(); err != nil {
		return err
	}
	return hsm.MachineTransition(node, func(scheduler *Scheduler) (hsm.TransitionOutput, error) {
		return TransitionSchedulerActivate.Apply(scheduler, EventSchedulerActivate{})
	})
}

func (e taskExecutor) executeSchedulerRunTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task SchedulerActivateTask,
) error {

	nextWakeup, s, err := e.runSchedulingLogic(ctx, env, ref)
	if err != nil {
		return err
	}
	// try starting workflows in the buffer
	// nolint:revive
	for {
		hasNext, err := e.processBuffer(ctx, env, ref, s)
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}
	}

	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}
		s.NextInvocationTime = timestamppb.New(nextWakeup)

		return hsm.MachineTransition(node, func(scheduler *Scheduler) (hsm.TransitionOutput, error) {
			return TransitionSchedulerWait.Apply(scheduler, EventSchedulerWait{})
		})
	})

}

func (e taskExecutor) runSchedulingLogic(ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref) (nextWakeup time.Time, s *Scheduler, err error) {
	err = env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err = node.CheckRunning(); err != nil {
			return err
		}
		s, err = hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}

		if s.cspec == nil {
			cspec, err := e.options.SpecBuilder.NewCompiledSpec(s.Args.Schedule.Spec)
			if err != nil {
				if e.options.Logger != nil {
					e.options.Logger.Error("Invalid schedule", tag.Error(err))
				}
				s.cspec = nil
				return err
			}
			s.cspec = cspec
		}

		if s.Args.State.LastProcessedTime == nil {
			// log these as json since it's more readable than the Go representation
			specJson, _ := protojson.Marshal(s.Args.Schedule.Spec)
			policiesJson, _ := protojson.Marshal(s.Args.Schedule.Policies)
			e.options.Logger.Info("Starting schedule", tag.NewStringTag("spec", string(specJson)), tag.NewStringTag("policies", string(policiesJson)))
			s.Args.State.LastProcessedTime = timestamppb.Now()
			s.Args.State.ConflictToken = scheduler.InitialConflictToken
			s.Args.Info.CreateTime = s.Args.State.LastProcessedTime
		}

		t1 := timestamp.TimeValue(s.Args.State.LastProcessedTime)
		t2 := time.Now()
		if t2.Before(t1) {
			e.options.Logger.Warn("Time went backwards", tag.NewStringTag("time", t1.String()), tag.NewStringTag("time", t2.String()))
			t2 = t1
		}
		var lastAction time.Time
		nextWakeup, lastAction = e.processTimeRange(
			s,
			t1, t2,
			// resolve this to the schedule's policy as late as possible
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
			false,
			nil,
		)
		s.Args.State.LastProcessedTime = timestamppb.New(lastAction)
		// process backfills if we have any too
		e.processBackfills(s)
		return nil
	})
	return
}

func (e taskExecutor) processBuffer(ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	s *Scheduler) (tryAgain bool, err error) {
	e.options.Logger.Debug("processBuffer", tag.NewInt("buffer", len(s.Args.State.BufferedStarts)), tag.NewInt("running", len(s.Args.Info.RunningWorkflows)))
	tryAgain = false

	// Make sure we have something to start. If not, we can clear the buffer.
	req := s.Args.Schedule.Action.GetStartWorkflow()
	if req == nil || len(s.Args.State.BufferedStarts) == 0 {
		err = env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
			s.Args.State.BufferedStarts = nil
			return nil
		})
		return
	}

	isRunning := len(s.Args.Info.RunningWorkflows) > 0
	action := scheduler.ProcessBuffer(s.Args.State.BufferedStarts, isRunning, s.resolveOverlapPolicy)

	// Try starting whatever we're supposed to start now
	allStarts := action.OverlappingStarts
	if action.NonOverlappingStart != nil {
		allStarts = append(allStarts, action.NonOverlappingStart)
	}

	results := make([]startWorkflowResult, len(allStarts))

	for _, start := range allStarts {
		result, err := e.startWorkflow(s, start, req)
		metricsWithTag := e.options.MetricsHandler.WithTags(metrics.StringTag(metrics.ScheduleActionTypeTag, metrics.ScheduleActionStartWorkflow))
		if err != nil {
			e.options.Logger.Error("Failed to start workflow", tag.NewErrorTag(err))
			metricsWithTag.Counter(metrics.ScheduleActionErrors.Name()).Record(1)
			// TODO: we could put this back in the buffer and retry (after a delay) up until
			// the catchup window. of course, it's unlikely that this workflow would be making
			// progress while we're unable to start a new one, so maybe it's not that valuable.
			tryAgain = true
			continue
		}
		metricsWithTag.Counter(metrics.ScheduleActionSuccess.Name()).Record(1)

		results = append(results, startWorkflowResult{
			result:         result,
			nonOverlapping: start == action.NonOverlappingStart,
		})
	}

	// Terminate or cancel if required (terminate overrides cancel if both are present)
	if action.NeedTerminate {
		for _, ex := range s.Args.Info.RunningWorkflows {
			e.terminateWorkflow(s, ex)
		}
	} else if action.NeedCancel {
		for _, ex := range s.Args.Info.RunningWorkflows {
			e.cancelWorkflow(s, ex)
		}
	}

	err = env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		s.Args.State.BufferedStarts = action.NewBuffer
		s.Args.Info.OverlapSkipped += action.OverlapSkipped
		for _, r := range results {
			e.recordAction(s, r.result, r.nonOverlapping)
		}
		return nil
	})
	if err != nil {
		return
	}
	return
}

func (e taskExecutor) bufferWorkflowStart(s *Scheduler, nominalTime, actualTime time.Time, overlapPolicy enumspb.ScheduleOverlapPolicy, manual bool) {
	e.options.Logger.Debug("bufferWorkflowStart", tag.NewTimeTag("start-time", nominalTime), tag.NewTimeTag("actual-start-time", actualTime),
		tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))
	tweakables := e.config.Tweakables()
	if tweakables.MaxBufferSize > 0 && len(s.Args.State.BufferedStarts) >= tweakables.MaxBufferSize {
		e.options.Logger.Warn("Buffer too large", tag.NewTimeTag("start-time", nominalTime), tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))
		e.options.MetricsHandler.Counter(metrics.ScheduleBufferOverruns.Name()).Record(1)
		s.Args.Info.BufferDropped += 1
		return
	}
	s.Args.State.BufferedStarts = append(s.Args.State.BufferedStarts, &schedspb.BufferedStart{
		NominalTime:   timestamppb.New(nominalTime),
		ActualTime:    timestamppb.New(actualTime),
		OverlapPolicy: overlapPolicy,
		Manual:        manual,
	})
}

func (e taskExecutor) getNextTime(s *Scheduler, after time.Time) scheduler.GetNextTimeResult {
	return s.cspec.GetNextTime(s.jitterSeed(), after)
}

func (e taskExecutor) processTimeRange(
	s *Scheduler,
	start, end time.Time,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
	manual bool,
	limit *int,
) (time.Time, time.Time) {
	e.options.Logger.Debug("processTimeRange", tag.NewTimeTag("start", start), tag.NewTimeTag("end", end),
		tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))
	tweakables := e.config.Tweakables()

	if s.cspec == nil {
		return time.Time{}, end
	}
	catchupWindow := s.getCatchupWindow(&tweakables)

	lastAction := start
	var next scheduler.GetNextTimeResult
	for next = e.getNextTime(s, start); !(next.Next.IsZero() || next.Next.After(end)); next = e.getNextTime(s, next.Next) {
		if !manual && s.Args.Info.UpdateTime.AsTime().After(next.Next) {
			// We're reprocessing since the most recent event after an update. Discard actions before
			// the update time (which was just set to "now"). This doesn't have to be guarded with
			// hasMinVersion because this condition couldn't happen in previous versions.
			continue
		}
		if !manual && end.Sub(next.Next) > catchupWindow {
			e.options.Logger.Warn("Schedule missed catchup window", tag.NewTimeTag("now", end), tag.NewTimeTag("time", next.Next))
			e.options.MetricsHandler.Counter(metrics.ScheduleMissedCatchupWindow.Name()).Record(1)
			s.Args.Info.MissedCatchupWindow++
			continue
		}
		e.bufferWorkflowStart(s, next.Nominal, next.Next, overlapPolicy, manual)
		lastAction = next.Next

		if limit != nil {
			if (*limit)--; *limit <= 0 {
				break
			}
		}
	}
	return next.Next, lastAction
}

func (e taskExecutor) processBackfills(s *Scheduler) {
	tweakables := e.config.Tweakables()

	limit := tweakables.BackfillsPerIteration

	for len(s.Args.State.OngoingBackfills) > 0 &&
		limit > 0 &&
		// use only half the buffer for backfills
		len(s.Args.State.BufferedStarts) < tweakables.MaxBufferSize/2 {
		bfr := s.Args.State.OngoingBackfills[0]
		startTime := timestamp.TimeValue(bfr.GetStartTime())
		endTime := timestamp.TimeValue(bfr.GetEndTime())
		next, _ := e.processTimeRange(
			s,
			startTime,
			endTime,
			bfr.GetOverlapPolicy(),
			true,
			&limit,
		)
		if next.IsZero() || next.After(endTime) {
			// done with this one
			s.Args.State.OngoingBackfills = s.Args.State.OngoingBackfills[1:]
		} else {
			// adjust start time for next iteration
			bfr.StartTime = timestamppb.New(next)
		}
	}
}

func (e taskExecutor) recordAction(s *Scheduler, result *schedpb.ScheduleActionResult, nonOverlapping bool) {
	s.Args.Info.ActionCount++
	s.Args.Info.RecentActions = util.SliceTail(append(s.Args.Info.RecentActions, result), RecentActionCount)
	if nonOverlapping && result.StartWorkflowResult != nil {
		s.Args.Info.RunningWorkflows = append(s.Args.Info.RunningWorkflows, result.StartWorkflowResult)
	}
}

func (e taskExecutor) startWorkflow(
	s *Scheduler,
	start *schedspb.BufferedStart,
	newWorkflow *workflowpb.NewWorkflowExecutionInfo,
) (*schedpb.ScheduleActionResult, error) {
	nominalTimeSec := start.NominalTime.AsTime().UTC().Truncate(time.Second)
	workflowID := newWorkflow.WorkflowId

	// must match AppendedTimestampForValidation
	workflowID += "-" + nominalTimeSec.Format(time.RFC3339)

	lastCompletionResult, continuedFailure := s.Args.State.LastCompletionResult, s.Args.State.ContinuedFailure
	if start.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		// ALLOW_ALL runs don't participate in lastCompletionResult/continuedFailure at all
		lastCompletionResult = nil
		continuedFailure = nil
	}

	// TODO(Tianyu): Add callback here
	req := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:               workflowID,
		WorkflowType:             newWorkflow.WorkflowType,
		TaskQueue:                newWorkflow.TaskQueue,
		Input:                    newWorkflow.Input,
		WorkflowExecutionTimeout: newWorkflow.WorkflowExecutionTimeout,
		WorkflowRunTimeout:       newWorkflow.WorkflowRunTimeout,
		WorkflowTaskTimeout:      newWorkflow.WorkflowTaskTimeout,
		Identity:                 s.identity(),
		RequestId:                uuid.NewString(),
		WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		RetryPolicy:              newWorkflow.RetryPolicy,
		Memo:                     newWorkflow.Memo,
		// TODO(Tianyu): Search is not implemented for now
		SearchAttributes:     &commonpb.SearchAttributes{IndexedFields: nil},
		Header:               newWorkflow.Header,
		LastCompletionResult: lastCompletionResult,
		ContinuedFailure:     continuedFailure,
		Namespace:            s.Args.State.Namespace,
	}

	res, err := e.options.FrontendClient.StartWorkflowExecution(context.Background(), req)
	if err != nil {
		// TODO(Tianyu): original implementation translates this error which may not be relevant any more
		// TODO(Tianyu): On failure, need to figure out how HSM-based implementation should retry, as we can no longer rely on automatic activity retries.
		return nil, err
	}
	// this will not match the time in the workflow execution started event
	// exactly, but it's just informational so it's close enough.
	now := time.Now()

	if !start.Manual {
		// record metric only for _scheduled_ actions, not trigger/backfill, otherwise it's not meaningful
		desiredTime := cmp.Or(start.DesiredTime, start.ActualTime)
		e.options.MetricsHandler.Timer(metrics.ScheduleActionDelay.Name()).Record(now.Sub(desiredTime.AsTime()))
	}

	return &schedpb.ScheduleActionResult{
		ScheduleTime: start.ActualTime,
		ActualTime:   timestamppb.New(now),
		StartWorkflowResult: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      res.RunId,
		},
	}, nil
}

func (e taskExecutor) terminateWorkflow(s *Scheduler, ex *commonpb.WorkflowExecution) {
	// TODO: remove after https://github.com/temporalio/sdk-go/issues/1066
	// TODO(Tianyu): hardcoded wait time
	workflowCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rreq := &historyservice.TerminateWorkflowExecutionRequest{
		NamespaceId: s.Args.State.NamespaceId,
		TerminateRequest: &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: s.Args.State.Namespace,
			// only set WorkflowId so we cancel the latest, but restricted by FirstExecutionRunId
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: ex.WorkflowId},
			Reason:              "terminated by schedule overlap policy",
			Identity:            s.identity(),
			FirstExecutionRunId: ex.RunId,
		},
	}
	_, err := e.options.HistoryClient.TerminateWorkflowExecution(workflowCtx, rreq)

	// TODO(Tianyu): original implementation translates this error which may not be relevant any more
	// TODO(Tianyu): On failure, need to figure out how HSM-based implementation should retry, as we can no longer rely on automatic activity retries.
	if err != nil {
		e.options.Logger.Error("terminate workflow failed", tag.WorkflowID(ex.WorkflowId), tag.Error(err))
		e.options.MetricsHandler.Counter(metrics.ScheduleTerminateWorkflowErrors.Name()).Record(1)
	}
}

func (e taskExecutor) cancelWorkflow(s *Scheduler, ex *commonpb.WorkflowExecution) {
	// TODO: remove after https://github.com/temporalio/sdk-go/issues/1066
	// TODO(Tianyu): hardcoded wait time
	workflowCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rreq := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: s.Args.State.NamespaceId,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace: s.Args.State.Namespace,
			// only set WorkflowId so we cancel the latest, but restricted by FirstExecutionRunId
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: ex.WorkflowId},
			Identity:            s.identity(),
			RequestId:           uuid.NewString(),
			FirstExecutionRunId: ex.RunId,
			Reason:              "cancelled by schedule overlap policy",
		},
	}
	_, err := e.options.HistoryClient.RequestCancelWorkflowExecution(workflowCtx, rreq)
	// Note: cancellation has completed (or failed) here but the workflow might take time
	// to close since a cancel is only a request.
	// If this failed, that's okay, we'll try it again the next time we try to take an action.
	// TODO(Tianyu): original implementation translates this error which may not be relevant any more
	// TODO(Tianyu): On failure, need to figure out how HSM-based implementation should retry, as we can no longer rely on automatic activity retries.
	if err != nil {
		e.options.Logger.Error("cancelling workflow failed", tag.WorkflowID(ex.WorkflowId), tag.Error(err))
		e.options.MetricsHandler.Counter(metrics.ScheduleCancelWorkflowErrors.Name()).Record(1)
	}
}
