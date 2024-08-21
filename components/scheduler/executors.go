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
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/failure/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencepb "go.temporal.io/server/api/persistence/v1"
	"go.uber.org/fx"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/api/historyservice/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"
)

var (
	errTryAgain             = errors.New("try again")
	errWrongChain           = errors.New("found running workflow with wrong FirstExecutionRunId")
	errNoEvents             = errors.New("GetEvents didn't return any events")
	errNoAttrs              = errors.New("last event did not have correct attrs")
	errBlocked              = errors.New("rate limiter doesn't allow any progress")
	errUnkownWorkflowStatus = errors.New("unknown workflow status")
)

type (
	TaskExecutorOptions struct {
		fx.In
		MetricsHandler    metrics.Handler
		Logger            log.Logger
		SpecBuilder       *scheduler.SpecBuilder
		FrontendClient    workflowservice.WorkflowServiceClient
		HistoryClient     resource.HistoryClient
		NamespaceRegistry namespace.Registry
		Config            *Config
	}

	taskExecutor struct {
		TaskExecutorOptions
	}

	errFollow string
)

func (e errFollow) Error() string { return string(e) }

var (
	errUpdateConflict = errors.New("conflicting concurrent update")
)

func RegisterExecutor(
	registry *hsm.Registry,
	executorOptions TaskExecutorOptions,
) error {
	exec := taskExecutor{executorOptions}
	if err := hsm.RegisterTimerExecutor(
		registry,
		exec.executeSchedulerWaitTask); err != nil {
		return err
	}
	if err := hsm.RegisterImmediateExecutor(registry, exec.executeSchedulerRunTask); err != nil {
		return err
	}
	if err := hsm.RegisterImmediateExecutor(registry, exec.executeSchedulerProcessBufferTask); err != nil {
		return err
	}
	if err := hsm.RegisterRemoteMethod(registry, ProcessWorkflowCompletionEvent{}, exec.processWorkflowCompletionEvent); err != nil {
		return err
	}
	if err := hsm.RegisterRemoteMethod(registry, Describe{}, exec.describe); err != nil {
		return err
	}
	if err := hsm.RegisterRemoteMethod(registry, ListMatchingTimes{}, exec.listMatchingTimes); err != nil {
		return err
	}
	if err := hsm.RegisterRemoteMethod(registry, PatchSchedule{}, exec.patchSchedule); err != nil {
		return err
	}
	return hsm.RegisterRemoteMethod(registry, UpdateSchedule{}, exec.updateSchedule)
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

func (e taskExecutor) tryCompileSpec(s *Scheduler) error {
	if s.cspec == nil {
		cspec, err := e.SpecBuilder.NewCompiledSpec(s.Args.Schedule.Spec)
		if err != nil {
			if e.Logger != nil {
				e.Logger.Error("Invalid schedule", tag.Error(err))
			}
			return err
		}
		s.cspec = cspec
	}
	return nil
}

func (e taskExecutor) executeSchedulerRunTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task SchedulerActivateTask,
) error {
	ns, err := e.NamespaceRegistry.GetNamespaceByID(namespace.ID(ref.WorkflowKey.NamespaceID))
	if err != nil {
		return fmt.Errorf("failed to get namespace by ID: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, e.Config.ExecutionTimeout(ns.Name().String()))
	defer cancel()

	// This block computes, in a critical section, the actions that the scheduler should take. It then proceeds to log
	// the intents down and update scheduler state. We perform the intended actions outside the critical section to
	// avoid blocking
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
			return err
		}
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}

		// Update in case namespace was renamed since creation
		s.Args.State.Namespace = ns.Name().String()
		tweakables := e.Config.Tweakables(s.Args.State.Namespace)
		err = e.tryCompileSpec(s)
		if err != nil {
			return err
		}

		if s.Args.State.LastProcessedTime == nil {
			// log these as json since it's more readable than the Go representation
			specJson, _ := protojson.Marshal(s.Args.Schedule.Spec)
			policiesJson, _ := protojson.Marshal(s.Args.Schedule.Policies)
			e.Logger.Info("Starting schedule", tag.NewStringTag("spec", string(specJson)), tag.NewStringTag("policies", string(policiesJson)))
			s.Args.State.LastProcessedTime = timestamppb.Now()
			s.Args.State.ConflictToken = scheduler.InitialConflictToken
			s.Args.Info.CreateTime = s.Args.State.LastProcessedTime
		}

		t1 := timestamp.TimeValue(s.Args.State.LastProcessedTime)
		t2 := time.Now()
		if t2.Before(t1) {
			e.Logger.Warn("Time went backwards", tag.NewStringTag("time", t1.String()), tag.NewStringTag("time", t2.String()))
			t2 = t1
		}
		nextWakeup, lastAction := e.processTimeRange(
			s,
			&tweakables,
			t1, t2,
			// resolve this to the schedule's policy as late as possible
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
			false,
			nil,
		)

		s.Args.State.LastProcessedTime = timestamppb.New(lastAction)
		s.NextInvocationTime = timestamppb.New(nextWakeup)

		// process backfills if we have any too
		e.processBackfills(s, &tweakables)
		return hsm.MachineTransition(node, func(scheduler *Scheduler) (hsm.TransitionOutput, error) {
			return TransitionSchedulerWait.Apply(scheduler, EventSchedulerWait{})
		})
	})
}

func (e taskExecutor) executeSchedulerProcessBufferTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task SchedulerProcessBufferTask,
) error {
	var sCopy Scheduler
	err := env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		if err := node.CheckRunning(); err != nil {
			return err
		}
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}

		// Copy the scheduler state for use outside of the critical section
		// TODO(Tianyu): This copy is mostly here for convenience, as the existing scheduler code is written assuming
		// access to the scheduler data structure. Probably the better HSM way is to copy out necessary information, but
		// that is left for future refactor.
		sCopy = Scheduler{
			HsmSchedulerState: common.CloneProto(s.HsmSchedulerState),
			cspec:             s.cspec,
		}
		return nil
	})

	if err != nil {
		return err
	}
	// try starting workflows in the buffer
	// nolint:revive
	for {
		hasNext, err := e.processBuffer(ctx, env, ref, &sCopy)
		if err != nil {
			return err
		}
		if !hasNext {
			break
		}
	}
	return nil
}

func (e taskExecutor) canTakeScheduledAction(s *Scheduler, manual, decrement bool) bool {
	// If manual (trigger immediately or backfill), always allow
	if manual {
		return true
	}
	// If paused, don't do anything
	if s.Args.Schedule.State.Paused {
		return false
	}
	// If unlimited actions, allow
	if !s.Args.Schedule.State.LimitedActions {
		return true
	}
	// Otherwise check and decrement limit
	if s.Args.Schedule.State.RemainingActions > 0 {
		if decrement {
			s.Args.Schedule.State.RemainingActions--
			s.Args.State.ConflictToken++
		}
		return true
	}
	// No actions left
	return false
}

type startWorkflowResult struct {
	result         *schedpb.ScheduleActionResult
	nonOverlapping bool
}

func (e taskExecutor) processBuffer(ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	sCopy *Scheduler) (tryAgain bool, err error) {
	e.Logger.Debug("processBuffer", tag.NewInt("buffer", len(sCopy.Args.State.BufferedStarts)), tag.NewInt("running", len(sCopy.Args.Info.RunningWorkflows)))
	tryAgain = false

	// Make sure we have something to start. If not, we can exit.
	req := sCopy.Args.Schedule.Action.GetStartWorkflow()
	if req == nil || len(sCopy.Args.State.BufferedStarts) == 0 {
		return
	}

	isRunning := len(sCopy.Args.Info.RunningWorkflows) > 0
	action := scheduler.ProcessBuffer(sCopy.Args.State.BufferedStarts, isRunning, sCopy.resolveOverlapPolicy)

	// Try starting whatever we're supposed to start now
	allStarts := action.OverlappingStarts
	if action.NonOverlappingStart != nil {
		allStarts = append(allStarts, action.NonOverlappingStart)
	}

	// For checking how many actions were taken
	remainingActions := sCopy.Args.Schedule.State.RemainingActions
	results := make([]startWorkflowResult, 0)

	for _, start := range allStarts {
		// TODO(Tianyu): This is likely not correct -- by mutating actions remaining here as part of canTakeScheduledAction
		// it assumes no concurrent tasks running to start workflows.  Even though this is a reasonable assumption, it probably
		//isn't enforced by the HSM framework atm.
		if !e.canTakeScheduledAction(sCopy, start.Manual, true) {
			// try again to drain the buffer if paused or out of actions
			tryAgain = true
			continue
		}
		result, err := e.startWorkflow(ctx, sCopy, ref, start, req)
		metricsWithTag := e.MetricsHandler.WithTags(metrics.StringTag(metrics.ScheduleActionTypeTag, metrics.ScheduleActionStartWorkflow))
		if err != nil {
			e.Logger.Error("Failed to start workflow", tag.Error(err))
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
	actionsDecremented := remainingActions - sCopy.Args.Schedule.State.RemainingActions

	// Terminate or cancel if required (terminate overrides cancel if both are present)
	if action.NeedTerminate {
		for _, ex := range sCopy.Args.Info.RunningWorkflows {
			e.terminateWorkflow(ctx, sCopy, ex)
		}
	} else if action.NeedCancel {
		for _, ex := range sCopy.Args.Info.RunningWorkflows {
			e.cancelWorkflow(ctx, sCopy, ex)
		}
	}

	// TODO(Tianyu): Check for retention period and complete schedule after retention period has passed
	err = env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}

		s.Args.State.BufferedStarts = action.NewBuffer
		s.Args.Info.OverlapSkipped += action.OverlapSkipped
		s.Args.Schedule.State.RemainingActions -= actionsDecremented
		for _, r := range results {
			s.recordAction(r.result, r.nonOverlapping)
		}
		return nil
	})
	return
}

func (e taskExecutor) bufferWorkflowStart(s *Scheduler, tweakables *Tweakables, nominalTime, actualTime time.Time, overlapPolicy enumspb.ScheduleOverlapPolicy, manual bool) {
	e.Logger.Debug("bufferWorkflowStart", tag.NewTimeTag("start-time", nominalTime), tag.NewTimeTag("actual-start-time", actualTime),
		tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))
	if tweakables.MaxBufferSize > 0 && len(s.Args.State.BufferedStarts) >= tweakables.MaxBufferSize {
		e.Logger.Warn("Buffer too large", tag.NewTimeTag("start-time", nominalTime), tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))
		e.MetricsHandler.Counter(metrics.ScheduleBufferOverruns.Name()).Record(1)
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
	tweakables *Tweakables,
	start, end time.Time,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
	manual bool,
	limit *int,
) (time.Time, time.Time) {
	e.Logger.Debug("processTimeRange", tag.NewTimeTag("start", start), tag.NewTimeTag("end", end),
		tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))

	catchupWindow := s.catchupWindow(tweakables)

	// Peek at paused/remaining actions state and don't bother if we're not going to
	// take an action now. (Don't count as missed catchup window either.)
	// Skip over entire time range if paused or no actions can be taken
	if !e.canTakeScheduledAction(s, manual, false) {
		// use end as last action time so that we don't reprocess time spent paused
		return e.getNextTime(s, end).Next, end
	}

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
			e.Logger.Warn("Schedule missed catchup window", tag.NewTimeTag("now", end), tag.NewTimeTag("time", next.Next))
			e.MetricsHandler.Counter(metrics.ScheduleMissedCatchupWindow.Name()).Record(1)
			s.Args.Info.MissedCatchupWindow++
			continue
		}
		e.bufferWorkflowStart(s, tweakables, next.Nominal, next.Next, overlapPolicy, manual)
		lastAction = next.Next

		if limit != nil {
			if (*limit)--; *limit <= 0 {
				break
			}
		}
	}
	return next.Next, lastAction
}

func (e taskExecutor) processBackfills(s *Scheduler, tweakables *Tweakables) {

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
			tweakables,
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

func (e taskExecutor) startWorkflow(
	ctx context.Context,
	s *Scheduler,
	ref hsm.Ref,
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

	callbackInternal := &persistencepb.Callback{
		Variant: &persistencepb.Callback_Hsm{
			Hsm: &persistencepb.Callback_HSM{
				NamespaceId: ref.WorkflowKey.NamespaceID,
				WorkflowId:  ref.WorkflowKey.WorkflowID,
				RunId:       ref.WorkflowKey.RunID,
				Ref:         ref.StateMachineRef,
				Method:      ProcessWorkflowCompletionEvent{}.Name(),
			},
		},
	}
	serializedCallback, err := proto.Marshal(callbackInternal)
	if err != nil {
		// TODO(Tianyu): original implementation translates this error which may not be relevant any more
		// TODO(Tianyu): On failure, need to figure out how HSM-based implementation should retry, as we can no longer rely on automatic activity retries.
		return nil, err
	}

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
		CompletionCallbacks: []*commonpb.Callback{{
			Variant: &commonpb.Callback_Internal_{
				Internal: &commonpb.Callback_Internal{
					Data: serializedCallback,
				},
			},
		}},
	}

	res, err := e.FrontendClient.StartWorkflowExecution(ctx, req)
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
		e.MetricsHandler.Timer(metrics.ScheduleActionDelay.Name()).Record(now.Sub(desiredTime.AsTime()))
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

func (e taskExecutor) terminateWorkflow(
	ctx context.Context,
	s *Scheduler,
	ex *commonpb.WorkflowExecution) {
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
	_, err := e.HistoryClient.TerminateWorkflowExecution(ctx, rreq)

	// TODO(Tianyu): original implementation translates this error which may not be relevant any more
	// TODO(Tianyu): On failure, need to figure out how HSM-based implementation should retry, as we can no longer rely on automatic activity retries.
	if err != nil {
		e.Logger.Error("terminate workflow failed", tag.WorkflowID(ex.WorkflowId), tag.Error(err))
		e.MetricsHandler.Counter(metrics.ScheduleTerminateWorkflowErrors.Name()).Record(1)
	}
}

func (e taskExecutor) cancelWorkflow(
	ctx context.Context,
	s *Scheduler,
	ex *commonpb.WorkflowExecution) {
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
	_, err := e.HistoryClient.RequestCancelWorkflowExecution(ctx, rreq)
	// Note: cancellation has completed (or failed) here but the workflow might take time
	// to close since a cancel is only a request.
	// If this failed, that's okay, we'll try it again the next time we try to take an action.
	// TODO(Tianyu): original implementation translates this error which may not be relevant any more
	// TODO(Tianyu): On failure, need to figure out how HSM-based implementation should retry, as we can no longer rely on automatic activity retries.
	if err != nil {
		e.Logger.Error("cancelling workflow failed", tag.WorkflowID(ex.WorkflowId), tag.Error(err))
		e.MetricsHandler.Counter(metrics.ScheduleCancelWorkflowErrors.Name()).Record(1)
	}
}

func checkConflict(s *Scheduler, token int64) error {
	if token == 0 || token == s.Args.State.ConflictToken {
		return nil
	}
	return errUpdateConflict
}

func getWorkflowResult(input *persistencepb.HSMCompletionCallbackArg) (*commonpb.Payloads, *failure.Failure, error) {
	switch input.LastEvent.EventType { // nolint:exhaustive
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		if attrs := input.LastEvent.GetWorkflowExecutionCompletedEventAttributes(); attrs == nil {
			return nil, nil, errNoAttrs
		} else if len(attrs.NewExecutionRunId) > 0 {
			// this shouldn't happen because we don't allow old-cron workflows as scheduled, but follow it anyway
			return nil, nil, errFollow(attrs.NewExecutionRunId)
		} else {
			return attrs.Result, nil, nil
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		if attrs := input.LastEvent.GetWorkflowExecutionFailedEventAttributes(); attrs == nil {
			return nil, nil, errNoAttrs
		} else if len(attrs.NewExecutionRunId) > 0 {
			return nil, nil, errFollow(attrs.NewExecutionRunId)
		} else {
			return nil, attrs.Failure, nil
		}
	}
	return nil, nil, nil
}

func (e taskExecutor) logPauseOnError(s *Scheduler, lastEventType enumspb.EventType, id string, f *failure.Failure) {
	if lastEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED {
		s.Args.Schedule.State.Notes = fmt.Sprintf("paused due to workflow failure: %s: %s", id, f.Message)
		e.Logger.Debug("paused due to workflow failure", tag.WorkflowID(id), tag.NewStringTag("message", f.Message))
	} else if lastEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT {
		s.Args.Schedule.State.Notes = fmt.Sprintf("paused due to workflow timeout: %s", id)
		e.Logger.Debug("paused due to workflow timeout", tag.WorkflowID(id))
	}
}

func (e taskExecutor) prepareRefForRemoteMethod(ref hsm.Ref) hsm.Ref {
	// TODO(Tianyu): This is here temporarily to ensure that hsm framework does not reject this as stale.
	// This is a workaround and should be removed after relevant changes are made in the HSM framework
	smRef := common.CloneProto(ref.StateMachineRef)
	// Unset the machine last update versioned transition so it is ignored in the completion staleness check.
	// This is to account for either the operation transitioning to STARTED state after a successful call but also to
	// account for the task timing out before we get a successful result and a transition to BACKING_OFF.
	smRef.MachineLastUpdateVersionedTransition = nil
	smRef.MachineTransitionCount = 0
	newRef := hsm.Ref{
		WorkflowKey:     ref.WorkflowKey,
		StateMachineRef: smRef,
		TaskID:          0,
	}
	return newRef
}

func (e taskExecutor) processWorkflowCompletionEvent(ctx context.Context, env hsm.Environment, ref hsm.Ref, input any) (any, error) {
	// TODO(Tianyu): This is here temporarily to ensure that hsm framework does not reject this as stale.
	// This is a workaround and should be removed after relevant changes are made in the HSM framework
	smRef := common.CloneProto(ref.StateMachineRef)
	// Unset the machine last update versioned transition so it is ignored in the completion staleness check.
	// This is to account for either the operation transitioning to STARTED state after a successful call but also to
	// account for the task timing out before we get a successful result and a transition to BACKING_OFF.
	smRef.MachineLastUpdateVersionedTransition = nil
	smRef.MachineTransitionCount = 0
	newRef := hsm.Ref{
		WorkflowKey:     ref.WorkflowKey,
		StateMachineRef: smRef,
		TaskID:          0,
	}

	castInput := input.(*persistencepb.HSMCompletionCallbackArg) //nolint:revive
	return nil, env.Access(ctx, newRef, hsm.AccessWrite, func(node *hsm.Node) error {
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}

		tweakables := e.Config.Tweakables(s.Args.State.Namespace)
		match := func(ex *commonpb.WorkflowExecution) bool { return ex.WorkflowId == castInput.WorkflowId }

		if idx := slices.IndexFunc(s.Args.Info.RunningWorkflows, match); idx >= 0 {
			s.Args.Info.RunningWorkflows = slices.Delete(s.Args.Info.RunningWorkflows, idx, idx+1)
		} else {
			// This could happen if the callback is retried.
			e.Logger.Error("just-closed workflow not found in running list", tag.WorkflowID(castInput.WorkflowId))
		}

		lastEventType := castInput.LastEvent.GetEventType()
		r, f, err := getWorkflowResult(castInput)
		if err != nil {
			return err
		}

		// handle pause-on-failure
		failedStatus := lastEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED ||
			lastEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT ||
			(tweakables.CanceledTerminatedCountAsFailures &&
				lastEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED || lastEventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED)

		pauseOnFailure := s.Args.Schedule.Policies.PauseOnFailure && failedStatus && !s.Args.Schedule.State.Paused
		if pauseOnFailure {
			s.Args.Schedule.State.Paused = true
			e.logPauseOnError(s, lastEventType, castInput.WorkflowId, f)
			s.Args.State.ConflictToken++
		}

		if r != nil {
			// Even though workflows may complete out of order, it is still ok to overwrite with the result of a workflow
			// that started earlier but completed later -- this is because last result is mostly for debug and visibility
			// purposes. Note that this behavior is slightly different from the old code that polls in order and will
			// always show results in the order they start
			s.Args.State.LastCompletionResult = r
			s.Args.State.ContinuedFailure = nil
		} else if f != nil {
			// leave LastCompletionResult from previous run
			s.Args.State.ContinuedFailure = f
		}

		// Update desired time of next start if it's buffered. This is used for metrics only.
		if len(s.Args.State.BufferedStarts) > 0 {
			s.Args.State.BufferedStarts[0].DesiredTime = castInput.LastEvent.GetEventTime()
		}

		e.Logger.Debug("started workflow finished", tag.WorkflowID(castInput.WorkflowId), tag.NewStringTag("status", castInput.LastEvent.EventType.String()),
			tag.NewBoolTag("pause-after-failure", pauseOnFailure))
		return nil
	})
}

// Returns up to `n` future action times.
//
// After workflow version `AccurateFutureActionTimes`, No more than the
// schedule's `RemainingActions` will be returned. Future action times that
// precede the schedule's UpdateTime are not included.
func getFutureActionTimes(s *Scheduler, n int) []*timestamppb.Timestamp {
	// Note that `s` may be a `scheduler` created outside of a workflow context, used to
	// compute list info at creation time or in a query. In that case inWorkflowContext will
	// be false, and this function and anything it calls should not use s.ctx.

	base := timestamp.TimeValue(s.Args.State.LastProcessedTime)

	// Pure version not using workflow context
	next := func(t time.Time) time.Time {
		return s.cspec.GetNextTime(s.jitterSeed(), t).Next
	}

	if s.Args.Schedule.State.LimitedActions {
		n = min(int(s.Args.Schedule.State.RemainingActions), n)
	}

	if s.cspec == nil {
		return nil
	}
	out := make([]*timestamppb.Timestamp, 0, n)
	t1 := base
	for len(out) < n {
		t1 = next(t1)
		if t1.IsZero() {
			break
		}

		if s.Args.Info.UpdateTime.AsTime().After(t1) {
			// Skip action times whose nominal times are prior to the schedule's update time
			continue
		}

		out = append(out, timestamppb.New(t1))
	}
	return out
}

func (e taskExecutor) describe(ctx context.Context, env hsm.Environment, ref hsm.Ref, _ any) (any, error) {
	result := &schedspb.DescribeResponse{}
	err := env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}
		tweakables := e.Config.Tweakables(s.Args.State.Namespace)
		// this is a query handler, don't modify s.Info directly
		infoCopy := common.CloneProto(s.Args.Info)
		infoCopy.FutureActionTimes = getFutureActionTimes(s, tweakables.FutureActionCount)
		infoCopy.BufferSize = int64(len(s.Args.State.BufferedStarts))
		result.Schedule = s.Args.Schedule
		result.Info = infoCopy
		result.ConflictToken = s.Args.State.ConflictToken

		return nil
	})
	return result, err
}

func (e taskExecutor) listMatchingTimes(ctx context.Context, env hsm.Environment, ref hsm.Ref, req any) (any, error) {
	castReq := req.(*workflowservice.ListScheduleMatchingTimesRequest)
	if castReq == nil || castReq.StartTime == nil || castReq.EndTime == nil {
		return nil, errors.New("missing or invalid query")
	}
	result := &workflowservice.ListScheduleMatchingTimesResponse{}
	err := env.Access(ctx, ref, hsm.AccessRead, func(node *hsm.Node) error {
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}
		if s.cspec == nil {
			return fmt.Errorf("invalid schedule: %s", s.Args.Info.InvalidScheduleError)
		}

		var out []*timestamppb.Timestamp
		t1 := timestamp.TimeValue(castReq.StartTime)
		for i := 0; i < MaxListMatchingTimesCount; i++ {
			// don't need to call GetNextTime in SideEffect because this is just a query
			t1 = s.cspec.GetNextTime(s.jitterSeed(), t1).Next
			if t1.IsZero() || t1.After(timestamp.TimeValue(castReq.EndTime)) {
				break
			}
			out = append(out, timestamppb.New(t1))
		}
		result.StartTime = out
		return nil
	})

	return result, err
}

func (e taskExecutor) patchSchedule(ctx context.Context, env hsm.Environment, ref hsm.Ref, req any) (any, error) {
	castReq := req.(*schedpb.SchedulePatch)
	// TODO(Tianyu): This is here temporarily to ensure that hsm framework does not reject this as stale.
	// This is a workaround and should be removed after relevant changes are made in the HSM framework
	smRef := common.CloneProto(ref.StateMachineRef)
	// Unset the machine last update versioned transition so it is ignored in the completion staleness check.
	smRef.MachineLastUpdateVersionedTransition = nil
	smRef.MachineTransitionCount = 0
	newRef := hsm.Ref{
		WorkflowKey:     ref.WorkflowKey,
		StateMachineRef: smRef,
		TaskID:          0,
	}

	return nil, env.Access(ctx, newRef, hsm.AccessWrite, func(node *hsm.Node) error {
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}
		e.Logger.Debug("Schedule patch")
		tweakables := e.Config.Tweakables(s.Args.State.Namespace)

		if trigger := castReq.TriggerImmediately; trigger != nil {
			now := time.Now()
			e.bufferWorkflowStart(s, &tweakables, now, now, trigger.OverlapPolicy, true)
		}

		for _, bfr := range castReq.BackfillRequest {
			startTime := timestamp.TimeValue(bfr.GetStartTime()).Add(-1 * time.Millisecond)
			bfr.StartTime = timestamppb.New(startTime)

			// Add to ongoing backfills to process incrementally
			if len(s.Args.State.OngoingBackfills) >= tweakables.MaxBufferSize {
				e.Logger.Warn("Buffer overrun for backfill requests")
				e.MetricsHandler.Counter(metrics.ScheduleBufferOverruns.Name()).Record(1)
				s.Args.Info.BufferDropped += 1
				continue
			}
			s.Args.State.OngoingBackfills = append(s.Args.State.OngoingBackfills, common.CloneProto(bfr))
		}

		if castReq.Pause != "" {
			s.Args.Schedule.State.Paused = true
			s.Args.Schedule.State.Notes = castReq.Pause
			s.Args.State.ConflictToken++
		}
		if castReq.Unpause != "" {
			s.Args.Schedule.State.Paused = false
			s.Args.Schedule.State.Notes = castReq.Unpause
			s.Args.State.ConflictToken++
		}

		// Attempt to immediately trigger the scheduler
		return hsm.MachineTransition(node, func(scheduler *Scheduler) (hsm.TransitionOutput, error) {
			return TransitionSchedulerActivate.Apply(scheduler, EventSchedulerActivate{})
		})
	})
}

func (e taskExecutor) updateSchedule(ctx context.Context, env hsm.Environment, ref hsm.Ref, req any) (any, error) {
	castReq := req.(*schedspb.FullUpdateRequest)
	// TODO(Tianyu): This is here temporarily to ensure that hsm framework does not reject this as stale.
	// This is a workaround and should be removed after relevant changes are made in the HSM framework
	smRef := common.CloneProto(ref.StateMachineRef)
	// Unset the machine last update versioned transition so it is ignored in the completion staleness check.
	// This is to account for either the operation transitioning to STARTED state after a successful call but also to
	// account for the task timing out before we get a successful result and a transition to BACKING_OFF.
	smRef.MachineLastUpdateVersionedTransition = nil
	smRef.MachineTransitionCount = 0
	newRef := hsm.Ref{
		WorkflowKey:     ref.WorkflowKey,
		StateMachineRef: smRef,
		TaskID:          0,
	}

	return nil, env.Access(ctx, newRef, hsm.AccessWrite, func(node *hsm.Node) error {
		s, err := hsm.MachineData[*Scheduler](node)
		if err != nil {
			return err
		}
		tweakables := e.Config.Tweakables(s.Args.State.Namespace)

		if err := checkConflict(s, castReq.ConflictToken); err != nil {
			e.Logger.Warn("Update conflicted with concurrent change")
			return errUpdateConflict
		}

		e.Logger.Debug("Schedule update")

		s.Args.Schedule.Spec = castReq.Schedule.GetSpec()
		s.Args.Schedule.Action = castReq.Schedule.GetAction()
		s.Args.Schedule.Policies = castReq.Schedule.GetPolicies()
		s.Args.Schedule.State = castReq.Schedule.GetState()
		// don't touch Info

		s.ensureFields(&tweakables)
		// Original scheduler code does not propagate failure here. Instead, other parts of the code will generate and propagate error when they see no compiled spec.
		_ = e.tryCompileSpec(s)

		s.Args.Info.UpdateTime = timestamppb.Now()
		s.Args.State.ConflictToken++

		// Attempt to immediately trigger the scheduler.
		return hsm.MachineTransition(node, func(scheduler *Scheduler) (hsm.TransitionOutput, error) {
			return TransitionSchedulerActivate.Apply(scheduler, EventSchedulerActivate{})
		})
	})
}
