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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	// This is an example of a timestamp that's appended to the workflow
	// id, used for validation in the frontend.
	AppendedTimestampForValidation = "-2009-11-10T23:00:00Z"

	SignalNameUpdate  = "update"
	SignalNamePatch   = "patch"
	SignalNameRefresh = "refresh"

	QueryNameDescribe          = "describe"
	QueryNameListMatchingTimes = "listMatchingTimes"

	// The number of future action times to include in Describe.
	futureActionCount = 10
	// The number of recent actual action results to include in Describe.
	recentActionCount = 10

	// Maximum number of times to list per ListMatchingTimes query.
	maxListMatchingTimesCount = 1000

	// TODO: replace with event count or hint from server
	iterationsBeforeContinueAsNew = 500

	searchAttrStartTime    = "TemporalScheduledStartTime"
	searchAttrScheduleById = "TemporalScheduledById"
)

type (
	scheduler struct {
		schedspb.StartScheduleArgs

		ctx    workflow.Context
		a      *activities
		logger sdklog.Logger

		cspec *compiledSpec

		// Watchers for currently-running workflows. There are two types of watchers: refresh
		// and long-poll. Refresh are created when we want to start a new workflow but think we
		// might have one or more already running, or when we get a refresh signal because
		// someone is doing Describe. They do a short poll to check if a workflow is still
		// running. Long-poll are created when we have buffered workflows and need to know when
		// a running one is closed. There should be at most one watcher for a given workflow
		// id, of either type.
		watchers map[string]watcher
	}

	watcher struct {
		id       string
		future   workflow.Future
		longPoll bool
	}

	catchupWindowParams struct {
		Default, Min time.Duration
	}
)

var (
	InitialConflictToken = make([]byte, 8)

	defaultActivityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval: 1 * time.Second,
		MaximumInterval: 30 * time.Second,
	}

	defaultCatchupWindowParams = catchupWindowParams{
		Default: 60 * time.Second,
		Min:     10 * time.Second,
	}

	errUpdateConflict = errors.New("conflicting concurrent update")
)

func SchedulerWorkflow(ctx workflow.Context, args *schedspb.StartScheduleArgs) error {
	scheduler := &scheduler{
		StartScheduleArgs: *args,
		ctx:               ctx,
		a:                 nil,
		logger:            sdklog.With(workflow.GetLogger(ctx), "schedule-id", args.State.ScheduleId),
		watchers:          make(map[string]watcher),
	}
	return scheduler.run()
}

func (s *scheduler) run() error {
	s.logger.Info("Schedule starting", "schedule", s.Schedule)

	s.ensureFields()
	s.compileSpec()

	if err := workflow.SetQueryHandler(s.ctx, QueryNameDescribe, s.handleDescribeQuery); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(s.ctx, QueryNameListMatchingTimes, s.handleListMatchingTimesQuery); err != nil {
		return err
	}

	if s.State.LastProcessedTime == nil {
		s.logger.Debug("Initializing internal state")
		s.State.LastProcessedTime = timestamp.TimePtr(s.now())
		s.Info = &schedpb.ScheduleInfo{
			CreateTime: s.State.LastProcessedTime,
		}
		if len(s.State.ConflictToken) != 8 {
			s.State.ConflictToken = InitialConflictToken
		}
	}

	// A schedule may be created with an initial Patch, e.g. start one immediately. Handle that now.
	s.processPatch(s.InitialPatch)
	s.InitialPatch = nil

	for iters := iterationsBeforeContinueAsNew; iters >= 0; iters-- {
		t1 := timestamp.TimeValue(s.State.LastProcessedTime)
		t2 := s.now()
		if t2.Before(t1) {
			// Time went backwards. Currently this can only happen across a continue-as-new boundary.
			s.logger.Warn("Time went backwards", "from", t1, "to", t2)
			t2 = t1
		}
		nextSleep, hasNext := s.processTimeRange(
			t1, t2,
			// resolve this to the schedule's policy as late as possible
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
			false,
		)
		s.State.LastProcessedTime = timestamp.TimePtr(t2)
		for s.processBuffer() {
		}
		// sleep returns on any of:
		// 1. requested time elapsed
		// 2. we got a signal (update, request, refresh)
		// 3. a workflow that we were watching finished
		s.sleep(nextSleep, hasNext)
	}

	// Any watcher activities will get cancelled automatically if running.

	s.logger.Info("Schedule doing continue-as-new")
	return workflow.NewContinueAsNewError(s.ctx, WorkflowType, &s.StartScheduleArgs)
}

func (s *scheduler) ensureFields() {
	if s.Schedule == nil {
		s.Schedule = &schedpb.Schedule{}
	}
	if s.Schedule.Spec == nil {
		s.Schedule.Spec = &schedpb.ScheduleSpec{}
	}
	if s.Schedule.Action == nil {
		s.Schedule.Action = &schedpb.ScheduleAction{}
	}
	if s.Schedule.Policies == nil {
		s.Schedule.Policies = &schedpb.SchedulePolicies{}
	}
	if s.Schedule.State == nil {
		s.Schedule.State = &schedpb.ScheduleState{}
	}
	if s.Info == nil {
		s.Info = &schedpb.ScheduleInfo{}
	}
	if s.State == nil {
		s.State = &schedspb.InternalState{}
	}
}

func (s *scheduler) compileSpec() {
	cspec, err := newCompiledSpec(s.Schedule.Spec)
	if err != nil {
		s.logger.Error("Invalid schedule", "error", err)
		s.Info.InvalidScheduleError = err.Error()
		s.cspec = nil
	} else {
		s.Info.InvalidScheduleError = ""
		s.cspec = cspec
	}
}

func (s *scheduler) now() time.Time {
	// Notes:
	// 1. The time returned here is actually the timestamp of the WorkflowTaskStarted
	// event, which is generated in history, not any time on the worker itself.
	// 2. There will be some delay between when history stamps the time on the
	// WorkflowTaskStarted event and when this code runs, as the event+task goes through
	// matching and frontend. But it should be well under a second, which is our minimum
	// granularity anyway.
	// 3. It's actually the maximum of all of those events: the go sdk enforces that
	// workflow time is monotonic. So if the clock on a history node is wrong and workflow
	// time is ahead of real time for a while, and then goes back, scheduled jobs will run
	// ahead of time, and then nothing will happen until real time catches up to where it
	// was temporarily ahead. Currently the only way to "recover" from this situation is
	// to recreate the schedule/scheduler workflow.
	// 4. Actually there is one way time can appear to go backwards from the point of view
	// of this workflow: across a continue-as-new, since monotonicity isn't preserved
	// there (as far as I know). We'll treat that the same since we keep track of the last
	// processed schedule time.
	return workflow.Now(s.ctx)
}

func (s *scheduler) processPatch(patch *schedpb.SchedulePatch) {
	s.logger.Debug("processPatch", "patch", patch)

	if patch == nil {
		return
	}

	if trigger := patch.TriggerImmediately; trigger != nil {
		now := s.now()
		s.addStart(now, now, trigger.OverlapPolicy, true)
	}

	for _, bfr := range patch.BackfillRequest {
		s.processTimeRange(
			timestamp.TimeValue(bfr.GetStartTime()),
			timestamp.TimeValue(bfr.GetEndTime()),
			bfr.GetOverlapPolicy(),
			true,
		)
	}

	if patch.Pause != "" {
		s.Schedule.State.Paused = true
		s.Schedule.State.Notes = patch.Pause
		s.incSeqNo()
	}
	if patch.Unpause != "" {
		s.Schedule.State.Paused = false
		s.Schedule.State.Notes = patch.Unpause
		s.incSeqNo()
	}
}

func (s *scheduler) processTimeRange(
	t1, t2 time.Time,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
	manual bool,
) (nextSleep time.Duration, hasNext bool) {
	s.logger.Debug("processTimeRange", "t1", t1, "t2", t2, "overlapPolicy", overlapPolicy, "manual", manual)

	if s.cspec == nil {
		return 0, false
	}

	catchupWindow := s.getCatchupWindow()

	for {
		nominalTime, nextTime, hasNext := s.cspec.getNextTime(t1)
		t1 = nextTime
		if !hasNext {
			return 0, false
		} else if nextTime.After(t2) {
			return nextTime.Sub(t2), true
		}
		if !manual && t2.Sub(nextTime) > catchupWindow {
			s.logger.Warn("Schedule missed catchup window", "now", t2, "time", nextTime)
			s.Info.MissedCatchupWindow++
			continue
		}
		// Peek at paused/remaining actions state and don't even bother adding
		// to buffer if we're not going to take an action now.
		if !s.canTakeScheduledAction(manual, false) {
			continue
		}
		s.addStart(nominalTime, nextTime, overlapPolicy, manual)
	}
	return 0, false
}

func (s *scheduler) canTakeScheduledAction(manual, decrement bool) bool {
	// If manual (trigger immediately or backfill), always allow
	if manual {
		return true
	}
	// If paused, don't do anything
	if s.Schedule.State.Paused {
		return false
	}
	// If unlimited actions, allow
	if !s.Schedule.State.LimitedActions {
		return true
	}
	// Otherwise check and decrement limit
	if s.Schedule.State.RemainingActions > 0 {
		if decrement {
			s.Schedule.State.RemainingActions--
			s.incSeqNo()
		}
		return true
	}
	// No actions left
	return false
}

func (s *scheduler) sleep(nextSleep time.Duration, hasNext bool) {
	sel := workflow.NewSelector(s.ctx)

	upCh := workflow.GetSignalChannel(s.ctx, SignalNameUpdate)
	sel.AddReceive(upCh, s.handleUpdateSignal)

	reqCh := workflow.GetSignalChannel(s.ctx, SignalNamePatch)
	sel.AddReceive(reqCh, s.handlePatchSignal)

	refreshCh := workflow.GetSignalChannel(s.ctx, SignalNameRefresh)
	sel.AddReceive(refreshCh, s.handleRefreshSignal)

	if hasNext {
		tmr := workflow.NewTimer(s.ctx, nextSleep)
		sel.AddFuture(tmr, func(_ workflow.Future) {})
	}

	// need to sort watchers for determinism
	for _, watcher := range s.sortedWatchers() {
		sel.AddFuture(watcher.future, func(f workflow.Future) { s.wfWatcherReturned(watcher.id, f) })
	}

	s.logger.Debug("sleeping", "hasNext", hasNext, "watchers", len(s.watchers))
	sel.Select(s.ctx)
	for sel.HasPending() {
		sel.Select(s.ctx)
	}
}

func (s *scheduler) wfWatcherReturned(id string, f workflow.Future) {
	delete(s.watchers, id)

	var res schedspb.WatchWorkflowResponse
	err := f.Get(s.ctx, &res)
	if err != nil {
		s.logger.Error("error from workflow watcher future", "error", err)
		return
	}

	if res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		// this should only happen for a refresh, not a long-poll
		s.logger.Debug("watcher returned for running workflow")
		return
	}

	// now we know it's not running, remove from running workflow list
	match := func(ex *commonpb.WorkflowExecution) bool { return ex.WorkflowId == id }
	if idx := slices.IndexFunc(s.Info.RunningWorkflows, match); idx >= 0 {
		s.Info.RunningWorkflows = slices.Delete(s.Info.RunningWorkflows, idx, idx+1)
	} else {
		s.logger.Error("closed workflow not found in running list", "workflow", id)
	}

	// handle pause-on-failure
	failedStatus := res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED || res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
	pauseOnFailure := s.Schedule.Policies.PauseOnFailure && failedStatus && !s.Schedule.State.Paused
	if pauseOnFailure {
		s.Schedule.State.Paused = true
		if res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED {
			s.Schedule.State.Notes = fmt.Sprintf("paused due to workflow failure: %s: %s", id, res.GetFailure().GetMessage())
			s.logger.Info("paused due to workflow failure", "workflow", id, "message", res.GetFailure().GetMessage())
		} else if res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT {
			s.Schedule.State.Notes = fmt.Sprintf("paused due to workflow timeout: %s", id)
			s.logger.Info("paused due to workflow timeout", "workflow", id)
		}
		s.incSeqNo()
	}

	// handle last completion/failure
	if res.GetResult() != nil {
		s.State.LastCompletionResult = res.GetResult()
		s.State.ContinuedFailure = nil
	} else if res.GetFailure() != nil {
		// leave LastCompletionResult from previous run
		s.State.ContinuedFailure = res.GetFailure()
	}

	s.logger.Info("started workflow finished", "workflow", id, "status", res.Status, "pause-after-failure", pauseOnFailure)
}

func (s *scheduler) handleUpdateSignal(ch workflow.ReceiveChannel, _ bool) {
	var req schedspb.FullUpdateRequest
	ch.Receive(s.ctx, &req)
	if err := s.checkConflict(req.ConflictToken); err != nil {
		s.logger.Warn("Update conflicted with concurrent change")
		return
	}

	s.logger.Info("Schedule update", "new-schedule", req.Schedule.String())

	s.Schedule.Spec = req.Schedule.Spec
	s.Schedule.Action = req.Schedule.Action
	s.Schedule.Policies = req.Schedule.Policies
	s.Schedule.State = req.Schedule.State
	// don't touch Info

	s.ensureFields()
	s.compileSpec()

	s.Info.UpdateTime = timestamp.TimePtr(s.now())
	s.incSeqNo()
}

func (s *scheduler) handlePatchSignal(ch workflow.ReceiveChannel, _ bool) {
	var patch schedpb.SchedulePatch
	ch.Receive(s.ctx, &patch)
	s.logger.Info("Schedule patch", "patch", patch.String())
	s.processPatch(&patch)
}

func (s *scheduler) handleRefreshSignal(ch workflow.ReceiveChannel, _ bool) {
	var refresh schedspb.RefreshRequest
	ch.Receive(s.ctx, &refresh)
	s.logger.Debug("Got refresh signal", "refresh", refresh.String())
	for _, ex := range refresh.Workflows {
		s.startWatcher(ex, false)
	}
}

func (s *scheduler) handleDescribeQuery() (*schedspb.DescribeResponse, error) {
	// update future actions
	if s.cspec != nil {
		s.Info.FutureActionTimes = make([]*time.Time, 0, futureActionCount)
		t1 := timestamp.TimeValue(s.State.LastProcessedTime)
		for len(s.Info.FutureActionTimes) < futureActionCount {
			var has bool
			_, t1, has = s.cspec.getNextTime(t1)
			if !has {
				break
			}
			s.Info.FutureActionTimes = append(s.Info.FutureActionTimes, timestamp.TimePtr(t1))
		}
	} else {
		s.Info.FutureActionTimes = nil
	}

	return &schedspb.DescribeResponse{
		Schedule:      s.Schedule,
		Info:          s.Info,
		ConflictToken: s.State.ConflictToken,
	}, nil
}

func (s *scheduler) handleListMatchingTimesQuery(req *workflowservice.ListScheduleMatchingTimesRequest) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
	if req == nil || req.StartTime == nil || req.EndTime == nil {
		return nil, errors.New("missing or invalid query")
	}
	if s.cspec == nil {
		return nil, errors.New("invalid schedule: " + s.Info.InvalidScheduleError)
	}

	var out []*time.Time
	t1 := timestamp.TimeValue(req.StartTime)
	for i := 0; i < maxListMatchingTimesCount; i++ {
		_, t1, has := s.cspec.getNextTime(t1)
		if !has || t1.After(timestamp.TimeValue(req.EndTime)) {
			break
		}
		out = append(out, timestamp.TimePtr(t1))
	}
	return &workflowservice.ListScheduleMatchingTimesResponse{StartTime: out}, nil
}

func (s *scheduler) incSeqNo() {
	if len(s.State.ConflictToken) != 8 {
		s.State.ConflictToken = InitialConflictToken
	}
	v := binary.BigEndian.Uint64(s.State.ConflictToken)
	binary.BigEndian.PutUint64(s.State.ConflictToken, v+1)
}

func (s *scheduler) checkConflict(token []byte) error {
	if token == nil || bytes.Equal(token, s.State.ConflictToken) {
		return nil
	}
	return errUpdateConflict
}

func (s *scheduler) getCatchupWindow() time.Duration {
	// TODO: re-enable this after it works?
	// // Use MutableSideEffect so that we can change the defaults without breaking determinism.
	// get := func(ctx workflow.Context) interface{} { return defaultCatchupWindowParams }
	// eq := func(a, b interface{}) bool { return a.(catchupWindowParams) == b.(catchupWindowParams) }
	// var params catchupWindowParams
	// if err := workflow.MutableSideEffect(s.ctx, "defaultCatchupWindowParams", get, eq).Get(&params); err != nil {
	// 	panic("can't decode catchupWindowParams:" + err.Error())
	// }
	params := defaultCatchupWindowParams
	cw := s.Schedule.Policies.CatchupWindow
	if cw == nil {
		return params.Default
	} else if *cw < params.Min {
		return params.Min
	} else {
		return *cw
	}
}

func (s *scheduler) resolveOverlapPolicy(overlapPolicy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		overlapPolicy = s.Schedule.Policies.OverlapPolicy
	}
	if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		overlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return overlapPolicy
}

func (s *scheduler) addStart(nominalTime, actualTime time.Time, overlapPolicy enumspb.ScheduleOverlapPolicy, manual bool) {
	s.logger.Debug("addStart", "nominal", nominalTime, "actual", actualTime, "overlapPolicy", overlapPolicy, "manual", manual)
	s.State.BufferedStarts = append(s.State.BufferedStarts, &schedspb.BufferedStart{
		NominalTime:   timestamp.TimePtr(nominalTime),
		ActualTime:    timestamp.TimePtr(actualTime),
		OverlapPolicy: overlapPolicy,
		Manual:        manual,
	})
	// we have a new start to process, so we need to make sure that we have up-to-date status
	// on any workflows that we started.
	s.refreshAll()
}

func (s *scheduler) refreshAll() {
	for _, ex := range s.Info.RunningWorkflows {
		s.startWatcher(ex, false)
	}
}

func (s *scheduler) anyRefreshing() bool {
	for _, watcher := range s.watchers {
		if !watcher.longPoll {
			return true
		}
	}
	return false
}

// processBuffer should return true if there might be more work to do right now.
func (s *scheduler) processBuffer() bool {
	s.logger.Debug("processBuffer")

	// Make sure we have something to start. If not, we can clear the buffer.
	req := s.Schedule.Action.GetStartWorkflow()
	if req == nil || len(s.State.BufferedStarts) == 0 {
		s.State.BufferedStarts = nil
		s.logger.Debug("processBuffer: no action")
		return false
	}

	if s.anyRefreshing() {
		// blocked until all refreshes complete
		s.logger.Debug("processBuffer: waiting for refresh")
		return false
	}

	isRunning := len(s.Info.RunningWorkflows) > 0
	tryAgain := false

	action := processBuffer(s.State.BufferedStarts, isRunning, s.resolveOverlapPolicy)

	s.State.BufferedStarts = action.newBuffer
	s.Info.OverlapSkipped += action.overlapSkipped

	// Try starting whatever we're supposed to start now
	allStarts := action.overlappingStarts
	if action.nonOverlappingStart != nil {
		allStarts = append(allStarts, action.nonOverlappingStart)
	}
	for _, start := range allStarts {
		if !s.canTakeScheduledAction(start.Manual, true) {
			// try again to drain the buffer if paused or out of actions
			tryAgain = true
			continue
		}
		result, err := s.startWorkflow(start, req)
		if err != nil {
			s.logger.Error("Failed to start workflow", "error", err)
			// The "start workflow" activity has an unlimited retry policy, so if we get an
			// error here, it must be an unretryable one. Drop this from the buffer.
			tryAgain = true
			continue
		}
		s.recordAction(result)
	}

	// Terminate or cancel if required (terminate overrides cancel if both are present)
	if action.needTerminate {
		for _, ex := range s.Info.RunningWorkflows {
			s.terminateWorkflow(ex)
		}
	} else if action.needCancel {
		for _, ex := range s.Info.RunningWorkflows {
			s.cancelWorkflow(ex)
		}
	}

	// If we still have a buffer here, then we're waiting for started workflow(s) to be closed
	// (maybe one we just started). In order to get woken up, we need to be watching at least
	// one of them with an activity. We only need one watcher at a time, though: after that one
	// returns, we'll end up back here and start the next one.
	if len(s.State.BufferedStarts) > 0 && len(s.watchers) == 0 {
		if len(s.Info.RunningWorkflows) > 0 {
			s.startWatcher(s.Info.RunningWorkflows[0], true)
		} else {
			s.logger.Error("have buffered workflows but none running")
		}
	}

	return tryAgain
}

func (s *scheduler) recordAction(result *schedpb.ScheduleActionResult) {
	s.Info.ActionCount++
	s.Info.RecentActions = append(s.Info.RecentActions, result)
	extra := len(s.Info.RecentActions) - 10
	if extra > 0 {
		s.Info.RecentActions = s.Info.RecentActions[extra:]
	}
	if result.StartWorkflowResult != nil {
		s.Info.RunningWorkflows = append(s.Info.RunningWorkflows, result.StartWorkflowResult)
	}
}

func (s *scheduler) startWorkflow(
	start *schedspb.BufferedStart,
	newWorkflow *workflowpb.NewWorkflowExecutionInfo,
) (*schedpb.ScheduleActionResult, error) {
	// must match AppendedTimestampForValidation
	nominalTimeSec := start.NominalTime.UTC().Truncate(time.Second)
	workflowID := newWorkflow.WorkflowId + "-" + nominalTimeSec.Format(time.RFC3339)

	scheduleToCloseTimeout := 60 * time.Second
	if !start.Manual {
		// use catchup window to set timeout
		deadline := start.ActualTime.Add(s.getCatchupWindow())
		scheduleToCloseTimeout = deadline.Sub(s.now())
	}
	if scheduleToCloseTimeout < 5*time.Second {
		scheduleToCloseTimeout = 5 * time.Second
	}

	ctx := workflow.WithActivityOptions(s.ctx, workflow.ActivityOptions{
		ScheduleToCloseTimeout: scheduleToCloseTimeout,
		RetryPolicy:            defaultActivityRetryPolicy,
	})

	// this timestamp is the one used to set the deadline for workflow execution timeout
	startTime := timestamp.TimePtr(s.now())
	if !start.Manual {
		// use the scheduled time for workflow execution timeout even if we started late
		// TODO: is this being too smart? should we just leave it as the current time?
		startTime = start.ActualTime
	}

	req := &schedspb.StartWorkflowRequest{
		NamespaceId: s.State.NamespaceId,
		Request: &workflowservice.StartWorkflowExecutionRequest{
			Namespace:                s.State.Namespace,
			WorkflowId:               workflowID,
			WorkflowType:             newWorkflow.WorkflowType,
			TaskQueue:                newWorkflow.TaskQueue,
			Input:                    newWorkflow.Input,
			WorkflowExecutionTimeout: newWorkflow.WorkflowExecutionTimeout,
			WorkflowRunTimeout:       newWorkflow.WorkflowRunTimeout,
			WorkflowTaskTimeout:      newWorkflow.WorkflowTaskTimeout,
			Identity:                 s.identity(),
			RequestId:                s.newUUIDString(),
			WorkflowIdReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			RetryPolicy:              newWorkflow.RetryPolicy,
			Memo:                     newWorkflow.Memo,
			SearchAttributes:         s.addSearchAttrs(newWorkflow.SearchAttributes, nominalTimeSec),
			Header:                   newWorkflow.Header,
		},
		StartTime:            startTime,
		LastCompletionResult: s.State.LastCompletionResult,
		ContinuedFailure:     s.State.ContinuedFailure,
	}
	var res schedspb.StartWorkflowResponse
	// TODO: should this be a local activity instead? or should it be async?
	err := workflow.ExecuteActivity(ctx, s.a.StartWorkflow, req).Get(s.ctx, &res)
	if err != nil {
		return nil, err
	}

	return &schedpb.ScheduleActionResult{
		ScheduleTime: start.ActualTime,
		ActualTime:   res.RealStartTime,
		StartWorkflowResult: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      res.RunId,
		},
	}, nil
}

func (s *scheduler) identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.State.Namespace, s.State.ScheduleId)
}

func (s *scheduler) addSearchAttrs(
	attrs *commonpb.SearchAttributes,
	nominal time.Time,
) *commonpb.SearchAttributes {
	fields := maps.Clone(attrs.GetIndexedFields())
	if p, err := payload.Encode(nominal); err == nil {
		fields[searchAttrStartTime] = p
	}
	if p, err := payload.Encode(s.State.ScheduleId); err == nil {
		fields[searchAttrScheduleById] = p
	}
	return &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
}

func (s *scheduler) startWatcher(ex *commonpb.WorkflowExecution, longPoll bool) {
	if _, ok := s.watchers[ex.WorkflowId]; ok {
		// only one watcher per workflow id
		return
	}

	ctx := workflow.WithActivityOptions(s.ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 365 * 24 * time.Hour,
		RetryPolicy:         defaultActivityRetryPolicy,
		HeartbeatTimeout:    65 * time.Second,
	})
	req := &schedspb.WatchWorkflowRequest{
		Namespace:   s.State.Namespace,
		NamespaceId: s.State.NamespaceId,
		// Note: do not send runid here so that we always get the latest one
		Execution:           &commonpb.WorkflowExecution{WorkflowId: ex.WorkflowId},
		FirstExecutionRunId: ex.RunId,
		LongPoll:            longPoll,
	}
	future := workflow.ExecuteActivity(ctx, s.a.WatchWorkflow, req)
	s.watchers[ex.WorkflowId] = watcher{
		id:       ex.WorkflowId,
		future:   future,
		longPoll: longPoll,
	}
}

func (s *scheduler) cancelWorkflow(ex *commonpb.WorkflowExecution) {
	ctx := workflow.WithActivityOptions(s.ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
		RetryPolicy:         defaultActivityRetryPolicy,
	})
	areq := &schedspb.CancelWorkflowRequest{
		NamespaceId: s.State.NamespaceId,
		Namespace:   s.State.Namespace,
		RequestId:   s.newUUIDString(),
		Identity:    s.identity(),
		Execution:   ex,
		Reason:      "cancelled by schedule overlap policy",
	}
	// TODO: should this be a local activity instead?
	workflow.ExecuteActivity(ctx, s.a.CancelWorkflow, areq)
	// do not wait for cancel to complete
}

func (s *scheduler) terminateWorkflow(ex *commonpb.WorkflowExecution) {
	ctx := workflow.WithActivityOptions(s.ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Minute,
		RetryPolicy:         defaultActivityRetryPolicy,
	})
	areq := &schedspb.TerminateWorkflowRequest{
		NamespaceId: s.State.NamespaceId,
		Namespace:   s.State.Namespace,
		RequestId:   s.newUUIDString(),
		Identity:    s.identity(),
		Execution:   ex,
		Reason:      "terminated by schedule overlap policy",
	}
	// TODO: should this be a local activity instead?
	workflow.ExecuteActivity(ctx, s.a.TerminateWorkflow, areq)
	// do not wait for terminate to complete
}

func (s *scheduler) newUUIDString() string {
	var str string
	workflow.SideEffect(s.ctx, func(ctx workflow.Context) interface{} {
		return uuid.NewString()
	}).Get(&str)
	return str
}

func (s *scheduler) sortedWatchers() []watcher {
	watchers := maps.Values(s.watchers)
	slices.SortFunc(watchers, func(a, b watcher) bool { return a.id < b.id })
	return watchers
}
