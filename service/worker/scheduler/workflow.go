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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"golang.org/x/exp/slices"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
)

type SchedulerWorkflowVersion int64

const (
	// Versions of workflow logic. When introducing a new version, consider generating a new
	// history for TestReplays using generate_history.sh.

	// represents the state before Version is introduced
	InitialVersion SchedulerWorkflowVersion = iota
	// skip over entire time range if paused and batch and cache getNextTime queries
	BatchAndCacheTimeQueries
)

const (
	// Schedules are implemented by a workflow whose ID is this string plus the schedule ID.
	WorkflowIDPrefix = "temporal-sys-scheduler:"

	// This is an example of a timestamp that's appended to the workflow
	// id, used for validation in the frontend.
	AppendedTimestampForValidation = "-2009-11-10T23:00:00Z"

	SignalNameUpdate  = "update"
	SignalNamePatch   = "patch"
	SignalNameRefresh = "refresh"

	QueryNameDescribe          = "describe"
	QueryNameListMatchingTimes = "listMatchingTimes"

	MemoFieldInfo = "ScheduleInfo"

	InitialConflictToken = 1

	// Maximum number of times to list per ListMatchingTimes query. (This is used only in a
	// query so it can be changed without breaking history.)
	maxListMatchingTimesCount = 1000

	rateLimitedErrorType = "RateLimited"

	maxNextTimeResultCacheSize = 10
)

type (
	scheduler struct {
		schedspb.StartScheduleArgs

		ctx     workflow.Context
		a       *activities
		logger  sdklog.Logger
		metrics sdkclient.MetricsHandler

		cspec *CompiledSpec

		tweakables tweakablePolicies

		currentTimer         workflow.Future
		currentTimerDeadline time.Time

		// We might have zero or one long-poll watcher activity running. If so, these are set:
		watchingWorkflowId string
		watchingFuture     workflow.Future

		// Signal requests
		pendingPatch  *schedpb.SchedulePatch
		pendingUpdate *schedspb.FullUpdateRequest

		uuidBatch []string

		// This cache is used to store time results after batching getNextTime queries
		// in a single SideEffect
		nextTimeResultCache map[time.Time]getNextTimeResult
	}

	tweakablePolicies struct {
		DefaultCatchupWindow              time.Duration // Default for catchup window
		MinCatchupWindow                  time.Duration // Minimum for catchup window
		RetentionTime                     time.Duration // How long to keep schedules after they're done
		CanceledTerminatedCountAsFailures bool          // Whether cancelled+terminated count for pause-on-failure
		AlwaysAppendTimestamp             bool          // Whether to append timestamp for non-overlapping workflows too
		FutureActionCount                 int           // The number of future action times to include in Describe.
		RecentActionCount                 int           // The number of recent actual action results to include in Describe.
		FutureActionCountForList          int           // The number of future action times to include in List (search attr).
		RecentActionCountForList          int           // The number of recent actual action results to include in List (search attr).
		IterationsBeforeContinueAsNew     int
		SleepWhilePaused                  bool // If true, don't set timers while paused/out of actions
		// MaxBufferSize limits the number of buffered starts. This also limits the number of
		// workflows that can be backfilled at once (since they all have to fit in the buffer).
		MaxBufferSize  int
		AllowZeroSleep bool                     // Whether to allow a zero-length timer. Used for workflow compatibility.
		ReuseTimer     bool                     // Whether to reuse timer. Used for workflow compatibility.
		Version        SchedulerWorkflowVersion // Used to keep track of schedules version to release new features and for backward compatibility
		// version 0 corresponds to the schedule version that comes before introducing the Version parameter

		// When introducing a new field with new workflow logic, consider generating a new
		// history for TestReplays using generate_history.sh.
	}
)

var (
	defaultLocalActivityOptions = workflow.LocalActivityOptions{
		// This applies to watch, cancel, and terminate. Start workflow overrides this.
		ScheduleToCloseTimeout: 1 * time.Hour,
		// Each local activity is one or a few local RPCs.
		// Currently this is applied manually, see https://github.com/temporalio/sdk-go/issues/1066
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
			MaximumInterval: 60 * time.Second,
		},
	}

	// We put a handful of options in a static value and use it as a MutableSideEffect within
	// the workflow so that we can change them without breaking existing executions or having
	// to use versioning.
	currentTweakablePolicies = tweakablePolicies{
		DefaultCatchupWindow:              365 * 24 * time.Hour,
		MinCatchupWindow:                  10 * time.Second,
		RetentionTime:                     7 * 24 * time.Hour,
		CanceledTerminatedCountAsFailures: false,
		AlwaysAppendTimestamp:             true,
		FutureActionCount:                 10,
		RecentActionCount:                 10,
		FutureActionCountForList:          5,
		RecentActionCountForList:          5,
		IterationsBeforeContinueAsNew:     500,
		SleepWhilePaused:                  true,
		MaxBufferSize:                     1000,
		AllowZeroSleep:                    true,
		ReuseTimer:                        true,
		Version:                           BatchAndCacheTimeQueries,
	}

	errUpdateConflict = errors.New("conflicting concurrent update")
)

func SchedulerWorkflow(ctx workflow.Context, args *schedspb.StartScheduleArgs) error {
	scheduler := &scheduler{
		StartScheduleArgs: *args,
		ctx:               ctx,
		a:                 nil,
		logger:            sdklog.With(workflow.GetLogger(ctx), "wf-namespace", args.State.Namespace, "schedule-id", args.State.ScheduleId),
		metrics:           workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": args.State.Namespace}),
	}
	return scheduler.run()
}

func (s *scheduler) run() error {
	s.updateTweakables()
	s.ensureFields()
	s.compileSpec()

	if err := workflow.SetQueryHandler(s.ctx, QueryNameDescribe, s.handleDescribeQuery); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(s.ctx, QueryNameListMatchingTimes, s.handleListMatchingTimesQuery); err != nil {
		return err
	}

	if s.State.LastProcessedTime == nil {
		// log these as json since it's more readable than the Go representation
		var m jsonpb.Marshaler
		var specJson, policiesJson strings.Builder
		_ = m.Marshal(&specJson, s.Schedule.Spec)
		_ = m.Marshal(&policiesJson, s.Schedule.Policies)
		s.logger.Info("Starting schedule", "spec", specJson.String(), "policies", policiesJson.String())

		s.State.LastProcessedTime = timestamp.TimePtr(s.now())
		s.State.ConflictToken = InitialConflictToken
		s.Info.CreateTime = s.State.LastProcessedTime
	}

	// A schedule may be created with an initial Patch, e.g. start one immediately. Put that in
	// the state so it takes effect below.
	s.pendingPatch = s.InitialPatch
	s.InitialPatch = nil

	for iters := s.tweakables.IterationsBeforeContinueAsNew; iters > 0 || s.pendingUpdate != nil || s.pendingPatch != nil; iters-- {

		t1 := timestamp.TimeValue(s.State.LastProcessedTime)
		t2 := s.now()
		if t2.Before(t1) {
			// Time went backwards. Currently this can only happen across a continue-as-new boundary.
			s.logger.Warn("Time went backwards", "from", t1, "to", t2)
			t2 = t1
		}
		nextWakeup := s.processTimeRange(
			t1, t2,
			// resolve this to the schedule's policy as late as possible
			enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
			false,
		)
		s.State.LastProcessedTime = timestamp.TimePtr(t2)
		// handle signals after processing time range that just elapsed
		scheduleChanged := s.processSignals()
		if scheduleChanged {
			// need to calculate sleep again
			nextWakeup = s.processTimeRange(t2, t2, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, false)
		}
		// try starting workflows in the buffer
		//nolint:revive
		for s.processBuffer() {
		}
		s.updateMemoAndSearchAttributes()

		// if schedule is not paused and out of actions or do not have anything scheduled, exit the schedule workflow after retention period has passed
		if exp := s.getRetentionExpiration(nextWakeup); !exp.IsZero() && !exp.After(s.now()) {
			return nil
		}

		// sleep returns on any of:
		// 1. requested time elapsed
		// 2. we got a signal (update, request, refresh)
		// 3. a workflow that we were watching finished
		s.sleep(nextWakeup)
		s.updateTweakables()

	}

	// Any watcher activities will get cancelled automatically if running.

	s.logger.Debug("Schedule doing continue-as-new")
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

	// set defaults eagerly so they show up in describe output
	s.Schedule.Policies.OverlapPolicy = s.resolveOverlapPolicy(s.Schedule.Policies.OverlapPolicy)
	s.Schedule.Policies.CatchupWindow = timestamp.DurationPtr(s.getCatchupWindow())

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
	// if spec changes invalidate current nextTimeResult cache
	s.nextTimeResultCache = nil

	cspec, err := NewCompiledSpec(s.Schedule.Spec)
	if err != nil {
		if s.logger != nil {
			s.logger.Error("Invalid schedule", "error", err)
		}
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
	s.logger.Debug("Schedule patch", "patch", patch.String())

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

func (s *scheduler) getNextTime(after time.Time) getNextTimeResult {

	// we populate the map sequentially, if after is not in the map, it means we either exhausted
	// all items, or we jumped through time (forward or backward), in either case, refresh the cache
	next, ok := s.nextTimeResultCache[after]
	if ok {
		return next
	}
	s.nextTimeResultCache = nil
	// Run this logic in a SideEffect so that we can fix bugs there without breaking
	// existing schedule workflows.
	panicIfErr(workflow.SideEffect(s.ctx, func(ctx workflow.Context) interface{} {
		results := make(map[time.Time]getNextTimeResult)
		for t := after; !t.IsZero() && len(results) < maxNextTimeResultCacheSize; {
			next := s.cspec.getNextTime(t)
			results[t] = next
			t = next.Next
		}
		return results
	}).Get(&s.nextTimeResultCache))
	return s.nextTimeResultCache[after]
}

func (s *scheduler) processTimeRange(
	t1, t2 time.Time,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
	manual bool,
) time.Time {
	s.logger.Debug("processTimeRange", "t1", t1, "t2", t2, "overlap-policy", overlapPolicy, "manual", manual)

	if s.cspec == nil {
		return time.Time{}
	}

	catchupWindow := s.getCatchupWindow()

	// A previous version would record a marker for each time which could make a workflow
	// fail. With the new version, the entire time range is skipped if the workflow is paused
	// or we are not going to take an action now
	if s.hasMinVersion(BatchAndCacheTimeQueries) {
		// Peek at paused/remaining actions state and don't bother if we're not going to
		// take an action now. (Don't count as missed catchup window either.)
		// Skip over entire time range if paused or no actions can be taken
		if !s.canTakeScheduledAction(manual, false) {
			return s.getNextTime(t2).Next
		}
	}

	for {
		var next getNextTimeResult
		if !s.hasMinVersion(BatchAndCacheTimeQueries) {
			// Run this logic in a SideEffect so that we can fix bugs there without breaking
			// existing schedule workflows.
			panicIfErr(workflow.SideEffect(s.ctx, func(ctx workflow.Context) interface{} {
				return s.cspec.getNextTime(t1)
			}).Get(&next))
		} else {
			next = s.getNextTime(t1)
		}
		t1 = next.Next
		if t1.IsZero() || t1.After(t2) {
			return t1
		}
		if !s.hasMinVersion(BatchAndCacheTimeQueries) && !s.canTakeScheduledAction(manual, false) {
			continue
		}
		if !manual && t2.Sub(t1) > catchupWindow {
			s.logger.Warn("Schedule missed catchup window", "now", t2, "time", t1)
			s.metrics.Counter(metrics.ScheduleMissedCatchupWindow.GetMetricName()).Inc(1)
			s.Info.MissedCatchupWindow++
			continue
		}
		s.addStart(next.Nominal, next.Next, overlapPolicy, manual)
	}
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

func (s *scheduler) sleep(nextWakeup time.Time) {
	sel := workflow.NewSelector(s.ctx)

	upCh := workflow.GetSignalChannel(s.ctx, SignalNameUpdate)
	sel.AddReceive(upCh, func(ch workflow.ReceiveChannel, _ bool) {
		ch.Receive(s.ctx, &s.pendingUpdate)
	})

	reqCh := workflow.GetSignalChannel(s.ctx, SignalNamePatch)
	sel.AddReceive(reqCh, func(ch workflow.ReceiveChannel, _ bool) {
		ch.Receive(s.ctx, &s.pendingPatch)
	})

	refreshCh := workflow.GetSignalChannel(s.ctx, SignalNameRefresh)
	sel.AddReceive(refreshCh, s.handleRefreshSignal)

	// if we're paused or out of actions, we don't need to wake up until we get an update
	if s.tweakables.SleepWhilePaused && !s.canTakeScheduledAction(false, false) {
		nextWakeup = time.Time{}
	}

	// if retention is not zero, it means there is no more job to schedule, so sleep for retention time and then exit if no signal is received
	if exp := s.getRetentionExpiration(nextWakeup); !exp.IsZero() {
		nextWakeup = exp
	}

	if !nextWakeup.IsZero() {
		sleepTime := nextWakeup.Sub(s.now())
		// A previous version of this workflow passed around sleep duration instead of wakeup time,
		// which means it always set a timer even in cases where sleepTime comes out negative. For
		// compatibility, we have to continue setting a positive timer in those cases. The value
		// doesn't have to match, though.
		if !s.tweakables.AllowZeroSleep && sleepTime <= 0 {
			sleepTime = time.Second
		}
		// A previous version of this workflow always created a new timer here, which is wasteful.
		// We can reuse a previous timer if we have the same deadline and it didn't fire yet.
		if s.tweakables.ReuseTimer {
			if s.currentTimer == nil || !s.currentTimerDeadline.Equal(nextWakeup) {
				s.currentTimer = workflow.NewTimer(s.ctx, sleepTime)
				s.currentTimerDeadline = nextWakeup
			}
			sel.AddFuture(s.currentTimer, func(_ workflow.Future) {
				s.currentTimer = nil
			})
		} else {
			tmr := workflow.NewTimer(s.ctx, sleepTime)
			sel.AddFuture(tmr, func(_ workflow.Future) {})
		}
	}

	if s.watchingFuture != nil {
		sel.AddFuture(s.watchingFuture, s.wfWatcherReturned)
	}

	s.logger.Debug("sleeping", "next-wakeup", nextWakeup, "watching", s.watchingFuture != nil)
	sel.Select(s.ctx)
	for sel.HasPending() {
		sel.Select(s.ctx)
	}
}

func (s *scheduler) wfWatcherReturned(f workflow.Future) {
	id := s.watchingWorkflowId
	s.watchingWorkflowId = ""
	s.watchingFuture = nil
	s.processWatcherResult(id, f)
}

func (s *scheduler) processWatcherResult(id string, f workflow.Future) {
	var res schedspb.WatchWorkflowResponse
	err := f.Get(s.ctx, &res)
	if err != nil {
		s.logger.Error("error from workflow watcher future", "workflow", id, "error", err)
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
	failedStatus := res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED ||
		res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT ||
		(s.tweakables.CanceledTerminatedCountAsFailures &&
			(res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED || res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED))
	pauseOnFailure := s.Schedule.Policies.PauseOnFailure && failedStatus && !s.Schedule.State.Paused
	if pauseOnFailure {
		s.Schedule.State.Paused = true
		if res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED {
			s.Schedule.State.Notes = fmt.Sprintf("paused due to workflow failure: %s: %s", id, res.GetFailure().GetMessage())
			s.logger.Debug("paused due to workflow failure", "workflow", id, "message", res.GetFailure().GetMessage())
		} else if res.Status == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT {
			s.Schedule.State.Notes = fmt.Sprintf("paused due to workflow timeout: %s", id)
			s.logger.Debug("paused due to workflow timeout", "workflow", id)
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

	s.logger.Debug("started workflow finished", "workflow", id, "status", res.Status, "pause-after-failure", pauseOnFailure)
}

func (s *scheduler) processUpdate(req *schedspb.FullUpdateRequest) {
	if err := s.checkConflict(req.ConflictToken); err != nil {
		s.logger.Warn("Update conflicted with concurrent change")
		return
	}

	s.logger.Debug("Schedule update", "new-schedule", req.Schedule.String())

	s.Schedule.Spec = req.Schedule.GetSpec()
	s.Schedule.Action = req.Schedule.GetAction()
	s.Schedule.Policies = req.Schedule.GetPolicies()
	s.Schedule.State = req.Schedule.GetState()
	// don't touch Info

	s.ensureFields()
	s.compileSpec()

	s.Info.UpdateTime = timestamp.TimePtr(s.now())
	s.incSeqNo()
}

func (s *scheduler) handleRefreshSignal(ch workflow.ReceiveChannel, _ bool) {
	ch.Receive(s.ctx, nil)
	s.logger.Debug("got refresh signal")
	// If we're woken up by any signal, we'll pass through processBuffer before sleeping again.
	// processBuffer will see this flag and refresh everything.
	s.State.NeedRefresh = true
}

func (s *scheduler) processSignals() bool {
	scheduleChanged := false
	if s.pendingPatch != nil {
		s.processPatch(s.pendingPatch)
		s.pendingPatch = nil
	}
	if s.pendingUpdate != nil {
		s.processUpdate(s.pendingUpdate)
		s.pendingUpdate = nil
		scheduleChanged = true
	}
	return scheduleChanged
}

func (s *scheduler) getFutureActionTimes(n int) []*time.Time {
	// Note that `s` may be a fake scheduler used to compute list info at creation time.

	if s.cspec == nil {
		return nil
	}
	out := make([]*time.Time, 0, n)
	t1 := timestamp.TimeValue(s.State.LastProcessedTime)
	for len(out) < n {
		// don't need to call getNextTime in SideEffect because this is only used in a query
		// handler and for the UpsertMemo value
		t1 = s.cspec.getNextTime(t1).Next
		if t1.IsZero() {
			break
		}
		out = append(out, timestamp.TimePtr(t1))
	}
	return out
}

func (s *scheduler) handleDescribeQuery() (*schedspb.DescribeResponse, error) {
	// this is a query handler, don't modify s.Info directly
	infoCopy := *s.Info
	infoCopy.FutureActionTimes = s.getFutureActionTimes(s.tweakables.FutureActionCount)

	return &schedspb.DescribeResponse{
		Schedule:      s.Schedule,
		Info:          &infoCopy,
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
		// don't need to call getNextTime in SideEffect because this is just a query
		t1 = s.cspec.getNextTime(t1).Next
		if t1.IsZero() || t1.After(timestamp.TimeValue(req.EndTime)) {
			break
		}
		out = append(out, timestamp.TimePtr(t1))
	}
	return &workflowservice.ListScheduleMatchingTimesResponse{StartTime: out}, nil
}

func (s *scheduler) incSeqNo() {
	s.State.ConflictToken++
}

func (s *scheduler) getListInfo() *schedpb.ScheduleListInfo {
	// Note that `s` may be a fake scheduler used to compute list info at creation time, before
	// the first workflow task. This function and anything it calls should not use s.ctx.

	// make shallow copy
	spec := *s.Schedule.Spec
	// clear fields that are too large/not useful for the list view
	spec.TimezoneData = nil

	return &schedpb.ScheduleListInfo{
		Spec:              &spec,
		WorkflowType:      s.Schedule.Action.GetStartWorkflow().GetWorkflowType(),
		Notes:             s.Schedule.State.Notes,
		Paused:            s.Schedule.State.Paused,
		RecentActions:     util.SliceTail(s.Info.RecentActions, s.tweakables.RecentActionCountForList),
		FutureActionTimes: s.getFutureActionTimes(s.tweakables.FutureActionCountForList),
	}
}

func (s *scheduler) updateMemoAndSearchAttributes() {
	newInfo := s.getListInfo()

	workflowInfo := workflow.GetInfo(s.ctx)
	currentInfoPayload := workflowInfo.Memo.GetFields()[MemoFieldInfo]

	var currentInfoBytes []byte
	var currentInfo schedpb.ScheduleListInfo

	if currentInfoPayload == nil ||
		payload.Decode(currentInfoPayload, &currentInfoBytes) != nil ||
		currentInfo.Unmarshal(currentInfoBytes) != nil ||
		!proto.Equal(&currentInfo, newInfo) {
		// marshal manually to get proto encoding (default dataconverter will use json)
		newInfoBytes, err := newInfo.Marshal()
		if err == nil {
			err = workflow.UpsertMemo(s.ctx, map[string]interface{}{
				MemoFieldInfo: newInfoBytes,
			})
		}
		if err != nil {
			s.logger.Error("error updating memo", "error", err)
		}
	}

	currentPausedPayload := workflowInfo.SearchAttributes.GetIndexedFields()[searchattribute.TemporalSchedulePaused]
	var currentPaused bool
	if currentPausedPayload == nil ||
		payload.Decode(currentPausedPayload, &currentPaused) != nil ||
		currentPaused != s.Schedule.State.Paused {
		err := workflow.UpsertSearchAttributes(s.ctx, map[string]interface{}{
			searchattribute.TemporalSchedulePaused: s.Schedule.State.Paused,
		})
		if err != nil {
			s.logger.Error("error updating search attributes", "error", err)
		}
	}
}

func (s *scheduler) checkConflict(token int64) error {
	if token == 0 || token == s.State.ConflictToken {
		return nil
	}
	return errUpdateConflict
}

func (s *scheduler) updateTweakables() {
	// Use MutableSideEffect so that we can change the defaults without breaking determinism.
	get := func(ctx workflow.Context) interface{} { return currentTweakablePolicies }
	eq := func(a, b interface{}) bool { return a.(tweakablePolicies) == b.(tweakablePolicies) }
	if err := workflow.MutableSideEffect(s.ctx, "tweakables", get, eq).Get(&s.tweakables); err != nil {
		panic("can't decode tweakablePolicies:" + err.Error())
	}
}

func (s *scheduler) getCatchupWindow() time.Duration {
	cw := s.Schedule.Policies.CatchupWindow
	if cw == nil {
		return s.tweakables.DefaultCatchupWindow
	} else if *cw < s.tweakables.MinCatchupWindow {
		return s.tweakables.MinCatchupWindow
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
	s.logger.Debug("addStart", "start-time", nominalTime, "actual-start-time", actualTime, "overlap-policy", overlapPolicy, "manual", manual)
	if s.tweakables.MaxBufferSize > 0 && len(s.State.BufferedStarts) >= s.tweakables.MaxBufferSize {
		s.logger.Warn("Buffer too large", "start-time", nominalTime, "overlap-policy", overlapPolicy, "manual", manual)
		s.metrics.Counter(metrics.ScheduleBufferOverruns.GetMetricName()).Inc(1)
		return
	}
	s.State.BufferedStarts = append(s.State.BufferedStarts, &schedspb.BufferedStart{
		NominalTime:   timestamp.TimePtr(nominalTime),
		ActualTime:    timestamp.TimePtr(actualTime),
		OverlapPolicy: overlapPolicy,
		Manual:        manual,
	})
	// we have a new start to process, so we need to make sure that we have up-to-date status
	// on any workflows that we started.
	s.State.NeedRefresh = true
}

// processBuffer should return true if there might be more work to do right now.
//
//nolint:revive
func (s *scheduler) processBuffer() bool {
	s.logger.Debug("processBuffer", "buffer", len(s.State.BufferedStarts), "running", len(s.Info.RunningWorkflows), "need-refresh", s.State.NeedRefresh)

	// TODO: consider doing this always and removing needRefresh? we only end up here without
	// needRefresh in the case of update, or patch without an immediate run, so it's not much
	// wasted work.
	// TODO: on the other hand, we don't have to refresh if we have one workflow running and a
	// long-poll watcher running, because we would have gotten woken up already.
	if s.State.NeedRefresh {
		s.refreshWorkflows(slices.Clone(s.Info.RunningWorkflows))
		s.State.NeedRefresh = false
	}

	// Make sure we have something to start. If not, we can clear the buffer.
	req := s.Schedule.Action.GetStartWorkflow()
	if req == nil || len(s.State.BufferedStarts) == 0 {
		s.State.BufferedStarts = nil
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
		metricsWithTag := s.metrics.WithTags(map[string]string{
			metrics.ScheduleActionTypeTag: metrics.ScheduleActionStartWorkflow})
		if err != nil {
			s.logger.Error("Failed to start workflow", "error", err)
			metricsWithTag.Counter(metrics.ScheduleActionErrors.GetMetricName()).Inc(1)
			// TODO: we could put this back in the buffer and retry (after a delay) up until
			// the catchup window. of course, it's unlikely that this workflow would be making
			// progress while we're unable to start a new one, so maybe it's not that valuable.
			tryAgain = true
			continue
		}
		metricsWithTag.Counter(metrics.ScheduleActionSuccess.GetMetricName()).Inc(1)
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
	if len(s.State.BufferedStarts) > 0 && s.watchingFuture == nil {
		if len(s.Info.RunningWorkflows) > 0 {
			s.startLongPollWatcher(s.Info.RunningWorkflows[0])
		} else {
			s.logger.Error("have buffered workflows but none running")
		}
	}

	return tryAgain
}

func (s *scheduler) recordAction(result *schedpb.ScheduleActionResult) {
	s.Info.ActionCount++
	s.Info.RecentActions = util.SliceTail(append(s.Info.RecentActions, result), s.tweakables.RecentActionCount)
	if result.StartWorkflowResult != nil {
		s.Info.RunningWorkflows = append(s.Info.RunningWorkflows, result.StartWorkflowResult)
	}
}

func (s *scheduler) startWorkflow(
	start *schedspb.BufferedStart,
	newWorkflow *workflowpb.NewWorkflowExecutionInfo,
) (*schedpb.ScheduleActionResult, error) {
	nominalTimeSec := start.NominalTime.UTC().Truncate(time.Second)
	workflowID := newWorkflow.WorkflowId
	if start.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || s.tweakables.AlwaysAppendTimestamp {
		// must match AppendedTimestampForValidation
		workflowID += "-" + nominalTimeSec.Format(time.RFC3339)
	}

	// Set scheduleToCloseTimeout based on catchup window, which is the latest time that it's
	// acceptable to start this workflow. For manual starts (trigger immediately or backfill),
	// catch up window doesn't apply, so just use 60s.
	options := defaultLocalActivityOptions
	if start.Manual {
		options.ScheduleToCloseTimeout = 60 * time.Second
	} else {
		deadline := start.ActualTime.Add(s.getCatchupWindow())
		options.ScheduleToCloseTimeout = deadline.Sub(s.now())
		if options.ScheduleToCloseTimeout < options.StartToCloseTimeout {
			options.ScheduleToCloseTimeout = options.StartToCloseTimeout
		} else if options.ScheduleToCloseTimeout > 1*time.Hour {
			options.ScheduleToCloseTimeout = 1 * time.Hour
		}
	}
	ctx := workflow.WithLocalActivityOptions(s.ctx, options)

	req := &schedspb.StartWorkflowRequest{
		Request: &workflowservice.StartWorkflowExecutionRequest{
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
			SearchAttributes:         s.addSearchAttributes(newWorkflow.SearchAttributes, nominalTimeSec),
			Header:                   newWorkflow.Header,
			LastCompletionResult:     s.State.LastCompletionResult,
			ContinuedFailure:         s.State.ContinuedFailure,
		},
	}
	for {
		var res schedspb.StartWorkflowResponse
		err := workflow.ExecuteLocalActivity(ctx, s.a.StartWorkflow, req).Get(s.ctx, &res)

		var appErr *temporal.ApplicationError
		var details rateLimitedDetails
		if errors.As(err, &appErr) && appErr.Type() == rateLimitedErrorType && appErr.Details(&details) == nil {
			s.metrics.Counter(metrics.ScheduleRateLimited.GetMetricName()).Inc(1)
			workflow.Sleep(s.ctx, details.Delay)
			req.CompletedRateLimitSleep = true // only use rate limiter once
			continue
		}
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
}

func (s *scheduler) identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.State.Namespace, s.State.ScheduleId)
}

func (s *scheduler) addSearchAttributes(
	attributes *commonpb.SearchAttributes,
	nominal time.Time,
) *commonpb.SearchAttributes {
	fields := util.CloneMapNonNil(attributes.GetIndexedFields())
	if p, err := payload.Encode(nominal); err == nil {
		fields[searchattribute.TemporalScheduledStartTime] = p
	}
	if p, err := payload.Encode(s.State.ScheduleId); err == nil {
		fields[searchattribute.TemporalScheduledById] = p
	}
	return &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
}

func (s *scheduler) refreshWorkflows(executions []*commonpb.WorkflowExecution) {
	ctx := workflow.WithLocalActivityOptions(s.ctx, defaultLocalActivityOptions)
	futures := make([]workflow.Future, len(executions))
	for i, ex := range executions {
		req := &schedspb.WatchWorkflowRequest{
			// Note: do not send runid here so that we always get the latest one
			Execution:           &commonpb.WorkflowExecution{WorkflowId: ex.WorkflowId},
			FirstExecutionRunId: ex.RunId,
			LongPoll:            false,
		}
		futures[i] = workflow.ExecuteLocalActivity(ctx, s.a.WatchWorkflow, req)
	}
	for i, ex := range executions {
		s.processWatcherResult(ex.WorkflowId, futures[i])
	}
}

func (s *scheduler) startLongPollWatcher(ex *commonpb.WorkflowExecution) {
	if s.watchingFuture != nil {
		s.logger.Error("startLongPollWatcher called with watcher already running")
		return
	}

	ctx := workflow.WithActivityOptions(s.ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 365 * 24 * time.Hour,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval: 1 * time.Second,
			MaximumInterval: 30 * time.Second,
		},
		HeartbeatTimeout: 65 * time.Second,
	})
	req := &schedspb.WatchWorkflowRequest{
		// Note: do not send runid here so that we always get the latest one
		Execution:           &commonpb.WorkflowExecution{WorkflowId: ex.WorkflowId},
		FirstExecutionRunId: ex.RunId,
		LongPoll:            true,
	}
	s.watchingFuture = workflow.ExecuteActivity(ctx, s.a.WatchWorkflow, req)
	s.watchingWorkflowId = ex.WorkflowId
}

func (s *scheduler) cancelWorkflow(ex *commonpb.WorkflowExecution) {
	ctx := workflow.WithLocalActivityOptions(s.ctx, defaultLocalActivityOptions)
	areq := &schedspb.CancelWorkflowRequest{
		RequestId: s.newUUIDString(),
		Identity:  s.identity(),
		Execution: ex,
		Reason:    "cancelled by schedule overlap policy",
	}
	err := workflow.ExecuteLocalActivity(ctx, s.a.CancelWorkflow, areq).Get(s.ctx, nil)
	if err != nil {
		s.logger.Error("cancel workflow failed", "workflow", ex.WorkflowId, "error", err)
		s.metrics.Counter(metrics.ScheduleCancelWorkflowErrors.GetMetricName()).Inc(1)
	}
	// Note: the local activity has completed (or failed) here but the workflow might take time
	// to close since a cancel is only a request.
	// If this failed, that's okay, we'll try it again the next time we try to take an action.
}

func (s *scheduler) terminateWorkflow(ex *commonpb.WorkflowExecution) {
	ctx := workflow.WithLocalActivityOptions(s.ctx, defaultLocalActivityOptions)
	areq := &schedspb.TerminateWorkflowRequest{
		RequestId: s.newUUIDString(),
		Identity:  s.identity(),
		Execution: ex,
		Reason:    "terminated by schedule overlap policy",
	}
	err := workflow.ExecuteLocalActivity(ctx, s.a.TerminateWorkflow, areq).Get(s.ctx, nil)
	if err != nil {
		s.logger.Error("terminate workflow failed", "workflow", ex.WorkflowId, "error", err)
		s.metrics.Counter(metrics.ScheduleTerminateWorkflowErrors.GetMetricName()).Inc(1)
	}
	// Note: the local activity has completed (or failed) here but we'll still wait until we
	// observe the workflow close (with a watcher) to start the next one.
	// If this failed, that's okay, we'll try it again the next time we try to take an action.
}

func (s *scheduler) getRetentionExpiration(nextWakeup time.Time) time.Time {
	// if RetentionTime is not set or the schedule is paused or nextWakeup time is not zero
	// or there is more action to take, there is no need for retention
	if s.tweakables.RetentionTime == 0 || s.Schedule.State.Paused || (!nextWakeup.IsZero() && s.canTakeScheduledAction(false, false)) {
		return time.Time{}
	}

	lastActionTime := timestamp.TimePtr(time.Time{})
	if len(s.Info.RecentActions) > 0 {
		lastActionTime = s.Info.RecentActions[len(s.Info.RecentActions)-1].ActualTime
	}

	// retention base is max(CreateTime, UpdateTime, and last action time)
	retentionBase := lastActionTime

	if s.Info.CreateTime != nil && s.Info.CreateTime.After(*retentionBase) {
		retentionBase = s.Info.CreateTime
	}
	if s.Info.UpdateTime != nil && s.Info.UpdateTime.After(*retentionBase) {
		retentionBase = s.Info.UpdateTime
	}

	return retentionBase.Add(s.tweakables.RetentionTime)
}

func (s *scheduler) newUUIDString() string {
	if len(s.uuidBatch) == 0 {
		panicIfErr(workflow.SideEffect(s.ctx, func(ctx workflow.Context) interface{} {
			out := make([]string, 10)
			for i := range out {
				out[i] = uuid.NewString()
			}
			return out
		}).Get(&s.uuidBatch))
	}
	next := s.uuidBatch[0]
	s.uuidBatch = s.uuidBatch[1:]
	return next
}

func (s *scheduler) hasMinVersion(version SchedulerWorkflowVersion) bool {
	return s.tweakables.Version >= version
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func GetListInfoFromStartArgs(args *schedspb.StartScheduleArgs, now time.Time) *schedpb.ScheduleListInfo {
	// note that this does not take into account InitialPatch
	fakeScheduler := &scheduler{
		StartScheduleArgs: *args,
		tweakables:        currentTweakablePolicies,
	}
	fakeScheduler.ensureFields()
	fakeScheduler.compileSpec()
	fakeScheduler.State.LastProcessedTime = timestamp.TimePtr(now)
	return fakeScheduler.getListInfo()
}
