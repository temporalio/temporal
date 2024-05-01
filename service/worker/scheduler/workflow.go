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
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdklog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/encoding/protojson"

	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/utf8validator"
	"go.temporal.io/server/common/util"
)

type SchedulerWorkflowVersion int64

const (
	// Versions of workflow logic. When introducing a new version, consider generating a new
	// history for TestReplays using generate_history.sh.

	// represents the state before Version is introduced
	InitialVersion SchedulerWorkflowVersion = 0
	// skip over entire time range if paused and batch and cache getNextTime queries
	BatchAndCacheTimeQueries = 1
	// use cache v2, and include ids in jitter
	NewCacheAndJitter = 2
	// Don't put possibly-overlapping runs (from SCHEDULE_OVERLAP_POLICY_ALLOW_ALL) in
	// RunningWorkflows.
	DontTrackOverlapping = 3
	// start time in backfill is inclusive rather than exclusive
	InclusiveBackfillStartTime = 4
	// do backfill incrementally
	IncrementalBackfill = 5
	// update from previous action instead of current time
	UpdateFromPrevious = 6
	// do continue-as-new after pending signals
	CANAfterSignals = 7
)

const (
	// Schedules are implemented by a workflow whose ID is this string plus the schedule ID.
	WorkflowIDPrefix = "temporal-sys-scheduler:"

	// This is an example of a timestamp that's appended to the workflow
	// id, used for validation in the frontend.
	AppendedTimestampForValidation = "-2009-11-10T23:00:00Z"

	SignalNameUpdate   = "update"
	SignalNamePatch    = "patch"
	SignalNameRefresh  = "refresh"
	SignalNameForceCAN = "force-continue-as-new"

	QueryNameDescribe          = "describe"
	QueryNameListMatchingTimes = "listMatchingTimes"

	MemoFieldInfo = "ScheduleInfo"

	InitialConflictToken = 1

	// Maximum number of times to list per ListMatchingTimes query. (This is used only in a
	// query so it can be changed without breaking history.)
	maxListMatchingTimesCount = 1000

	rateLimitedErrorType = "RateLimited"

	nextTimeCacheV1Size = 10

	impossibleHistorySize = 1e6 // just for testing, no real history can be this long
)

type (
	scheduler struct {
		*schedspb.StartScheduleArgs

		ctx     workflow.Context
		a       *activities
		logger  sdklog.Logger
		metrics sdkclient.MetricsHandler

		// SpecBuilder is technically a non-deterministic dependency, but it's safe as
		// long as we only call methods on cspec inside of SideEffect (or in a query
		// without modifying state).
		specBuilder *SpecBuilder
		cspec       *CompiledSpec

		tweakables tweakablePolicies

		currentTimer         workflow.Future
		currentTimerDeadline time.Time

		// We might have zero or one long-poll watcher activity running. If so, these are set:
		watchingWorkflowId string
		watchingFuture     workflow.Future

		// Signal requests
		pendingPatch  *schedpb.SchedulePatch
		pendingUpdate *schedspb.FullUpdateRequest
		forceCAN      bool

		uuidBatch []string

		// This cache is used to store time results after batching getNextTime queries
		// in a single SideEffect
		nextTimeCacheV1 map[time.Time]getNextTimeResult
		nextTimeCacheV2 *schedspb.NextTimeCache
	}

	tweakablePolicies struct {
		DefaultCatchupWindow              time.Duration            // Default for catchup window
		MinCatchupWindow                  time.Duration            // Minimum for catchup window
		RetentionTime                     time.Duration            // How long to keep schedules after they're done
		CanceledTerminatedCountAsFailures bool                     // Whether cancelled+terminated count for pause-on-failure
		AlwaysAppendTimestamp             bool                     // Whether to append timestamp for non-overlapping workflows too
		FutureActionCount                 int                      // The number of future action times to include in Describe.
		RecentActionCount                 int                      // The number of recent actual action results to include in Describe.
		FutureActionCountForList          int                      // The number of future action times to include in List (search attr).
		RecentActionCountForList          int                      // The number of recent actual action results to include in List (search attr).
		IterationsBeforeContinueAsNew     int                      // Number of iterations per run, or 0 to use server-suggested
		SleepWhilePaused                  bool                     // If true, don't set timers while paused/out of actions
		MaxBufferSize                     int                      // MaxBufferSize limits the number of buffered starts and backfills
		BackfillsPerIteration             int                      // How many backfilled actions to take per iteration (implies rate limit since min sleep is 1s)
		AllowZeroSleep                    bool                     // Whether to allow a zero-length timer. Used for workflow compatibility.
		ReuseTimer                        bool                     // Whether to reuse timer. Used for workflow compatibility.
		NextTimeCacheV2Size               int                      // Size of next time cache (v2)
		Version                           SchedulerWorkflowVersion // Used to keep track of schedules version to release new features and for backward compatibility
		// version 0 corresponds to the schedule version that comes before introducing the Version parameter

		// When introducing a new field with new workflow logic, consider generating a new
		// history for TestReplays using generate_history.sh.
	}

	// this is for backwards compatibility; current code serializes cache as proto
	jsonNextTimeCacheV2 struct {
		Version   SchedulerWorkflowVersion
		Start     time.Time           // start time that the results were calculated from
		Results   []getNextTimeResult // results of getNextTime in sequence
		Completed bool                // whether the end of results represents the end of the schedule
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
		IterationsBeforeContinueAsNew:     0,
		SleepWhilePaused:                  true,
		MaxBufferSize:                     1000,
		BackfillsPerIteration:             10,
		AllowZeroSleep:                    true,
		ReuseTimer:                        true,
		NextTimeCacheV2Size:               14, // see note below
		Version:                           UpdateFromPrevious,
	}

	// Note on NextTimeCacheV2Size: This value must be > FutureActionCountForList. Each
	// iteration we ask for FutureActionCountForList times from the cache. If the cache size
	// was 10, we would need to refill it on the 7th iteration, to get times 7, 8, 9, 10, 11,
	// so the effective size would only be 6. To get an effective size of 10, we need the size
	// to be 10 + FutureActionCountForList - 1 = 14. With that size, we'll fill every 10
	// iterations, on 1, 11, 21, etc.

	errUpdateConflict = errors.New("conflicting concurrent update")
)

func SchedulerWorkflow(ctx workflow.Context, args *schedspb.StartScheduleArgs) error {
	return schedulerWorkflowWithSpecBuilder(ctx, args, NewSpecBuilder())
}

func schedulerWorkflowWithSpecBuilder(ctx workflow.Context, args *schedspb.StartScheduleArgs, specBuilder *SpecBuilder) error {
	scheduler := &scheduler{
		StartScheduleArgs: args,
		ctx:               ctx,
		a:                 nil,
		logger:            sdklog.With(workflow.GetLogger(ctx), "wf-namespace", args.State.Namespace, "schedule-id", args.State.ScheduleId),
		metrics:           workflow.GetMetricsHandler(ctx).WithTags(map[string]string{"namespace": args.State.Namespace}),
		specBuilder:       specBuilder,
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
		specJson, _ := protojson.Marshal(s.Schedule.Spec)
		policiesJson, _ := protojson.Marshal(s.Schedule.Policies)
		s.logger.Info("Starting schedule", "spec", string(specJson), "policies", string(policiesJson))

		s.State.LastProcessedTime = timestamppb.New(s.now())
		s.State.ConflictToken = InitialConflictToken
		s.Info.CreateTime = s.State.LastProcessedTime
	}

	// A schedule may be created with an initial Patch, e.g. start one immediately. Put that in
	// the state so it takes effect below.
	s.pendingPatch = s.InitialPatch
	s.InitialPatch = nil

	iters := s.tweakables.IterationsBeforeContinueAsNew
	for {
		info := workflow.GetInfo(s.ctx)
		suggestContinueAsNew := info.GetCurrentHistoryLength() >= impossibleHistorySize
		if s.tweakables.IterationsBeforeContinueAsNew > 0 {
			suggestContinueAsNew = suggestContinueAsNew || iters <= 0
			iters--
		} else {
			suggestContinueAsNew = suggestContinueAsNew || info.GetContinueAsNewSuggested() || s.forceCAN
		}
		if suggestContinueAsNew && s.pendingUpdate == nil && s.pendingPatch == nil {
			break
		}

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
			nil,
		)
		s.State.LastProcessedTime = timestamppb.New(t2)
		// handle signals after processing time range that just elapsed
		scheduleChanged := s.processSignals()
		if scheduleChanged {
			// need to calculate sleep again. note that processSignals may move LastProcessedTime backwards.
			nextWakeup = s.processTimeRange(
				s.State.LastProcessedTime.AsTime(),
				t2,
				enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED,
				false,
				nil,
			)
		}
		// process backfills if we have any too
		s.processBackfills()
		// try starting workflows in the buffer
		//nolint:revive
		for s.processBuffer() {
		}
		s.updateMemoAndSearchAttributes()

		// if schedule is not paused and out of actions or do not have anything scheduled, exit the schedule workflow after retention period has passed
		if exp := s.getRetentionExpiration(nextWakeup); !exp.IsZero() && !exp.After(s.now()) {
			return nil
		}
		if suggestContinueAsNew && s.pendingUpdate == nil && s.pendingPatch == nil && s.hasMinVersion(CANAfterSignals) {
			// If suggestContinueAsNew was true but we had a pending update or patch, we would
			// not break above, but process the update/patch. Now that we're done, we should
			// break here to do CAN. (pendingUpdate and pendingPatch should always nil here,
			// the check check above is just being defensive.)
			break
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
	return workflow.NewContinueAsNewError(s.ctx, WorkflowType, s.StartScheduleArgs)
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
	s.Schedule.Policies.CatchupWindow = durationpb.New(s.getCatchupWindow())

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
	// if spec changes invalidate current cache
	s.nextTimeCacheV1 = nil
	s.nextTimeCacheV2 = nil

	cspec, err := s.specBuilder.NewCompiledSpec(s.Schedule.Spec)
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
	s.logger.Debug("Schedule patch")

	if trigger := patch.TriggerImmediately; trigger != nil {
		now := s.now()
		s.addStart(now, now, trigger.OverlapPolicy, true)
	}

	for _, bfr := range patch.BackfillRequest {
		// In previous versions the backfill start time was exclusive, ie when
		// the start time of the backfill matched the schedule's spec, it would
		// not be executed. This new version makes it inclusive instead.
		if s.hasMinVersion(InclusiveBackfillStartTime) {
			startTime := timestamp.TimeValue(bfr.GetStartTime()).Add(-1 * time.Millisecond)
			bfr.StartTime = timestamppb.New(startTime)
		}
		if s.hasMinVersion(IncrementalBackfill) {
			// Add to ongoing backfills to process incrementally
			if len(s.State.OngoingBackfills) >= s.tweakables.MaxBufferSize {
				s.logger.Warn("Buffer overrun for backfill requests")
				s.metrics.Counter(metrics.ScheduleBufferOverruns.Name()).Inc(1)
				s.Info.BufferDropped += 1
				continue
			}
			s.State.OngoingBackfills = append(s.State.OngoingBackfills, common.CloneProto(bfr))
		} else {
			// Old version: process whole backfill synchronously
			s.processTimeRange(
				timestamp.TimeValue(bfr.GetStartTime()),
				timestamp.TimeValue(bfr.GetEndTime()),
				bfr.GetOverlapPolicy(),
				true,
				nil,
			)
		}
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

func (s *scheduler) getNextTimeV1(after time.Time) getNextTimeResult {
	// we populate the map sequentially, if after is not in the map, it means we either exhausted
	// all items, or we jumped through time (forward or backward), in either case, refresh the cache
	next, ok := s.nextTimeCacheV1[after]
	if ok {
		return next
	}
	s.nextTimeCacheV1 = nil
	// Run this logic in a SideEffect so that we can fix bugs there without breaking
	// existing schedule workflows.
	panicIfErr(workflow.SideEffect(s.ctx, func(ctx workflow.Context) interface{} {
		results := make(map[time.Time]getNextTimeResult)
		for t := after; !t.IsZero() && len(results) < nextTimeCacheV1Size; {
			next := s.cspec.getNextTime(s.jitterSeed(), t)
			results[t] = next
			t = next.Next
		}
		return results
	}).Get(&s.nextTimeCacheV1))
	return s.nextTimeCacheV1[after]
}

// Gets the next scheduled time after `after`, making use of a cache. If the cache needs to be
// refilled, try to refill it starting from `cacheBase` instead of `after`. This avoids having
// the cache range jump back and forth when generating a sequence of times.
func (s *scheduler) getNextTimeV2(cacheBase, after time.Time) getNextTimeResult {
	// cacheBase must be before after
	cacheBase = util.MinTime(cacheBase, after)

	// Asking for a time before the cache, need to refill.
	// Also if version changed (so we can fix a bug immediately).
	if s.nextTimeCacheV2 == nil ||
		after.Before(s.nextTimeCacheV2.StartTime.AsTime()) ||
		SchedulerWorkflowVersion(s.nextTimeCacheV2.Version) != s.tweakables.Version {
		s.fillNextTimeCacheV2(cacheBase)
	}

	// We may need up to three tries: the first is in the cache as it exists now,
	// the second is refilled from cacheBase, and the third is if cacheBase was set
	// too far in the past, we ignore it and fill the cache from after.
	for try := 1; try <= 3; try++ {
		if res, ok := searchCache(s.nextTimeCacheV2, after); ok {
			return res
		}
		// Otherwise refill from base
		s.fillNextTimeCacheV2(cacheBase)
		// Reset cacheBase so that if it was set too early and we run off the end again, the
		// third try will work.
		cacheBase = after
	}

	// This should never happen unless there's a bug.
	s.logger.Error("getNextTimeV2: time not found in cache", "after", after)
	return getNextTimeResult{}
}

func searchCache(cache *schedspb.NextTimeCache, after time.Time) (getNextTimeResult, bool) {
	// The cache covers a contiguous time range so we can do a linear search in it.
	start := cache.StartTime.AsTime()
	afterOffset := int64(after.Sub(start))
	for i, nextOffset := range cache.NextTimes {
		if nextOffset > afterOffset {
			next := start.Add(time.Duration(nextOffset))
			nominal := next
			if i < len(cache.NominalTimes) && cache.NominalTimes[i] != 0 {
				nominal = start.Add(time.Duration(cache.NominalTimes[i]))
			}
			return getNextTimeResult{Nominal: nominal, Next: next}, true
		}
	}
	// Ran off end: if completed, then we're done
	if cache.Completed {
		return getNextTimeResult{}, true
	}
	return getNextTimeResult{}, false
}

func (s *scheduler) fillNextTimeCacheV2(start time.Time) {
	// Clear value so we can Get into it
	s.nextTimeCacheV2 = nil
	// Run this logic in a SideEffect so that we can fix bugs there without breaking
	// existing schedule workflows.
	val := workflow.SideEffect(s.ctx, func(ctx workflow.Context) interface{} {
		cache := &schedspb.NextTimeCache{
			Version:      int64(s.tweakables.Version),
			StartTime:    timestamppb.New(start),
			NextTimes:    make([]int64, 0, s.tweakables.NextTimeCacheV2Size),
			NominalTimes: make([]int64, 0, s.tweakables.NextTimeCacheV2Size),
		}
		for t := start; len(cache.NextTimes) < s.tweakables.NextTimeCacheV2Size; {
			next := s.cspec.getNextTime(s.jitterSeed(), t)
			if next.Next.IsZero() {
				cache.Completed = true
				break
			}
			// Only include this if it's not equal to Next, otherwise default to Next
			if !next.Nominal.Equal(next.Next) {
				cache.NominalTimes = cache.NominalTimes[0:len(cache.NextTimes)]
				cache.NominalTimes = append(cache.NominalTimes, int64(next.Nominal.Sub(start)))
			}
			cache.NextTimes = append(cache.NextTimes, int64(next.Next.Sub(start)))
			t = next.Next
		}
		return cache
	})
	// Previous versions of this workflow returned a json-encoded value here. This will attempt
	// to unmarshal it into a proto struct. We want this to fail so we can convert it manually,
	// but it might not. To be sure, check StartTime also (the field names differ between json
	// and proto so json.Unmarshal will never fill in StartTime).
	if val.Get(&s.nextTimeCacheV2) == nil && s.nextTimeCacheV2.GetStartTime() != nil {
		return
	}
	// Try as json value
	var jsonVal jsonNextTimeCacheV2
	if val.Get(&jsonVal) != nil || jsonVal.Start.IsZero() {
		panic("could not decode next time cache as proto or json")
	}
	s.nextTimeCacheV2 = &schedspb.NextTimeCache{
		Version:      int64(jsonVal.Version),
		StartTime:    timestamppb.New(jsonVal.Start),
		NextTimes:    make([]int64, len(jsonVal.Results)),
		NominalTimes: make([]int64, len(jsonVal.Results)),
		Completed:    jsonVal.Completed,
	}
	for i, res := range jsonVal.Results {
		s.nextTimeCacheV2.NextTimes[i] = int64(res.Next.Sub(jsonVal.Start))
		s.nextTimeCacheV2.NominalTimes[i] = int64(res.Nominal.Sub(jsonVal.Start))
	}
}

func (s *scheduler) getNextTime(after time.Time) getNextTimeResult {
	// Implementation using a cache to save markers + computation.
	if s.hasMinVersion(NewCacheAndJitter) {
		return s.getNextTimeV2(after, after)
	} else if s.hasMinVersion(BatchAndCacheTimeQueries) {
		return s.getNextTimeV1(after)
	}
	// Run this logic in a SideEffect so that we can fix bugs there without breaking
	// existing schedule workflows.
	var next getNextTimeResult
	panicIfErr(workflow.SideEffect(s.ctx, func(ctx workflow.Context) interface{} {
		return s.cspec.getNextTime(s.jitterSeed(), after)
	}).Get(&next))
	return next
}

func (s *scheduler) processTimeRange(
	start, end time.Time,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
	manual bool,
	limit *int,
) time.Time {
	s.logger.Debug("processTimeRange", "start", start, "end", end, "overlap-policy", overlapPolicy, "manual", manual)

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
			return s.getNextTime(end).Next
		}
	}

	var next getNextTimeResult
	for next = s.getNextTime(start); !(next.Next.IsZero() || next.Next.After(end)); next = s.getNextTime(next.Next) {
		if !s.hasMinVersion(BatchAndCacheTimeQueries) && !s.canTakeScheduledAction(manual, false) {
			continue
		}
		if !manual && s.Info.UpdateTime.AsTime().After(next.Next) {
			// We're reprocessing since the most recent event after an update. Discard actions before
			// the update time (which was just set to "now"). This doesn't have to be guarded with
			// hasMinVersion because this condition couldn't happen in previous versions.
			continue
		}
		if !manual && end.Sub(next.Next) > catchupWindow {
			s.logger.Warn("Schedule missed catchup window", "now", end, "time", next.Next)
			s.metrics.Counter(metrics.ScheduleMissedCatchupWindow.Name()).Inc(1)
			s.Info.MissedCatchupWindow++
			continue
		}
		s.addStart(next.Nominal, next.Next, overlapPolicy, manual)

		if limit != nil {
			if (*limit)--; *limit <= 0 {
				break
			}
		}
	}
	return next.Next
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

	forceCAN := workflow.GetSignalChannel(s.ctx, SignalNameForceCAN)
	sel.AddReceive(forceCAN, s.handleForceCANSignal)

	if s.hasMoreAllowAllBackfills() {
		// if we have more allow-all backfills to do, do a short sleep and continue
		nextWakeup = s.now().Add(1 * time.Second)
	} else if s.tweakables.SleepWhilePaused && !s.canTakeScheduledAction(false, false) {
		// if we're paused or out of actions, we don't need to wake up until we get an update
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

func (s *scheduler) processBackfills() {
	limit := s.tweakables.BackfillsPerIteration

	for len(s.State.OngoingBackfills) > 0 &&
		limit > 0 &&
		// use only half the buffer for backfills
		len(s.State.BufferedStarts) < s.tweakables.MaxBufferSize/2 {
		bfr := s.State.OngoingBackfills[0]
		startTime := timestamp.TimeValue(bfr.GetStartTime())
		endTime := timestamp.TimeValue(bfr.GetEndTime())
		next := s.processTimeRange(
			startTime,
			endTime,
			bfr.GetOverlapPolicy(),
			true,
			&limit,
		)
		if next.IsZero() || next.After(endTime) {
			// done with this one
			s.State.OngoingBackfills = s.State.OngoingBackfills[1:]
		} else {
			// adjust start time for next iteration
			bfr.StartTime = timestamppb.New(next)
		}
	}
}

func (s *scheduler) hasMoreAllowAllBackfills() bool {
	return len(s.State.OngoingBackfills) > 0 &&
		s.resolveOverlapPolicy(s.State.OngoingBackfills[0].OverlapPolicy) == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
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

	s.logger.Debug("Schedule update")

	s.Schedule.Spec = req.Schedule.GetSpec()
	s.Schedule.Action = req.Schedule.GetAction()
	s.Schedule.Policies = req.Schedule.GetPolicies()
	s.Schedule.State = req.Schedule.GetState()
	// don't touch Info

	s.ensureFields()
	s.compileSpec()

	s.updateCustomSearchAttributes(req.SearchAttributes)

	if s.hasMinVersion(UpdateFromPrevious) {
		// We need to start re-processing from the last event, so that we catch actions whose
		// nominal time is before now but actual time (with jitter) is after now. Logic in
		// processTimeRange will discard actions before the UpdateTime.
		// Note: get last event time before updating s.Info.UpdateTime, otherwise it'll always be now.
		s.State.LastProcessedTime = timestamppb.New(s.getLastEvent())
	}

	s.Info.UpdateTime = timestamppb.New(s.now())
	s.incSeqNo()
}

func (s *scheduler) handleRefreshSignal(ch workflow.ReceiveChannel, _ bool) {
	ch.Receive(s.ctx, nil)
	s.logger.Debug("got refresh signal")
	// If we're woken up by any signal, we'll pass through processBuffer before sleeping again.
	// processBuffer will see this flag and refresh everything.
	s.State.NeedRefresh = true
}

func (s *scheduler) handleForceCANSignal(ch workflow.ReceiveChannel, _ bool) {
	ch.Receive(s.ctx, nil)
	s.logger.Debug("got force-continue-as-new signal")
	s.forceCAN = true
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

func (s *scheduler) getFutureActionTimes(inWorkflowContext bool, n int) []*timestamppb.Timestamp {
	// Note that `s` may be a `scheduler` created outside of a workflow context, used to
	// compute list info at creation time or in a query. In that case inWorkflowContext will
	// be false, and this function and anything it calls should not use s.ctx.

	base := timestamp.TimeValue(s.State.LastProcessedTime)

	// Pure version not using workflow context
	next := func(t time.Time) time.Time {
		return s.cspec.getNextTime(s.jitterSeed(), t).Next
	}

	if inWorkflowContext && s.hasMinVersion(NewCacheAndJitter) {
		// We can use the cache here
		next = func(t time.Time) time.Time {
			return s.getNextTimeV2(base, t).Next
		}
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
		out = append(out, timestamppb.New(t1))
	}
	return out
}

func (s *scheduler) handleDescribeQuery() (*schedspb.DescribeResponse, error) {
	// this is a query handler, don't modify s.Info directly
	infoCopy := common.CloneProto(s.Info)
	infoCopy.FutureActionTimes = s.getFutureActionTimes(false, s.tweakables.FutureActionCount)
	infoCopy.BufferSize = int64(len(s.State.BufferedStarts))

	return &schedspb.DescribeResponse{
		Schedule:      s.Schedule,
		Info:          infoCopy,
		ConflictToken: s.State.ConflictToken,
	}, nil
}

func (s *scheduler) handleListMatchingTimesQuery(req *workflowservice.ListScheduleMatchingTimesRequest) (*workflowservice.ListScheduleMatchingTimesResponse, error) {
	if req == nil || req.StartTime == nil || req.EndTime == nil {
		return nil, errors.New("missing or invalid query")
	}
	if s.cspec == nil {
		return nil, fmt.Errorf("invalid schedule: %s", s.Info.InvalidScheduleError)
	}

	var out []*timestamppb.Timestamp
	t1 := timestamp.TimeValue(req.StartTime)
	for i := 0; i < maxListMatchingTimesCount; i++ {
		// don't need to call getNextTime in SideEffect because this is just a query
		t1 = s.cspec.getNextTime(s.jitterSeed(), t1).Next
		if t1.IsZero() || t1.After(timestamp.TimeValue(req.EndTime)) {
			break
		}
		out = append(out, timestamppb.New(t1))
	}
	return &workflowservice.ListScheduleMatchingTimesResponse{StartTime: out}, nil
}

func (s *scheduler) incSeqNo() {
	s.State.ConflictToken++
}

func (s *scheduler) getListInfo(inWorkflowContext bool) *schedpb.ScheduleListInfo {
	// Note that `s` may be a `scheduler` created outside of a workflow context, used to
	// compute list info at creation time. In that case inWorkflowContext will be false,
	// and this function and anything it calls should not use s.ctx.

	spec := common.CloneProto(s.Schedule.Spec)
	// clear fields that are too large/not useful for the list view
	spec.TimezoneData = nil

	return &schedpb.ScheduleListInfo{
		Spec:              spec,
		WorkflowType:      s.Schedule.Action.GetStartWorkflow().GetWorkflowType(),
		Notes:             s.Schedule.State.Notes,
		Paused:            s.Schedule.State.Paused,
		RecentActions:     util.SliceTail(s.Info.RecentActions, s.tweakables.RecentActionCountForList),
		FutureActionTimes: s.getFutureActionTimes(inWorkflowContext, s.tweakables.FutureActionCountForList),
	}
}

func (s *scheduler) updateCustomSearchAttributes(searchAttributes *commonpb.SearchAttributes) {
	// We want to distinguish nil value from an empty object value.
	// When it is nil, then it's a no-op. Otherwise, it will overwrite the search attributes.
	// That is, if it is an empty map, it will unset all search attributes.
	if searchAttributes == nil {
		return
	}

	upsertMap := map[string]any{}
	for key, valuePayload := range searchAttributes.GetIndexedFields() {
		var value any
		if err := payload.Decode(valuePayload, &value); err != nil {
			s.logger.Error("error updating search attributes of the scheule", "error", err)
			return
		}
		upsertMap[key] = value
	}

	//nolint:staticcheck // SA1019 Use untyped version for backwards compatibility.
	currentSearchAttributes := workflow.GetInfo(s.ctx).SearchAttributes
	for key, currentValuePayload := range currentSearchAttributes.GetIndexedFields() {
		// This might violate determinism when a new system search attribute is added
		// and the user already had a custom search attribute with same name. This is
		// a general issue in the system, and it will be fixed when we introduce
		// a system prefix to fix those conflicts.
		if searchattribute.IsReserved(key) {
			continue
		}
		if newValuePayload, exists := searchAttributes.GetIndexedFields()[key]; !exists {
			// Key is not set, so needs to be deleted.
			upsertMap[key] = nil
		} else if bytes.Equal(currentValuePayload.Data, newValuePayload.Data) {
			// If the payloads data are the same, then we don't need to update the value.
			delete(upsertMap, key)
		}
	}
	if len(upsertMap) == 0 {
		return
	}
	//nolint:staticcheck // SA1019 The untyped function here is more convenient.
	if err := workflow.UpsertSearchAttributes(s.ctx, upsertMap); err != nil {
		s.logger.Error("error updating search attributes of the scheule", "error", err)
	}
}

func (s *scheduler) updateMemoAndSearchAttributes() {
	newInfo := s.getListInfo(true)

	workflowInfo := workflow.GetInfo(s.ctx)
	currentInfoPayload := workflowInfo.Memo.GetFields()[MemoFieldInfo]

	var currentInfoBytes []byte
	var currentInfo schedpb.ScheduleListInfo

	if currentInfoPayload == nil ||
		payload.Decode(currentInfoPayload, &currentInfoBytes) != nil ||
		currentInfo.Unmarshal(currentInfoBytes) != nil ||
		!proto.Equal(&currentInfo, newInfo) {
		// marshal manually to get proto encoding (default dataconverter will use json)
		s.logUTF8ValidationErrors(&currentInfo, newInfo)
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

func (s *scheduler) logUTF8ValidationErrors(msgs ...proto.Message) {
	for _, msg := range msgs {
		// log errors only, don't affect control flow
		_ = utf8validator.Validate(
			msg,
			utf8validator.SourcePersistence,
			tag.WorkflowNamespace(s.State.Namespace),
			tag.ScheduleID(s.State.ScheduleId),
		)
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
	} else if cw.AsDuration() < s.tweakables.MinCatchupWindow {
		return s.tweakables.MinCatchupWindow
	} else {
		return cw.AsDuration()
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
		s.metrics.Counter(metrics.ScheduleBufferOverruns.Name()).Inc(1)
		s.Info.BufferDropped += 1
		return
	}
	s.State.BufferedStarts = append(s.State.BufferedStarts, &schedspb.BufferedStart{
		NominalTime:   timestamppb.New(nominalTime),
		ActualTime:    timestamppb.New(actualTime),
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
			metricsWithTag.Counter(metrics.ScheduleActionErrors.Name()).Inc(1)
			// TODO: we could put this back in the buffer and retry (after a delay) up until
			// the catchup window. of course, it's unlikely that this workflow would be making
			// progress while we're unable to start a new one, so maybe it's not that valuable.
			tryAgain = true
			continue
		}
		metricsWithTag.Counter(metrics.ScheduleActionSuccess.Name()).Inc(1)
		nonOverlapping := start == action.nonOverlappingStart
		s.recordAction(result, nonOverlapping)
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

func (s *scheduler) recordAction(result *schedpb.ScheduleActionResult, nonOverlapping bool) {
	s.Info.ActionCount++
	s.Info.RecentActions = util.SliceTail(append(s.Info.RecentActions, result), s.tweakables.RecentActionCount)
	canTrack := nonOverlapping || !s.hasMinVersion(DontTrackOverlapping)
	if canTrack && result.StartWorkflowResult != nil {
		s.Info.RunningWorkflows = append(s.Info.RunningWorkflows, result.StartWorkflowResult)
	}
}

func (s *scheduler) startWorkflow(
	start *schedspb.BufferedStart,
	newWorkflow *workflowpb.NewWorkflowExecutionInfo,
) (*schedpb.ScheduleActionResult, error) {
	nominalTimeSec := start.NominalTime.AsTime().UTC().Truncate(time.Second)
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
		deadline := start.ActualTime.AsTime().Add(s.getCatchupWindow())
		options.ScheduleToCloseTimeout = deadline.Sub(s.now())
		if options.ScheduleToCloseTimeout < options.StartToCloseTimeout {
			options.ScheduleToCloseTimeout = options.StartToCloseTimeout
		} else if options.ScheduleToCloseTimeout > 1*time.Hour {
			options.ScheduleToCloseTimeout = 1 * time.Hour
		}
	}
	ctx := workflow.WithLocalActivityOptions(s.ctx, options)

	lastCompletionResult, continuedFailure := s.State.LastCompletionResult, s.State.ContinuedFailure
	if start.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL && s.hasMinVersion(DontTrackOverlapping) {
		// ALLOW_ALL runs don't participate in lastCompletionResult/continuedFailure at all
		lastCompletionResult = nil
		continuedFailure = nil
	}

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
			LastCompletionResult:     lastCompletionResult,
			ContinuedFailure:         continuedFailure,
		},
	}
	for {
		var res schedspb.StartWorkflowResponse
		err := workflow.ExecuteLocalActivity(ctx, s.a.StartWorkflow, req).Get(s.ctx, &res)

		var appErr *temporal.ApplicationError
		var details rateLimitedDetails
		if errors.As(err, &appErr) && appErr.Type() == rateLimitedErrorType && appErr.Details(&details) == nil {
			s.metrics.Counter(metrics.ScheduleRateLimited.Name()).Inc(1)
			workflow.Sleep(s.ctx, details.Delay)
			req.CompletedRateLimitSleep = true // only use rate limiter once
			continue
		}
		if err != nil {
			return nil, err
		}

		if !start.Manual {
			// record metric only for _scheduled_ actions, not trigger/backfill, otherwise it's not meaningful
			s.metrics.Timer(metrics.ScheduleActionDelay.Name()).Record(res.RealStartTime.AsTime().Sub(start.ActualTime.AsTime()))
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

func (s *scheduler) jitterSeed() string {
	if s.hasMinVersion(NewCacheAndJitter) {
		return fmt.Sprintf("%s-%s", s.State.NamespaceId, s.State.ScheduleId)
	}
	return ""
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
		s.metrics.Counter(metrics.ScheduleCancelWorkflowErrors.Name()).Inc(1)
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
		s.metrics.Counter(metrics.ScheduleTerminateWorkflowErrors.Name()).Inc(1)
	}
	// Note: the local activity has completed (or failed) here but we'll still wait until we
	// observe the workflow close (with a watcher) to start the next one.
	// If this failed, that's okay, we'll try it again the next time we try to take an action.
}

func (s *scheduler) getRetentionExpiration(nextWakeup time.Time) time.Time {
	// if RetentionTime is not set or the schedule is paused or nextWakeup time is not zero
	// or there is more action to take, there is no need for retention
	if s.tweakables.RetentionTime == 0 ||
		s.Schedule.State.Paused ||
		(!nextWakeup.IsZero() && s.canTakeScheduledAction(false, false)) ||
		s.hasMoreAllowAllBackfills() {
		return time.Time{}
	}

	// retention starts from the last "event"
	return s.getLastEvent().Add(s.tweakables.RetentionTime)
}

// Returns the time of the last "event" to happen to the schedule. An event here is the
// schedule getting created or updated, or an action. This value is used for calculating the
// retention time (how long an idle schedule lives after becoming idle), and also for
// recalculating times after an update to account for jitter.
func (s *scheduler) getLastEvent() time.Time {
	var lastEvent time.Time
	if len(s.Info.RecentActions) > 0 {
		lastEvent = s.Info.RecentActions[len(s.Info.RecentActions)-1].ActualTime.AsTime()
	}
	lastEvent = util.MaxTime(lastEvent, s.Info.CreateTime.AsTime())
	lastEvent = util.MaxTime(lastEvent, s.Info.UpdateTime.AsTime())
	return lastEvent
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

func GetListInfoFromStartArgs(args *schedspb.StartScheduleArgs, now time.Time, specBuilder *SpecBuilder) *schedpb.ScheduleListInfo {
	// Create a scheduler outside of workflow context with just the fields we need to call
	// getListInfo. Note that this does not take into account InitialPatch.
	s := &scheduler{
		StartScheduleArgs: args,
		tweakables:        currentTweakablePolicies,
		specBuilder:       specBuilder,
	}
	s.ensureFields()
	s.compileSpec()
	s.State.LastProcessedTime = timestamppb.New(now)
	return s.getListInfo(false)
}
