package scheduler

import (
	"cmp"
	"context"
	"fmt"
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedpb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type Scheduler struct {
	*schedspb.HsmSchedulerState

	tweakables     scheduler.TweakablePolicies
	specBuilder    *scheduler.SpecBuilder
	cspec          *scheduler.CompiledSpec
	nextTimeCache  *schedspb.NextTimeCache
	nextInvokeTime time.Time
	uuidBatch      []string
	frontendClient workflowservice.WorkflowServiceClient
	historyClient  resource.HistoryClient

	logger  log.Logger
	metrics metrics.Handler
}

func (s *Scheduler) resolveOverlapPolicy(overlapPolicy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		overlapPolicy = s.Args.Schedule.Policies.OverlapPolicy
	}
	if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		overlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return overlapPolicy
}

func (s *Scheduler) populateTransientFieldsIfAbsent(logger log.Logger, handler metrics.Handler, frontendClient workflowservice.WorkflowServiceClient, historyClient resource.HistoryClient) {
	if s.logger == nil {
		s.logger = logger
	}
	if s.metrics == nil {
		s.metrics = handler
	}
	if s.frontendClient == nil {
		s.frontendClient = frontendClient
	}
	if s.historyClient == nil {
		s.historyClient = historyClient
	}
	if s.cspec == nil {
		s.specBuilder = &scheduler.SpecBuilder{}
		cspec, err := s.specBuilder.NewCompiledSpec(s.Args.Schedule.Spec)
		if err != nil {
			if s.logger != nil {
				s.logger.Error("Invalid schedule", tag.Error(err))
			}
			s.cspec = nil
		} else {
			s.cspec = cspec
		}
	}
}

func (s *Scheduler) ensureFields() {
	if s.Args.Schedule == nil {
		s.Args.Schedule = &schedpb.Schedule{}
	}
	if s.Args.Schedule.Spec == nil {
		s.Args.Schedule.Spec = &schedpb.ScheduleSpec{}
	}
	if s.Args.Schedule.Action == nil {
		s.Args.Schedule.Action = &schedpb.ScheduleAction{}
	}
	if s.Args.Schedule.Policies == nil {
		s.Args.Schedule.Policies = &schedpb.SchedulePolicies{}
	}

	s.Args.Schedule.Policies.OverlapPolicy = s.resolveOverlapPolicy(s.Args.Schedule.Policies.OverlapPolicy)
	s.Args.Schedule.Policies.CatchupWindow = durationpb.New(s.getCatchupWindow())

	if s.Args.Schedule.State == nil {
		s.Args.Schedule.State = &schedpb.ScheduleState{}
	}
	if s.Args.Info == nil {
		s.Args.Info = &schedpb.ScheduleInfo{}
	}
	if s.Args.State == nil {
		s.Args.State = &schedspb.InternalState{}
	}
}

func (s *Scheduler) getCatchupWindow() time.Duration {
	cw := s.Args.Schedule.Policies.CatchupWindow
	if cw == nil {
		return s.tweakables.DefaultCatchupWindow
	} else if cw.AsDuration() < s.tweakables.MinCatchupWindow {
		return s.tweakables.MinCatchupWindow
	} else {
		return cw.AsDuration()
	}
}

func (s *Scheduler) jitterSeed() string {
	return fmt.Sprintf("%s-%s", s.Args.State.NamespaceId, s.Args.State.ScheduleId)
}

func (s *Scheduler) identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.Args.State.Namespace, s.Args.State.ScheduleId)
}

func (s *Scheduler) newUUIDString() string {
	if len(s.uuidBatch) == 0 {
		s.uuidBatch = make([]string, 10)
		for i := range s.uuidBatch {
			s.uuidBatch[i] = uuid.NewString()
		}
	}
	next := s.uuidBatch[0]
	s.uuidBatch = s.uuidBatch[1:]
	return next
}

func (s *Scheduler) fillNextTimeCache(start time.Time) {
	s.nextTimeCache = &schedspb.NextTimeCache{
		Version:      int64(s.tweakables.Version),
		StartTime:    timestamppb.New(start),
		NextTimes:    make([]int64, 0, s.tweakables.NextTimeCacheV2Size),
		NominalTimes: make([]int64, 0, s.tweakables.NextTimeCacheV2Size),
	}
	for t := start; len(s.nextTimeCache.NextTimes) < s.tweakables.NextTimeCacheV2Size; {
		next := s.cspec.GetNextTime(s.jitterSeed(), t)
		if next.Next.IsZero() {
			s.nextTimeCache.Completed = true
			break
		}
		// Only include this if it's not equal to Next, otherwise default to Next
		if !next.Nominal.Equal(next.Next) {
			s.nextTimeCache.NominalTimes = s.nextTimeCache.NominalTimes[0:len(s.nextTimeCache.NextTimes)]
			s.nextTimeCache.NominalTimes = append(s.nextTimeCache.NominalTimes, int64(next.Nominal.Sub(start)))
		}
		s.nextTimeCache.NextTimes = append(s.nextTimeCache.NextTimes, int64(next.Next.Sub(start)))
		t = next.Next
	}
}

func searchCache(cache *schedspb.NextTimeCache, after time.Time) (scheduler.GetNextTimeResult, bool) {
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
			return scheduler.GetNextTimeResult{Nominal: nominal, Next: next}, true
		}
	}
	// Ran off end: if completed, then we're done
	if cache.Completed {
		return scheduler.GetNextTimeResult{}, true
	}
	return scheduler.GetNextTimeResult{}, false
}

func (s *Scheduler) getNextTime(after time.Time) scheduler.GetNextTimeResult {
	// Asking for a time before the cache, need to refill.
	// Also if version changed (so we can fix a bug immediately).
	if s.nextTimeCache == nil ||
		after.Before(s.nextTimeCache.StartTime.AsTime()) {
		s.fillNextTimeCache(after)
	}

	// We may need up to three tries: the first is in the cache as it exists now,
	// the second is refilled from cacheBase, and the third is if cacheBase was set
	// too far in the past, we ignore it and fill the cache from after.
	for try := 1; try <= 3; try++ {
		if res, ok := searchCache(s.nextTimeCache, after); ok {
			return res
		}
		// Otherwise refill from base
		s.fillNextTimeCache(after)
	}

	// This should never happen unless there's a bug.
	s.logger.Error("getNextTime: time not found in cache", tag.NewTimeTag("after", after))
	return scheduler.GetNextTimeResult{}
}

func (s *Scheduler) bufferWorkflowStart(nominalTime, actualTime time.Time, overlapPolicy enumspb.ScheduleOverlapPolicy, manual bool) {
	s.logger.Debug("bufferWorkflowStart", tag.NewTimeTag("start-time", nominalTime), tag.NewTimeTag("actual-start-time", actualTime),
		tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))
	if s.tweakables.MaxBufferSize > 0 && len(s.Args.State.BufferedStarts) >= s.tweakables.MaxBufferSize {
		s.logger.Warn("Buffer too large", tag.NewTimeTag("start-time", nominalTime), tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))
		s.metrics.Counter(metrics.ScheduleBufferOverruns.Name()).Record(1)
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

func (s *Scheduler) processTimeRange(
	start, end time.Time,
	overlapPolicy enumspb.ScheduleOverlapPolicy,
	manual bool,
	limit *int,
) time.Time {
	s.logger.Debug("processTimeRange", tag.NewTimeTag("start", start), tag.NewTimeTag("end", end),
		tag.NewAnyTag("overlap-policy", overlapPolicy), tag.NewBoolTag("manual", manual))

	if s.cspec == nil {
		return time.Time{}
	}
	catchupWindow := s.getCatchupWindow()
	var next scheduler.GetNextTimeResult
	for next = s.getNextTime(start); !(next.Next.IsZero() || next.Next.After(end)); next = s.getNextTime(next.Next) {
		if !manual && s.Args.Info.UpdateTime.AsTime().After(next.Next) {
			// We're reprocessing since the most recent event after an update. Discard actions before
			// the update time (which was just set to "now"). This doesn't have to be guarded with
			// hasMinVersion because this condition couldn't happen in previous versions.
			continue
		}
		if !manual && end.Sub(next.Next) > catchupWindow {
			s.logger.Warn("Schedule missed catchup window", tag.NewTimeTag("now", end), tag.NewTimeTag("time", next.Next))
			s.metrics.Counter(metrics.ScheduleMissedCatchupWindow.Name()).Record(1)
			s.Args.Info.MissedCatchupWindow++
			continue
		}
		s.bufferWorkflowStart(next.Nominal, next.Next, overlapPolicy, manual)

		if limit != nil {
			if (*limit)--; *limit <= 0 {
				break
			}
		}
	}
	return next.Next
}

func (s *Scheduler) processBackfills() {
	limit := s.tweakables.BackfillsPerIteration

	for len(s.Args.State.OngoingBackfills) > 0 &&
		limit > 0 &&
		// use only half the buffer for backfills
		len(s.Args.State.BufferedStarts) < s.tweakables.MaxBufferSize/2 {
		bfr := s.Args.State.OngoingBackfills[0]
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
			s.Args.State.OngoingBackfills = s.Args.State.OngoingBackfills[1:]
		} else {
			// adjust start time for next iteration
			bfr.StartTime = timestamppb.New(next)
		}
	}
}

func (s *Scheduler) recordAction(result *schedpb.ScheduleActionResult, nonOverlapping bool) {
	s.Args.Info.ActionCount++
	s.Args.Info.RecentActions = util.SliceTail(append(s.Args.Info.RecentActions, result), s.tweakables.RecentActionCount)
	if nonOverlapping && result.StartWorkflowResult != nil {
		s.Args.Info.RunningWorkflows = append(s.Args.Info.RunningWorkflows, result.StartWorkflowResult)
	}
}

func (s *Scheduler) processBuffer() bool {
	s.logger.Debug("processBuffer", tag.NewInt("buffer", len(s.Args.State.BufferedStarts)), tag.NewInt("running", len(s.Args.Info.RunningWorkflows)))

	// Make sure we have something to start. If not, we can clear the buffer.
	req := s.Args.Schedule.Action.GetStartWorkflow()
	if req == nil || len(s.Args.State.BufferedStarts) == 0 {
		s.Args.State.BufferedStarts = nil
		return false
	}

	isRunning := len(s.Args.Info.RunningWorkflows) > 0
	tryAgain := false

	action := scheduler.ProcessBuffer(s.Args.State.BufferedStarts, isRunning, s.resolveOverlapPolicy)

	s.Args.State.BufferedStarts = action.NewBuffer
	s.Args.Info.OverlapSkipped += action.OverlapSkipped

	// Try starting whatever we're supposed to start now
	allStarts := action.OverlappingStarts
	if action.NonOverlappingStart != nil {
		allStarts = append(allStarts, action.NonOverlappingStart)
	}
	for _, start := range allStarts {
		result, err := s.startWorkflow(start, req)
		metricsWithTag := s.metrics.WithTags(metrics.StringTag(metrics.ScheduleActionTypeTag, metrics.ScheduleActionStartWorkflow))
		if err != nil {
			s.logger.Error("Failed to start workflow", tag.NewErrorTag(err))
			metricsWithTag.Counter(metrics.ScheduleActionErrors.Name()).Record(1)
			// TODO: we could put this back in the buffer and retry (after a delay) up until
			// the catchup window. of course, it's unlikely that this workflow would be making
			// progress while we're unable to start a new one, so maybe it's not that valuable.
			tryAgain = true
			continue
		}
		metricsWithTag.Counter(metrics.ScheduleActionSuccess.Name()).Record(1)
		nonOverlapping := start == action.NonOverlappingStart
		s.recordAction(result, nonOverlapping)
	}

	// Terminate or cancel if required (terminate overrides cancel if both are present)
	if action.NeedTerminate {
		for _, ex := range s.Args.Info.RunningWorkflows {
			s.terminateWorkflow(ex)
		}
	} else if action.NeedCancel {
		for _, ex := range s.Args.Info.RunningWorkflows {
			s.cancelWorkflow(ex)
		}
	}

	return tryAgain
}

func (s *Scheduler) startWorkflow(
	start *schedspb.BufferedStart,
	newWorkflow *workflowpb.NewWorkflowExecutionInfo,
) (*schedpb.ScheduleActionResult, error) {
	nominalTimeSec := start.NominalTime.AsTime().UTC().Truncate(time.Second)
	workflowID := newWorkflow.WorkflowId
	if start.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL || s.tweakables.AlwaysAppendTimestamp {
		// must match AppendedTimestampForValidation
		workflowID += "-" + nominalTimeSec.Format(time.RFC3339)
	}

	lastCompletionResult, continuedFailure := s.Args.State.LastCompletionResult, s.Args.State.ContinuedFailure
	if start.OverlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
		// ALLOW_ALL runs don't participate in lastCompletionResult/continuedFailure at all
		lastCompletionResult = nil
		continuedFailure = nil
	}

	// TODO(Tianyu): Add callback here
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
			// TODO(Tianyu): Search is not implemented for now
			SearchAttributes:     &commonpb.SearchAttributes{IndexedFields: nil},
			Header:               newWorkflow.Header,
			LastCompletionResult: lastCompletionResult,
			ContinuedFailure:     continuedFailure,
			Namespace:            s.Args.State.NamespaceId,
		},
	}

	res, err := s.frontendClient.StartWorkflowExecution(context.Background(), req.Request)
	if err != nil {
		// TODO(Tianyu): original implementation translates this error which may not be relevant any more
		return nil, err
	}
	// this will not match the time in the workflow execution started event
	// exactly, but it's just informational so it's close enough.
	now := time.Now()

	if !start.Manual {
		// record metric only for _scheduled_ actions, not trigger/backfill, otherwise it's not meaningful
		desiredTime := cmp.Or(start.DesiredTime, start.ActualTime)
		s.metrics.Timer(metrics.ScheduleActionDelay.Name()).Record(now.Sub(desiredTime.AsTime()))
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

func (s *Scheduler) terminateWorkflow(ex *commonpb.WorkflowExecution) {
	// TODO: remove after https://github.com/temporalio/sdk-go/issues/1066
	// TODO(Tianyu): hardcoded wait time
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	_, err := s.historyClient.TerminateWorkflowExecution(ctx, rreq)

	// TODO(Tianyu): original implementation translates this error which may not be relevant any more
	if err != nil {
		s.logger.Error("terminate workflow failed", tag.WorkflowID(ex.WorkflowId), tag.Error(err))
		s.metrics.Counter(metrics.ScheduleTerminateWorkflowErrors.Name()).Record(1)
	}
}

func (s *Scheduler) cancelWorkflow(ex *commonpb.WorkflowExecution) {
	// TODO: remove after https://github.com/temporalio/sdk-go/issues/1066
	// TODO(Tianyu): hardcoded wait time
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rreq := &historyservice.RequestCancelWorkflowExecutionRequest{
		NamespaceId: s.Args.State.NamespaceId,
		CancelRequest: &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace: s.Args.State.Namespace,
			// only set WorkflowId so we cancel the latest, but restricted by FirstExecutionRunId
			WorkflowExecution:   &commonpb.WorkflowExecution{WorkflowId: ex.WorkflowId},
			Identity:            s.identity(),
			RequestId:           s.newUUIDString(),
			FirstExecutionRunId: ex.RunId,
			Reason:              "cancelled by schedule overlap policy",
		},
	}
	_, _ = s.historyClient.RequestCancelWorkflowExecution(ctx, rreq)
	// Note: the local activity has completed (or failed) here but the workflow might take time
	// to close since a cancel is only a request.
	// If this failed, that's okay, we'll try it again the next time we try to take an action.
}
