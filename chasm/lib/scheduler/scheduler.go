package scheduler

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Scheduler is the root component of a CHASM scheduler tree. The rest of the
// tree will consist of 2 or more sub-components:
// - Generator: buffers actions according to the schedule specification
// - Invoker: executes buffered actions
// - Backfiller: buffers actions according to requested backfills
type Scheduler struct {
	chasm.UnimplementedComponent

	// Persisted internal state, consisting of state relevant to all components in
	// the scheduler tree.
	*schedulerpb.SchedulerState

	// Last success/failure payloads, stored on this separate data node
	// to minimize write traffic.
	LastCompletionResult chasm.Field[*schedulerpb.LastCompletionResult]

	Generator   chasm.Field[*Generator]
	Invoker     chasm.Field[*Invoker]
	Backfillers chasm.Map[string, *Backfiller] // Backfill ID => *Backfiller

	Visibility chasm.Field[*chasm.Visibility]

	// Locally-cached state, invalidated whenever cacheConflictToken != ConflictToken.
	cacheConflictToken int64
	compiledSpec       *scheduler.CompiledSpec // compiledSpec is only ever replaced whole, not mutated.
}

var (
	_ (chasm.VisibilitySearchAttributesProvider) = (*Scheduler)(nil)
	_ (chasm.VisibilityMemoProvider)             = (*Scheduler)(nil)
)

var (
	executionStatusRunning   = "Running"
	executionStatusCompleted = "Completed"
)

var executionStatusSearchAttribute = chasm.NewSearchAttributeKeyword("ExecutionStatus", chasm.SearchAttributeFieldLowCardinalityKeyword01)
var initialSerializedConflictToken = serializeConflictToken(scheduler.InitialConflictToken)

const (
	// How many recent actions to keep on the Info.RecentActions list.
	recentActionCount = 10

	// Item limit per spec field on the ScheduleInfo memo.
	listInfoSpecFieldLimit = 10

	// Field in which the schedule's memo is stored.
	visibilityMemoFieldInfo = "ScheduleInfo"

	// Maximum number of matching times to return.
	maxListMatchingTimesCount = 1000

	// Maximum number of backfillers allowed on a single scheduler.
	maxBackfillers = 100
)

var (
	ErrConflictTokenMismatch = serviceerror.NewFailedPrecondition("mismatched conflict token")
	ErrClosed                = serviceerror.NewFailedPrecondition("schedule closed")
	ErrTooManyBackfillers    = serviceerror.NewFailedPrecondition("too many concurrent backfillers")
	ErrInvalidQuery          = serviceerror.NewInvalidArgument("missing or invalid query")
	ErrSentinel              = serviceerror.NewNotFound("schedule is a sentinel")
)

// NewScheduler returns an initialized CHASM scheduler root component.
func NewScheduler(
	ctx chasm.MutableContext,
	namespace, namespaceID, scheduleID string,
	input *schedulepb.Schedule,
	patch *schedulepb.SchedulePatch,
) (*Scheduler, error) {
	var zero time.Time

	sched := &Scheduler{
		SchedulerState: &schedulerpb.SchedulerState{
			Schedule: input,
			Info: &schedulepb.ScheduleInfo{
				UpdateTime: timestamppb.New(zero),
			},
			Namespace:     namespace,
			NamespaceId:   namespaceID,
			ScheduleId:    scheduleID,
			ConflictToken: scheduler.InitialConflictToken,
		},
		cacheConflictToken:   scheduler.InitialConflictToken,
		Backfillers:          make(chasm.Map[string, *Backfiller]),
		LastCompletionResult: chasm.NewDataField(ctx, &schedulerpb.LastCompletionResult{}),
	}
	sched.setNullableFields()
	sched.Info.CreateTime = timestamppb.New(ctx.Now(sched))

	invoker := NewInvoker(ctx)
	sched.Invoker = chasm.NewComponentField(ctx, invoker)

	generator := NewGenerator(ctx)
	sched.Generator = chasm.NewComponentField(ctx, generator)

	// Create backfillers to fulfill initialPatch.
	if err := sched.handlePatch(ctx, patch); err != nil {
		return nil, err
	}
	visibility := chasm.NewVisibility(ctx)
	sched.Visibility = chasm.NewComponentField(ctx, visibility)

	return sched, nil
}

// NewSentinel returns a sentinel CHASM scheduler that exists only to reserve
// the schedule ID. Sentinels have no sub-components and return NotFound
// on all API operations.
func NewSentinel(
	ctx chasm.MutableContext,
	namespace, namespaceID, scheduleID string,
) *Scheduler {
	return &Scheduler{
		SchedulerState: &schedulerpb.SchedulerState{
			Namespace:     namespace,
			NamespaceId:   namespaceID,
			ScheduleId:    scheduleID,
			Sentinel:      true,
			ConflictToken: scheduler.InitialConflictToken,
		},
		cacheConflictToken: scheduler.InitialConflictToken,
	}
}

// IsSentinel returns true if this is a sentinel scheduler.
func (s *Scheduler) IsSentinel() bool {
	return s.Sentinel
}

// setNullableFields sets fields that are nullable in API requests.
func (s *Scheduler) setNullableFields() {
	if s.Schedule.Policies == nil {
		s.Schedule.Policies = &schedulepb.SchedulePolicies{}
	}
	if s.Schedule.State == nil {
		s.Schedule.State = &schedulepb.ScheduleState{}
	}
}

// handlePatch creates backfillers to fulfill the given patch request.
func (s *Scheduler) handlePatch(ctx chasm.MutableContext, patch *schedulepb.SchedulePatch) error {
	if patch == nil {
		return nil
	}

	// Each TriggerImmediately and BackfillRequest creates exactly one backfiller.
	newCount := len(patch.BackfillRequest)
	if patch.TriggerImmediately != nil {
		newCount++
	}
	if len(s.Backfillers)+newCount > maxBackfillers {
		return ErrTooManyBackfillers
	}

	if patch.TriggerImmediately != nil {
		s.NewImmediateBackfiller(ctx, patch.TriggerImmediately)
	}
	for _, backfill := range patch.BackfillRequest {
		s.NewRangeBackfiller(ctx, backfill)
	}
	return nil
}

// CreateScheduler initializes a new Scheduler for CreateSchedule requests.
func CreateScheduler(
	ctx chasm.MutableContext,
	req *schedulerpb.CreateScheduleRequest,
) (*Scheduler, error) {
	sched, err := NewScheduler(
		ctx,
		req.FrontendRequest.Namespace,
		req.NamespaceId,
		req.FrontendRequest.ScheduleId,
		req.FrontendRequest.Schedule,
		req.FrontendRequest.InitialPatch,
	)
	if err != nil {
		return nil, err
	}

	// Update visibility with custom attributes.
	visibility := sched.Visibility.Get(ctx)
	visibility.MergeCustomSearchAttributes(ctx, req.FrontendRequest.GetSearchAttributes().GetIndexedFields())
	visibility.MergeCustomMemo(ctx, req.FrontendRequest.GetMemo().GetFields())

	return sched, nil
}

// LifecycleState implements the chasm.Component interface.
func (s *Scheduler) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	if s.Closed {
		return chasm.LifecycleStateCompleted
	}

	return chasm.LifecycleStateRunning
}

// Terminate implements the chasm.RootComponent interface.
func (s *Scheduler) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	// TODO: Implement terminate logic.
	return chasm.TerminateComponentResponse{}, nil
}

// NewRangeBackfiller returns an intialized Backfiller component, which should
// be parented under a Scheduler root node.
func (s *Scheduler) NewRangeBackfiller(
	ctx chasm.MutableContext,
	request *schedulepb.BackfillRequest,
) *Backfiller {
	backfiller := addBackfiller(ctx, s)
	backfiller.Request = &schedulerpb.BackfillerState_BackfillRequest{
		BackfillRequest: request,
	}
	return backfiller
}

// NewImmediateBackfiller returns an intialized Backfiller component, which should
// be parented under a Scheduler root node.
func (s *Scheduler) NewImmediateBackfiller(
	ctx chasm.MutableContext,
	request *schedulepb.TriggerImmediatelyRequest,
) *Backfiller {
	backfiller := addBackfiller(ctx, s)
	backfiller.Request = &schedulerpb.BackfillerState_TriggerRequest{
		TriggerRequest: request,
	}
	return backfiller
}

// useScheduledAction returns true when the Scheduler should allow scheduled
// actions to be taken.
//
// When decrement is true, the schedule's state's `RemainingActions` counter is
// decremented when an action can be taken. When decrement is false, no state
// is mutated.
func (s *Scheduler) useScheduledAction(decrement bool) bool {
	scheduleState := s.Schedule.GetState()

	// If paused, don't do anything.
	if scheduleState.Paused {
		return false
	}

	// If unlimited actions, allow.
	if !scheduleState.LimitedActions {
		return true
	}

	// Otherwise check and decrement limit.
	if scheduleState.RemainingActions > 0 {
		if decrement {
			scheduleState.RemainingActions--

			// The conflict token is updated because a client might be in the process of
			// preparing an update request that increments their schedule's RemainingActions
			// field.
			s.updateConflictToken()
		}
		return true
	}

	// No actions left
	return false
}

func (s *Scheduler) getCompiledSpec(specBuilder *scheduler.SpecBuilder) (*scheduler.CompiledSpec, error) {
	s.validateCachedState()

	// Cache compiled spec.
	if s.compiledSpec == nil {
		cspec, err := specBuilder.NewCompiledSpec(s.Schedule.Spec)
		if err != nil {
			return nil, err
		}
		s.compiledSpec = cspec
	}

	return s.compiledSpec, nil
}

// WorkflowID returns the Workflow ID given as part of the request spec.
// During start generation, nominal time is suffixed to this ID.
func (s *Scheduler) WorkflowID() string {
	return s.Schedule.GetAction().GetStartWorkflow().GetWorkflowId()
}

func (s *Scheduler) jitterSeed() string {
	return fmt.Sprintf("%s-%s", s.NamespaceId, s.ScheduleId)
}

func (s *Scheduler) identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.Namespace, s.ScheduleId)
}

func (s *Scheduler) overlapPolicy() enumspb.ScheduleOverlapPolicy {
	policy := s.Schedule.GetPolicies().GetOverlapPolicy()
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return policy
}

func (s *Scheduler) resolveOverlapPolicy(overlapPolicy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
	if overlapPolicy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		overlapPolicy = s.overlapPolicy()
	}
	return overlapPolicy
}

// validateCachedState clears cached fields whenever the Scheduler's
// ConflictToken doesn't match its cacheConflictToken field. Validation is only
// as effective as the Scheduler's backing persisted state is up-to-date.
func (s *Scheduler) validateCachedState() {
	if s.cacheConflictToken != s.ConflictToken {
		// Bust stale cached fields.
		s.compiledSpec = nil

		// We're now up-to-date.
		s.cacheConflictToken = s.ConflictToken
	}
}

// updateConflictToken bumps the Scheduler's conflict token. This has a side
// effect of invalidating the local cache. Use whenever applying a mutation that
// should invalidate other in-flight updates.
func (s *Scheduler) updateConflictToken() {
	s.ConflictToken++
}

// getLastEventTime returns the time of the last "event" to happen to the schedule.
// An event here is the schedule getting created or updated, or an action. This
// value is used for calculating the retention time (how long an idle schedule
// lives after becoming idle).
func (s *Scheduler) getLastEventTime(ctx chasm.Context) time.Time {
	var lastEvent time.Time
	invoker := s.Invoker.Get(ctx)
	recentActions := invoker.recentActions()
	if len(recentActions) > 0 {
		lastEvent = recentActions[len(recentActions)-1].GetActualTime().AsTime()
	}
	lastEvent = util.MaxTime(lastEvent, s.Info.GetCreateTime().AsTime())
	lastEvent = util.MaxTime(lastEvent, s.Info.GetUpdateTime().AsTime())
	return lastEvent
}

// getIdleExpiration returns an idle close time and the boolean value of 'true'
// for when a schedule is idle (pending soft delete).
func (s *Scheduler) getIdleExpiration(
	ctx chasm.Context,
	idleTime time.Duration,
	nextWakeup time.Time,
) (time.Time, bool) {
	// The idle timer to close off the component is started only for schedules with
	// no more work to do. Paused schedules are held open indefinitely.
	if idleTime == 0 ||
		s.Schedule.State.Paused ||
		(!nextWakeup.IsZero() && s.useScheduledAction(false)) ||
		s.hasMoreAllowAllBackfills(ctx) {
		return time.Time{}, false
	}

	return s.getLastEventTime(ctx).Add(idleTime), true
}

func (s *Scheduler) hasMoreAllowAllBackfills(ctx chasm.Context) bool {
	for _, field := range s.Backfillers {
		backfiller := field.Get(ctx)
		var policy enumspb.ScheduleOverlapPolicy
		switch request := backfiller.GetRequest().(type) {
		case *schedulerpb.BackfillerState_BackfillRequest:
			policy = request.BackfillRequest.OverlapPolicy
		case *schedulerpb.BackfillerState_TriggerRequest:
			policy = request.TriggerRequest.OverlapPolicy
		default:
			return false
		}

		if enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL == s.resolveOverlapPolicy(policy) {
			return true
		}
	}

	return false
}

type schedulerActionResult struct {
	overlapSkipped      int64
	missedCatchupWindow int64
	actionCount         int64
}

// recordActionResult updates the Scheduler's customer-facing metrics.
// RunningWorkflows and RecentActions are computed from BufferedStarts.
func (s *Scheduler) recordActionResult(result *schedulerActionResult) {
	s.Info.ActionCount += result.actionCount
	s.Info.OverlapSkipped += result.overlapSkipped
	s.Info.MissedCatchupWindow += result.missedCatchupWindow
}

var _ chasm.NexusCompletionHandler = &Scheduler{}

func executionStatusFromFailure(failure *failurepb.Failure) enumspb.WorkflowExecutionStatus {
	switch failure.FailureInfo.(type) {
	case *failurepb.Failure_CanceledFailureInfo:
		return enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
	case *failurepb.Failure_TimeoutFailureInfo:
		return enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
	default:
		return enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	}
}

// HandleNexusCompletion allows Scheduler to record workflow completions from
// worfklows started by the same scheduler tree's Invoker.
func (s *Scheduler) HandleNexusCompletion(
	ctx chasm.MutableContext,
	info *persistencespb.ChasmNexusCompletion,
) error {
	invoker := s.Invoker.Get(ctx)

	workflowID := invoker.runningWorkflowID(info.RequestId)
	if workflowID == "" {
		// If the request ID was removed, the request must have already been processed;
		// fast-succeed.
		return nil
	}

	// Handle last completed/failed status and payloads.
	//
	// TODO - also record payload sizes once we have metrics wired into CHASM context.
	var wfStatus enumspb.WorkflowExecutionStatus
	switch outcome := info.Outcome.(type) {
	case *persistencespb.ChasmNexusCompletion_Failure:
		previousResult := s.LastCompletionResult.Get(ctx) // Most-recent success is kept after failure.
		wfStatus = executionStatusFromFailure(outcome.Failure)
		s.LastCompletionResult = chasm.NewDataField(ctx, &schedulerpb.LastCompletionResult{
			Failure: outcome.Failure,
			Success: previousResult.Success,
		})
	case *persistencespb.ChasmNexusCompletion_Success:
		wfStatus = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s.LastCompletionResult = chasm.NewDataField(ctx, &schedulerpb.LastCompletionResult{
			Success: outcome.Success,
		})
	default:
		wfStatus = enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	}

	// Handle pause-on-failure.
	if wfStatus != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED &&
		s.Schedule.Policies.PauseOnFailure && !s.Schedule.State.Paused {
		s.Schedule.State.Paused = true
		s.Schedule.State.Notes = fmt.Sprintf(
			"paused, workflow %s: %s",
			strings.ToLower(wfStatus.String()),
			workflowID,
		)
	}

	// Record the completed action in the Invoker.
	completed := &schedulespb.CompletedResult{
		Status:    wfStatus,
		CloseTime: info.CloseTime,
	}
	invoker.recordCompletedAction(ctx, completed, info.RequestId)

	return nil
}

// Describe returns the current state of the Scheduler for DescribeSchedule requests.
func (s *Scheduler) Describe(
	ctx chasm.Context,
	req *schedulerpb.DescribeScheduleRequest,
	specBuilder *scheduler.SpecBuilder,
) (*schedulerpb.DescribeScheduleResponse, error) {
	if s.Sentinel {
		return nil, ErrSentinel
	}
	if s.Closed {
		return nil, ErrClosed
	}

	visibility := s.Visibility.Get(ctx)
	memo := visibility.CustomMemo(ctx)
	delete(memo, visibilityMemoFieldInfo) // We don't need to return a redundant info block.

	if s.Schedule.GetPolicies().GetOverlapPolicy() == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		s.Schedule.Policies.OverlapPolicy = s.overlapPolicy()
	}
	if !s.Schedule.GetPolicies().GetCatchupWindow().IsValid() {
		// TODO - this should be set from Tweakables.DefaultCatchupWindow.
		s.Schedule.Policies.CatchupWindow = durationpb.New(365 * 24 * time.Hour)
	}

	schedule := common.CloneProto(s.Schedule)
	cleanSpec(schedule.Spec)

	generator := s.Generator.Get(ctx)
	if generator.GetFutureActionTimes() == nil {
		// FutureActionTimes is populated asynchronously by the GeneratorTask. If a
		// newly-created schedule is described before the task executes, this field may be
		// nil. In that case, compute it on-demand.
		generator.UpdateFutureActionTimes(ctx, specBuilder)
	}

	// Populate computed views from Invoker's BufferedStarts.
	invoker := s.Invoker.Get(ctx)
	info := common.CloneProto(s.Info)
	info.RunningWorkflows = invoker.runningWorkflowExecutions()
	info.RecentActions = invoker.recentActions()
	info.FutureActionTimes = generator.GetFutureActionTimes()

	return &schedulerpb.DescribeScheduleResponse{
		FrontendResponse: &workflowservice.DescribeScheduleResponse{
			Schedule:         schedule,
			Info:             info,
			ConflictToken:    s.generateConflictToken(),
			Memo:             &commonpb.Memo{Fields: memo},
			SearchAttributes: &commonpb.SearchAttributes{IndexedFields: visibility.CustomSearchAttributes(ctx)},
		},
	}, nil
}

// cleanSpec sets default values in ranges for the DescribeSchedule response.
func cleanSpec(spec *schedulepb.ScheduleSpec) {
	cleanRanges := func(ranges []*schedulepb.Range) {
		for _, r := range ranges {
			if r.End < r.Start {
				r.End = r.Start
			}
			if r.Step == 0 {
				r.Step = 1
			}
		}
	}
	cleanCal := func(structured *schedulepb.StructuredCalendarSpec) {
		cleanRanges(structured.Second)
		cleanRanges(structured.Minute)
		cleanRanges(structured.Hour)
		cleanRanges(structured.DayOfMonth)
		cleanRanges(structured.Month)
		cleanRanges(structured.Year)
		cleanRanges(structured.DayOfWeek)
	}
	for _, structured := range spec.StructuredCalendar {
		cleanCal(structured)
	}
	for _, structured := range spec.ExcludeStructuredCalendar {
		cleanCal(structured)
	}
}

// ListMatchingTimes returns the upcoming times that the schedule will trigger
// within the given time range.
func (s *Scheduler) ListMatchingTimes(
	ctx chasm.Context,
	req *schedulerpb.ListScheduleMatchingTimesRequest,
	specBuilder *scheduler.SpecBuilder,
) (*schedulerpb.ListScheduleMatchingTimesResponse, error) {
	if s.Sentinel {
		return nil, ErrSentinel
	}
	if s.Closed {
		return nil, ErrClosed
	}

	frontendReq := req.FrontendRequest
	if frontendReq == nil || frontendReq.StartTime == nil || frontendReq.EndTime == nil {
		return nil, ErrInvalidQuery
	}

	cspec, err := s.getCompiledSpec(specBuilder)
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("invalid schedule: %v", err)
	}

	var out []*timestamppb.Timestamp
	t1 := timestamp.TimeValue(frontendReq.StartTime)
	for range maxListMatchingTimesCount {
		t1 = cspec.GetNextTime(s.jitterSeed(), t1).Next
		if t1.IsZero() || t1.After(timestamp.TimeValue(frontendReq.EndTime)) {
			break
		}
		out = append(out, timestamppb.New(t1))
	}

	return &schedulerpb.ListScheduleMatchingTimesResponse{
		FrontendResponse: &workflowservice.ListScheduleMatchingTimesResponse{
			StartTime: out,
		},
	}, nil
}

// Delete marks the Scheduler as closed without an idle timer.
func (s *Scheduler) Delete(
	ctx chasm.MutableContext,
	req *schedulerpb.DeleteScheduleRequest,
) (*schedulerpb.DeleteScheduleResponse, error) {
	if s.Sentinel {
		return nil, ErrSentinel
	}
	s.Closed = true
	return &schedulerpb.DeleteScheduleResponse{
		FrontendResponse: &workflowservice.DeleteScheduleResponse{},
	}, nil
}

// Update replaces the schedule with a new one for UpdateSchedule requests.
func (s *Scheduler) Update(
	ctx chasm.MutableContext,
	req *schedulerpb.UpdateScheduleRequest,
) (*schedulerpb.UpdateScheduleResponse, error) {
	if s.Sentinel {
		return nil, ErrSentinel
	}
	if !s.validateConflictToken(req.FrontendRequest.ConflictToken) {
		return nil, ErrConflictTokenMismatch
	}

	// Update custom search attributes.
	//
	// TODO - we could also easily support allowing the customer to update their
	// memo here.
	if req.FrontendRequest.GetSearchAttributes() != nil {
		// To preserve compatibility with V1 scheduler, we do a full replacement
		// of search attributes, dropping any that aren't a part of the update's
		// `CustomSearchAttributes` map. Search attribute replacement is ignored entirely
		// when that map is unset, however, an allocated yet empty map will clear all
		// attributes.

		// Preserve the old custom memo in the new Visibility component.
		oldVisibility := s.Visibility.Get(ctx)
		oldMemo := oldVisibility.CustomMemo(ctx)

		visibility := chasm.NewVisibilityWithData(ctx, req.FrontendRequest.GetSearchAttributes().GetIndexedFields(), oldMemo)
		s.Visibility = chasm.NewComponentField(ctx, visibility)
	}

	s.Schedule = req.FrontendRequest.Schedule
	s.setNullableFields()
	s.Info.UpdateTime = timestamppb.New(ctx.Now(s))
	s.updateConflictToken()

	// Since the spec may have been updated, kick off the generator.
	generator := s.Generator.Get(ctx)
	generator.Generate(ctx)

	return &schedulerpb.UpdateScheduleResponse{
		FrontendResponse: &workflowservice.UpdateScheduleResponse{},
	}, nil
}

// Patch applies a patch to the schedule for PatchSchedule requests.
func (s *Scheduler) Patch(
	ctx chasm.MutableContext,
	req *schedulerpb.PatchScheduleRequest,
) (*schedulerpb.PatchScheduleResponse, error) {
	if s.Sentinel {
		return nil, ErrSentinel
	}
	// Handle paused status.
	if req.FrontendRequest.Patch.Pause != "" {
		s.Schedule.State.Paused = true
		s.Schedule.State.Notes = req.FrontendRequest.Patch.Pause
	}
	if req.FrontendRequest.Patch.Unpause != "" {
		s.Schedule.State.Paused = false
		s.Schedule.State.Notes = req.FrontendRequest.Patch.Unpause
	}

	if err := s.handlePatch(ctx, req.FrontendRequest.Patch); err != nil {
		return nil, err
	}

	s.Info.UpdateTime = timestamppb.New(ctx.Now(s))
	s.updateConflictToken()

	return &schedulerpb.PatchScheduleResponse{
		FrontendResponse: &workflowservice.PatchScheduleResponse{},
	}, nil
}

func (s *Scheduler) generateConflictToken() []byte {
	return serializeConflictToken(s.ConflictToken)
}

func (s *Scheduler) validateConflictToken(token []byte) bool {
	// When unset in mutate requests, the schedule should update unconditionally.
	if token == nil {
		return true
	}

	current := s.generateConflictToken()
	return bytes.Equal(current, token)
}

func (s *Scheduler) executionStatus() string {
	if s.Closed {
		return executionStatusCompleted
	}
	return executionStatusRunning
}

// SearchAttributes returns the Temporal-managed key values for visibility.
func (s *Scheduler) SearchAttributes(chasm.Context) []chasm.SearchAttributeKeyValue {
	if s.Sentinel {
		return []chasm.SearchAttributeKeyValue{
			executionStatusSearchAttribute.Value(s.executionStatus()),
		}
	}
	return []chasm.SearchAttributeKeyValue{
		executionStatusSearchAttribute.Value(s.executionStatus()),
		chasm.SearchAttributeTemporalSchedulePaused.Value(s.Schedule.GetState().GetPaused()),
	}
}

// Memo returns the scheduler's info block for visibility.
func (s *Scheduler) Memo(
	ctx chasm.Context,
) proto.Message {
	if s.Sentinel {
		return nil
	}
	return s.ListInfo(ctx)
}

// ListInfo returns the ScheduleListInfo, used as the visibility memo, and to
// answer List queries.
func (s *Scheduler) ListInfo(
	ctx chasm.Context,
) *schedulepb.ScheduleListInfo {
	spec := common.CloneProto(s.Schedule.Spec)

	// Clear fields that are too large/not useful for the list view.
	spec.TimezoneData = nil

	// Limit the number of specs and exclusions stored on the memo.
	spec.ExcludeStructuredCalendar = util.SliceHead(spec.ExcludeStructuredCalendar, listInfoSpecFieldLimit)
	spec.Interval = util.SliceHead(spec.Interval, listInfoSpecFieldLimit)
	spec.StructuredCalendar = util.SliceHead(spec.StructuredCalendar, listInfoSpecFieldLimit)

	generator := s.Generator.Get(ctx)
	invoker := s.Invoker.Get(ctx)

	return &schedulepb.ScheduleListInfo{
		Spec:              spec,
		WorkflowType:      s.Schedule.Action.GetStartWorkflow().GetWorkflowType(),
		Notes:             s.Schedule.State.Notes,
		Paused:            s.Schedule.State.Paused,
		RecentActions:     invoker.recentActions(),
		FutureActionTimes: generator.FutureActionTimes,
	}
}

// startWorkflowSearchAttributes returns the search attributes to be applied to
// workflows kicked off. Includes custom search attributes and Temporal-managed.
func (s *Scheduler) startWorkflowSearchAttributes(
	nominal time.Time,
) *commonpb.SearchAttributes {
	attributes := s.Schedule.GetAction().GetStartWorkflow().GetSearchAttributes()

	fields := util.CloneMapNonNil(attributes.GetIndexedFields())
	if p, err := payload.Encode(nominal); err == nil {
		fields[sadefs.TemporalScheduledStartTime] = p
	}
	if p, err := payload.Encode(s.ScheduleId); err == nil {
		fields[sadefs.TemporalScheduledById] = p
	}
	return &commonpb.SearchAttributes{
		IndexedFields: fields,
	}
}
