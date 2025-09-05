package scheduler

import (
	"bytes"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/worker/scheduler"
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

	Generator   chasm.Field[*Generator]
	Invoker     chasm.Field[*Invoker]
	Backfillers chasm.Map[string, *Backfiller] // Backfill ID => *Backfiller

	Visibility chasm.Field[*chasm.Visibility]

	// Locally-cached state, invalidated whenever cacheConflictToken != ConflictToken.
	cacheConflictToken int64
	compiledSpec       *scheduler.CompiledSpec // compiledSpec is only ever replaced whole, not mutated.
}

const (
	// How many recent actions to keep on the Info.RecentActions list.
	recentActionCount = 10

	// Item limit per spec field on the ScheduleInfo memo.
	listInfoSpecFieldLimit = 10

	// Field in which the schedule's memo is stored.
	visibilityMemoFieldInfo = "ScheduleInfo"
)

// NewScheduler returns an initialized CHASM scheduler root component.
func NewScheduler(
	ctx chasm.MutableContext,
	namespace, namespaceID, scheduleID string,
	input *schedulepb.Schedule,
	patch *schedulepb.SchedulePatch,
) *Scheduler {
	var zero time.Time

	sched := &Scheduler{
		SchedulerState: &schedulerpb.SchedulerState{
			Schedule: input,
			Info: &schedulepb.ScheduleInfo{
				CreateTime: timestamppb.Now(),
				UpdateTime: timestamppb.New(zero),
			},
			InitialPatch:  patch,
			Namespace:     namespace,
			NamespaceId:   namespaceID,
			ScheduleId:    scheduleID,
			ConflictToken: scheduler.InitialConflictToken,
		},
		cacheConflictToken: scheduler.InitialConflictToken,
		Backfillers:        make(chasm.Map[string, *Backfiller]),
	}

	invoker := NewInvoker(ctx, sched)
	sched.Invoker = chasm.NewComponentField(ctx, invoker)

	generator := NewGenerator(ctx, sched, invoker)
	sched.Generator = chasm.NewComponentField(ctx, generator)

	visibility := chasm.NewVisibility(ctx)
	sched.Visibility = chasm.NewComponentField(ctx, visibility)

	return sched
}

func (s *Scheduler) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	if s.Closed {
		return chasm.LifecycleStateCompleted
	}

	return chasm.LifecycleStateRunning
}

// NewRangeBackfiller returns an intialized Backfiller component, which should
// be parented under a Scheduler root node.
func (s *Scheduler) NewRangeBackfiller(
	ctx chasm.MutableContext,
	request *schedulepb.BackfillRequest,
) *Backfiller {
	backfiller := newBackfiller(ctx, s)
	backfiller.Request = &schedulerpb.BackfillerState_BackfillRequest{
		BackfillRequest: request,
	}
	s.addBackfiller(ctx, backfiller)
	return backfiller
}

// NewImmediateBackfiller returns an intialized Backfiller component, which should
// be parented under a Scheduler root node.
func (s *Scheduler) NewImmediateBackfiller(
	ctx chasm.MutableContext,
	request *schedulepb.TriggerImmediatelyRequest,
) *Backfiller {
	backfiller := newBackfiller(ctx, s)
	backfiller.Request = &schedulerpb.BackfillerState_TriggerRequest{
		TriggerRequest: request,
	}
	s.addBackfiller(ctx, backfiller)
	return backfiller
}

// addBackfiller adds the backfiller to the scheduler tree, and adds a task to
// kick off backfill processing.
func (s *Scheduler) addBackfiller(
	ctx chasm.MutableContext,
	backfiller *Backfiller,
) {
	s.Backfillers[backfiller.BackfillId] = chasm.NewComponentField(ctx, backfiller)
	ctx.AddTask(backfiller, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{})
}

// useScheduledAction returns true when the Scheduler should allow scheduled
// actions to be taken.
//
// When decrement is true, the schedule's state's `RemainingActions` counter is
// decremented when an action can be taken. When decrement is false, no state
// is mutated.
func (s *Scheduler) useScheduledAction(decrement bool) bool {
	scheduleState := s.Schedule.State

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

func (s *Scheduler) jitterSeed() string {
	return fmt.Sprintf("%s-%s", s.NamespaceId, s.ScheduleId)
}

func (s *Scheduler) identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.Namespace, s.ScheduleId)
}

func (s *Scheduler) overlapPolicy() enumspb.ScheduleOverlapPolicy {
	policy := s.Schedule.Policies.OverlapPolicy
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
func (s *Scheduler) getLastEventTime() time.Time {
	var lastEvent time.Time
	if len(s.Info.RecentActions) > 0 {
		lastEvent = s.Info.RecentActions[len(s.Info.RecentActions)-1].ActualTime.AsTime()
	}
	lastEvent = util.MaxTime(lastEvent, s.Info.CreateTime.AsTime())
	lastEvent = util.MaxTime(lastEvent, s.Info.UpdateTime.AsTime())
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

	return s.getLastEventTime().Add(idleTime), true
}

func (s *Scheduler) hasMoreAllowAllBackfills(ctx chasm.Context) bool {
	for _, field := range s.Backfillers {
		backfiller, err := field.Get(ctx)
		if err != nil {
			continue
		}

		var policy enumspb.ScheduleOverlapPolicy
		if backfiller.GetBackfillRequest() != nil {
			policy = backfiller.GetBackfillRequest().OverlapPolicy
		} else {
			policy = backfiller.GetTriggerRequest().OverlapPolicy
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
	starts              []*schedulepb.ScheduleActionResult
}

// recordActionResult updates the Scheduler's customer-facing metadata with execution results.
func (s *Scheduler) recordActionResult(result *schedulerActionResult) {
	s.Info.ActionCount += int64(len(result.starts))
	s.Info.OverlapSkipped += result.overlapSkipped
	s.Info.MissedCatchupWindow += result.missedCatchupWindow

	if len(result.starts) > 0 {
		s.Info.RecentActions = util.SliceTail(append(s.Info.RecentActions, result.starts...), recentActionCount)
	}

	for _, start := range result.starts {
		if start.StartWorkflowResult != nil {
			s.Info.RunningWorkflows = append(s.Info.RunningWorkflows, start.StartWorkflowResult)
		}
	}
}

// UpdateVisibility updates the schedule's visibility record, including memo
// and search attributes. customSearchAttributes is optional, and custom search
// attributes will be left as-is with it unset.
//
// See mergeCustomSearchAttributes for how custom search attributes are merged.
func (s *Scheduler) UpdateVisibility(
	ctx chasm.MutableContext,
	specProcessor SpecProcessor,
	customSearchAttributes *commonpb.SearchAttributes,
) error {
	needsTask := false // Set to true if we need to write anything to Visibility.
	visibility, err := s.Visibility.Get(ctx)
	if err != nil {
		return err
	}

	// Update the schedule's search attributes. This includes both Temporal-managed
	// fields (paused state), as well as upserts for customer-specified fields.
	upsertAttrs := make(map[string]any)
	currentAttrs, err := visibility.GetSearchAttributes(ctx)
	if err != nil {
		return err
	}

	// Only attempt to merge additional search attributes if any are given, otherwise
	// we'll unset existing attributes.
	if customSearchAttributes != nil &&
		len(customSearchAttributes.GetIndexedFields()) > 0 {
		mergeCustomSearchAttributes(
			currentAttrs,
			customSearchAttributes.GetIndexedFields(),
			upsertAttrs,
		)
	}

	// Update Paused status.
	var currentPaused bool
	currentPausedPayload, ok := currentAttrs[searchattribute.TemporalSchedulePaused]
	if ok {
		err = payload.Decode(currentPausedPayload, &currentPaused)
		if err != nil {
			return err
		}
	}
	if !ok || currentPaused != s.Schedule.State.Paused {
		upsertAttrs[searchattribute.TemporalSchedulePaused] = s.Schedule.State.Paused
	}

	if len(upsertAttrs) > 0 {
		err = visibility.UpsertSearchAttributes(ctx, upsertAttrs)
		if err != nil {
			return err
		}
		needsTask = true
	}

	newInfo, err := s.GetListInfo(ctx, specProcessor)
	if err != nil {
		return err
	}
	newInfoPayload, err := newInfo.Marshal()
	if err != nil {
		return err
	}
	currentMemo, err := visibility.GetMemo(ctx)
	if err != nil {
		return err
	}
	currentInfoPayload := currentMemo[visibilityMemoFieldInfo]

	// Update visibility if the memo is out-of-date or absent.
	if currentInfoPayload == nil ||
		!bytes.Equal(currentInfoPayload.Data, newInfoPayload) {
		newMemo := map[string]any{visibilityMemoFieldInfo: newInfoPayload}
		err = visibility.UpsertMemo(ctx, newMemo)
		if err != nil {
			return err
		}
		needsTask = true
	}

	if needsTask {
		visibility.GenerateTask(ctx)
	}

	return nil
}

// GetListInfo returns the ScheduleListInfo, used as the visibility memo, and to
// answer List queries.
func (s *Scheduler) GetListInfo(
	ctx chasm.Context,
	specProcessor SpecProcessor,
) (*schedulepb.ScheduleListInfo, error) {
	spec := common.CloneProto(s.Schedule.Spec)

	// Clear fields that are too large/not useful for the list view.
	spec.TimezoneData = nil

	// Limit the number of specs and exclusions stored on the memo.
	spec.ExcludeStructuredCalendar = util.SliceHead(spec.ExcludeStructuredCalendar, listInfoSpecFieldLimit)
	spec.Interval = util.SliceHead(spec.Interval, listInfoSpecFieldLimit)
	spec.StructuredCalendar = util.SliceHead(spec.StructuredCalendar, listInfoSpecFieldLimit)

	futureActionTimes, err := s.getFutureActionTimes(ctx, specProcessor)
	if err != nil {
		return nil, err
	}

	return &schedulepb.ScheduleListInfo{
		Spec:              spec,
		WorkflowType:      s.Schedule.Action.GetStartWorkflow().GetWorkflowType(),
		Notes:             s.Schedule.State.Notes,
		Paused:            s.Schedule.State.Paused,
		RecentActions:     util.SliceTail(s.Info.RecentActions, recentActionCount),
		FutureActionTimes: futureActionTimes,
	}, nil
}

// getFutureActionTimes returns up to min(`recentActionCount`, `RemainingActions`)
// future action times. Future action times that precede the schedule's UpdateTime
// are not included.
func (s *Scheduler) getFutureActionTimes(
	ctx chasm.Context,
	specProcessor SpecProcessor,
) ([]*timestamppb.Timestamp, error) {
	generator, err := s.Generator.Get(ctx)
	if err != nil {
		return nil, err
	}

	nextTime := func(t time.Time) (time.Time, error) {
		res, err := specProcessor.GetNextTime(s, t)
		return res.Next, err
	}

	count := recentActionCount
	if s.Schedule.State.LimitedActions {
		count = min(int(s.Schedule.State.RemainingActions), recentActionCount)
	}
	out := make([]*timestamppb.Timestamp, 0, count)
	t := timestamp.TimeValue(generator.LastProcessedTime)
	for len(out) < count {
		t, err = nextTime(t)
		if err != nil {
			return nil, err
		}
		if t.IsZero() {
			break
		}

		if s.Info.UpdateTime.AsTime().After(t) {
			// Skip action times whose nominal times are prior to the schedule's update time.
			continue
		}

		out = append(out, timestamppb.New(t))
	}

	return out, nil
}
