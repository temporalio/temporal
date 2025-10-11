package scheduler

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
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

	// Locally-cached state, invalidated whenever cacheConflictToken != ConflictToken.
	cacheConflictToken int64
	compiledSpec       *scheduler.CompiledSpec // compiledSpec is only ever replaced whole, not mutated.
}

const (
	// How many recent actions to keep on the Info.RecentActions list.
	recentActionCount = 10
)

var ErrConflictTokenMismatch = errors.New("conflict token mismatch")

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
				UpdateTime: timestamppb.New(zero),
			},
			Namespace:     namespace,
			NamespaceId:   namespaceID,
			ScheduleId:    scheduleID,
			ConflictToken: scheduler.InitialConflictToken,
		},
		cacheConflictToken: scheduler.InitialConflictToken,
		Backfillers:        make(chasm.Map[string, *Backfiller]),
	}
	sched.Info.CreateTime = timestamppb.New(ctx.Now(sched))

	invoker := NewInvoker(ctx, sched)
	generator := NewGenerator(ctx, sched, invoker)
	sched.Invoker = chasm.NewComponentField(ctx, invoker)
	sched.Generator = chasm.NewComponentField(ctx, generator)

	// Create backfillers to fulfill initialPatch.
	sched.handlePatch(ctx, patch)

	return sched
}

// handlePatch creates backfillers to fulfill the given patch request.
func (s *Scheduler) handlePatch(ctx chasm.MutableContext, patch *schedulepb.SchedulePatch) {
	if patch != nil {
		if patch.TriggerImmediately != nil {
			s.NewImmediateBackfiller(ctx, patch.TriggerImmediately)
		}

		for _, backfill := range patch.BackfillRequest {
			s.NewRangeBackfiller(ctx, backfill)
		}
	}
}

// Create initializes a new Scheduler for CreateSchedule requests.
func Create(
	ctx chasm.MutableContext,
	req *schedulerpb.CreateScheduleRequest,
) (*Scheduler, *schedulerpb.CreateScheduleResponse, error) {
	// TODO: namespace name should be resolved from namespace_id via namespace registry
	sched := NewScheduler(
		ctx,
		"", // TODO: should be namespace name
		req.NamespaceId,
		req.ScheduleId,
		req.Schedule,
		req.InitialPatch,
	)
	return sched, &schedulerpb.CreateScheduleResponse{
		ConflictToken: sched.ConflictToken,
	}, nil
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

// Describe returns the current state of the Scheduler for DescribeSchedule requests.
func (s *Scheduler) Describe(
	ctx chasm.Context,
	req *schedulerpb.DescribeScheduleRequest,
) (*schedulerpb.DescribeScheduleResponse, error) {
	return &schedulerpb.DescribeScheduleResponse{
		Schedule:      common.CloneProto(s.Schedule),
		Info:          common.CloneProto(s.Info),
		ConflictToken: s.ConflictToken,
		// TODO - memo and search_attributes are handled by visibility (separate PR)
	}, nil
}

// Delete marks the Scheduler as closed without an idle timer.
func (s *Scheduler) Delete(
	ctx chasm.MutableContext,
	req *schedulerpb.DeleteScheduleRequest,
) (*schedulerpb.DeleteScheduleResponse, error) {
	s.Closed = true
	return &schedulerpb.DeleteScheduleResponse{}, nil
}

// Update replaces the schedule with a new one for UpdateSchedule requests.
func (s *Scheduler) Update(
	ctx chasm.MutableContext,
	req *schedulerpb.UpdateScheduleRequest,
) (*schedulerpb.UpdateScheduleResponse, error) {
	if req.ConflictToken != s.ConflictToken {
		return nil, ErrConflictTokenMismatch
	}

	s.Schedule = common.CloneProto(req.Schedule)
	// TODO - also merge custom search attributes here

	s.Info.UpdateTime = timestamppb.New(ctx.Now(s))
	s.updateConflictToken()

	return &schedulerpb.UpdateScheduleResponse{
		ConflictToken: s.ConflictToken,
	}, nil
}

// Patch applies a patch to the schedule for PatchSchedule requests.
func (s *Scheduler) Patch(
	ctx chasm.MutableContext,
	req *schedulerpb.PatchScheduleRequest,
) (*schedulerpb.PatchScheduleResponse, error) {
	if req.ConflictToken != s.ConflictToken {
		return nil, ErrConflictTokenMismatch
	}

	// Handle paused status.
	// TODO - use SetPaused from visibility for this
	if req.Patch.Pause != "" {
		s.Schedule.State.Paused = true
		s.Schedule.State.Notes = req.Patch.Pause
	}
	if req.Patch.Unpause != "" {
		s.Schedule.State.Paused = false
		s.Schedule.State.Notes = req.Patch.Unpause
	}

	s.handlePatch(ctx, req.Patch)

	s.Info.UpdateTime = timestamppb.New(ctx.Now(s))
	s.updateConflictToken()

	return &schedulerpb.PatchScheduleResponse{
		ConflictToken: s.ConflictToken,
	}, nil
}
