package scheduler

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
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
	*schedulespb.SchedulerInternal

	Generator   chasm.Field[*Generator]
	Invoker     chasm.Field[*Invoker]
	Backfillers chasm.Map[string, *Backfiller] // Backfill ID => *Backfiller

	// Locally-cached state, invalidated whenever cacheConflictToken != ConflictToken.
	cacheConflictToken int64
	compiledSpec       *scheduler.CompiledSpec
}

const (
	// How many recent actions to keep on the Info.RecentActions list.
	recentActionCount = 10
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
		SchedulerInternal: &schedulespb.SchedulerInternal{
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
	generator := NewGenerator(ctx, sched, invoker)
	sched.Invoker = chasm.NewComponentField(ctx, invoker)
	sched.Generator = chasm.NewComponentField(ctx, generator)

	return sched
}

func (s *Scheduler) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
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

func (s Scheduler) jitterSeed() string {
	return fmt.Sprintf("%s-%s", s.NamespaceId, s.ScheduleId)
}

func (s Scheduler) identity() string {
	return fmt.Sprintf("temporal-scheduler-%s-%s", s.Namespace, s.ScheduleId)
}

func (s Scheduler) overlapPolicy() enumspb.ScheduleOverlapPolicy {
	policy := s.Schedule.Policies.OverlapPolicy
	if policy == enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED {
		policy = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	}
	return policy
}

func (s Scheduler) resolveOverlapPolicy(overlapPolicy enumspb.ScheduleOverlapPolicy) enumspb.ScheduleOverlapPolicy {
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
	OverlapSkipped      int64
	BufferDropped       int64
	MissedCatchupWindow int64
	Starts              []*schedulepb.ScheduleActionResult
}

// recordActionResult updates the Scheduler's customer-facing metadata with execution results.
func (s *Scheduler) recordActionResult(
	ctx chasm.MutableContext,
	result *schedulerActionResult,
) (struct{}, error) {
	s.Info.ActionCount += int64(len(result.Starts))
	s.Info.OverlapSkipped += result.OverlapSkipped
	s.Info.BufferDropped += result.BufferDropped
	s.Info.MissedCatchupWindow += result.MissedCatchupWindow

	if len(result.Starts) > 0 {
		s.Info.RecentActions = util.SliceTail(append(s.Info.RecentActions, result.Starts...), recentActionCount)
	}

	for _, start := range result.Starts {
		if start.StartWorkflowResult != nil {
			s.Info.RunningWorkflows = append(s.Info.RunningWorkflows, start.StartWorkflowResult)
		}
	}

	return struct{}{}, nil
}
