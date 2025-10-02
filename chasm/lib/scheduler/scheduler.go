package scheduler

import (
	"fmt"
	"slices"
	"strings"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
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

	// Last success/failure payloads, stored on this separate data node
	// to minimize write traffic.
	LastCompletionState chasm.Field[*schedulerpb.LastCompletionState]

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
		cacheConflictToken:  scheduler.InitialConflictToken,
		Backfillers:         make(chasm.Map[string, *Backfiller]),
		LastCompletionState: chasm.NewDataField(ctx, &schedulerpb.LastCompletionState{}),
	}

	invoker := NewInvoker(ctx, sched)
	generator := NewGenerator(ctx, sched, invoker)
	sched.Invoker = chasm.NewComponentField(ctx, invoker)
	sched.Generator = chasm.NewComponentField(ctx, generator)

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

// GetWorkflowID returns the Workflow ID given as part of the request spec.
// During start generation, nominal time is suffixed to this ID.
func (s *Scheduler) GetWorkflowID() string {
	return s.Schedule.Action.GetStartWorkflow().WorkflowId
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
	invoker, err := s.Invoker.Get(ctx)
	if err != nil {
		return err
	}

	workflowID := invoker.GetWorkflowID(info.RequestId)
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
		wfStatus = executionStatusFromFailure(outcome.Failure)
		s.LastCompletionState = chasm.NewDataField(ctx, &schedulerpb.LastCompletionState{
			Outcome: &schedulerpb.LastCompletionState_Failure{
				Failure: outcome.Failure,
			},
		})
	case *persistencespb.ChasmNexusCompletion_Success:
		wfStatus = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
		s.LastCompletionState = chasm.NewDataField(ctx, &schedulerpb.LastCompletionState{
			Outcome: &schedulerpb.LastCompletionState_Success{
				Success: outcome.Success,
			},
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

	// Record the completed action in the Invoker to queue off any
	// sequentially-buffered starts. This also updates BufferedStarts.
	scheduleTime := invoker.recordCompletedAction(ctx, info.CloseTime.AsTime(), info.RequestId)

	// Record the completed action into Scheduler's metadata. This updates
	// RecentActions and RunningWorkflows.
	s.recordCompletedAction(ctx, scheduleTime, workflowID, wfStatus)

	return nil
}

// recordCompletedAction ensures that the given action is recorded in
// RecentActions and cleaned up from other state.
func (s *Scheduler) recordCompletedAction(
	ctx chasm.MutableContext,
	scheduleTime time.Time,
	workflowID string,
	workflowStatus enumspb.WorkflowExecutionStatus,
) {
	// Clear out closed workflows from RunningWorkflows.
	s.Info.RunningWorkflows = slices.DeleteFunc(s.Info.RunningWorkflows, func(wf *commonpb.WorkflowExecution) bool {
		// We don't evaluate RunId here, since the action may have retried.
		return wf.WorkflowId == workflowID
	})

	// Update the RecentActions entry's status.
	found := false
	for _, action := range s.Info.RecentActions {
		if action.StartWorkflowResult.WorkflowId == workflowID {
			action.StartWorkflowStatus = workflowStatus
			found = true
			break
		}
	}

	// If we didn't find an entry in RecentActions, add one.
	if !found {
		if scheduleTime.IsZero() {
			// We're completing a workflow that wasn't in BufferedStarts, RunningWorkflows,
			// or RecentActions, but *did* have a request ID entry. That shouldn't be possible.
			//
			// TODO - softassert here when we have a logger wired into CHASM. Skip recording
			// the action for now.
			return
		}

		actionResult := &schedulepb.ScheduleActionResult{
			ScheduleTime: timestamppb.New(scheduleTime),
			ActualTime:   timestamppb.New(scheduleTime), // best guess, as we're recording complete before start was recorded
			StartWorkflowResult: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
			},
		}

		s.Info.RecentActions = util.SliceTail(
			append(s.Info.RecentActions, actionResult), recentActionCount)
	}
}

// isActionCompleted returns true for when the given action has already been
// recorded as complete in the Scheduler's state. Side effect tasks should use
// this function to determine if a started action was marked as completed before
// the action was even marked as started.
func (s *Scheduler) isActionCompleted(workflowID string) bool {
	for _, action := range s.Info.RecentActions {
		if action.StartWorkflowResult.WorkflowId == workflowID {
			return true
		}
	}

	// A workflow could have completed and subsequently truncated from RecentActions,
	// but we don't care about that case.
	return false
}
