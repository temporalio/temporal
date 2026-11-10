package scheduler

import (
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file owns the Scheduler's lifecycle: when a schedule is considered done,
// and when it may close. Two features ask "does this schedule still have work?":
//
//   - Idle close (reconcileIdle, SchedulerIdleTaskHandler) asks whether the
//     schedule may start its IdleTime countdown, and whether it may close when
//     the countdown ends.
//   - Time skipping (IsExecutionSkippable) asks whether the virtual clock may
//     jump to the next timer without leaving internal work behind.
//
// They answer slightly different questions (time skipping must also wait for
// running workflows, idle close must not - see hasPendingInvokerWork), but they
// are built from the same predicates below so they can't drift apart on what
// "pending" means.

// hasPendingControlActions reports whether the Invoker still owes cancel or
// terminate calls.
func (i *Invoker) hasPendingControlActions() bool {
	return len(i.GetCancelWorkflows()) > 0 || len(i.GetTerminateWorkflows()) > 0
}

// hasUnstartedStarts reports whether any BufferedStart has not yet been
// recorded as started. This includes starts awaiting their first ProcessBuffer
// pass, starts deferred behind a running workflow (BUFFER_ONE/BUFFER_ALL), and
// starts backing off after a retryable StartWorkflowExecution failure.
func (i *Invoker) hasUnstartedStarts() bool {
	return i.bufferedStartsCount() > 0
}

// hasIncompleteStarts reports whether any BufferedStart has not yet completed:
// every unstarted start, plus every running workflow whose completion is tracked.
func (i *Invoker) hasIncompleteStarts() bool {
	for _, start := range i.GetBufferedStarts() {
		if start.GetCompleted() == nil {
			return true
		}
	}
	return false
}

// hasCaughtUp reports whether the Generator has processed the schedule's most
// recent update, so no spec change is waiting on a GeneratorTask.
func (g *Generator) hasCaughtUp(sched *Scheduler) bool {
	lastProcessedTime := g.GetLastProcessedTime()
	return lastProcessedTime != nil &&
		!lastProcessedTime.AsTime().Before(sched.Info.GetUpdateTime().AsTime())
}

// hasPendingSpecActions reports whether the spec will still produce actions,
// as of the Generator's last tick. GeneratorTask passes its freshly computed
// next wakeup to reconcileIdle instead; this is for callers outside the tick.
func (g *Generator) hasPendingSpecActions(sched *Scheduler) bool {
	return len(g.GetFutureActionTimes()) > 0 && sched.useScheduledAction(false)
}

// hasPendingInvokerWork reports whether the Invoker still owes work that must
// finish before the schedule may close: unstarted starts and pending
// cancels/terminates.
//
// Running workflows are deliberately not included. V1's RetentionTime anchored
// on the last action and closed regardless of running workflows, and a
// schedule's final workflow can run for far longer than IdleTime. Time
// skipping, which must not jump past a running workflow, uses
// hasIncompleteStarts instead.
func (s *Scheduler) hasPendingInvokerWork(ctx chasm.Context) bool {
	invoker := s.Invoker.Get(ctx)
	return invoker.hasPendingControlActions() || invoker.hasUnstartedStarts()
}

// hasOutstandingWork reports whether the schedule has any reason to stay open
// beyond its idle deadline. specPending is whether the spec will produce more
// actions (see hasPendingSpecActions).
func (s *Scheduler) hasOutstandingWork(ctx chasm.Context, specPending bool) bool {
	return s.isHeldOpen() || specPending || s.hasPendingInvokerWork(ctx)
}

// reconcileIdle is the single place that arms or clears the idle task. Call it
// after any mutation that can create or drain work: the GeneratorTask, Invoker
// state changes, and the idle task itself. It returns true if an idle task is
// armed afterwards (newly, or already pending at the same deadline).
//
//   - Held open (paused, backfilling) or the spec has more actions: not idle.
//     IdleCloseTime is cleared and no task is armed. Whatever ends that state
//     (Unpause, backfill completion, the next GeneratorTask) reconciles again.
//   - The Invoker still has work: the schedule is active right now, so the last
//     event time advances to now and an idle task is armed IdleTime from now
//     as a backstop. The backstop never closes a schedule with outstanding
//     work (SchedulerIdleTaskHandler.Execute re-checks and re-arms), so a drain
//     path that forgets to reconcile can delay a close, but can no longer drop
//     a run or leave the schedule stuck open with no task armed.
//   - Otherwise: idle. Arm the idle task at lastEventTime + IdleTime.
func (s *Scheduler) reconcileIdle(
	ctx chasm.MutableContext,
	metricsHandler metrics.Handler,
	idleTime time.Duration,
	specPending bool,
) bool {
	if s.Closed || s.IsSentinel() {
		// Sentinels arm their own fixed idle task (see NewSentinel).
		return false
	}
	if idleTime == 0 || s.isHeldOpen() || specPending {
		s.IdleCloseTime = nil
		return false
	}

	if s.hasPendingInvokerWork(ctx) {
		s.advanceLastEventTimeTo(ctx.Now(s))
	}
	s.armIdleTask(ctx, metricsHandler, s.idleDeadline(ctx, idleTime), idleTime)
	return true
}

// reconcileIdleFromContext reconciles using the IdleTime from the CHASM context
// and the Generator's last-known spec state. Used by callers outside the
// GeneratorTask, which don't hold the handler's Config.
func (s *Scheduler) reconcileIdleFromContext(ctx chasm.MutableContext) {
	s.reconcileIdle(
		ctx,
		newTaggedMetricsHandler(ctx.MetricsHandler(), s),
		tweakablesFromContext(ctx).IdleTime,
		s.Generator.Get(ctx).hasPendingSpecActions(s),
	)
}

// armIdleTask arms an idle task at deadline, unless one is already pending at
// exactly that deadline. Arming a duplicate would leave it in the component's
// PureTasks until it fired, because SchedulerIdleTaskHandler.Validate only
// invalidates tasks whose deadline has moved later.
func (s *Scheduler) armIdleTask(
	ctx chasm.MutableContext,
	metricsHandler metrics.Handler,
	deadline time.Time,
	idleTime time.Duration,
) {
	if s.GetIdleCloseTime().AsTime().Equal(deadline) {
		metricsHandler.Counter(metrics.ScheduleIdleTask.Name()).
			Record(1, metrics.OutcomeTag(outcomeSkipped), metrics.ReasonTag(idleAlreadyArmed))
		return
	}

	s.getOrCreateEventLog(ctx).LogEvent(ctx,
		fmt.Sprintf("scheduled idle task for %s", deadline.Format(time.RFC3339)))
	ctx.AddTask(s, chasm.TaskAttributes{
		ScheduledTime: deadline,
	}, &schedulerpb.SchedulerIdleTask{
		IdleTimeTotal: durationpb.New(idleTime),
	})
	// Surfaced as the ScheduleIdleCloseTime search attribute for stuck-schedule
	// detection.
	s.IdleCloseTime = timestamppb.New(deadline)
}
