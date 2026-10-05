package scheduler_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// This file implements the "stuckness" invariant for Schedules/V2.
//
// Production has repeatedly produced schedules that are open, unpaused, and
// carry no pending work: they never re-arm and never close. Every confirmed
// instance violated exactly one predicate:
//
//	  !sched.Closed  =>  the tree carries at least one live logical task.
//
// Note that this is a liveness property, not a transition-validity property.
// The scheduler has no state enum to validate transitions against:
// Scheduler.LifecycleState is derived and two-valued (Completed iff Closed),
// and every sub-component hardcodes LifecycleStateRunning. In each known bug
// every transition taken was legal; the failure was that no transition
// happened at all.
//
// IMPORTANT: nothing here may touch live components. Materializing a chasm
// Field (e.g. sched.Invoker.Get(ctx)) escalates the root node's valueState and
// marks the tree dirty even for a read, which is itself one of the fault
// mechanisms behind the production bugs. So this check reads only:
//   - plain Go field access on the embedded *schedulerpb.SchedulerState proto
//   - the serialized snapshot returned by Node.Snapshot
// It never constructs a chasm.Context.

// schedState is a test-only vocabulary for the scheduler's lifecycle. It is
// derived, never persisted: there is deliberately no state enum on the
// component, and adding one is a proto change with V1->V2 migration and
// rollback implications that this check does not require.
type schedState string

const (
	// stateSentinel is a schedule-ID reservation. Inert by design.
	stateSentinel schedState = "sentinel"
	// stateMigrating is mid-migration to a workflow-backed (V1) scheduler,
	// driven by SchedulerMigrateToWorkflowTask.
	stateMigrating schedState = "migrating"
	// stateCompleted is terminal: Closed is set.
	stateCompleted schedState = "completed"
	// stateIdle is non-terminal and quiescent, awaiting its idle timer.
	stateIdle schedState = "idle"
	// stateRunning is non-terminal with pending work armed.
	stateRunning schedState = "running"
	// stateStuck is the bug: non-terminal, but nothing will ever wake it.
	stateStuck schedState = "stuck"
)

// liveTaskCount counts logical tasks across every node in the tree.
//
// This deliberately counts *logical* tasks, from ChasmComponentAttributes.
// PureTasks and SideEffectTasks, rather than physical tasks. A physical task
// can exist on the backend while the logical task it points at has been reaped
// or invalidated, so counting physical tasks reports a schedule as armed when
// it is in fact stuck. Side-effect tasks count because some valid states, such
// as migration callback attachment, rely on one before arming pure tasks.
// (chasmtest.Engine.Tasks and testEnv.NodeBackend.TasksByCategory both expose
// physical tasks, and are the wrong source for this check.)
//
// node must be the root and the tree must be clean; Node.Snapshot panics
// otherwise. Both hold immediately after a successful CloseTransaction.
func liveTaskCount(node *chasm.Node) (int, map[string]int) {
	byPath := make(map[string]int)
	total := 0
	for path, n := range node.Snapshot(nil).Nodes {
		attrs := n.GetMetadata().GetComponentAttributes()
		taskCount := len(attrs.GetPureTasks()) + len(attrs.GetSideEffectTasks())
		if taskCount == 0 {
			continue
		}
		byPath[path] = taskCount
		total += taskCount
	}
	return total, byPath
}

// derivedState computes the scheduler's lifecycle state from the tuple that
// actually represents it: (Sentinel, WorkflowMigration, Closed, IdleCloseTime,
// live logical task count).
func derivedState(sched *scheduler.Scheduler, liveTasks int) schedState {
	switch {
	case sched.GetSentinel():
		return stateSentinel
	case sched.GetWorkflowMigration() != nil:
		return stateMigrating
	case sched.GetClosed():
		return stateCompleted
	case liveTasks == 0:
		return stateStuck
	case sched.GetIdleCloseTime() != nil:
		return stateIdle
	default:
		return stateRunning
	}
}

// idleTaskCount counts armed SchedulerIdleTasks across the tree, resolving the
// task's registered type ID rather than matching on node path (the root
// Scheduler component also carries callback and migration tasks).
func idleTaskCount(registry *chasm.Registry, node *chasm.Node) int {
	wantTypeID, ok := registry.TaskIDFor(&schedulerpb.SchedulerIdleTask{})
	if !ok {
		return -1
	}
	count := 0
	for _, n := range node.Snapshot(nil).Nodes {
		for _, task := range n.GetMetadata().GetComponentAttributes().GetPureTasks() {
			if task.GetTypeId() == wantTypeID {
				count++
			}
		}
	}
	return count
}

// requireIdleCloseTimeBacked asserts that a declared idle deadline is actually
// backed by an armed idle task.
//
// IdleCloseTime is surfaced as the ScheduleIdleCloseTime search attribute and
// is what the production stuck-schedule scanner reads. When
// SchedulerIdleTaskHandler.Validate drops a task as expiration_shift and
// nothing arms a replacement, IdleCloseTime is left behind: the schedule never
// closes, and the very signal built to detect that reports a close time that
// will never arrive. That combination is always wrong.
//
// Only asserted in this direction. Sentinels legitimately arm an idle task
// without setting IdleCloseTime (see NewSentinel), so an armed task with no
// declared deadline is not a defect.
func requireIdleCloseTimeBacked(t *testing.T, registry *chasm.Registry, node *chasm.Node, sched *scheduler.Scheduler) {
	t.Helper()

	if sched.GetSentinel() || sched.GetClosed() || sched.GetIdleCloseTime() == nil {
		return
	}
	total, byPath := liveTaskCount(node)
	require.NotZero(t, idleTaskCount(registry, node),
		"IdleCloseTime is set to %v but no SchedulerIdleTask is armed, so the schedule will "+
			"never close and ScheduleIdleCloseTime reports a deadline that will never arrive: %s",
		sched.GetIdleCloseTime().AsTime(), describeSched(sched, total, byPath))
}

// requireNotStuck asserts the stuckness invariant against a clean tree.
func requireNotStuck(t *testing.T, node *chasm.Node, sched *scheduler.Scheduler) {
	t.Helper()

	total, byPath := liveTaskCount(node)
	state := derivedState(sched, total)
	require.NotEqual(t, stateStuck, state, "scheduler is stuck: %s", describeSched(sched, total, byPath))
}

func requireValidSchedulerState(
	t *testing.T,
	registry *chasm.Registry,
	node *chasm.Node,
	root chasm.RootComponent,
) {
	t.Helper()

	sched, ok := root.(*scheduler.Scheduler)
	require.Truef(t, ok, "scheduler test engine root has unexpected type %T", root)
	requireNotStuck(t, node, sched)
	requireIdleCloseTimeBacked(t, registry, node, sched)
}

// describeSched renders the full state tuple for a failure message, so a
// triage does not require re-running under a debugger.
func describeSched(sched *scheduler.Scheduler, total int, byPath map[string]int) string {
	paths := make([]string, 0, len(byPath))
	for p := range byPath {
		paths = append(paths, fmt.Sprintf("%s=%d", p, byPath[p]))
	}
	slices.Sort(paths)

	var b strings.Builder
	fmt.Fprintf(&b, "\n  scheduleID:     %s", sched.GetScheduleId())
	fmt.Fprintf(&b, "\n  closed:         %v", sched.GetClosed())
	fmt.Fprintf(&b, "\n  sentinel:       %v", sched.GetSentinel())
	fmt.Fprintf(&b, "\n  paused:         %v", sched.GetSchedule().GetState().GetPaused())
	fmt.Fprintf(&b, "\n  limitedActions: %v", sched.GetSchedule().GetState().GetLimitedActions())
	fmt.Fprintf(&b, "\n  remaining:      %d", sched.GetSchedule().GetState().GetRemainingActions())
	fmt.Fprintf(&b, "\n  conflictToken:  %d", sched.GetConflictToken())
	fmt.Fprintf(&b, "\n  idleCloseTime:  %v", sched.GetIdleCloseTime().AsTime())
	fmt.Fprintf(&b, "\n  migration:      %v", sched.GetWorkflowMigration() != nil)
	fmt.Fprintf(&b, "\n  liveTasks:      %d [%s]", total, strings.Join(paths, " "))
	return b.String()
}

// TestDerivedState pins the classification tuple. Without this, a refactor
// that made derivedState never return stateStuck would silently disarm every
// stuckness assertion in the package while leaving the suite green.
func TestDerivedState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		state     *schedulerpb.SchedulerState
		liveTasks int
		want      schedState
	}{
		{
			name:      "open with an armed task is running",
			state:     &schedulerpb.SchedulerState{},
			liveTasks: 1,
			want:      stateRunning,
		},
		{
			name:      "open with an armed task and an idle deadline is idle",
			state:     &schedulerpb.SchedulerState{IdleCloseTime: timestamppb.New(time.Unix(100, 0))},
			liveTasks: 1,
			want:      stateIdle,
		},
		{
			name:      "open with no armed task is stuck",
			state:     &schedulerpb.SchedulerState{},
			liveTasks: 0,
			want:      stateStuck,
		},
		{
			name:      "an idle deadline does not excuse a missing task",
			state:     &schedulerpb.SchedulerState{IdleCloseTime: timestamppb.New(time.Unix(100, 0))},
			liveTasks: 0,
			want:      stateStuck,
		},
		{
			name:      "closed with no armed task is terminal, not stuck",
			state:     &schedulerpb.SchedulerState{Closed: true},
			liveTasks: 0,
			want:      stateCompleted,
		},
		{
			name:      "sentinel is inert by design",
			state:     &schedulerpb.SchedulerState{Sentinel: true},
			liveTasks: 0,
			want:      stateSentinel,
		},
		{
			name:      "mid-migration is excused",
			state:     &schedulerpb.SchedulerState{WorkflowMigration: &schedulerpb.WorkflowMigrationState{}},
			liveTasks: 0,
			want:      stateMigrating,
		},
		{
			name:      "closed takes precedence over a stale idle deadline",
			state:     &schedulerpb.SchedulerState{Closed: true, IdleCloseTime: timestamppb.New(time.Unix(100, 0))},
			liveTasks: 0,
			want:      stateCompleted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sched := &scheduler.Scheduler{SchedulerState: tc.state}
			require.Equal(t, tc.want, derivedState(sched, tc.liveTasks))
		})
	}
}
