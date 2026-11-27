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

// Stuckness invariant: !sched.Closed => the tree carries at least one live logical task.
// Reads only the proto and Node.Snapshot, since materializing a chasm Field marks the tree dirty even for a read.

// schedState is derived, never persisted; the component deliberately has no state enum.
type schedState string

const (
	// A schedule-ID reservation; inert by design.
	stateSentinel  schedState = "sentinel"
	stateMigrating schedState = "migrating"
	stateCompleted schedState = "completed"
	stateIdle      schedState = "idle"
	stateRunning   schedState = "running"
	// Non-terminal, but nothing will ever wake it.
	stateStuck schedState = "stuck"
)

// liveTaskCount counts logical, not physical, tasks: a physical task can outlive the logical task it points at.
// Requires the root node and a clean tree; Node.Snapshot panics otherwise.
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

// Matches on task type ID, not node path: the root also carries callback and migration tasks.
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

// A declared IdleCloseTime with no armed idle task never closes. The converse is fine: sentinels arm an idle task without IdleCloseTime.
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

// Guards against derivedState never returning stateStuck, which would silently disarm every stuckness check.
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
