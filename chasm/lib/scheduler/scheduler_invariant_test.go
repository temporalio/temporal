package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"pgregory.net/rapid"
)

// taskSummary categorizes the pending CHASM tasks that the most recent
// CloseTransaction pushed to the test NodeBackend. Pure tasks (timers like
// GeneratorTask, SchedulerIdleTask, BackfillerTask, InvokerProcessBufferTask)
// and side-effect tasks (InvokerExecuteTask) live in different queue
// categories; both count toward "this scheduler is making progress."
type taskSummary struct {
	Pure       []*tasks.ChasmTaskPure
	SideEffect []*tasks.ChasmTask
}

func (s taskSummary) Total() int { return len(s.Pure) + len(s.SideEffect) }

// pendingTaskSummary enumerates every CHASM task currently sitting on
// env.NodeBackend.TasksByCategory — the same storage existing tests inspect
// via env.HasTask, but enumerated rather than probed for a specific type.
//
// Note: this only sees tasks at the persistence boundary (post-CloseTransaction).
// Tasks added mid-transaction but not yet flushed are NOT visible here.
func pendingTaskSummary(env *testEnv) taskSummary {
	var s taskSummary
	for _, ts := range env.NodeBackend.TasksByCategory {
		for _, t := range ts {
			switch tt := t.(type) {
			case *tasks.ChasmTaskPure:
				s.Pure = append(s.Pure, tt)
			case *tasks.ChasmTask:
				s.SideEffect = append(s.SideEffect, tt)
			}
		}
	}
	return s
}

// TestPendingTaskSummary_DefaultScheduler confirms the helper sees the
// initial tasks (GeneratorTask + SchedulerIdleTask, per scheduler.go ctor)
// kicked when a default scheduler is created via the existing newTestEnv.
func TestPendingTaskSummary_DefaultScheduler(t *testing.T) {
	env := newTestEnv(t)
	// newTestEnv already calls CloseTransaction, so tasks are flushed.
	s := pendingTaskSummary(env)

	t.Logf("default scheduler tasks: pure=%d sideEffect=%d", len(s.Pure), len(s.SideEffect))
	require.Greater(t, s.Total(), 0, "default scheduler must have at least one pending task")
	require.Greater(t, len(s.Pure), 0, "expected at least one pure task (Generator/Idle)")
}

// --- rapid integration ---

// propertyT is the subset of testing.T that *rapid.T also satisfies, sufficient
// to construct the scheduler test fixtures from a rapid property function.
//
// *rapid.T notably lacks testing.T's Cleanup / TempDir methods, which is why
// newTestEnv (typed against *testing.T) can't be used directly inside
// rapid.Check; this looser interface lets newPropertyEnv accept either.
type propertyT interface {
	testlogger.TestingT
	Errorf(format string, args ...any)
}

// newPropertyEnv mirrors newTestEnv (helper_test.go) but takes the looser
// propertyT so it can be called from a rapid.Check property function.
func newPropertyEnv(tb propertyT, schedule *schedulepb.Schedule) *testEnv {
	ctrl := gomock.NewController(tb)
	logger := testlogger.NewTestLogger(tb, testlogger.FailOnExpectedErrorOnly)
	specProcessor := newRealSpecProcessor(ctrl, logger)

	registry := chasm.NewRegistry(logger)
	if err := registry.Register(&chasm.CoreLibrary{}); err != nil {
		tb.Fatalf("register core lib: %v", err)
	}
	if err := registry.Register(newTestLibrary(logger, specProcessor)); err != nil {
		tb.Fatalf("register scheduler lib: %v", err)
	}

	timeSource := clock.NewEventTimeSource()
	now := time.Now()
	timeSource.Update(now)

	tv := testvars.New(tb)
	nodeBackend := &chasm.MockNodeBackend{
		HandleNextTransitionCount: func() int64 { return 2 },
		HandleGetCurrentVersion:   func() int64 { return 1 },
		HandleGetWorkflowKey:      tv.Any().WorkflowKey,
		HandleIsWorkflow:          func() bool { return false },
		HandleCurrentVersionedTransition: func() *persistencespb.VersionedTransition {
			return &persistencespb.VersionedTransition{
				NamespaceFailoverVersion: 1,
				TransitionCount:          1,
			}
		},
	}
	node := chasm.NewEmptyTree(registry, timeSource, nodeBackend, chasm.DefaultPathEncoder, logger, metrics.NoopMetricsHandler)
	ctx := chasm.NewMutableContext(context.Background(), node)
	sched, err := scheduler.NewScheduler(ctx, namespace, namespaceID, scheduleID, schedule, nil)
	if err != nil {
		tb.Fatalf("new scheduler: %v", err)
	}
	if err := node.SetRootComponent(sched); err != nil {
		tb.Fatalf("set root component: %v", err)
	}
	sched.Generator.Get(ctx).LastProcessedTime = timestamppb.New(now)
	if _, err := node.CloseTransaction(); err != nil {
		tb.Fatalf("close transaction: %v", err)
	}

	return &testEnv{
		Ctrl:          ctrl,
		Registry:      registry,
		Node:          node,
		NodeBackend:   nodeBackend,
		TimeSource:    timeSource,
		Scheduler:     sched,
		SpecProcessor: specProcessor,
		Logger:        logger,
	}
}

// scheduleForProperty draws a small valid Schedule: defaultSchedule with the
// paused flag and interval duration varied. Enough to exercise the wiring
// without yet generating an interesting input space.
func scheduleForProperty(rt *rapid.T) *schedulepb.Schedule {
	s := defaultSchedule()
	s.State.Paused = rapid.Bool().Draw(rt, "paused")
	intervalSec := rapid.IntRange(1, 3600).Draw(rt, "intervalSec")
	s.Spec.Interval[0].Interval = durationpb.New(time.Duration(intervalSec) * time.Second)
	return s
}

// TestSchedulerInvariant_AtCreation_RapidSmoke is the wiring proof: it shows
// rapid + pendingTaskSummary work end-to-end against the real scheduler
// fixture, over a small input space (paused flag, interval).
//
// Property under test:
//   freshly-created scheduler ⇒ at least one pending CHASM task.
//
// Both paused and unpaused schedulers add their initial Generator/Idle tasks
// at construction, so paused does NOT weaken this particular invariant.
// A real property test (the eventual goal) drives sequences of Update/Patch/
// Backfill/task-execution and re-asserts the open-and-unpaused form after
// each step — that's where the stuck-open bug would actually show up.
func TestSchedulerInvariant_AtCreation_RapidSmoke(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		schedule := scheduleForProperty(rt)
		env := newPropertyEnv(rt, schedule)
		s := pendingTaskSummary(env)
		if s.Total() == 0 {
			rt.Fatalf(
				"freshly-created scheduler has 0 pending tasks (paused=%v, interval=%v)",
				schedule.State.Paused,
				schedule.Spec.Interval[0].Interval.AsDuration(),
			)
		}
	})
}

// ---------------------------------------------------------------------------
// Live pure-task helper (tree introspection)
// ---------------------------------------------------------------------------
//
// pendingTaskSummary reads NodeBackend.TasksByCategory, which ACCUMULATES across
// CloseTransactions and reflects what the scheduler ever queued — not what's
// currently still pending. For the state machine we need the live picture:
// tasks that are still on the tree (have not yet executed and been removed).
//
// Node.EachPureTask iterates the tree's pending pure tasks. It filters by
// expiration (only fires the callback for tasks whose ScheduledTime <=
// referenceTime), so passing a far-future referenceTime enumerates everything.
// The callback returns (executed=false) so we don't accidentally trigger
// task cleanup.

type pureTaskEntry struct {
	Attrs chasm.TaskAttributes
	Task  any // deserialized payload, e.g. *schedulerpb.GeneratorTask
}

func (e pureTaskEntry) TypeName() string {
	if e.Task == nil {
		return "<nil>"
	}
	t := reflect.TypeOf(e.Task)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.String()
}

// livePureTasksOnTree returns every pure task currently sitting on any
// component node in the scheduler's CHASM tree, regardless of fire time.
func livePureTasksOnTree(env *testEnv) ([]pureTaskEntry, error) {
	var out []pureTaskEntry
	farFuture := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	err := env.Node.EachPureTask(farFuture, func(_ chasm.NodePureTask, attrs chasm.TaskAttributes, taskInstance any) (bool, error) {
		out = append(out, pureTaskEntry{Attrs: attrs, Task: taskInstance})
		return false, nil
	})
	return out, err
}

// summarizeLiveTasks returns a compact "type:count" map for error messages.
func summarizeLiveTasks(tasks []pureTaskEntry) map[string]int {
	out := map[string]int{}
	for _, t := range tasks {
		out[t.TypeName()]++
	}
	return out
}

// ---------------------------------------------------------------------------
// Schedule generator: more variety than scheduleForProperty
// ---------------------------------------------------------------------------

// scheduleForStateMachine draws a schedule with varied spec shape and state.
// We want to exercise the paths most likely to produce stuck-open: limited
// actions (analogue of the production one-shot), short intervals, paused.
func scheduleForStateMachine(rt *rapid.T) *schedulepb.Schedule {
	s := defaultSchedule()
	s.State.Paused = rapid.Bool().Draw(rt, "paused")

	intervalSec := rapid.IntRange(1, 600).Draw(rt, "intervalSec")
	s.Spec.Interval[0].Interval = durationpb.New(time.Duration(intervalSec) * time.Second)

	if rapid.Bool().Draw(rt, "limited") {
		s.State.LimitedActions = true
		s.State.RemainingActions = int64(rapid.IntRange(0, 3).Draw(rt, "remainingActions"))
	}
	return s
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

type schedulerMachine struct {
	rt    *rapid.T
	env   *testEnv
	clock time.Time // mirrors env.TimeSource for ops that need a reference
	ops   []string  // op history for diagnostics
}

func newSchedulerMachine(rt *rapid.T) *schedulerMachine {
	schedule := scheduleForStateMachine(rt)
	env := newPropertyEnv(rt, schedule)
	return &schedulerMachine{
		rt:    rt,
		env:   env,
		clock: env.TimeSource.Now(),
		ops:   []string{fmt.Sprintf("init(paused=%v, limited=%v, remaining=%d, intervalSec=%v)", schedule.State.Paused, schedule.State.LimitedActions, schedule.State.RemainingActions, schedule.Spec.Interval[0].Interval.AsDuration().Seconds())},
	}
}

func (m *schedulerMachine) record(op string) {
	m.ops = append(m.ops, op)
}

func (m *schedulerMachine) ctx() chasm.MutableContext {
	return chasm.NewMutableContext(context.Background(), m.env.Node)
}

func (m *schedulerMachine) closeTx() {
	if _, err := m.env.Node.CloseTransaction(); err != nil {
		m.rt.Fatalf("CloseTransaction failed after ops %v: %v", m.ops, err)
	}
}

// permittedMutationError returns true for errors the scheduler legitimately
// returns when an op is invalid in the current state. These don't count as
// bugs — they're just no-ops.
func permittedMutationError(err error) bool {
	return errors.Is(err, scheduler.ErrClosed) ||
		errors.Is(err, scheduler.ErrSentinel) ||
		errors.Is(err, scheduler.ErrMigrationPending)
}

func (m *schedulerMachine) pause() {
	m.record("Pause")
	_, err := m.env.Scheduler.Patch(m.ctx(), &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch:      &schedulepb.SchedulePatch{Pause: "rapid-pause"},
		},
	})
	if err != nil && !permittedMutationError(err) {
		m.rt.Fatalf("Pause failed: %v (ops=%v)", err, m.ops)
	}
	m.closeTx()
}

func (m *schedulerMachine) unpause() {
	m.record("Unpause")
	_, err := m.env.Scheduler.Patch(m.ctx(), &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch:      &schedulepb.SchedulePatch{Unpause: "rapid-unpause"},
		},
	})
	if err != nil && !permittedMutationError(err) {
		m.rt.Fatalf("Unpause failed: %v (ops=%v)", err, m.ops)
	}
	m.closeTx()
}

func (m *schedulerMachine) trigger() {
	m.record("Trigger")
	_, err := m.env.Scheduler.Patch(m.ctx(), &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				},
			},
		},
	})
	if err != nil && !permittedMutationError(err) {
		m.rt.Fatalf("Trigger failed: %v (ops=%v)", err, m.ops)
	}
	m.closeTx()
}

func (m *schedulerMachine) backfill() {
	hoursBack := rapid.IntRange(1, 240).Draw(m.rt, "backfillHoursBack")
	hoursLen := rapid.IntRange(1, 24).Draw(m.rt, "backfillHoursLen")
	start := m.clock.Add(-time.Duration(hoursBack) * time.Hour)
	end := start.Add(time.Duration(hoursLen) * time.Hour)
	m.record(fmt.Sprintf("Backfill(start=%s end=%s)", start, end))

	_, err := m.env.Scheduler.Patch(m.ctx(), &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				BackfillRequest: []*schedulepb.BackfillRequest{{
					StartTime:     timestamppb.New(start),
					EndTime:       timestamppb.New(end),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				}},
			},
		},
	})
	if err != nil && !permittedMutationError(err) {
		m.rt.Fatalf("Backfill failed: %v (ops=%v)", err, m.ops)
	}
	m.closeTx()
}

func (m *schedulerMachine) update() {
	newSchedule := scheduleForStateMachine(m.rt)
	m.record(fmt.Sprintf("Update(paused=%v, limited=%v, remaining=%d)", newSchedule.State.Paused, newSchedule.State.LimitedActions, newSchedule.State.RemainingActions))
	_, err := m.env.Scheduler.Update(m.ctx(), &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Schedule:   newSchedule,
			// ConflictToken nil => unconditional update.
		},
	})
	if err != nil && !permittedMutationError(err) {
		m.rt.Fatalf("Update failed: %v (ops=%v)", err, m.ops)
	}
	m.closeTx()
}

// completeAction simulates a workflow run completing (success or failure)
// by picking a BufferedStart and invoking RecordCompletedAction. This is the
// key path missing from a pure "API-only" property test: in production the
// stuck-open state happens AFTER a workflow completes and no follow-up
// task is scheduled.
func (m *schedulerMachine) completeAction() {
	ctx := m.ctx()
	invoker := m.env.Scheduler.Invoker.Get(ctx)

	var candidates []*schedulespb.BufferedStart
	for _, start := range invoker.BufferedStarts {
		if start.Completed == nil {
			candidates = append(candidates, start)
		}
	}
	if len(candidates) == 0 {
		m.record("CompleteAction(noop: no candidates)")
		return
	}

	idx := rapid.IntRange(0, len(candidates)-1).Draw(m.rt, "completeIdx")
	start := candidates[idx]

	// Pretend the workflow actually started so RecordCompletedAction sees a
	// "running" entry. The existing recordCompletedAction test does the same.
	if start.RunId == "" {
		start.RunId = "test-run-" + start.RequestId
		start.StartTime = timestamppb.New(m.clock)
	}

	success := rapid.Bool().Draw(m.rt, "completeSuccess")
	status := enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	if success {
		status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}
	m.record(fmt.Sprintf("CompleteAction(req=%s, status=%s)", start.RequestId, status))

	m.env.Scheduler.RecordCompletedAction(ctx, &schedulespb.CompletedResult{
		Status:    status,
		CloseTime: timestamppb.New(m.clock),
	}, start.RequestId)

	m.closeTx()
}

func (m *schedulerMachine) advanceTime() {
	sec := rapid.IntRange(1, 7200).Draw(m.rt, "advanceSec")
	m.clock = m.clock.Add(time.Duration(sec) * time.Second)
	m.record(fmt.Sprintf("AdvanceTime(+%ds)", sec))
	m.env.TimeSource.Update(m.clock)
	// CloseTransaction will run any pure tasks that just became expired.
	m.closeTx()
}

// invariantHolds asserts: open ∧ unpaused ⇒ ≥1 live pure task on the tree.
// Closed and paused are both legitimate "no tasks needed" terminal-ish states.
func (m *schedulerMachine) invariantHolds() {
	s := m.env.Scheduler
	if s.Closed {
		return
	}
	if s.Schedule.State.Paused {
		return
	}
	live, err := livePureTasksOnTree(m.env)
	if err != nil {
		m.rt.Fatalf("livePureTasksOnTree failed: %v (ops=%v)", err, m.ops)
	}
	if len(live) == 0 {
		backend := pendingTaskSummary(m.env)
		// Categorize backend tasks by category for diagnostics.
		byCat := map[string]int{}
		for cat, ts := range m.env.NodeBackend.TasksByCategory {
			byCat[cat.Name()] = len(ts)
		}
		m.rt.Fatalf(
			"INVARIANT VIOLATED: open unpaused scheduler has 0 live pure tasks on tree.\n"+
				"  ops=%v\n"+
				"  paused=%v closed=%v limited=%v remaining=%d\n"+
				"  cumulative backend tasks: pure=%d sideEffect=%d byCategory=%v\n"+
				"  buffered starts: %d  backfillers: %d",
			m.ops,
			s.Schedule.State.Paused,
			s.Closed,
			s.Schedule.State.LimitedActions,
			s.Schedule.State.RemainingActions,
			len(backend.Pure), len(backend.SideEffect), byCat,
			len(s.Invoker.Get(m.ctx()).BufferedStarts),
			len(s.Backfillers),
		)
	}
}

// TestSchedulerInvariant_TriggerOnExhaustedLimitedSchedule is the hand-written
// reproducer of the bug rapid found.
//
// Path (minimum from shrinker):
//   1. NewScheduler with LimitedActions=true, RemainingActions=0, intervalSec=1
//   2. CloseTransaction → initial GeneratorTask fires, sees no future wakeups,
//      schedules a SchedulerIdleTask. Tree now has: [SchedulerIdleTask].
//   3. ANY no-op MutableContext read of a sub-component (e.g.
//      `Invoker.Get(env.MutableContext())` with no mutation, no closeTx).
//   4. Patch{TriggerImmediately} → handlePatch creates a Backfiller whose
//      constructor adds an immediate BackfillerTask.
//   5. CloseTransaction.
//
// Observed end state:
//   - Live pure tasks on tree: 0 (SchedulerIdleTask gone, BackfillerTask never
//     fired and is not on the tree either)
//   - len(Backfillers) == 1 (Backfiller component exists but is stuck — its
//     immediate task never executed, so processTrigger never deleted it)
//   - len(Invoker.BufferedStarts) == 0 (Trigger produced no BufferedStart)
//   - Scheduler.Closed = false, State.Paused = false
//
// This matches the production failure shape: open, unpaused, no pure tasks.
// The intermediate no-op read in step 3 is LOAD-BEARING — without it, the
// SchedulerIdleTask survives Trigger and the invariant holds. That points
// at a CHASM transaction-internals interaction rather than pure scheduler
// logic. Suspect: the no-op MutableContext + Get() materializes the Invoker
// into n.value, and the subsequent CloseTransaction's task-validation pass
// runs in a path that invalidates the existing SchedulerIdleTask AND
// somehow skips the newly-added immediate BackfillerTask (whose component
// pointer may not be in n.valueToNode at executeImmediatePureTasks time —
// see chasm/tree.go:1535-1538).
func TestSchedulerInvariant_TriggerOnExhaustedLimitedSchedule(t *testing.T) {
	schedule := defaultSchedule()
	schedule.State.LimitedActions = true
	schedule.State.RemainingActions = 0
	schedule.Spec.Interval[0].Interval = durationpb.New(1 * time.Second)

	env := newPropertyEnv(t, schedule)

	before, _ := livePureTasksOnTree(env)
	require.Len(t, before, 1, "expected initial SchedulerIdleTask after exhausted-limited setup")
	t.Logf("before:    %v", summarizeLiveTasks(before))

	// LOAD-BEARING: this no-op read changes the outcome of the subsequent
	// Trigger. Comment it out and the invariant assertion below passes
	// (but the Backfiller's initial task is still silently dropped — see
	// project_scheduler_stuck_open_bug.md for the two-bug analysis).
	_ = env.Scheduler.Invoker.Get(env.MutableContext())

	_, err := env.Scheduler.Patch(env.MutableContext(), &schedulerpb.PatchScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.PatchScheduleRequest{
			Namespace:  namespace,
			ScheduleId: scheduleID,
			Patch: &schedulepb.SchedulePatch{
				TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	after, _ := livePureTasksOnTree(env)
	t.Logf("after:     %v", summarizeLiveTasks(after))
	t.Logf("backfillers=%d  bufferedStarts=%d  closed=%v paused=%v",
		len(env.Scheduler.Backfillers),
		len(env.Scheduler.Invoker.Get(env.ReadContext()).BufferedStarts),
		env.Scheduler.Closed,
		env.Scheduler.Schedule.State.Paused,
	)
	require.Greater(t, len(after), 0,
		"BUG: open+unpaused scheduler has 0 live pure tasks after Trigger; "+
			"Backfiller exists but its immediate task never fired, and the prior SchedulerIdleTask was dropped")
}

// TestSchedulerInvariant_StateMachine drives a Scheduler through a randomized
// sequence of API mutations and clock advances, asserting after every step
// that an open, unpaused scheduler always has at least one pending pure task
// on its CHASM tree. A failure means we found a path where the scheduler
// gets "stuck open" — alive but with nothing to do, mirroring the production
// failure shape (one-shot calendar, action completed, no follow-up task).
func TestSchedulerInvariant_StateMachine(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := newSchedulerMachine(rt)
		m.invariantHolds() // post-construction

		nOps := rapid.IntRange(1, 25).Draw(rt, "nOps")
		for i := 0; i < nOps; i++ {
			op := rapid.SampledFrom([]string{
				"pause", "unpause", "trigger", "backfill", "update", "advance", "complete",
			}).Draw(rt, fmt.Sprintf("op[%d]", i))
			switch op {
			case "pause":
				m.pause()
			case "unpause":
				m.unpause()
			case "trigger":
				m.trigger()
			case "backfill":
				m.backfill()
			case "update":
				m.update()
			case "advance":
				m.advanceTime()
			case "complete":
				m.completeAction()
			}
			m.invariantHolds()
		}
	})
}
