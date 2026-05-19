package scheduler_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// pureTaskEntry is one entry returned by livePureTasksOnTree.
type pureTaskEntry struct {
	Attrs chasm.TaskAttributes
	Task  any // deserialized payload, e.g. *schedulerpb.SchedulerIdleTask
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
// Reflects the LIVE pending set (tasks already executed are gone) — unlike
// env.NodeBackend.TasksByCategory which accumulates across CloseTransactions.
func livePureTasksOnTree(env *testEnv) []pureTaskEntry {
	var out []pureTaskEntry
	farFuture := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = env.Node.EachPureTask(farFuture, func(_ chasm.NodePureTask, attrs chasm.TaskAttributes, taskInstance any) (bool, error) {
		out = append(out, pureTaskEntry{Attrs: attrs, Task: taskInstance})
		return false, nil
	})
	return out
}

func summarizeLiveTasks(tasks []pureTaskEntry) map[string]int {
	out := map[string]int{}
	for _, t := range tasks {
		out[t.TypeName()]++
	}
	return out
}

// newSchedulerEnvWith builds a scheduler test env with the given schedule.
// Mirrors newTestEnv from helper_test.go but lets callers control the schedule
// at construction time — load-bearing for this reproducer because the
// Generator's initial behavior depends on State.LimitedActions/RemainingActions
// being set when NewScheduler runs (not patched in afterwards).
func newSchedulerEnvWith(t *testing.T, schedule *schedulepb.Schedule) *testEnv {
	ctrl := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	specProcessor := newRealSpecProcessor(ctrl, logger)

	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))

	timeSource := clock.NewEventTimeSource()
	now := time.Now()
	timeSource.Update(now)

	tv := testvars.New(t)
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
	require.NoError(t, err)
	require.NoError(t, node.SetRootComponent(sched))
	sched.Generator.Get(ctx).LastProcessedTime = timestamppb.New(now)
	_, err = node.CloseTransaction()
	require.NoError(t, err)

	return &testEnv{
		t:             t,
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

// TestSchedulerInvariant_TriggerOnExhaustedLimitedSchedule reproduces the
// production "stuck open" scheduler shape (open + unpaused, zero pure tasks
// on tree) via two cooperating CHASM tree bugs. Trace verified by
// CHASM_TRACE-instrumented runs of chasm/tree.go.
//
// Sequence:
//  1. NewScheduler(LimitedActions=true, RemainingActions=0)
//  2. CloseTransaction → initial GeneratorTask fires, sees no future
//     wakeups → schedules SchedulerIdleTask. Tree: [SchedulerIdleTask].
//  3. Read-only env.Scheduler.Invoker.Get(env.MutableContext()) — no
//     mutation, no closeTx. (Load-bearing — see BUG 2.)
//  4. Patch{TriggerImmediately} + CloseTransaction.
//  5. End state: 0 live pure tasks, 1 dangling Backfiller, scheduler
//     open + unpaused → stuck.
//
// BUG 1 (latent, primary): chasm.Map mutations don't propagate dirty
// state. addBackfiller does `Scheduler.Backfillers[id] = ...` but the
// Scheduler node's valueState is never raised to valueStateNeedSyncStructure.
// syncSubComponents (chasm/tree.go:861) early-outs without traversing the
// map → new Backfiller is never registered in n.valueToNode →
// executeImmediatePureTasks (chasm/tree.go:1535-1538) silently skips the
// Backfiller's immediate task.
//
// BUG 2 (interaction trigger): a read-only env.MutableContext() +
// Invoker.Get(ctx) call (no mutation, no commit) calls setValueState on
// the root, marking isActiveStateDirty=true. Next CloseTransaction's
// closeTransactionCleanupInvalidTasks then validates the existing
// SchedulerIdleTask, which fails (a Backfiller is pending so isIdle=false)
// and the task is removed.
//
// BUG 1 fires on every Trigger/Backfill — manual triggers in production
// only work because later unrelated mutations eventually promote
// valueState. BUG 2 alone is harmless. Together they produce the
// production failure shape. A fix for BUG 1 alone resolves the production
// case.
func TestSchedulerInvariant_TriggerOnExhaustedLimitedSchedule(t *testing.T) {
	schedule := defaultSchedule()
	schedule.State.LimitedActions = true
	schedule.State.RemainingActions = 0
	schedule.Spec.Interval[0].Interval = durationpb.New(1 * time.Second)

	env := newSchedulerEnvWith(t, schedule)

	before := livePureTasksOnTree(env)
	require.Len(t, before, 1, "expected initial SchedulerIdleTask after exhausted-limited setup")
	t.Logf("before:    %v", summarizeLiveTasks(before))

	// LOAD-BEARING: this read-only MutableContext+Get changes the outcome
	// of the subsequent Trigger (BUG 2). Comment it out and the
	// SchedulerIdleTask survives — but BUG 1 still silently drops the
	// BackfillerTask in the same way (just not visibly via this invariant).
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

	after := livePureTasksOnTree(env)
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
