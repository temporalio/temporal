// Regression guard for a framework assumption every CHASM pure task must hold:
//
//	A pure task MUST become invalid once it has executed.
//
// A validator returning false is the only thing that prunes a task from
// ComponentAttributes.PureTasks. A task that stays valid is never pruned, remains
// past-due, and therefore stays the tree's earliest pure task - and since only the
// earliest one is ever considered for a physical timer, and its
// PhysicalTaskStatus is already `created`, no further timer is generated for
// ANYTHING in the tree. For a schedule that means the Generator, the Invoker's
// ProcessBuffer, the idle task and every Backfiller all stop firing at once,
// silently and without self-healing.
//
// The Backfiller previously violated this: it validated a wall-clock task
// ScheduledTime against LastProcessedTime, which for a Backfiller is a position
// in the requested backfill *range*. For a historical range the comparison never
// returned false, so executed tasks were never pruned. These tests exist so that
// class of bug fails loudly rather than silently stalling schedules.
//
// Each test asserts the invariant via chasmtest.FirePureTasksStrict, and also
// asserts that the pure task type it targets was actually exercised - a test that
// silently stops covering its subject is worse than no test.
package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Pure task types registered in library.go. Side-effect tasks (execute,
// callbacks, migrateToWorkflow) are out of scope: each gets its own physical
// task, so they have no single-timer bottleneck and cannot strand the execution.
const (
	generatorTaskType     = "*schedulerpb.GeneratorTask"
	processBufferTaskType = "*schedulerpb.InvokerProcessBufferTask"
	backfillerTaskType    = "*schedulerpb.BackfillerTask"
	idleTaskType          = "*schedulerpb.SchedulerIdleTask"
)

type pureTaskEnv struct {
	t          *testing.T
	engine     *chasmtest.Engine
	engineCtx  context.Context
	rootRef    chasm.ComponentRef
	timeSource *clock.EventTimeSource
	executed   map[string]int
}

func newPureTaskEnv(t *testing.T, sched *schedulepb.Schedule) *pureTaskEnv {
	logger := log.NewNoopLogger()
	specProcessor := scheduler.NewSpecProcessor(
		defaultConfig(), metrics.NoopMetricsHandler, logger, newLegacySpecBuilder(0, 0))
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))

	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now())
	engine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(timeSource))
	engineCtx := chasm.NewEngineContext(context.Background(), engine)

	_, err := scheduler.NewTestHandler(testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)).
		CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				Schedule:   sched,
				RequestId:  "req-create",
			},
		})
	require.NoError(t, err)

	return &pureTaskEnv{
		t:         t,
		engine:    engine,
		engineCtx: engineCtx,
		rootRef: chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
			NamespaceID: namespaceID,
			BusinessID:  scheduleID,
		}),
		timeSource: timeSource,
		executed:   map[string]int{},
	}
}

// step advances the clock and fires every due pure task, enforcing the invariant
// on each one that executes.
func (e *pureTaskEnv) step(advance time.Duration) {
	e.timeSource.Update(e.timeSource.Now().Add(advance))
	types, violations, err := e.engine.FirePureTasksStrict(e.rootRef, e.timeSource.Now())
	require.NoError(e.t, err)
	for _, taskType := range types {
		e.executed[taskType]++
	}
	for _, v := range violations {
		e.t.Errorf("pure task invariant violated: %s", v)
	}
}

// drainBuffer clears buffered starts so the Invoker and Backfiller keep making
// progress instead of parking on a full buffer.
func (e *pureTaskEnv) drainBuffer() {
	_, _, err := chasm.UpdateComponent(e.engineCtx, e.rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.Invoker.Get(ctx).BufferedStarts = nil
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(e.t, err)
}

func (e *pureTaskEnv) requireCovered(taskTypes ...string) {
	for _, taskType := range taskTypes {
		require.Positive(e.t, e.executed[taskType],
			"pure task %s was never exercised, so this test proves nothing about it (executed: %v)",
			taskType, e.executed)
	}
}

// The Generator's task is the most exposed of the four: its invalidation depends
// on LastProcessedTime advancing to at least the fired task's ScheduledTime, via
// ProcessTimeRange's LastActionTime - a derived business value rather than a
// counter or a wall clock. It also has two exit paths that schedule no successor
// (idle, and "hold open without a task"), so on those the executed task's
// invalidation is the only thing keeping the execution's timers alive.
func TestPureTaskInvariant_GeneratorTask(t *testing.T) {
	env := newPureTaskEnv(t, defaultSchedule())
	for i := range 12 {
		if i%3 == 0 {
			env.drainBuffer()
		}
		env.step(2 * defaultInterval)
	}
	env.requireCovered(generatorTaskType)
}

// The Backfiller across a range backfill: first batch, buffer-full back off,
// continuation batches, and completion (which deletes the component).
//
// A trigger-immediately Backfiller is deliberately not covered here: its task is
// scheduled and consumed inline within the PatchSchedule transaction, so it never
// becomes a persisted timer-driven task and cannot strand a timer.
func TestPureTaskInvariant_BackfillerTask(t *testing.T) {
	env := newPureTaskEnv(t, defaultSchedule())

	now := env.timeSource.Now()
	_, err := scheduler.NewTestHandler(log.NewNoopLogger()).PatchSchedule(env.engineCtx,
		&schedulerpb.PatchScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.PatchScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				RequestId:  "req-backfill",
				Patch: &schedulepb.SchedulePatch{
					BackfillRequest: []*schedulepb.BackfillRequest{{
						// A historical range: the case the old high-water-mark
						// validator could never invalidate.
						StartTime:     timestamppb.New(now.Add(-1000 * defaultInterval)),
						EndTime:       timestamppb.New(now.Add(-1 * defaultInterval)),
						OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					}},
				},
			},
		})
	require.NoError(t, err)

	// Leave the buffer full for the first rounds to exercise the "buffer full,
	// back off" branch, then drain so the backfill runs to completion.
	for i := range 10 {
		if i >= 2 {
			env.drainBuffer()
		}
		env.step(2 * time.Minute)
	}
	env.requireCovered(backfillerTaskType, generatorTaskType)
}

// The Invoker's ProcessBuffer task has two forms. The immediate form (addTasks
// schedules TaskScheduledTimeImmediate while starts await their first pass)
// executes inline during CloseTransaction and is never persisted, so it carries
// no exposure. The delayed form, armed at nextBackoffDeadline for starts that are
// retrying, is persisted and timer-driven - so it is the one that matters here.
// A retrying start is constructed directly rather than by driving a
// StartWorkflow failure.
func TestPureTaskInvariant_DelayedProcessBufferTask(t *testing.T) {
	env := newPureTaskEnv(t, defaultSchedule())

	backoffAt := env.timeSource.Now().Add(5 * time.Minute)
	_, _, err := chasm.UpdateComponent(env.engineCtx, env.rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			invoker := s.Invoker.Get(ctx)
			// Attempt > 0, a future BackoffTime and no RunId is what
			// nextBackoffDeadline selects on.
			invoker.BufferedStarts = []*schedulespb.BufferedStart{{
				Attempt:     1,
				BackoffTime: timestamppb.New(backoffAt),
				RequestId:   "retrying-start",
				WorkflowId:  "retrying-wf",
				NominalTime: timestamppb.New(env.timeSource.Now()),
				ActualTime:  timestamppb.New(env.timeSource.Now()),
			}}
			// EnqueueBufferedStarts calls addTasks, which arms the delayed task.
			invoker.EnqueueBufferedStarts(ctx, nil)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)

	env.step(10 * time.Minute) // cross the backoff deadline
	env.requireCovered(processBufferTaskType)
}

// The idle task fires once, to close a schedule whose spec is exhausted. It has
// the fewest natural re-arms of the four, so a validator regression here would
// strand the execution with nothing left to revive it.
func TestPureTaskInvariant_IdleTask(t *testing.T) {
	sched := defaultSchedule()
	// Bound the spec so the Generator runs out of wakeups and arms the idle task.
	sched.Spec.EndTime = timestamppb.New(time.Now().Add(3 * defaultInterval))

	env := newPureTaskEnv(t, sched)
	for range 6 {
		env.drainBuffer()
		env.step(2 * defaultInterval)
	}
	// Jump past the idle deadline so the idle task becomes due.
	env.drainBuffer()
	env.step(scheduler.DefaultTweakables.IdleTime + time.Hour)
	env.step(time.Minute)

	env.requireCovered(idleTaskType)
}
