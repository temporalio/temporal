package scheduler_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// idleCloseSafetyError checks the pre-close state, independently of the idle
// deadline formula. Running workflows may outlive the schedule, but unresolved
// starts must be recorded or explicitly discarded before idle closes it.
func idleCloseSafetyError(s *scheduler.Scheduler, ctx chasm.Context) error {
	if s.IsSentinel() {
		return nil
	}
	for _, start := range s.Invoker.Get(ctx).GetBufferedStarts() {
		if start.GetRunId() == "" {
			return fmt.Errorf("idle close abandoned pending action %q scheduled for %s", start.GetRequestId(), start.GetActualTime().AsTime())
		}
	}
	return nil
}

// idleCloseObserver observes the real transition without preventing it. Errors
// are asserted by the test goroutine, including when a task runs in a worker.
type idleCloseObserver struct {
	sync.Mutex
	violations []error
	closes     int
}

// Check clean snapshots rather than materializing components after commit. A
// declared deadline must have a matching logical timer, not merely a visibility
// task or an obsolete physical task. This callback never asserts in a worker.
func (o *idleCloseObserver) checkState(_ *testing.T, node *chasm.Node, root chasm.RootComponent) {
	o.Lock()
	defer o.Unlock()
	s, ok := root.(*scheduler.Scheduler)
	if !ok {
		o.violations = append(o.violations, fmt.Errorf("unexpected root %T", root))
		return
	}
	if s.Closed || s.IsSentinel() || s.WorkflowMigration != nil {
		return
	}
	total, _ := liveTaskCount(node)
	if total == 0 {
		o.violations = append(o.violations, errors.New("open schedule has no logical task to make progress"))
	}
	if s.IdleCloseTime == nil {
		return
	}
	deadline := s.IdleCloseTime.AsTime().Truncate(common.ScheduledTaskMinPrecision)
	idleID := chasm.GenerateTypeID(chasm.FullyQualifiedName(chasm.SchedulerLibraryName, "idle"))
	for _, n := range node.Snapshot(nil).Nodes {
		for _, task := range n.GetMetadata().GetComponentAttributes().GetPureTasks() {
			if task.GetTypeId() == idleID && task.GetScheduledTime().AsTime().Truncate(common.ScheduledTaskMinPrecision).Equal(deadline) {
				return
			}
		}
	}
	o.violations = append(o.violations, fmt.Errorf("idle deadline %s has no matching logical idle task", deadline))
}

func (o *idleCloseObserver) snapshot() ([]error, int) {
	o.Lock()
	defer o.Unlock()
	return slices.Clone(o.violations), o.closes
}

type observedIdleHandler struct {
	*scheduler.SchedulerIdleTaskHandler
	observer *idleCloseObserver
}

func (h *observedIdleHandler) Execute(ctx chasm.MutableContext, s *scheduler.Scheduler, attrs chasm.TaskAttributes, task *schedulerpb.SchedulerIdleTask) error {
	violation := idleCloseSafetyError(s, ctx)
	if err := h.SchedulerIdleTaskHandler.Execute(ctx, s, attrs, task); err != nil {
		return err
	}
	h.observer.Lock()
	defer h.observer.Unlock()
	h.observer.closes++
	if violation != nil {
		h.observer.violations = append(h.observer.violations, violation)
	}
	return nil
}

// This wrapper replaces only task registration, leaving production handlers,
// validation, component registration, and explicit Delete behavior intact.
type idleGuaranteesLibrary struct {
	*scheduler.Library
	observer *idleCloseObserver
}

func (l *idleGuaranteesLibrary) Tasks() []*chasm.RegistrableTask {
	tasks := l.Library.Tasks()
	for i, task := range tasks {
		if task.GoType() == reflect.TypeFor[*schedulerpb.SchedulerIdleTask]() {
			tasks[i] = chasm.NewRegistrablePureTask("idle", &observedIdleHandler{
				SchedulerIdleTaskHandler: l.SchedulerIdleTaskHandler,
				observer:                 l.observer,
			})
		}
	}
	return tasks
}

type idleGuaranteesEnv struct {
	*schedulerTestEngine
	frontend   *workflowservicemock.MockWorkflowServiceClient
	execute    *scheduler.InvokerExecuteTaskHandler
	observer   *idleCloseObserver
	tweakables scheduler.Tweakables
}

func newIdleGuaranteesEnv(t *testing.T, schedule *schedulepb.Schedule) *idleGuaranteesEnv {
	t.Helper()
	e := &idleGuaranteesEnv{
		frontend:   workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t)),
		observer:   &idleCloseObserver{},
		tweakables: scheduler.DefaultTweakables,
	}
	ts := clock.NewEventTimeSource()
	ts.Update(pendingCreateTime)
	config := defaultConfig()
	// Tests change configuration only while no handler is accessing it.
	config.Tweakables = func(string) scheduler.Tweakables { return e.tweakables }
	e.schedulerTestEngine = newSchedulerTestEngine(t, schedule, withEngineTimeSource(ts), func(c *engineTestConfig) {
		c.invariantCheck = e.observer.checkState
		c.configureScheduler = func(target *scheduler.Config) { target.Tweakables = config.Tweakables }
		c.wrapLibrary = func(library *scheduler.Library) chasm.Library {
			e.execute = scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
				Config: config, MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: log.NewTestLogger(),
				SpecProcessor: library.GeneratorTaskHandler.SpecProcessor, FrontendClient: e.frontend,
			})
			library.InvokerExecuteTaskHandler = e.execute
			return &idleGuaranteesLibrary{Library: library, observer: e.observer}
		}
	})
	return e
}

func idleGuaranteesSchedule(fire time.Time) *schedulepb.Schedule {
	s := defaultSchedule()
	s.Spec = oneShotSpec(fire)
	s.Policies = &schedulepb.SchedulePolicies{}
	return s
}

func (e *idleGuaranteesEnv) fireAt(t *testing.T, at time.Time) {
	t.Helper()
	firePureTasksAt(t, e.schedulerTestEngine, e.timeSource, at)
	violations, _ := e.observer.snapshot()
	require.Empty(t, violations, "idle closure must not abandon unresolved starts")
}

func (e *idleGuaranteesEnv) executeOnce(ctx context.Context) error {
	var invoker *scheduler.Invoker
	err := e.readScheduler(func(s *scheduler.Scheduler, c chasm.Context) error {
		invoker = s.Invoker.Get(c)
		return nil
	})
	if err != nil {
		return err
	}
	_, err = chasmtest.ExecuteSideEffectTask(ctx, e.engine, invoker, e.execute,
		chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	return err
}

func (e *idleGuaranteesEnv) requireEventuallyClosed(t *testing.T) {
	t.Helper()
	var deadline time.Time
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
		require.False(t, s.Closed)
		require.NotNil(t, s.IdleCloseTime, "resolved work must leave an idle deadline")
		deadline = s.IdleCloseTime.AsTime()
		return nil
	}))
	e.fireAt(t, deadline.Add(-time.Second))
	requireSchedulerState(t, e.schedulerTestEngine, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.False(t, s.Closed)
	})
	e.fireAt(t, deadline)
	e.fireAt(t, deadline)
	requireSchedulerState(t, e.schedulerTestEngine, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.True(t, s.Closed, "drained schedule must eventually close")
	})
	violations, closes := e.observer.snapshot()
	require.Empty(t, violations)
	require.Equal(t, 1, closes, "idle closure must execute exactly once")
}

func TestIdleCloseSafetyOracle(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		runID    string
		sentinel bool
	}{
		{name: "unresolved start"},
		{name: "running workflow may outlive schedule", runID: "run"},
		{name: "sentinel has no invoker", sentinel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newTestEnv(t)
			ctx := env.MutableContext()
			env.Scheduler.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
				RequestId: "final", ActualTime: timestamppb.New(pendingCreateTime), RunId: tc.runID,
			}}
			if tc.sentinel {
				env.Scheduler.Sentinel = true
				env.Scheduler.Invoker = chasm.NewEmptyField[*scheduler.Invoker]()
			}
			err := idleCloseSafetyError(env.Scheduler, ctx)
			if tc.runID == "" && !tc.sentinel {
				require.ErrorContains(t, err, `idle close abandoned pending action "final"`)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestIdleGuaranteesStartOutcomes(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"success", "permanent failure", "lost response"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			fire := pendingCreateTime.Add(idleWeek + time.Hour).Truncate(time.Second)
			e := newIdleGuaranteesEnv(t, idleGuaranteesSchedule(fire))
			var requests []string
			e.frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
					requests = append(requests, req.RequestId)
					if outcome == "permanent failure" {
						return nil, serviceerror.NewInvalidArgument("invalid action")
					}
					if outcome == "lost response" {
						return nil, serviceerror.NewDeadlineExceeded("workflow started but response lost")
					}
					return &workflowservice.StartWorkflowExecutionResponse{RunId: "one-run"}, nil
				}).Times(1)

			e.fireAt(t, fire.Add(10*time.Millisecond))
			// The old, overdue idle task must not close before the start RPC.
			e.fireAt(t, e.timeSource.Now())
			require.NoError(t, e.executeOnce(t.Context()))
			if outcome == "lost response" {
				var backoff time.Time
				require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					starts := s.Invoker.Get(ctx).GetBufferedStarts()
					require.Len(t, starts, 1)
					backoff = starts[0].BackoffTime.AsTime()
					return nil
				}))
				e.frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
					DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
						requests = append(requests, req.RequestId)
						return &workflowservice.StartWorkflowExecutionResponse{RunId: "one-run"}, nil
					}).Times(1)
				e.fireAt(t, backoff.Add(time.Millisecond))
				require.NoError(t, e.executeOnce(t.Context()))
				require.Len(t, requests, 2)
				require.Equal(t, requests[0], requests[1], "retry must preserve start request identity")
			}
			requireSchedulerState(t, e.schedulerTestEngine, func(s *scheduler.Scheduler, ctx chasm.Context) {
				if outcome == "permanent failure" {
					require.Empty(t, s.Invoker.Get(ctx).GetBufferedStarts())
					require.Zero(t, s.Info.ActionCount)
					require.Equal(t, fire.Add(idleWeek), s.IdleCloseTime.AsTime())
				} else {
					require.EqualValues(t, 1, s.Info.ActionCount)
					start := s.Invoker.Get(ctx).GetBufferedStarts()[0]
					require.Equal(t, "one-run", start.GetRunId())
					require.Equal(t, start.GetStartTime().AsTime().Add(idleWeek), s.IdleCloseTime.AsTime())
				}
			})
			e.requireEventuallyClosed(t)
		})
	}
}

func TestIdleGuaranteesStartRPCInFlight(t *testing.T) {
	t.Parallel()
	testIdleStartRPCInFlight(t, time.Minute)
}

func testIdleStartRPCInFlight(t *testing.T, timerDelay time.Duration) {
	t.Helper()
	fire := pendingCreateTime.Add(idleWeek + time.Hour).Truncate(time.Second)
	e := newIdleGuaranteesEnv(t, idleGuaranteesSchedule(fire))
	entered := make(chan string, 1)
	release := make(chan struct{})
	e.frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			entered <- req.RequestId
			select {
			case <-release:
				return &workflowservice.StartWorkflowExecutionResponse{RunId: "in-flight-run"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
	e.fireAt(t, fire.Add(10*time.Millisecond))
	done := make(chan error, 1)
	go func() { done <- e.executeOnce(t.Context()) }()
	joined := false
	defer func() {
		close(release)
		if !joined {
			// A failed invariant must still release the RPC and join the task.
			if err := await.Rcv(t, done); err != nil {
				t.Logf("in-flight task returned after failed invariant: %v", err)
			}
		}
	}()
	require.NotEmpty(t, await.Rcv(t, entered))
	// RPC is blocked outside the state lock. Exercise the timer queue before
	// allowing the result to be recorded, without concurrent engine mutations.
	e.fireAt(t, fire.Add(timerDelay))
	requireSchedulerState(t, e.schedulerTestEngine, func(s *scheduler.Scheduler, ctx chasm.Context) {
		require.False(t, s.Closed)
		require.Empty(t, s.Invoker.Get(ctx).GetBufferedStarts()[0].GetRunId())
	})
	await.Snd(t, release, struct{}{})
	err := await.Rcv(t, done)
	joined = true
	require.NoError(t, err)
	e.requireEventuallyClosed(t)
}

func TestIdleGuaranteesExplicitDeleteMayDiscardPendingStart(t *testing.T) {
	t.Parallel()
	fire := pendingCreateTime.Add(idleWeek + time.Hour).Truncate(time.Second)
	e := newIdleGuaranteesEnv(t, idleGuaranteesSchedule(fire))
	e.fireAt(t, fire)
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		_, err := s.Delete(ctx, &schedulerpb.DeleteScheduleRequest{})
		return err
	}))
	requireSchedulerState(t, e.schedulerTestEngine, func(s *scheduler.Scheduler, _ chasm.Context) {
		require.True(t, s.Closed)
	})
	violations, closes := e.observer.snapshot()
	require.Empty(t, violations)
	require.Zero(t, closes, "explicit deletion is not an idle-close transition")
}

func TestIdleGuaranteesHistoricalBackfill(t *testing.T) {
	t.Parallel()
	testIdleBackfillRetention(t, -30*24*time.Hour)
}

func testIdleBackfillRetention(t *testing.T, offset time.Duration) {
	t.Helper()
	backfillTime := pendingCreateTime.Add(offset).Truncate(time.Second)
	s := idleGuaranteesSchedule(backfillTime)
	s.State.LimitedActions = true
	s.State.RemainingActions = 0
	e := newIdleGuaranteesEnv(t, s)
	e.frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&workflowservice.StartWorkflowExecutionResponse{RunId: "backfill-run"}, nil)
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		_, err := s.Patch(ctx, &schedulerpb.PatchScheduleRequest{FrontendRequest: &workflowservice.PatchScheduleRequest{
			Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
				StartTime: timestamppb.New(backfillTime), EndTime: timestamppb.New(backfillTime),
			}}},
		}})
		return err
	}))
	require.NoError(t, e.executeOnce(t.Context()))
	requireSchedulerState(t, e.schedulerTestEngine, func(s *scheduler.Scheduler, ctx chasm.Context) {
		starts := s.Invoker.Get(ctx).GetBufferedStarts()
		require.Len(t, starts, 1)
		require.True(t, starts[0].Manual)
		require.Equal(t, starts[0].StartTime.AsTime().Add(idleWeek), s.IdleCloseTime.AsTime(),
			"retention must follow activity time, not a backfill's future calendar label")
	})
	e.requireEventuallyClosed(t)
}
