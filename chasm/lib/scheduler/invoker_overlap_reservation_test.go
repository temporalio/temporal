package scheduler_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type reservationOutcome int

const (
	reservationSucceeds reservationOutcome = iota
	reservationRetries
	reservationFails
)

func (o reservationOutcome) String() string {
	return [...]string{"success", "retryable_failure", "non_retryable_failure"}[o]
}

// Attempt reported for a start no longer in BufferedStarts.
const reservationDropped = -2

type reservationExpectation struct {
	// While A is unresolved.
	bInflight      int64
	overlapSkipped int64
	// After A resolves and ProcessBuffer has run.
	bAfter      map[reservationOutcome]int64
	cancelAfter map[reservationOutcome]bool // A's run targeted for cancel/terminate
}

// Covers the window where A's StartWorkflowExecution is in flight (or backing
// off) with no RunId: B must not be admitted unless the policy is ALLOW_ALL.
func TestInvoker_UnresolvedStartReservesOverlapSlot(t *testing.T) {
	t.Parallel()

	buffered := reservationExpectation{
		bInflight:   -1,
		bAfter:      map[reservationOutcome]int64{reservationSucceeds: -1, reservationRetries: -1, reservationFails: 1},
		cancelAfter: map[reservationOutcome]bool{},
	}
	// B waits for A's RunId, then cancels/terminates it.
	replacing := reservationExpectation{
		bInflight:   -1,
		bAfter:      map[reservationOutcome]int64{reservationSucceeds: -1, reservationRetries: -1, reservationFails: 1},
		cancelAfter: map[reservationOutcome]bool{reservationSucceeds: true},
	}
	cases := map[enumspb.ScheduleOverlapPolicy]reservationExpectation{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP: {
			bInflight:      reservationDropped,
			overlapSkipped: 1,
			bAfter: map[reservationOutcome]int64{
				reservationSucceeds: reservationDropped, reservationRetries: reservationDropped, reservationFails: reservationDropped,
			},
			cancelAfter: map[reservationOutcome]bool{},
		},
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE:      buffered,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL:      buffered,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER:    replacing,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER: replacing,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL: {
			bInflight:   1,
			bAfter:      map[reservationOutcome]int64{reservationSucceeds: 1, reservationRetries: 1, reservationFails: 1},
			cancelAfter: map[reservationOutcome]bool{},
		},
	}

	for policy, want := range cases {
		for _, outcome := range []reservationOutcome{reservationSucceeds, reservationRetries, reservationFails} {
			// Manual (trigger/backfill) starts carry a resolved policy and obey it too.
			for _, manual := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/manual=%t", policy, outcome, manual), func(t *testing.T) {
					t.Parallel()
					runReservationCase(t, policy, outcome, manual, want)
				})
			}
		}
	}
}

func runReservationCase(
	t *testing.T,
	policy enumspb.ScheduleOverlapPolicy,
	outcome reservationOutcome,
	manual bool,
	want reservationExpectation,
) {
	const runA = "run-a"
	env, now := newReservationEngine(t, policy, time.Minute)
	env.enqueueStart(t, now, "a", policy, manual)

	entered := make(chan struct{})
	release := make(chan struct{})
	var startCalls atomic.Int32
	frontend := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			startCalls.Add(1)
			close(entered)
			<-release
			switch outcome {
			case reservationRetries:
				return nil, serviceerror.NewUnavailable("transient")
			case reservationFails:
				return nil, serviceerror.NewInvalidArgument("bad request")
			default:
				return &workflowservice.StartWorkflowExecutionResponse{RunId: runA}, nil
			}
		})
	invoker, handler := env.invoker(t), env.executeHandler(frontend)
	done := make(chan error, 1)
	go func() {
		_, err := chasmtest.ExecuteSideEffectTask(context.Background(), env.engine, invoker,
			handler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
		done <- err
	}()
	<-entered

	// A's RPC is in flight outside the lock.
	env.enqueueStart(t, now, "b", policy, manual)
	bAttempt, targets, skipped := env.inspectStart(t, "b")
	require.Equal(t, want.bInflight, bAttempt, "B while A is in flight")
	require.Empty(t, targets, "no run to cancel/terminate while A is unresolved")
	require.Equal(t, want.overlapSkipped, skipped)

	close(release)
	require.NoError(t, <-done)
	env.firePure(t)

	bAttempt, targets, _ = env.inspectStart(t, "b")
	require.Equal(t, want.bAfter[outcome], bAttempt, "B after A resolved")
	if want.cancelAfter[outcome] {
		require.Len(t, targets, 1)
		require.Equal(t, runA, targets[0].GetRunId())
	} else {
		require.Empty(t, targets)
	}
	require.Equal(t, int32(1), startCalls.Load())

	if outcome != reservationSucceeds || bAttempt != -1 {
		return
	}
	// A's completion releases the deferred B.
	require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.RecordCompletedAction(ctx, &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CloseTime: timestamppb.New(now),
		}, "a")
		return nil
	}))
	env.firePure(t)
	bAttempt, _, _ = env.inspectStart(t, "b")
	require.Equal(t, int64(1), bAttempt, "B after A completed")
}

// A deferred start whose reservation resolves after its catchup window is dropped.
func TestInvoker_DeferredStartExpiresAfterReservationResolves(t *testing.T) {
	t.Parallel()
	policy := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	env, now := newReservationEngine(t, policy, time.Hour)
	env.enqueueStart(t, now, "a", policy, false)
	env.enqueueStart(t, now, "b", policy, false)
	attempt, _, _ := env.inspectStart(t, "b")
	require.Equal(t, int64(-1), attempt)

	env.timeSource.Update(now.Add(defaultCatchupWindow + time.Minute))
	frontend := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewInvalidArgument("bad request"))
	_, err := chasmtest.ExecuteSideEffectTask(context.Background(), env.engine, env.invoker(t),
		env.executeHandler(frontend), chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	env.firePure(t)

	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		require.Equal(t, int64(1), s.Info.GetMissedCatchupWindow())
		require.Empty(t, s.Invoker.Get(ctx).GetBufferedStarts())
		return nil
	}))
}

// A deferred start left with nothing running (e.g. by an older binary that never
// re-enabled it) is reconsidered by the next ProcessBuffer pass, ahead of later starts.
func TestInvoker_OrphanedDeferredStartIsReconsidered(t *testing.T) {
	t.Parallel()
	policy := enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL
	env, now := newReservationEngine(t, policy, time.Hour)
	require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		nowpb := timestamppb.New(now)
		s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
			NominalTime: nowpb, ActualTime: nowpb, DesiredTime: nowpb,
			RequestId: "orphan", WorkflowId: "wf-orphan", OverlapPolicy: policy, Attempt: -1,
		}}
		return nil
	}))
	env.enqueueStart(t, now, "next", policy, false)

	orphan, _, _ := env.inspectStart(t, "orphan")
	next, _, _ := env.inspectStart(t, "next")
	require.Equal(t, int64(1), orphan)
	require.Equal(t, int64(-1), next)
}

type reservationEngine struct{ *schedulerTestEngine }

func newReservationEngine(t *testing.T, policy enumspb.ScheduleOverlapPolicy, interval time.Duration) (reservationEngine, time.Time) {
	ts := clock.NewEventTimeSource()
	now := time.Date(2026, 10, 28, 12, 0, 10, 0, time.UTC)
	ts.Update(now)
	sched := defaultSchedule()
	sched.Spec.Interval[0].Interval = durationpb.New(interval)
	sched.Policies.OverlapPolicy = policy
	return reservationEngine{newSchedulerTestEngine(t, sched, withEngineTimeSource(ts))}, now
}

// enqueueStart buffers a start and runs the resulting ProcessBuffer pass.
func (e reservationEngine) enqueueStart(t *testing.T, at time.Time, requestID string, policy enumspb.ScheduleOverlapPolicy, manual bool) {
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		atpb := timestamppb.New(at)
		s.Invoker.Get(ctx).EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{{
			NominalTime: atpb, ActualTime: atpb, DesiredTime: atpb, Manual: manual,
			RequestId: requestID, WorkflowId: "wf-" + requestID, OverlapPolicy: policy,
		}})
		return nil
	}))
	e.firePure(t)
}

func (e reservationEngine) firePure(t *testing.T) {
	_, err := e.engine.FirePureTasks(e.rootRef, e.timeSource.Now())
	require.NoError(t, err)
}

// inspectStart returns requestID's Attempt (reservationDropped if absent), pending
// cancel/terminate targets, and the OverlapSkipped count.
func (e reservationEngine) inspectStart(t *testing.T, requestID string) (attempt int64, targets []*commonpb.WorkflowExecution, skipped int64) {
	attempt = reservationDropped
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		inv := s.Invoker.Get(ctx)
		for _, start := range inv.GetBufferedStarts() {
			if start.GetRequestId() == requestID {
				attempt = start.GetAttempt()
			}
		}
		targets = append(targets, inv.GetCancelWorkflows()...)
		targets = append(targets, inv.GetTerminateWorkflows()...)
		skipped = s.Info.GetOverlapSkipped()
		return nil
	}))
	return attempt, targets, skipped
}

func (e reservationEngine) invoker(t *testing.T) *scheduler.Invoker {
	var invoker *scheduler.Invoker
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		invoker = s.Invoker.Get(ctx)
		return nil
	}))
	return invoker
}

func (e reservationEngine) executeHandler(frontend workflowservice.WorkflowServiceClient) *scheduler.InvokerExecuteTaskHandler {
	return scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     e.logger,
		FrontendClient: frontend,
	})
}
