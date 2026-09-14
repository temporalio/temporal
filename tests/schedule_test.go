package tests

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/callback"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// schedulerRoute applies per-request routing to the selected scheduler implementation.
type schedulerRoute func(context.Context) context.Context

var (
	routeToCHASMScheduler schedulerRoute = func(ctx context.Context) context.Context {
		return metadata.NewOutgoingContext(ctx, metadata.Pairs(
			headers.ExperimentHeaderName, "chasm-scheduler",
		))
	}
	routeToV1Scheduler schedulerRoute = func(ctx context.Context) context.Context {
		return ctx
	}
)

func schedulerRouteFor(chasmEnabled bool) schedulerRoute {
	if chasmEnabled {
		return routeToCHASMScheduler
	}
	return routeToV1Scheduler
}

func scheduleCommonOpts(t *testing.T) []testcore.TestOption {
	opts := []testcore.TestOption{
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerSentinels, true),
		testcore.WithDynamicConfig(dynamicconfig.FrontendAllowedExperiments, []string{"*"}),
	}
	if strings.HasPrefix(t.Name(), "TestScheduleV1") {
		// only v1 needs the worker service
		opts = append(opts, testcore.WithWorkerService("V1 scheduler"))
	}
	return opts
}

func newScheduleEnv(t *testing.T, opts ...testcore.TestOption) *testcore.TestEnv {
	t.Helper()
	opts = append(opts, testcore.WithDynamicConfig(dynamicconfig.FrontendAllowedExperiments, []string{"*"}))
	env := testcore.NewEnv(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(routeToCHASMScheduler(testcore.NewContext()), 30*time.Second)
		defer cancel()

		var scheduleIDs []string
		var nextPageToken []byte
		for {
			response, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
				Namespace:       env.Namespace().String(),
				MaximumPageSize: 1000,
				NextPageToken:   nextPageToken,
			})
			if err != nil {
				if t.Failed() {
					t.Logf("schedule cleanup failed: list schedules: %v", err)
				} else {
					t.Errorf("schedule cleanup failed: list schedules: %v", err)
				}
				return
			}
			for _, schedule := range response.GetSchedules() {
				scheduleIDs = append(scheduleIDs, schedule.GetScheduleId())
			}
			nextPageToken = response.GetNextPageToken()
			if len(nextPageToken) == 0 {
				break
			}
		}

		var cleanupErr error
		for _, scheduleID := range scheduleIDs {
			_, err := env.FrontendClient().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: scheduleID,
				Identity:   "test cleanup",
			})
			var notFoundErr *serviceerror.NotFound
			if err != nil && !errors.As(err, &notFoundErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete schedule %q: %w", scheduleID, err))
			}
		}
		if cleanupErr != nil {
			if t.Failed() {
				t.Logf("schedule cleanup failed: %v", cleanupErr)
			} else {
				t.Errorf("schedule cleanup failed: %v", cleanupErr)
			}
		}
	})
	return env
}

const (
	// fastInterval is the shortest interval that reliably ticks once per second.
	fastInterval = 1 * time.Second
	// noOpInterval is long enough that no spec tick fires during a test.
	noOpInterval = 1 * time.Hour
	// shortIdleTime is the CHASM IdleTime used by idle-close tests: short enough
	// to keep tests fast, long enough to be reached reliably under load.
	shortIdleTime = 3 * time.Second
	// pollInterval is the poll cadence shared by all await/Never checks.
	pollInterval = 200 * time.Millisecond
	// awaitTimeout bounds how long an await waits for a condition to become true.
	awaitTimeout = 30 * time.Second
	// neverWindow is how long a Never check waits to confirm a condition stays false.
	neverWindow = 5 * time.Second
)

// completeSignalName releases a workflow registered via registerGatedWorkflow.
const completeSignalName = "complete"

// intervalSpec is the single-interval schedule spec used by most tests.
func intervalSpec(every time.Duration) *schedulepb.ScheduleSpec {
	return &schedulepb.ScheduleSpec{
		Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(every)}},
	}
}

// newEnvWithIdleTime returns a schedule test env with the CHASM IdleTime
// tweakable set to idleTime, plus any extra options.
func newEnvWithIdleTime(t *testing.T, idleTime time.Duration, extra ...testcore.TestOption) *testcore.TestEnv {
	tweakables := chasmscheduler.DefaultTweakables
	tweakables.IdleTime = idleTime
	opts := append(scheduleCommonOpts(t), testcore.WithDynamicConfig(chasmscheduler.CurrentTweakables, tweakables))
	return newScheduleEnv(t, append(opts, extra...)...)
}

// createSchedule creates sched under sid and fails the test on error.
func createSchedule(ctx context.Context, t *testing.T, env *testcore.TestEnv, sid string, sched *schedulepb.Schedule) {
	t.Helper()
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   sched,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)
}

// patchSchedule applies patch to sid and fails the test on error.
func patchSchedule(ctx context.Context, t *testing.T, env *testcore.TestEnv, sid string, patch *schedulepb.SchedulePatch) {
	t.Helper()
	_, err := env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Patch:      patch,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)
}

// backfillPatch builds a single-range backfill patch.
func backfillPatch(start, end time.Time, policy enumspb.ScheduleOverlapPolicy) *schedulepb.SchedulePatch {
	return &schedulepb.SchedulePatch{
		BackfillRequest: []*schedulepb.BackfillRequest{{
			StartTime:     timestamppb.New(start),
			EndTime:       timestamppb.New(end),
			OverlapPolicy: policy,
		}},
	}
}

// triggerPatch builds a TriggerImmediately patch with the given overlap policy.
func triggerPatch(policy enumspb.ScheduleOverlapPolicy) *schedulepb.SchedulePatch {
	return &schedulepb.SchedulePatch{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{OverlapPolicy: policy},
	}
}

// startWorkflowAction builds the StartWorkflow action shared by these tests.
func startWorkflowAction(env *testcore.TestEnv, wid, wt string) *schedulepb.ScheduleAction {
	return &schedulepb.ScheduleAction{
		Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:            wid,
				WorkflowType:          &commonpb.WorkflowType{Name: wt},
				TaskQueue:             &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			},
		},
	}
}

// calendarSpec builds a single-instant calendar spec at the given time.
func calendarSpec(at time.Time) *schedulepb.CalendarSpec {
	return &schedulepb.CalendarSpec{
		Year:       fmt.Sprintf("%d", at.Year()),
		Month:      at.Month().String(),
		DayOfMonth: fmt.Sprintf("%d", at.Day()),
		Hour:       fmt.Sprintf("%d", at.Hour()),
		Minute:     fmt.Sprintf("%d", at.Minute()),
		Second:     fmt.Sprintf("%d", at.Second()),
	}
}

// registerCountingWorkflow registers a workflow that records each execution in
// runs (via SideEffect, so replays don't double-count) and returns immediately.
//
// Each registered counting workflow should be associated with a distinct `runs`
// atomic.
func registerCountingWorkflow(env *testcore.TestEnv, wt string, runs *atomic.Int32) {
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		_ = workflow.SideEffect(ctx, func(workflow.Context) any { runs.Add(1); return 0 })
		return nil
	}, workflow.RegisterOptions{Name: wt})
}

// registerGatedWorkflow is like registerCountingWorkflow but the workflow stays
// running until the test signals completeSignalName (via completeRunningWorkflows).
func registerGatedWorkflow(env *testcore.TestEnv, wt string, runs *atomic.Int32) {
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		_ = workflow.SideEffect(ctx, func(workflow.Context) any { runs.Add(1); return 0 })
		workflow.GetSignalChannel(ctx, completeSignalName).Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: wt})
}

// countMetric returns how many captured samples of metricName carry every tag in want.
func countMetric(capture *testcore.NamespaceMetricCapture, metricName string, want map[string]string) int {
	return len(capture.CollectMetric(metricName, func(rec *metricstest.CapturedRecording) bool {
		for key, value := range want {
			if rec.Tags[key] != value {
				return false
			}
		}
		return true
	}))
}

// scheduleClosed reports whether the schedule has closed, i.e. DescribeSchedule
// returns NotFound specifically (not just any error).
func scheduleClosed(ctx context.Context, env *testcore.TestEnv, sid string) bool {
	_, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

// completeRunningWorkflows signals completeSignalName to every running workflow
// of the schedule and returns the number it signaled.
func completeRunningWorkflows(ctx context.Context, t *testing.T, env *testcore.TestEnv, sid string) int {
	t.Helper()
	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	running := desc.GetInfo().GetRunningWorkflows()
	for _, wf := range running {
		_, err := env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wf.GetWorkflowId()},
			SignalName:        completeSignalName,
			Identity:          "test",
			RequestId:         uuid.NewString(),
		})
		require.NoError(t, err)
	}
	return len(running)
}

// terminalStop selects how a fired run is stopped in
// testPauseOnFailureIgnoresCancelTerminate.
type terminalStop int

const (
	stopByCancel terminalStop = iota
	stopByTerminate
)

// testPauseOnFailureIgnoresCancelTerminate verifies that manually canceling or
// terminating a fired run of a PauseOnFailure schedule does NOT pause the
// schedule. CanceledTerminatedCountAsFailures defaults to false, so a cancel or
// terminate is a routine operation, not an application failure -- matching V1.
//
// Regression guard for the CHASM HandleNexusCompletion fix: previously any
// non-COMPLETED status paused a PauseOnFailure schedule (and terminated runs
// were mislabeled FAILED), so a manual cancel/terminate would silently stop all
// future runs.
func (s *ScheduleCHASMSuite) TestPauseOnFailureCancelDoesNotPause() {
	routeToScheduler := routeToCHASMScheduler
	stop := stopByCancel
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	ctx := routeToScheduler(testcore.NewContext())

	sid := testcore.RandomizeStr("sched-pauseonfail")
	wid := testcore.RandomizeStr("sched-pauseonfail-wf")
	wt := testcore.RandomizeStr("sched-pauseonfail-wt")

	// A run that stays running until it is canceled or terminated. On
	// cancellation workflow.Sleep returns the cancellation error, which the
	// workflow returns so the run closes as CANCELED (rather than COMPLETED); a
	// terminate closes it forcefully as TERMINATED.
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, time.Hour)
	}, workflow.RegisterOptions{Name: wt})

	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:     intervalSpec(fastInterval),
		Action:   startWorkflowAction(env, wid, wt),
		Policies: &schedulepb.SchedulePolicies{PauseOnFailure: true},
	})

	describe := func() *workflowservice.DescribeScheduleResponse {
		d, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		s.Require().NoError(err)
		return d
	}

	// Wait for the schedule to fire a run that is RUNNING, and capture its actual
	// started execution (the started workflow id may differ from the configured
	// wid, e.g. a timestamp suffix, so read it from the action result rather than
	// assuming wid).
	var exec *commonpb.WorkflowExecution
	s.Require().Eventually(func() bool {
		for _, a := range describe().GetInfo().GetRecentActions() {
			if a.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				exec = a.GetStartWorkflowResult()
				return exec.GetRunId() != ""
			}
		}
		return false
	}, awaitTimeout, pollInterval, "schedule should fire a running workflow")
	runID := exec.GetRunId()

	// Stop that run the way a user would.
	wantStatus := enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
	if stop == stopByTerminate {
		wantStatus = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		_, err := env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: exec,
			Reason:            s.T().Name(),
			Identity:          "test",
		})
		s.Require().NoError(err)
	} else {
		_, err := env.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: exec,
			Identity:          "test",
			RequestId:         uuid.NewString(),
		})
		s.Require().NoError(err)
	}

	// Once the run's terminal status is recorded on the schedule,
	// HandleNexusCompletion has processed the completion -- and, in the same
	// mutation, made (or correctly declined) the pause-on-failure decision. So
	// reading Paused from the SAME describe response as the terminal status is
	// race-free: no later pause can slip in.
	var pausedAfter bool
	s.Require().Eventually(func() bool {
		d := describe()
		for _, a := range d.GetInfo().GetRecentActions() {
			if a.GetStartWorkflowResult().GetRunId() == runID && a.GetStartWorkflowStatus() == wantStatus {
				pausedAfter = d.GetSchedule().GetState().GetPaused()
				return true
			}
		}
		return false
	}, awaitTimeout, pollInterval, "run %s should reach %s and be recorded on the schedule", runID, wantStatus)

	s.Require().False(pausedAfter,
		"a %s workflow must not pause a PauseOnFailure schedule by default "+
			"(CanceledTerminatedCountAsFailures defaults to false)", wantStatus)
}

func (s *ScheduleCHASMSuite) TestPauseOnFailureTerminateDoesNotPause() {
	routeToScheduler := routeToCHASMScheduler
	stop := stopByTerminate
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	ctx := routeToScheduler(testcore.NewContext())

	sid := testcore.RandomizeStr("sched-pauseonfail")
	wid := testcore.RandomizeStr("sched-pauseonfail-wf")
	wt := testcore.RandomizeStr("sched-pauseonfail-wt")

	// A run that stays running until it is canceled or terminated. On
	// cancellation workflow.Sleep returns the cancellation error, which the
	// workflow returns so the run closes as CANCELED (rather than COMPLETED); a
	// terminate closes it forcefully as TERMINATED.
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, time.Hour)
	}, workflow.RegisterOptions{Name: wt})

	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:     intervalSpec(fastInterval),
		Action:   startWorkflowAction(env, wid, wt),
		Policies: &schedulepb.SchedulePolicies{PauseOnFailure: true},
	})

	describe := func() *workflowservice.DescribeScheduleResponse {
		d, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		s.Require().NoError(err)
		return d
	}

	// Wait for the schedule to fire a run that is RUNNING, and capture its actual
	// started execution (the started workflow id may differ from the configured
	// wid, e.g. a timestamp suffix, so read it from the action result rather than
	// assuming wid).
	var exec *commonpb.WorkflowExecution
	s.Require().Eventually(func() bool {
		for _, a := range describe().GetInfo().GetRecentActions() {
			if a.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				exec = a.GetStartWorkflowResult()
				return exec.GetRunId() != ""
			}
		}
		return false
	}, awaitTimeout, pollInterval, "schedule should fire a running workflow")
	runID := exec.GetRunId()

	// Stop that run the way a user would.
	wantStatus := enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
	if stop == stopByTerminate {
		wantStatus = enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		_, err := env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: exec,
			Reason:            s.T().Name(),
			Identity:          "test",
		})
		s.Require().NoError(err)
	} else {
		_, err := env.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: exec,
			Identity:          "test",
			RequestId:         uuid.NewString(),
		})
		s.Require().NoError(err)
	}

	// Once the run's terminal status is recorded on the schedule,
	// HandleNexusCompletion has processed the completion -- and, in the same
	// mutation, made (or correctly declined) the pause-on-failure decision. So
	// reading Paused from the SAME describe response as the terminal status is
	// race-free: no later pause can slip in.
	var pausedAfter bool
	s.Require().Eventually(func() bool {
		d := describe()
		for _, a := range d.GetInfo().GetRecentActions() {
			if a.GetStartWorkflowResult().GetRunId() == runID && a.GetStartWorkflowStatus() == wantStatus {
				pausedAfter = d.GetSchedule().GetState().GetPaused()
				return true
			}
		}
		return false
	}, awaitTimeout, pollInterval, "run %s should reach %s and be recorded on the schedule", runID, wantStatus)

	s.Require().False(pausedAfter,
		"a %s workflow must not pause a PauseOnFailure schedule by default "+
			"(CanceledTerminatedCountAsFailures defaults to false)", wantStatus)
}

func (s *ScheduleCHASMSuite) TestIdleClose() {
	t := s.T()
	routeToScheduler := routeToCHASMScheduler
	t.Run("ManualOnly", func(t *testing.T) {
		t.Parallel()
		testManualOnlyUnpausedClosesFromIdle(t, routeToScheduler)
	})
	t.Run("PauseDuringWindow", func(t *testing.T) {
		t.Parallel()
		testPauseDuringIdleWindow(t, routeToScheduler)
	})
	t.Run("BackfillBlocks", func(t *testing.T) {
		t.Parallel()
		testBackfillBlocksIdleClose(t, routeToScheduler)
	})
}

func (s *ScheduleCHASMSuite) TestPausedBehavior() {
	t := s.T()
	routeToScheduler := routeToCHASMScheduler
	t.Run("DropsCatchup", func(t *testing.T) {
		t.Parallel()
		testPausedDropsCatchup(t, routeToScheduler)
	})
	t.Run("RecentActionsAdvance", func(t *testing.T) {
		t.Parallel()
		testRecentActionsAdvanceWhilePaused(t, routeToScheduler)
	})
	t.Run("FutureActionTimesAdvance", func(t *testing.T) {
		t.Parallel()
		testFutureActionTimesAdvanceWhilePaused(t, routeToScheduler)
	})
	t.Run("BackfillDrains", func(t *testing.T) {
		testBackfillOnPausedSchedule(t, routeToScheduler)
	})
}

func (s *ScheduleCHASMSuite) TestDescribeCatchupWindowAfterCreateAndUpdate() {
	t := s.T()
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	ctx := routeToCHASMScheduler(testcontext.For(s.T()))
	sid := testcore.RandomizeStr("sched-catchup-window-desc")
	schedule := &schedulepb.Schedule{
		Spec:     intervalSpec(noOpInterval),
		Action:   startWorkflowAction(env, "catchup-window-wf", "catchup-window-wt"),
		Policies: &schedulepb.SchedulePolicies{},
		State:    &schedulepb.ScheduleState{Paused: true},
	}
	createSchedule(ctx, s.T(), env, sid, schedule)

	describe := func(t *testing.T) time.Duration {
		t.Helper()
		resp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		require.NoError(t, err)
		return resp.GetSchedule().GetPolicies().GetCatchupWindow().AsDuration()
	}
	s.Require().Equal(chasmscheduler.DefaultTweakables.DefaultCatchupWindow, describe(s.T()))

	updates := []struct {
		name     string
		window   *durationpb.Duration
		expected time.Duration
	}{
		{name: "unset", expected: chasmscheduler.DefaultTweakables.DefaultCatchupWindow},
		{name: "zero", window: durationpb.New(0), expected: chasmscheduler.DefaultTweakables.DefaultCatchupWindow},
		{name: "negative", window: durationpb.New(-time.Second), expected: chasmscheduler.DefaultTweakables.DefaultCatchupWindow},
		{name: "below minimum", window: durationpb.New(time.Second), expected: chasmscheduler.DefaultTweakables.MinCatchupWindow},
		{name: "above minimum", window: durationpb.New(time.Hour), expected: time.Hour},
	}
	for _, tc := range updates {
		t.Run(tc.name, func(t *testing.T) {
			schedule.Policies.CatchupWindow = tc.window
			_, err := env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
				Schedule:   schedule,
				Identity:   "test",
				RequestId:  uuid.NewString(),
			})
			require.NoError(t, err)
			require.Equal(t, tc.expected, describe(t))
		})
	}
}

type ScheduleV1Suite struct {
	parallelsuite.Suite[*ScheduleV1Suite]
}

type ScheduleV1SequentialSuite struct {
	parallelsuite.Suite[*ScheduleV1SequentialSuite]
}

func TestScheduleV1Suite(t *testing.T) {
	parallelsuite.Run(t, &ScheduleV1Suite{})
}

func TestScheduleV1SequentialSuite(t *testing.T) {
	parallelsuite.RunLegacySequential(t, &ScheduleV1SequentialSuite{}) //nolint:staticcheck // SA1019: mutates package-level scheduler tweakables.
}

func (s *ScheduleV1SequentialSuite) TestActionDelayMetrics() {
	routeToScheduler := routeToV1Scheduler
	// The short-refresh DesiredTime path this test exercises is gated behind
	// RefreshCompletionDesiredTime; the shipped default stays at TriggerImmediatelyTimestamp
	// until a follow-up deploy activates it. Force it on for this run (see the caller: this
	// subtest is intentionally not run with t.Parallel(), since this override is shared
	// package state).
	prevVersion := scheduler.CurrentTweakablePolicies.Version
	scheduler.CurrentTweakablePolicies.Version = scheduler.RefreshCompletionDesiredTime
	s.T().Cleanup(func() { scheduler.CurrentTweakablePolicies.Version = prevVersion })

	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sched-action-delay")
	wid := testcore.RandomizeStr("sched-action-delay-wf")
	wt := testcore.RandomizeStr("sched-action-delay-wt")

	var activityCalls atomic.Int32
	releaseManualActivity := make(chan struct{})
	releaseFirstScheduledActivity := make(chan struct{})
	activityFn := func(ctx context.Context) error {
		var release chan struct{}
		switch activityCalls.Add(1) {
		case 1:
			release = releaseManualActivity
		case 2:
			release = releaseFirstScheduledActivity
		default:
			return nil
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	env.SdkWorker().RegisterActivity(activityFn)
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
		})
		return workflow.ExecuteActivity(ctx, activityFn).Get(ctx, nil)
	}, workflow.RegisterOptions{Name: wt})

	ctx := routeToScheduler(testcontext.For(s.T()))
	metricCapture := env.StartNamespaceMetricCapture()
	interval := 10 * time.Second
	phaseOffset := 5 * time.Second
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{
					Interval: durationpb.New(interval),
					Phase:    durationpb.New(time.Duration((time.Now().Unix()+int64(phaseOffset/time.Second))%int64(interval/time.Second)) * time.Second),
				}},
			},
			Action: startWorkflowAction(env, wid, wt),
			Policies: &schedulepb.SchedulePolicies{
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
			},
			State: &schedulepb.ScheduleState{
				LimitedActions:   true,
				RemainingActions: 2,
			},
		},
		InitialPatch: triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED),
		Identity:     "test",
		RequestId:    uuid.NewString(),
	})
	s.Require().NoError(err)

	// Query the scheduler workflow directly (not DescribeSchedule, which signals a refresh and
	// would race the completion-discovery path each stage below is intended to cover).
	queryBufferAndRunning := func() (bufferSize int64, running int) {
		encoded, queryErr := env.SdkClient().QueryWorkflow(ctx, scheduler.WorkflowIDPrefix+sid, "", scheduler.QueryNameDescribe)
		if queryErr != nil {
			return -1, -1
		}
		var response schedulespb.DescribeResponse
		if encoded.Get(&response) != nil {
			return -1, -1
		}
		return response.GetInfo().GetBufferSize(), len(response.GetInfo().GetRunningWorkflows())
	}
	await.RequireTruef(s.T(), func() bool {
		bufferSize, running := queryBufferAndRunning()
		return activityCalls.Load() == 1 && bufferSize == 1 && running == 1
	}, awaitTimeout, pollInterval, "manual action should be running with the first scheduled action buffered")
	close(releaseManualActivity)

	// Hold the first scheduled action open past the second scheduled tick, so the second
	// scheduled action is buffered behind a still-running (not yet closed) action -- i.e.
	// genuinely blocked, not merely started after an unrelated, already-finished one. Note this
	// exercises DesiredTime backdating end-to-end (via whichever path discovers the completion --
	// a long-poll watcher is installed as soon as a start is buffered behind a running action, so
	// that watcher, not a later refresh, is what normally wins here). The specific
	// refresh-discovers-a-real-backlog branch (refreshBackdate == true in processWatcherResult) is
	// pinned deterministically by TestRefreshCompletionDesiredTimeBacklog in workflow_test.go,
	// which controls discovery timing directly instead of racing a real watcher.
	await.RequireTruef(s.T(), func() bool {
		bufferSize, running := queryBufferAndRunning()
		return activityCalls.Load() == 2 && bufferSize == 1 && running == 1
	}, awaitTimeout, pollInterval, "first scheduled action should be running with the second scheduled action buffered")
	close(releaseFirstScheduledActivity)

	// Do not describe the schedule while waiting. The second scheduled action must discover
	// the prior completion through processBuffer's short refresh, not a DescribeSchedule signal.
	await.RequireTruef(s.T(), func() bool {
		return activityCalls.Load() == 3 && len(metricCapture.Metric(metrics.ScheduleActionDelay.Name())) == 2
	}, awaitTimeout, pollInterval, "manual action and two scheduled actions should start")
	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	s.Require().Equal(int64(3), desc.GetInfo().GetActionCount())
	actions := desc.GetInfo().GetRecentActions()
	s.Require().Len(actions, 3)

	closeTime := func(action *schedulepb.ScheduleActionResult) time.Time {
		s.T().Helper()
		events := env.GetHistory(env.Namespace().String(), action.GetStartWorkflowResult())
		s.Require().NotEmpty(events)
		lastEvent := events[len(events)-1]
		s.Require().Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED, lastEvent.GetEventType())
		return lastEvent.GetEventTime().AsTime()
	}
	// The prior action's close only backdates the desired time when it comes after this
	// action's own scheduled time -- i.e. this action was actually blocked waiting on it.
	// Otherwise the action ran on time and the desired time stays at its scheduled time.
	expectedDesiredTime := func(priorClose time.Time, action *schedulepb.ScheduleActionResult) time.Time {
		if scheduleTime := action.GetScheduleTime().AsTime(); priorClose.After(scheduleTime) {
			return priorClose
		}
		return action.GetScheduleTime().AsTime()
	}
	recordings := metricCapture.Metric(metrics.ScheduleActionDelay.Name())
	for _, recording := range recordings {
		s.Require().Equal(metrics.ScheduleBackendLegacy, recording.Tags[metrics.ScheduleBackendTag])
	}
	s.Require().Equal(actions[1].GetActualTime().AsTime().Sub(expectedDesiredTime(closeTime(actions[0]), actions[1])), recordings[0].Value)
	s.Require().Equal(actions[2].GetActualTime().AsTime().Sub(expectedDesiredTime(closeTime(actions[1]), actions[2])), recordings[1].Value)
}

// testAllowAllDescribeContract verifies the customer-facing Describe state shared by V1 and CHASM.
// ALLOW_ALL executions appear in RecentActions but not RunningWorkflows; sequential executions remain active.
func (s *ScheduleSuite) TestAllowAllDescribeContract(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	sid := testcore.RandomizeStr("sched-allow-all-active")
	wid := testcore.RandomizeStr("sched-allow-all-active-wf")
	wt := testcore.RandomizeStr("sched-allow-all-active-wt")

	var runs atomic.Int32
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		_ = workflow.SideEffect(ctx, func(workflow.Context) any { runs.Add(1); return 0 })
		failed := false
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(workflow.GetSignalChannel(ctx, "complete"), func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
		})
		selector.AddReceive(workflow.GetSignalChannel(ctx, "fail"), func(c workflow.ReceiveChannel, _ bool) {
			c.Receive(ctx, nil)
			failed = true
		})
		selector.Select(ctx)
		if failed {
			return errors.New("allow-all failure")
		}
		return nil
	}, workflow.RegisterOptions{Name: wt})

	ctx := routeToScheduler(testcore.NewContext())
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:     &schedulepb.ScheduleSpec{},
		Action:   startWorkflowAction(env, wid, wt),
		Policies: &schedulepb.SchedulePolicies{PauseOnFailure: true},
	})

	patchSchedule(ctx, s.T(), env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
	var allowAllRun *commonpb.WorkflowExecution
	var allowAllDescribe *workflowservice.DescribeScheduleResponse
	s.Require().Eventually(func() bool {
		desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: env.Namespace().String(), ScheduleId: sid,
		})
		if err != nil || runs.Load() != 1 || desc.GetInfo().GetActionCount() != 1 ||
			desc.GetInfo().GetBufferSize() != 0 || len(desc.GetInfo().GetRecentActions()) != 1 ||
			len(desc.GetInfo().GetRunningWorkflows()) != 0 {
			return false
		}
		recent := desc.GetInfo().GetRecentActions()[0]
		if recent.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING ||
			recent.GetScheduleTime() == nil || recent.GetActualTime() == nil {
			return false
		}
		allowAllRun = recent.GetStartWorkflowResult()
		if allowAllRun.GetRunId() == "" {
			return false
		}
		allowAllDescribe = desc
		return true
	}, awaitTimeout, pollInterval, "ALLOW_ALL Describe state should be recent, running, and not active")
	s.Require().Empty(allowAllDescribe.GetInfo().GetRunningWorkflows())
	s.Require().Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, allowAllDescribe.GetInfo().GetRecentActions()[0].GetStartWorkflowStatus())

	allowAllNominal, err := time.Parse(time.RFC3339, strings.TrimPrefix(allowAllRun.GetWorkflowId(), wid+"-"))
	s.Require().NoError(err)
	s.Require().Eventually(func() bool {
		return time.Now().UTC().Truncate(time.Second).After(allowAllNominal)
	}, awaitTimeout, pollInterval, "next trigger should receive a distinct timestamp-based workflow ID")

	patchSchedule(ctx, s.T(), env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))
	var sequentialRun *commonpb.WorkflowExecution
	var sequentialDescribe *workflowservice.DescribeScheduleResponse
	s.Require().Eventually(func() bool {
		desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace: env.Namespace().String(), ScheduleId: sid,
		})
		if err != nil || runs.Load() != 2 || desc.GetInfo().GetActionCount() != 2 ||
			desc.GetInfo().GetBufferSize() != 0 || len(desc.GetInfo().GetRecentActions()) != 2 ||
			len(desc.GetInfo().GetRunningWorkflows()) != 1 {
			return false
		}
		sequentialRun = desc.GetInfo().GetRunningWorkflows()[0]
		if sequentialRun.GetRunId() == "" || sequentialRun.GetRunId() == allowAllRun.GetRunId() {
			return false
		}
		for _, recent := range desc.GetInfo().GetRecentActions() {
			if recent.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING ||
				recent.GetScheduleTime() == nil || recent.GetActualTime() == nil {
				return false
			}
		}
		sequentialDescribe = desc
		return true
	}, awaitTimeout, pollInterval, "ALLOW_ALL execution should not block a sequential trigger")
	s.Require().Equal([]*commonpb.WorkflowExecution{sequentialRun}, sequentialDescribe.GetInfo().GetRunningWorkflows())
	s.Require().ElementsMatch([]*commonpb.WorkflowExecution{allowAllRun, sequentialRun}, []*commonpb.WorkflowExecution{
		sequentialDescribe.GetInfo().GetRecentActions()[0].GetStartWorkflowResult(),
		sequentialDescribe.GetInfo().GetRecentActions()[1].GetStartWorkflowResult(),
	})

	s.Require().NoError(env.SdkClient().SignalWorkflow(ctx, allowAllRun.GetWorkflowId(), allowAllRun.GetRunId(), "fail", nil))
	s.Require().Eventually(func() bool {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(), Execution: allowAllRun,
		})
		return err == nil && resp.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	}, awaitTimeout, pollInterval, "ALLOW_ALL workflow should fail")

	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace: env.Namespace().String(), ScheduleId: sid,
	})
	s.Require().NoError(err)
	s.Require().False(desc.GetSchedule().GetState().GetPaused())
	s.Require().Equal(int64(2), desc.GetInfo().GetActionCount())
	s.Require().Zero(desc.GetInfo().GetBufferSize())
	s.Require().Equal([]*commonpb.WorkflowExecution{sequentialRun}, desc.GetInfo().GetRunningWorkflows())
	s.Require().Len(desc.GetInfo().GetRecentActions(), 2)
	for _, recent := range desc.GetInfo().GetRecentActions() {
		s.Require().Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, recent.GetStartWorkflowStatus())
	}
	s.Require().NoError(env.SdkClient().SignalWorkflow(ctx, sequentialRun.GetWorkflowId(), sequentialRun.GetRunId(), "complete", nil))
}

// testBufferSizeReportedWhenBuffered verifies that ScheduleInfo.BufferSize is
// populated by both the V1 and V2 (CHASM) schedulers when at least one fire is
// queued behind a still-running workflow. A schedule with a 1s interval and
// BUFFER_ONE keeps exactly one start in the buffer while the first workflow
// is still running.
func (s *ScheduleSuite) TestBufferSizeReportedWhenBuffered(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sched-buffer-size")
	wid := testcore.RandomizeStr("sched-buffer-size-wf")
	wt := testcore.RandomizeStr("sched-buffer-size-wt")

	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			return workflow.Sleep(ctx, time.Hour)
		},
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	ctx := routeToScheduler(testcontext.For(s.T()))
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	var lastDescribe *workflowservice.DescribeScheduleResponse
	env.Eventually(func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		lastDescribe = desc
		return desc.GetInfo().GetBufferSize() >= 1 && len(desc.GetInfo().GetRunningWorkflows()) >= 1
	}, 30*time.Second, 500*time.Millisecond, "DescribeSchedule should report BufferSize >= 1 with a running workflow blocking the buffer")

	s.Require().GreaterOrEqual(lastDescribe.GetInfo().GetBufferSize(), int64(1), "BufferSize must reflect at least one buffered start")
	s.Require().GreaterOrEqual(len(lastDescribe.GetInfo().GetRunningWorkflows()), 1, "expected the buffered fire to be queued behind a running workflow")
}

// A full buffer (MaxBufferSize reached) makes the CHASM generator drop further
// fires and increment ScheduleInfo.BufferDropped.
func (s *ScheduleCHASMSuite) TestBufferOverrunDropsActions() {
	routeToScheduler := routeToCHASMScheduler
	// A small buffer plus a gated workflow under BUFFER_ALL fills within a few
	// fast-interval ticks. Only the CHASM scheduler reads these tweakables.
	tweakables := chasmscheduler.DefaultTweakables
	tweakables.MaxBufferSize = 2
	opts := append(scheduleCommonOpts(s.T()), testcore.WithDynamicConfig(chasmscheduler.CurrentTweakables, tweakables))
	env := testcore.NewEnv(s.T(), opts...)

	sid := testcore.RandomizeStr("sched-buffer-overrun")
	wid := testcore.RandomizeStr("sched-buffer-overrun-wf")
	wt := testcore.RandomizeStr("sched-buffer-overrun-wt")

	var runs atomic.Int32
	registerGatedWorkflow(env, wt, &runs)

	ctx := routeToScheduler(s.Context()) //nolint:staticcheck // SA1019
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(env, wid, wt),
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		},
	})

	// RunningWorkflows lags the drop (it needs the async StartWorkflow to land), so
	// assert both together rather than after the wait.
	s.Require().Eventually(func() bool {
		desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if err != nil {
			return false
		}
		return desc.GetInfo().GetBufferDropped() > 0 &&
			len(desc.GetInfo().GetRunningWorkflows()) >= 1
	}, awaitTimeout, pollInterval, "expected BufferDropped > 0 with a gated workflow holding the buffer full")

	s.Require().Positive(runs.Load(), "the gated workflow should have started")

	completeRunningWorkflows(ctx, s.T(), env, sid)
}

// testRecentActionsAdvanceWhilePaused verifies that an in-flight workflow's
// completion status surfaces in ListSchedules' RecentActions even while the
// schedule is paused. CHASM-only: the live Invoker updates BufferedStart on
// Nexus completion regardless of paused state, so the listed status moves
// from RUNNING to COMPLETED. V1's scheduler workflow does not advance its
// memo while paused, so the listed status stays at RUNNING until unpause.
func testRecentActionsAdvanceWhilePaused(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-recentactions-paused")
	wid := testcore.RandomizeStr("sched-recentactions-paused-wf")
	wt := testcore.RandomizeStr("sched-recentactions-paused-wt")

	// Gate the run so the RUNNING -> COMPLETED transition is driven explicitly.
	var runs atomic.Int32
	registerGatedWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(env, wid, wt),
	})

	// Wait for the first workflow to be reported as RUNNING in ListSchedules.
	running := getScheduleEntryFromVisibility(testcontext.For(t), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.RecentActions) >= 1 &&
			ent.Info.RecentActions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	runningRunID := running.Info.RecentActions[0].GetStartWorkflowResult().GetRunId()
	require.NotEmpty(t, runningRunID)

	// Pause, then release the run: its COMPLETED status must surface while paused.
	patchSchedule(ctx, t, env, sid, &schedulepb.SchedulePatch{Pause: "pausing for the test"})

	signaled := completeRunningWorkflows(ctx, t, env, sid)
	require.Equal(t, 1, signaled, "exactly one run should be in flight under the default SKIP overlap policy")

	// While paused, the listed RecentActions entry transitions to COMPLETED.
	getScheduleEntryFromVisibility(testcontext.For(t), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		for _, a := range ent.Info.RecentActions {
			if a.GetStartWorkflowResult().GetRunId() == runningRunID &&
				a.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				return true
			}
		}
		return false
	})
	require.Equal(t, int32(1), runs.Load(), "exactly one run should have fired (SKIP overlap, then paused)")
}

// testFutureActionTimesAdvanceWhilePaused verifies that ListSchedules returns
// up-to-date FutureActionTimes for a paused schedule. CHASM's always-on
// Generator advances the high water mark and rebuilds FutureActionTimes
// against the spec even when paused, so listed times stay in the future and
// never roll into the past. The legacy V1 scheduler workflow does not advance
// while paused, so its projected times would freeze at pause time and
// eventually all sit in the past - hence this test is registered CHASM-only.
func testFutureActionTimesAdvanceWhilePaused(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-future-actions-paused")
	wid := testcore.RandomizeStr("sched-future-actions-paused-wf")
	wt := testcore.RandomizeStr("sched-future-actions-paused-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(env, wid, wt),
	})

	// Wait for visibility to surface an initial FutureActionTimes projection.
	initial := getScheduleEntryFromVisibility(testcontext.For(t), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.FutureActionTimes) > 0
	})
	initialFirst := initial.Info.FutureActionTimes[0].AsTime()

	// While still paused, the earliest projected time must advance past the
	// initial value: the Generator keeps ticking and republishing the
	// projection, even though no workflows fire.
	getScheduleEntryFromVisibility(testcontext.For(t), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.FutureActionTimes) > 0 &&
			ent.Info.FutureActionTimes[0].AsTime().After(initialFirst)
	})
	require.Zero(t, runs.Load(), "a paused schedule must not fire any workflows")
}

// testBufferOneDeferredFiresAfterCompletion exercises the BUFFER_ONE deferred
// lifecycle end-to-end. Later ticks must not displace or accumulate alongside
// the first buffered action, and that action must fire once the running workflow
// completes.
func (s *ScheduleSuite) TestBufferOneDeferredFiresAfterCompletion(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sched-buffer-one-deferred")
	wid := testcore.RandomizeStr("sched-buffer-one-deferred-wf")
	wt := testcore.RandomizeStr("sched-buffer-one-deferred-wt")

	// Gate runs so "first running, second buffered" and "deferred fires after
	// completion" are both reached deterministically.
	var runs atomic.Int32
	registerGatedWorkflow(env, wt, &runs)

	ctx := routeToScheduler(s.Context())
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(env, wid, wt),
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	})

	// Exactly one workflow runs with exactly one start buffered behind it (BUFFER_ONE caps the buffer at one).
	await.RequireTruef(s.T(), func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && desc.GetInfo().GetBufferSize() == 1 && len(desc.GetInfo().GetRunningWorkflows()) == 1
	}, awaitTimeout, pollInterval, "expected exactly one running workflow with one deferred start buffered behind it")
	s.Require().Equal(int32(1), runs.Load(), "only the first workflow should have fired before the running one completes")

	// Keep the first workflow open across several more ticks. V1 evaluates the
	// complete buffer and keeps its first entry; CHASM must do the same even after
	// that entry has been marked deferred (Attempt=-1).
	s.Require().Never(func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && (desc.GetInfo().GetBufferSize() != 1 ||
			len(desc.GetInfo().GetRunningWorkflows()) != 1 || runs.Load() != 1)
	}, 3*fastInterval, pollInterval,
		"V1 and CHASM must retain exactly one buffered occurrence while later ticks arrive")

	// Releasing the running workflow must re-enable the deferred start (Attempt=-1 -> 0) so it fires.
	s.Require().Equal(1, completeRunningWorkflows(ctx, s.T(), env, sid))
	await.RequireTruef(s.T(), func() bool { return runs.Load() == 2 },
		awaitTimeout, pollInterval,
		"deferred start must fire after the running workflow completes - regression for the Attempt=-1 -> 0 re-enable path")

	// The fire is specifically the tick buffered directly behind the first start,
	// not a fresh action generated after completion. RecentActions lists the
	// completed first tick and the still-running deferred start; with no jitter the
	// deferred start's nominal time is exactly one interval after the first tick's.
	await.RequireTruef(s.T(), func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		var first, deferred *schedulepb.ScheduleActionResult
		for _, r := range desc.GetInfo().GetRecentActions() {
			if r.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				deferred = r
			} else {
				first = r
			}
		}
		return first != nil && deferred != nil &&
			deferred.GetScheduleTime().AsTime().Sub(first.GetScheduleTime().AsTime()) == fastInterval
	}, awaitTimeout, pollInterval,
		"deferred fire must be the start buffered one interval after the first tick, not a later fresh action")
}

func (s *ScheduleSuite) TestDeletedScheduleOperations(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-deleted-ops"
	wid := "sched-test-deleted-ops-wf"
	wt := "sched-test-deleted-ops-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create a schedule.
	_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// Delete the schedule.
	_, err = env.FrontendClient().DeleteSchedule(routeToScheduler(s.Context()), &workflowservice.DeleteScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Identity:   "test",
	})
	s.Require().NoError(err)

	// Describe should return NotFound.
	var notFoundErr *serviceerror.NotFound
	env.Eventually(func() bool {
		_, descErr := env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		return errors.As(descErr, &notFoundErr)
	}, 10*time.Second, 200*time.Millisecond)

	// Update, Patch, and Delete behave differently across CHASM and V1,
	// so they are not tested here. See TestScheduleUpdateAfterDelete.
}

func (s *ScheduleSuite) TestBasics(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-basics"
	wid := "sched-test-basics-wf"
	wt := "sched-test-basics-wt"
	wt2 := "sched-test-basics-wt2"
	// switch this to test with search attribute mapper:
	// csaKeyword := "AliasForCustomKeywordField"
	csaKeyword := "CustomKeywordField"
	csaInt := "CustomIntField"
	csaBool := "CustomBoolField"

	wfMemo := payload.EncodeString("workflow memo")
	wfSAValue := payload.EncodeString("workflow sa value")
	schMemo := payload.EncodeString("schedule memo")
	schSAValue := payload.EncodeString("schedule sa value")
	schSAIntValue, _ := payload.Encode(123)
	schSABoolValue, _ := payload.Encode(true)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(5 * time.Second)},
			},
			Calendar: []*schedulepb.CalendarSpec{
				{DayOfMonth: "10", Year: "2010"},
			},
			CronString: []string{"11 11/11 11 11 1 2011"},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Memo: &commonpb.Memo{
						Fields: map[string]*commonpb.Payload{"wfmemo1": wfMemo},
					},
					SearchAttributes: &commonpb.SearchAttributes{
						IndexedFields: map[string]*commonpb.Payload{csaKeyword: wfSAValue},
					},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{"schedmemo1": schMemo},
		},
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				csaKeyword: schSAValue,
				csaInt:     schSAIntValue,
				csaBool:    schSABoolValue,
			},
		},
	}

	var runs, runs2 int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs, 1)
			return 0
		})
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})
	workflow2Fn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs2, 1)
			return 0
		})
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflow2Fn, workflow.RegisterOptions{Name: wt2})

	// create

	ctx := routeToScheduler(s.Context())
	createTime := time.Now()
	_, err := env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().NoError(err)

	// describe immediately after create and verify FutureActionTimes
	describeRespAfterCreate, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	env.NotEmpty(describeRespAfterCreate.Info.FutureActionTimes, "FutureActionTimes should be set immediately after create")
	// FutureActionTimes should be in the future (after createTime) and aligned to 5-second intervals
	for i, fat := range describeRespAfterCreate.Info.FutureActionTimes {
		env.True(fat.AsTime().After(createTime) || fat.AsTime().Equal(createTime),
			"FutureActionTimes[%d] should be >= createTime", i)
		env.Equal(int64(0), fat.AsTime().UnixNano()%int64(5*time.Second),
			"FutureActionTimes[%d] should be aligned to 5-second intervals", i)
	}

	// sleep until we see two runs, plus a bit more to ensure that the second run has completed
	env.Eventually(func() bool { return atomic.LoadInt32(&runs) == 2 }, 15*time.Second, 500*time.Millisecond)
	time.Sleep(2 * time.Second) //nolint:forbidigo

	// wait for visibility to stabilize on completed before calling describe,
	// otherwise their recent actions may flake and differ

	visibilityResponse := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		recentActions := ent.GetInfo().GetRecentActions()
		return len(recentActions) >= 2 && recentActions[1].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	})

	describeResp, err := env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)

	// validate describe response

	checkSpec := func(spec *schedulepb.ScheduleSpec) {
		protorequire.ProtoSliceEqual(s.T(), schedule.Spec.Interval, spec.Interval)
		env.Nil(spec.Calendar)
		env.Nil(spec.CronString)
		env.ProtoElementsMatch([]*schedulepb.StructuredCalendarSpec{
			{
				Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Minute:     []*schedulepb.Range{{Start: 11, End: 11, Step: 1}},
				Hour:       []*schedulepb.Range{{Start: 11, End: 23, Step: 11}},
				DayOfMonth: []*schedulepb.Range{{Start: 11, End: 11, Step: 1}},
				Month:      []*schedulepb.Range{{Start: 11, End: 11, Step: 1}},
				DayOfWeek:  []*schedulepb.Range{{Start: 1, End: 1, Step: 1}},
				Year:       []*schedulepb.Range{{Start: 2011, End: 2011, Step: 1}},
			},
			{
				Second:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Minute:     []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				Hour:       []*schedulepb.Range{{Start: 0, End: 0, Step: 1}},
				DayOfMonth: []*schedulepb.Range{{Start: 10, End: 10, Step: 1}},
				Month:      []*schedulepb.Range{{Start: 1, End: 12, Step: 1}},
				DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
				Year:       []*schedulepb.Range{{Start: 2010, End: 2010, Step: 1}},
			},
		}, spec.StructuredCalendar)
	}
	checkSpec(describeResp.Schedule.Spec)

	env.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, describeResp.Schedule.Policies.OverlapPolicy)            // set to default value
	s.Require().InDelta(365*24*3600, describeResp.Schedule.Policies.CatchupWindow.AsDuration().Seconds(), 0) // set to default value

	env.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csaKeyword].Data)
	env.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	env.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	env.Nil(describeResp.SearchAttributes.IndexedFields[sadefs.BinaryChecksums])
	env.Nil(describeResp.SearchAttributes.IndexedFields[sadefs.BuildIds])
	env.Nil(describeResp.SearchAttributes.IndexedFields[sadefs.TemporalNamespaceDivision])
	env.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	env.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csaKeyword].Data)
	env.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	// GreaterOrEqual is used as we may have had other runs start while waiting for visibility
	durationNear(s.T(), describeResp.Info.CreateTime.AsTime().Sub(createTime), 0)
	s.Require().GreaterOrEqual(describeResp.Info.ActionCount, int64(2))
	s.Require().EqualValues(0, describeResp.Info.MissedCatchupWindow)
	s.Require().EqualValues(0, describeResp.Info.OverlapSkipped)
	s.Require().GreaterOrEqual(len(describeResp.Info.RecentActions), 2)
	action0 := describeResp.Info.RecentActions[0]
	env.WithinRange(action0.ScheduleTime.AsTime(), createTime, time.Now())
	env.Equal(int64(0), action0.ScheduleTime.AsTime().UnixNano()%int64(5*time.Second))
	durationNear(s.T(), action0.ActualTime.AsTime().Sub(action0.ScheduleTime.AsTime()), 0)

	// validate list response

	env.Equal(sid, visibilityResponse.ScheduleId)
	env.Equal(schSAValue.Data, visibilityResponse.SearchAttributes.IndexedFields[csaKeyword].Data)
	env.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	env.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	env.Nil(visibilityResponse.SearchAttributes.IndexedFields[sadefs.BinaryChecksums])
	env.Nil(visibilityResponse.SearchAttributes.IndexedFields[sadefs.BuildIds])
	env.Nil(visibilityResponse.SearchAttributes.IndexedFields[sadefs.TemporalNamespaceDivision])
	env.Equal(schMemo.Data, visibilityResponse.Memo.Fields["schedmemo1"].Data)
	checkSpec(visibilityResponse.Info.Spec)
	env.Equal(wt, visibilityResponse.Info.WorkflowType.Name)
	env.False(visibilityResponse.Info.Paused)
	assertSameRecentActions(s.T(), describeResp, visibilityResponse)
	assertRecentActionsNoDuplicateRunIDs(s.T(), describeResp.Info.RecentActions)

	// list workflows

	wfResp, err := env.FrontendClient().ListWorkflowExecutions(routeToScheduler(s.Context()), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: env.Namespace().String(),
		PageSize:  5,
		Query:     "",
	})
	s.Require().NoError(err)
	s.Require().GreaterOrEqual(len(wfResp.Executions), 2) // could have had a 3rd run while waiting for visibility
	for _, ex := range wfResp.Executions {
		env.Equal(wt, ex.Type.Name, "should only see started workflows")
	}
	ex0 := wfResp.Executions[0]
	env.True(strings.HasPrefix(ex0.Execution.WorkflowId, wid))
	matchingRunId := false
	for _, recentAction := range describeResp.GetInfo().GetRecentActions() {
		if ex0.GetExecution().GetRunId() == recentAction.GetStartWorkflowResult().GetRunId() {
			matchingRunId = true
			break
		}
	}
	env.True(matchingRunId, "ListWorkflowExecutions returned a run ID wasn't in the describe response")
	env.Equal(wt, ex0.Type.Name)
	env.Nil(ex0.ParentExecution) // not a child workflow
	env.Equal(wfMemo.Data, ex0.Memo.Fields["wfmemo1"].Data)
	env.Equal(wfSAValue.Data, ex0.SearchAttributes.IndexedFields[csaKeyword].Data)
	env.Equal(payload.EncodeString(sid).Data, ex0.SearchAttributes.IndexedFields[sadefs.TemporalScheduledById].Data)
	var ex0StartTime time.Time
	s.Require().NoError(payload.Decode(ex0.SearchAttributes.IndexedFields[sadefs.TemporalScheduledStartTime], &ex0StartTime))
	env.WithinRange(ex0StartTime, createTime, time.Now())
	env.Equal(int64(0), ex0StartTime.UnixNano()%int64(5*time.Second))

	// list schedules with search attribute filter

	listResp, err := env.FrontendClient().ListSchedules(routeToScheduler(s.Context()), &workflowservice.ListSchedulesRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 5,
		Query:           "CustomKeywordField = 'schedule sa value' AND TemporalSchedulePaused = false",
	})
	s.Require().NoError(err)
	env.Len(listResp.Schedules, 1)
	entry := listResp.Schedules[0]
	env.Equal(sid, entry.ScheduleId)

	// list schedules with invalid search attribute filter

	_, err = env.FrontendClient().ListSchedules(routeToScheduler(s.Context()), &workflowservice.ListSchedulesRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 5,
		Query:           "ExecutionDuration > '1s'",
	})
	s.Require().Error(err)

	// update schedule, no updates to search attributes

	schedule.Spec.Interval[0].Phase = durationpb.New(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	updateTime := time.Now()
	_, err = env.FrontendClient().UpdateSchedule(routeToScheduler(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// wait for one new run
	env.Eventually(
		func() bool { return atomic.LoadInt32(&runs2) == 1 },
		7*time.Second,
		500*time.Millisecond,
	)

	// describe again
	describeResp, err = env.FrontendClient().DescribeSchedule(
		routeToScheduler(s.Context()),
		&workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		},
	)
	s.Require().NoError(err)

	env.Len(describeResp.SearchAttributes.GetIndexedFields(), 3)
	env.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csaKeyword].Data)
	env.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	env.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	env.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	env.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csaKeyword].Data)
	env.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	durationNear(s.T(), describeResp.Info.UpdateTime.AsTime().Sub(updateTime), 0)
	lastAction := describeResp.Info.RecentActions[len(describeResp.Info.RecentActions)-1]
	env.Equal(int64(1000000000), lastAction.ScheduleTime.AsTime().UnixNano()%int64(5*time.Second), lastAction.ScheduleTime.AsTime().UnixNano())

	// update schedule and search attributes

	schedule.Spec.Interval[0].Phase = durationpb.New(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	csaDouble := "CustomDoubleField"
	schSADoubleValue, _ := payload.Encode(3.14)
	schSAIntValue, _ = payload.Encode(321)
	_, err = env.FrontendClient().UpdateSchedule(routeToScheduler(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				csaKeyword: schSAValue,       // same key, same value
				csaInt:     schSAIntValue,    // same key, new value
				csaDouble:  schSADoubleValue, // new key
				// csaBool is removed
			},
		},
	})
	s.Require().NoError(err)

	// wait until search attributes are updated
	env.EventuallyWithT(
		func(c *assert.CollectT) {
			describeResp, err = env.FrontendClient().DescribeSchedule(
				routeToScheduler(s.Context()),
				&workflowservice.DescribeScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: sid,
				},
			)
			require.NoError(c, err)
			require.Len(c, describeResp.SearchAttributes.GetIndexedFields(), 3)
			require.Equal(c, schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csaKeyword].Data)
			require.Equal(c, schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
			require.Equal(c, schSADoubleValue.Data, describeResp.SearchAttributes.IndexedFields[csaDouble].Data)
			require.NotContains(c, describeResp.SearchAttributes.IndexedFields, csaBool)
		},
		2*time.Second,
		500*time.Millisecond,
	)

	// update schedule and unset search attributes

	schedule.Spec.Interval[0].Phase = durationpb.New(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	_, err = env.FrontendClient().UpdateSchedule(routeToScheduler(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:        env.Namespace().String(),
		ScheduleId:       sid,
		Schedule:         schedule,
		Identity:         "test",
		RequestId:        uuid.NewString(),
		SearchAttributes: &commonpb.SearchAttributes{},
	})
	s.Require().NoError(err)

	// wait until search attributes are updated
	env.EventuallyWithT(
		func(c *assert.CollectT) {
			describeResp, err = env.FrontendClient().DescribeSchedule(
				routeToScheduler(s.Context()),
				&workflowservice.DescribeScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: sid,
				},
			)
			require.NoError(c, err)
			require.Empty(c, describeResp.SearchAttributes.GetIndexedFields())
		},
		5*time.Second,
		500*time.Millisecond,
	)

	// pause

	_, err = env.FrontendClient().PatchSchedule(routeToScheduler(s.Context()), &workflowservice.PatchScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			Pause: "because I said so",
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	time.Sleep(7 * time.Second) //nolint:forbidigo
	s.Require().EqualValues(1, atomic.LoadInt32(&runs2), "has not run again")

	describeResp, err = env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)

	env.True(describeResp.Schedule.State.Paused)
	env.Equal("because I said so", describeResp.Schedule.State.Notes)

	// don't loop to wait for visibility, we already waited 7s from the patch
	listResp, err = env.FrontendClient().ListSchedules(routeToScheduler(s.Context()), &workflowservice.ListSchedulesRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 5,
	})
	s.Require().NoError(err)
	env.Len(listResp.Schedules, 1)
	entry = listResp.Schedules[0]
	env.Equal(sid, entry.ScheduleId)
	env.True(entry.Info.Paused)
	env.Equal("because I said so", entry.Info.Notes)

	// finally delete

	_, err = env.FrontendClient().DeleteSchedule(routeToScheduler(s.Context()), &workflowservice.DeleteScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Identity:   "test",
	})
	s.Require().NoError(err)

	describeResp, err = env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	var notFoundErr *serviceerror.NotFound
	env.ErrorAs(err, &notFoundErr)

	env.Eventually(func() bool { // wait for visibility
		listResp, err := env.FrontendClient().ListSchedules(routeToScheduler(s.Context()), &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 5,
		})
		s.Require().NoError(err)
		return len(listResp.Schedules) == 0
	}, 10*time.Second, 1*time.Second)
}

func (s *ScheduleSuite) TestInput(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-input"
	wid := "sched-test-input-wf"
	wt := "sched-test-input-wt"

	type myData struct {
		Stuff  string
		Things []int
	}

	input1 := &myData{
		Stuff:  "here's some data",
		Things: []int{7, 8, 9},
	}
	input2 := map[int]float64{11: 1.4375}
	inputPayloads, err := payloads.Encode(input1, input2)
	s.Require().NoError(err)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:        inputPayloads,
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	var runs atomic.Int32
	workflowFn := func(ctx workflow.Context, arg1 *myData, arg2 map[int]float64) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			env.Equal(*input1, *arg1)
			env.Equal(input2, arg2)
			runs.Add(1)
			return 0
		})
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	ctx := routeToScheduler(s.Context())
	_, err = env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().NoError(err)

	env.Eventually(func() bool { return runs.Load() == 1 }, 8*time.Second, 200*time.Millisecond)
}

func (s *ScheduleSuite) TestLastCompletionAndError(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-last"
	wid := "sched-test-last-wf"
	wt := "sched-test-last-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	runs := make(map[string]struct{})
	var testComplete atomic.Int32

	workflowFn := func(ctx workflow.Context) (string, error) {
		var num int
		_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs[workflow.GetInfo(ctx).WorkflowExecution.ID] = struct{}{}
			return len(runs)
		}).Get(&num)

		var lcr string
		if workflow.HasLastCompletionResult(ctx) {
			s.Require().NoError(workflow.GetLastCompletionResult(ctx, &lcr))
		}

		lastErr := workflow.GetLastError(ctx)

		switch num {
		case 1:
			env.Empty(lcr)
			s.Require().NoError(lastErr)
			return "this one succeeds", nil
		case 2:
			s.Require().NoError(lastErr)
			env.Equal("this one succeeds", lcr)
			return "", errors.New("this one fails")
		case 3:
			env.Equal("this one succeeds", lcr)
			env.ErrorContains(lastErr, "this one fails")
			testComplete.Store(1)
			return "done", nil
		default:
			panic("shouldn't be running anymore")
		}
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().NoError(err)

	env.Eventually(func() bool { return testComplete.Load() == 1 }, 20*time.Second, 200*time.Millisecond)
}

// testScheduleContinuesAfterWorkflowRetryFailure verifies a schedule keeps firing actions
// after a scheduled workflow exhausts its retry policy and fails.
func (s *ScheduleSuite) TestScheduleContinuesAfterWorkflowRetryFailure(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	// Recording FAILED actions across the workflow's retry chain relies on the scheduler matching
	// completions by the request ID carried in the completion callback token (which survives the new
	// runs created by retries). That requires the envelope token format, which is gated off by default
	// for safe rollout, so enable it explicitly here.
	opts := append(scheduleCommonOpts(s.T()), testcore.WithDynamicConfig(callback.EncodeInternalTokenWithEnvelope, true))
	env := newScheduleEnv(s.T(), opts...)

	sid := testcore.RandomizeStr("sched-retry-fail")
	wid := testcore.RandomizeStr("sched-retry-fail-wf")
	wt := testcore.RandomizeStr("sched-retry-fail-wt")

	var sawRetry atomic.Int32
	workflowFn := func(ctx workflow.Context) error {
		if workflow.GetInfo(ctx).Attempt > 1 {
			sawRetry.Store(1)
		}
		return errors.New("intentional failure to force a retry")
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					RetryPolicy: &commonpb.RetryPolicy{
						InitialInterval:    durationpb.New(1 * time.Second),
						BackoffCoefficient: 1.0,
						MaximumAttempts:    2,
					},
				},
			},
		},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// Two FAILED actions proves the schedule kept firing past the first retry-failure.
	var failedActions int
	var lastDescribe *workflowservice.DescribeScheduleResponse
	env.Eventually(func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		lastDescribe = desc
		failedActions = 0
		for _, a := range desc.GetInfo().GetRecentActions() {
			if a.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED {
				failedActions++
			}
		}
		return sawRetry.Load() == 1 && failedActions >= 2
	}, 30*time.Second, 500*time.Millisecond,
		"schedule should keep recording FAILED actions after the workflow retry-fails")

	env.Equal(int32(1), sawRetry.Load(), "scheduled workflow should have retried (attempt > 1)")
	s.Require().GreaterOrEqual(failedActions, 2, "schedule should record multiple retry-failed actions")
	s.Require().GreaterOrEqual(lastDescribe.GetInfo().GetActionCount(), int64(2))
	env.False(lastDescribe.GetSchedule().GetState().GetPaused(), "a retry-failed workflow must not pause the schedule")
}

// testScheduledWorkflowContinueAsNewCompletion validates that the CHASM scheduler observes the
// completion of a scheduled workflow that continues-as-new before completing. The scheduler matches
// completions by the request ID on the Nexus completion callback it attaches at start; if that ID is
// lost across continue-as-new the completion is silently dropped, so with a buffering overlap policy
// the scheduler believes the action is still running and never starts the buffered actions.
//
// It asserts the schedule records several COMPLETED actions (the buffer only drains as completions
// are observed) and that the completion callback is written intact into both the original and the
// continued-as-new run. CHASM-only: V1 uses no Nexus completion callbacks.
func (s *ScheduleCHASMSuite) TestScheduledWorkflowContinueAsNewCompletion() {
	routeToScheduler := routeToCHASMScheduler
	// The scheduler matches the continued-as-new run's completion by the request ID carried in the
	// completion callback token, which only survives continue-as-new in the envelope token format.
	// That format is gated off by default for safe rollout, so enable it explicitly here.
	opts := append(scheduleCommonOpts(s.T()), testcore.WithDynamicConfig(callback.EncodeInternalTokenWithEnvelope, true))
	env := newScheduleEnv(s.T(), opts...)

	sid := testcore.RandomizeStr("sched-can-completion")
	wid := testcore.RandomizeStr("sched-can-completion-wf")
	wt := testcore.RandomizeStr("sched-can-completion-wt")

	// Continue-as-new once, then complete: the completion is delivered from the continued run,
	// exercising completion-callback propagation across continue-as-new.
	workflowFn := func(ctx workflow.Context) error {
		if workflow.GetInfo(ctx).ContinuedExecutionRunID == "" {
			return workflow.NewContinueAsNewError(ctx, wt)
		}
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Second)},
			},
		},
		// BUFFER_ALL gates each start on the previous action completing. If a completion is dropped
		// because request ID is not carried over on continue-as-newthe scheduler believes the action
		// is still running and the buffered actions never start, so only the first action ever completes.
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().NoError(err)

	// getCompletionCallback returns the Nexus completion callback the scheduler attached, found in the
	// run's WorkflowExecutionStarted event.
	getCompletionCallback := func(events []*historypb.HistoryEvent) *commonpb.Callback {
		for _, e := range events {
			for _, cb := range e.GetWorkflowExecutionStartedEventAttributes().GetCompletionCallbacks() {
				if cb.GetNexus().GetUrl() == chasm.NexusCompletionHandlerURL {
					return cb
				}
			}
		}
		return nil
	}

	// The scheduler only starts the next buffered action after observing the previous one complete,
	// so it records multiple COMPLETED actions only if continue-as-new completions are delivered. With
	// the bug it stalls after the first. (StartWorkflowStatus is set solely from the completion
	// callback the scheduler records, not from visibility.)
	const wantCompleted = 3
	var completedWFID string
	env.Eventually(func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		completed := 0
		for _, a := range desc.GetInfo().GetRecentActions() {
			if a.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				completed++
				completedWFID = a.GetStartWorkflowResult().GetWorkflowId()
			}
		}
		return completed >= wantCompleted
	}, 15*time.Second, 200*time.Millisecond,
		"scheduler should record %d completed actions", wantCompleted)

	// Verify the completion callback was written into both runs of a completed action: the
	// continued-as-new run (latest) and the original run it continued from. The header (callback
	// token) must be identical, confirming the callback was propagated intact across continue-as-new.
	env.NotEmpty(completedWFID)
	canHist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: completedWFID})
	canCB := getCompletionCallback(canHist)
	env.NotNil(canCB, "continued-as-new run must carry the completion callback")

	var continuedFromRunID string
	for _, e := range canHist {
		if a := e.GetWorkflowExecutionStartedEventAttributes(); a != nil {
			continuedFromRunID = a.GetContinuedExecutionRunId()
			break
		}
	}
	env.NotEmpty(continuedFromRunID, "completed action should have continued-as-new")

	origHist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: completedWFID, RunId: continuedFromRunID})
	origCB := getCompletionCallback(origHist)
	env.NotNil(origCB, "original run must carry the completion callback")
	env.Equal(origCB.GetNexus().GetHeader(), canCB.GetNexus().GetHeader(), "completion callback must be propagated intact across continue-as-new")
}

func (s *ScheduleSuite) TestListSchedulesReturnsWorkflowStatus(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-list-running"
	wid := "sched-test-list-running-wf"
	wt := "sched-test-list-running-wt"

	// Set up a schedule that immediately starts a single running workflow
	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	patch := &schedulepb.SchedulePatch{
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
	}

	// The workflow sits open until we've asserted it can be listed as running
	resumeSignal := "resume"
	workflowFn := func(ctx workflow.Context) error {
		selector := workflow.NewSelector(ctx)
		selector.AddReceive(workflow.GetSignalChannel(ctx, resumeSignal), func(c workflow.ReceiveChannel, more bool) {
			// nothing to do
		})
		selector.Select(ctx)
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	req := &workflowservice.CreateScheduleRequest{
		Namespace:    env.Namespace().String(),
		ScheduleId:   sid,
		Schedule:     schedule,
		InitialPatch: patch,
		RequestId:    uuid.NewString(),
	}
	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().NoError(err)

	// validate RecentActions made it to visibility
	listResp := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(listResp *schedulepb.ScheduleListEntry) bool {
		return len(listResp.Info.RecentActions) >= 1
	})
	env.Len(listResp.Info.RecentActions, 1)

	a1 := listResp.Info.RecentActions[0]
	env.True(strings.HasPrefix(a1.StartWorkflowResult.WorkflowId, wid))
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, a1.StartWorkflowStatus)

	// let the started workflow complete
	_, err = env.FrontendClient().SignalWorkflowExecution(routeToScheduler(s.Context()), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: a1.StartWorkflowResult.WorkflowId,
			RunId:      a1.StartWorkflowResult.RunId,
		},
		SignalName: resumeSignal,
	})
	s.Require().NoError(err)

	// now wait for second recent action to land in visbility
	listResp = getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(listResp *schedulepb.ScheduleListEntry) bool {
		return len(listResp.Info.RecentActions) >= 2
	})

	a1 = listResp.Info.RecentActions[0]
	a2 := listResp.Info.RecentActions[1]
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, a1.StartWorkflowStatus)
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, a2.StartWorkflowStatus)

	// Also verify that DescribeSchedule's output matches
	descResp, err := env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	assertSameRecentActions(s.T(), descResp, listResp)

	// Verify no duplicate RunIds in recent actions (regression for migration dedup bug).
	assertRecentActionsNoDuplicateRunIDs(s.T(), descResp.Info.RecentActions)
	assertRecentActionsNoDuplicateRunIDs(s.T(), listResp.Info.RecentActions)
}

// testListSchedulesRecentActionsCapped verifies that RecentActions on the
// ListSchedules visibility memo stay hard-capped, independent of the (larger)
// window DescribeSchedule reports. The memo is persisted to visibility, so it
// must stay bounded no matter how many actions the schedule takes. Both the V1
// and V2 (CHASM) schedulers apply this cap, so it runs as a shared test.
func (s *ScheduleSuite) TestListSchedulesRecentActionsCapped(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	ctx := routeToScheduler(testcore.NewContext())

	// The list memo caps RecentActions at this many entries (V1
	// RecentActionCountForList / V2 recentActionCountForList).
	const memoCap = 5

	sid := testcore.RandomizeStr("sched-list-cap")
	wid := testcore.RandomizeStr("sched-list-cap-wf")
	wt := testcore.RandomizeStr("sched-list-cap-wt")

	// A workflow that returns immediately, so actions accrue quickly as completed
	// recent actions; a fast interval fires more than the memo cap in short order.
	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(env, wid, wt),
	})

	// DescribeSchedule reports a wider recent-action window than the memo. Wait
	// until it exceeds the cap, proving the two projections are independent.
	var describeResp *workflowservice.DescribeScheduleResponse
	s.Require().Eventually(func() bool {
		var err error
		describeResp, err = env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		return err == nil && len(describeResp.GetInfo().GetRecentActions()) > memoCap
	}, awaitTimeout, pollInterval, "DescribeSchedule should report more than %d recent actions", memoCap)

	// The ListSchedules memo must stay capped even though more actions exist.
	listResp := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.GetInfo().GetRecentActions()) >= memoCap
	})
	s.Require().Len(listResp.Info.RecentActions, memoCap,
		"ListSchedules memo RecentActions must be capped at %d (Describe reported %d)",
		memoCap, len(describeResp.Info.RecentActions))
}

func (s *ScheduleSuite) TestUpdateIntervalTakesEffect(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-update-interval"
	wid := "sched-test-update-interval-wf"
	wt := "sched-test-update-interval-wt"

	var runs atomic.Int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs.Add(1)
			return 0
		})
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	// Create schedule with a long interval (300s) - won't fire for 5 minutes.
	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(300 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// Update the interval to be very short (1s).
	schedule.Spec.Interval[0].Interval = durationpb.New(1 * time.Second)
	_, err = env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// After updating to 1s interval, we should see runs start within a few seconds.
	env.Eventually(
		func() bool { return runs.Load() >= 2 },
		10*time.Second,
		500*time.Millisecond,
		"expected at least 2 runs within 10s after updating interval to 1s",
	)
}

func (s *ScheduleSuite) TestListScheduleMatchingTimes(chasmEnabled bool) {
	t := s.T()
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-list-matching-times"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			StartTime: &timestamppb.Timestamp{},
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-list-matching-times",
					WorkflowType: &commonpb.WorkflowType{Name: "action"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().NoError(err)

	// Query for matching times over a 5-hour window.
	now := time.Now().UTC().Truncate(time.Hour).Add(time.Hour) // Start of next hour
	startTime := timestamppb.New(now)
	endTime := timestamppb.New(now.Add(5 * time.Hour))

	for _, tc := range []struct {
		name          string
		startTime     *timestamppb.Timestamp
		endTime       *timestamppb.Timestamp
		expectedTimes int
		errorMessage  string
	}{
		{
			name:          "valid range",
			startTime:     startTime,
			endTime:       endTime,
			expectedTimes: 5,
		},
		{
			name:      "epoch range",
			startTime: &timestamppb.Timestamp{},
			endTime:   &timestamppb.Timestamp{},
		},
		{
			name:         "invalid start time",
			startTime:    &timestamppb.Timestamp{Nanos: 1_000_000_000},
			endTime:      endTime,
			errorMessage: "start time is not a valid timestamp",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := env.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
				StartTime:  tc.startTime,
				EndTime:    tc.endTime,
			})
			if tc.errorMessage != "" {
				var invalidArgument *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgument)
				require.ErrorContains(t, err, tc.errorMessage)
				return
			}

			require.NoError(t, err)
			require.Len(t, resp.GetStartTime(), tc.expectedTimes)
		})
	}
}

func (s *ScheduleSuite) TestLimitMemoSpecSize(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	expectedLimit := scheduler.CurrentTweakablePolicies.SpecFieldLengthLimit

	sid := "sched-test-limit-memo-size"
	wid := "sched-test-limit-memo-size-wf"
	wt := "sched-test-limit-memo-size-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Set up a schedule with a large number of spec items that should be trimmed in
	// the memo block.
	for i := 0; i < expectedLimit*2; i++ {
		schedule.Spec.Interval = append(schedule.Spec.Interval, &schedulepb.IntervalSpec{
			Interval: durationpb.New(time.Duration(i+1) * time.Second),
		})
		schedule.Spec.StructuredCalendar = append(schedule.Spec.StructuredCalendar, &schedulepb.StructuredCalendarSpec{
			Minute: []*schedulepb.Range{
				{
					Start: int32(i + 1),
					End:   int32(i + 1),
				},
			},
		})
		schedule.Spec.ExcludeStructuredCalendar = append(schedule.Spec.ExcludeStructuredCalendar, &schedulepb.StructuredCalendarSpec{
			Second: []*schedulepb.Range{
				{
					Start: int32(i + 1),
					End:   int32(i + 1),
				},
			},
		})
	}

	// Create the schedule.
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)
	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().NoError(err)

	// Verify the memo field length limit was enforced.
	entry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, nil)
	s.Require().NotNil(entry)
	spec := entry.GetInfo().GetSpec()
	s.Require().Len(spec.GetInterval(), expectedLimit)
	s.Require().Len(spec.GetStructuredCalendar(), expectedLimit)
	s.Require().Len(spec.GetExcludeStructuredCalendar(), expectedLimit)
}

func (s *ScheduleSuite) TestCountSchedules(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	// Create multiple schedules with different paused states
	sidPrefix := "sched-test-count-"
	wid := "sched-test-count-wf"
	wt := "sched-test-count-wt"

	// Create 3 schedules: 2 active, 1 paused
	for i := range 3 {
		sid := fmt.Sprintf("%s%d", sidPrefix, i)
		paused := i == 2 // Third schedule is paused

		schedule := &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   fmt.Sprintf("%s-%d", wid, i),
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
			State: &schedulepb.ScheduleState{
				Paused: paused,
			},
		}

		_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), &workflowservice.CreateScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		s.Require().NoError(err)
	}

	// Test basic count (all schedules)
	env.Eventually(func() bool {
		countResp, err := env.FrontendClient().CountSchedules(routeToScheduler(s.Context()), &workflowservice.CountSchedulesRequest{
			Namespace: env.Namespace().String(),
		})
		return err == nil && countResp.Count >= 3
	}, 15*time.Second, 1*time.Second, "Expected at least 3 schedules")

	// Test count with query filter for paused schedules
	env.Eventually(func() bool {
		countResp, err := env.FrontendClient().CountSchedules(routeToScheduler(s.Context()), &workflowservice.CountSchedulesRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("%s = true", sadefs.TemporalSchedulePaused),
		})
		return err == nil && countResp.Count >= 1
	}, 15*time.Second, 1*time.Second, "Expected at least 1 paused schedule")

	// Test count with query filter for non-paused schedules
	env.Eventually(func() bool {
		countResp, err := env.FrontendClient().CountSchedules(routeToScheduler(s.Context()), &workflowservice.CountSchedulesRequest{
			Namespace: env.Namespace().String(),
			Query:     fmt.Sprintf("%s = false", sadefs.TemporalSchedulePaused),
		})
		return err == nil && countResp.Count >= 2
	}, 15*time.Second, 1*time.Second, "Expected at least 2 non-paused schedules")
}

func (s *ScheduleSuite) TestListSchedulesPagination(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	const numSchedules = 4
	sidPrefix := "sched-test-pagination-"

	for i := range numSchedules {
		sid := fmt.Sprintf("%s%d", sidPrefix, i)
		schedule := &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   fmt.Sprintf("wf-pagination-%d", i),
						WorkflowType: &commonpb.WorkflowType{Name: "action"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), &workflowservice.CreateScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		s.Require().NoError(err)
	}

	// Wait for all schedules to be visible.
	env.Eventually(func() bool {
		countResp, err := env.FrontendClient().CountSchedules(routeToScheduler(s.Context()), &workflowservice.CountSchedulesRequest{
			Namespace: env.Namespace().String(),
		})
		return err == nil && countResp.Count >= numSchedules
	}, 15*time.Second, 1*time.Second, "Expected all schedules to be visible")

	// Paginate with page size 2 and collect all schedule IDs.
	ctx := routeToScheduler(s.Context())
	var allIDs []string
	var nextPageToken []byte
	for {
		resp, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 2,
			NextPageToken:   nextPageToken,
		})
		s.Require().NoError(err)
		for _, entry := range resp.Schedules {
			allIDs = append(allIDs, entry.ScheduleId)
		}
		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
		// Each page except possibly the last should have entries.
		env.NotEmpty(resp.Schedules)
	}

	// Verify we found all created schedules.
	for i := range numSchedules {
		sid := fmt.Sprintf("%s%d", sidPrefix, i)
		env.Contains(allIDs, sid, "Expected schedule %s in paginated results", sid)
	}
}

func (s *ScheduleSuite) TestListSchedulesFilterAndEntryFields(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-list-fields"
	wt := "sched-test-list-fields-wt"

	schMemo, _ := payload.Encode("memo value")
	csaKeyword := "CustomKeywordField"
	schSAValue, _ := payload.Encode("sa-val")

	// Create a paused schedule with memo and custom search attributes.
	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-" + sid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		State: &schedulepb.ScheduleState{
			Paused: true,
			Notes:  "paused for test",
		},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"schedmemo1": schMemo,
			},
		},
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				csaKeyword: schSAValue,
			},
		},
	})
	s.Require().NoError(err)

	// Wait for the schedule to appear with correct paused state.
	entry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(e *schedulepb.ScheduleListEntry) bool {
		return e.Info.Paused
	})

	// Verify entry-level fields.
	env.Equal(schMemo.Data, entry.Memo.Fields["schedmemo1"].Data)
	env.Equal(schSAValue.Data, entry.SearchAttributes.IndexedFields[csaKeyword].Data)
	env.Equal(wt, entry.Info.WorkflowType.Name)
	env.True(entry.Info.Paused)
	env.Equal("paused for test", entry.Info.Notes)

	// Filter by TemporalSchedulePaused should find this schedule.
	s.Await(func(s *ScheduleSuite) {
		listResp, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 10,
			Query:           fmt.Sprintf("%s = true", sadefs.TemporalSchedulePaused),
		})
		s.Require().NoError(err)
		var ids []string
		for _, e := range listResp.Schedules {
			ids = append(ids, e.ScheduleId)
		}
		s.Require().Contains(ids, sid)
	}, 15*time.Second, 1*time.Second)

	// Filter for paused=false should not include this schedule.
	s.Await(func(s *ScheduleSuite) {
		listResp, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 10,
			Query:           fmt.Sprintf("%s = false", sadefs.TemporalSchedulePaused),
		})
		s.Require().NoError(err)
		for _, e := range listResp.Schedules {
			s.Require().NotEqual(sid, e.ScheduleId)
		}
	}, 15*time.Second, 1*time.Second)
}

func (s *ScheduleSuite) TestListSchedulesFilterByScheduleId(chasmEnabled bool) {
	t := s.T()
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid1 := "sched-filter-by-id-alpha"
	sid2 := "sched-filter-by-id-beta"

	schedule := func(sid string) *schedulepb.Schedule {
		return &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   "wf-" + sid,
						WorkflowType: &commonpb.WorkflowType{Name: "action"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
			State: &schedulepb.ScheduleState{Paused: true},
		}
	}

	ctx := routeToScheduler(s.Context())

	// Create two schedules.
	for _, sid := range []string{sid1, sid2} {
		_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule(sid),
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		s.Require().NoError(err)
	}

	// Wait for both schedules to appear in visibility.
	getScheduleEntryFromVisibility(s.Context(), env, sid1, routeToScheduler, nil)
	getScheduleEntryFromVisibility(s.Context(), env, sid2, routeToScheduler, nil)

	listScheduleIDs := func(query string) []string {
		s.T().Helper()
		listResp, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 10,
			Query:           query,
		})
		s.Require().NoError(err)
		var ids []string
		for _, e := range listResp.Schedules {
			ids = append(ids, e.ScheduleId)
		}
		return ids
	}

	// wantIDs is the exact set of schedule IDs expected in the result.
	// IsNegativeScheduleIDOperator drives whether an operator excludes or includes:
	// negative operators (!=, NOT IN, NOT STARTS_WITH) produce AND in the rewriter so both
	// V1 and V2 forms are excluded; positive operators produce OR so both forms are included.
	tests := []struct {
		name    string
		query   string
		wantIDs []string
	}{
		{
			name:    "Equal",
			query:   fmt.Sprintf("ScheduleId = '%s'", sid1),
			wantIDs: []string{sid1},
		},
		{
			// scheduler.IsNegativeScheduleIDOperator("!=") == true
			name:    "NotEqual",
			query:   fmt.Sprintf("ScheduleId != '%s'", sid1),
			wantIDs: []string{sid2},
		},
		{
			name:    "StartsWith",
			query:   "ScheduleId STARTS_WITH 'sched-filter-by-id-'",
			wantIDs: []string{sid1, sid2},
		},
		{
			name:    "StartsWithSpecific",
			query:   "ScheduleId STARTS_WITH 'sched-filter-by-id-a'",
			wantIDs: []string{sid1},
		},
		{
			// scheduler.IsNegativeScheduleIDOperator("not starts_with") == true
			name:    "NotStartsWith",
			query:   "ScheduleId NOT STARTS_WITH 'sched-filter-by-id-a'",
			wantIDs: []string{sid2},
		},
		{
			name:    "In",
			query:   fmt.Sprintf("ScheduleId IN ('%s', '%s')", sid1, sid2),
			wantIDs: []string{sid1, sid2},
		},
		{
			name:    "InSingle",
			query:   fmt.Sprintf("ScheduleId IN ('%s')", sid2),
			wantIDs: []string{sid2},
		},
		{
			// scheduler.IsNegativeScheduleIDOperator("not in") == true
			name:    "NotIn",
			query:   fmt.Sprintf("ScheduleId NOT IN ('%s')", sid1),
			wantIDs: []string{sid2},
		},
		{
			name:    "IsNotNull",
			query:   "ScheduleId IS NOT NULL",
			wantIDs: []string{sid1, sid2},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			await.Require(testcontext.For(t), t, func(c *await.T) {
				ids := listScheduleIDs(tc.query)
				require.Len(c, ids, len(tc.wantIDs))
				for _, want := range tc.wantIDs {
					require.Contains(c, ids, want)
				}
			}, 15*time.Second, 1*time.Second)
		})
	}
}

func (s *ScheduleSuite) TestSchedule_InternalTaskQueue(chasmEnabled bool) {
	t := s.T()
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	errorMessageKeyword := "internal per-namespace task queue"

	// Test CreateSchedule with internal task queue
	t.Run("CreateSchedule_PerNSWorkerTaskQueue", func(t *testing.T) {
		sid := "sched-test-internal-tq-create"
		schedule := &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   "wf-internal-tq",
						WorkflowType: &commonpb.WorkflowType{Name: "action"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: primitives.PerNSWorkerTaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		req := &workflowservice.CreateScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		}

		ctx := routeToScheduler(s.Context())
		_, err := env.FrontendClient().CreateSchedule(ctx, req)
		require.Error(t, err)
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
		require.Contains(t, err.Error(), errorMessageKeyword)
	})

	// Test UpdateSchedule with internal task queue
	t.Run("UpdateSchedule_PerNSWorkerTaskQueue", func(t *testing.T) {
		// First create a schedule with a valid task queue
		sid := "sched-test-internal-tq-update"
		schedule := &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   "wf-update-internal-tq",
						WorkflowType: &commonpb.WorkflowType{Name: "action"},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		req := &workflowservice.CreateScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		}

		ctx := routeToScheduler(s.Context())
		_, err := env.FrontendClient().CreateSchedule(ctx, req)
		require.NoError(t, err)

		// Now try to update with internal task queue
		schedule.Action.GetStartWorkflow().TaskQueue = &taskqueuepb.TaskQueue{
			Name: primitives.PerNSWorkerTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
		updateReq := &workflowservice.UpdateScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		}

		_, err = env.FrontendClient().UpdateSchedule(ctx, updateReq)
		require.Error(t, err)
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
		require.Contains(t, err.Error(), errorMessageKeyword)
	})
}

func (s *ScheduleCHASMSuite) TestDoubleReset_HSMCallbacks() {
	routeToScheduler := routeToCHASMScheduler
	enableCHASMCallbacks := false
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, enableCHASMCallbacks)

	sid := "sched-test-double-reset"
	wid := "sched-test-double-reset-wf"
	wt := "sched-test-double-reset-wt"

	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		ch := workflow.GetSignalChannel(ctx, "complete")
		var signal any
		ch.Receive(ctx, &signal)
		return nil
	}, workflow.RegisterOptions{Name: wt})

	ctx := routeToScheduler(testcontext.For(s.T()))

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(24 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	// Wait for scheduler to start the workflow and show it as RUNNING.
	listEntry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.RecentActions) >= 1 &&
			ent.Info.RecentActions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	a1 := listEntry.Info.RecentActions[0]
	wfExec := &commonpb.WorkflowExecution{
		WorkflowId: a1.StartWorkflowResult.WorkflowId,
		RunId:      a1.StartWorkflowResult.RunId,
	}

	env.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	origDesc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: wfExec,
	})
	s.Require().NoError(err)
	var originalStartReqID string
	for reqID, info := range origDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
		if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			originalStartReqID = reqID
			break
		}
	}
	env.NotEmpty(originalStartReqID, "original run must have a request ID for WorkflowExecutionStarted")

	resp1, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         wfExec,
		Reason:                    "double-reset-test-first",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.Require().NoError(err)
	resetRun1 := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resp1.RunId,
	}

	s.Await(func(suite *ScheduleCHASMSuite) {
		resetDesc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: resetRun1,
		})
		suite.NoError(err)
		var resetStartReqID string
		for reqID, info := range resetDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				resetStartReqID = reqID
				break
			}
		}
		suite.Equal(originalStartReqID, resetStartReqID,
			"start request ID must be preserved across first reset")
	}, 10*time.Second, 100*time.Millisecond)

	resp2, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         resetRun1,
		Reason:                    "double-reset-test-second",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.Require().NoError(err)
	resetRun2 := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resp2.RunId,
	}

	s.Await(func(suite *ScheduleCHASMSuite) {
		resetDesc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: resetRun2,
		})
		suite.NoError(err)
		var resetStartReqID string
		for reqID, info := range resetDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				resetStartReqID = reqID
				break
			}
		}
		suite.Equal(originalStartReqID, resetStartReqID,
			"start request ID must be preserved across double reset")
	}, 10*time.Second, 100*time.Millisecond)

	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfExec.WorkflowId,
		},
		SignalName: "complete",
	})
	s.Require().NoError(err)

	getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		for _, action := range ent.Info.RecentActions {
			if action.GetStartWorkflowResult().GetRunId() == wfExec.RunId {
				return action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			}
		}
		return false
	})
}

func (s *ScheduleCHASMSuite) TestDoubleReset_ChasmCallbacks() {
	routeToScheduler := routeToCHASMScheduler
	enableCHASMCallbacks := true
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, enableCHASMCallbacks)

	sid := "sched-test-double-reset"
	wid := "sched-test-double-reset-wf"
	wt := "sched-test-double-reset-wt"

	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		ch := workflow.GetSignalChannel(ctx, "complete")
		var signal any
		ch.Receive(ctx, &signal)
		return nil
	}, workflow.RegisterOptions{Name: wt})

	ctx := routeToScheduler(testcontext.For(s.T()))

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(24 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	// Wait for scheduler to start the workflow and show it as RUNNING.
	listEntry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.RecentActions) >= 1 &&
			ent.Info.RecentActions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	a1 := listEntry.Info.RecentActions[0]
	wfExec := &commonpb.WorkflowExecution{
		WorkflowId: a1.StartWorkflowResult.WorkflowId,
		RunId:      a1.StartWorkflowResult.RunId,
	}

	env.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	origDesc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: wfExec,
	})
	s.Require().NoError(err)
	var originalStartReqID string
	for reqID, info := range origDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
		if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			originalStartReqID = reqID
			break
		}
	}
	env.NotEmpty(originalStartReqID, "original run must have a request ID for WorkflowExecutionStarted")

	resp1, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         wfExec,
		Reason:                    "double-reset-test-first",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.Require().NoError(err)
	resetRun1 := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resp1.RunId,
	}

	s.Await(func(suite *ScheduleCHASMSuite) {
		resetDesc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: resetRun1,
		})
		suite.NoError(err)
		var resetStartReqID string
		for reqID, info := range resetDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				resetStartReqID = reqID
				break
			}
		}
		suite.Equal(originalStartReqID, resetStartReqID,
			"start request ID must be preserved across first reset")
	}, 10*time.Second, 100*time.Millisecond)

	resp2, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         resetRun1,
		Reason:                    "double-reset-test-second",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.Require().NoError(err)
	resetRun2 := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resp2.RunId,
	}

	s.Await(func(suite *ScheduleCHASMSuite) {
		resetDesc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: resetRun2,
		})
		suite.NoError(err)
		var resetStartReqID string
		for reqID, info := range resetDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				resetStartReqID = reqID
				break
			}
		}
		suite.Equal(originalStartReqID, resetStartReqID,
			"start request ID must be preserved across double reset")
	}, 10*time.Second, 100*time.Millisecond)

	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfExec.WorkflowId,
		},
		SignalName: "complete",
	})
	s.Require().NoError(err)

	getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		for _, action := range ent.Info.RecentActions {
			if action.GetStartWorkflowResult().GetRunId() == wfExec.RunId {
				return action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			}
		}
		return false
	})
}

func (s *ScheduleCHASMSuite) TestResetWithAdditionalCallback_HSMCallbacks() {
	routeToScheduler := routeToCHASMScheduler
	enableCHASMCallbacks := false
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, enableCHASMCallbacks)
	env.OverrideDynamicConfig(
		callback.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	sid := "sched-test-reset-extra-cb"
	wid := "sched-test-reset-extra-cb-wf"
	wt := "sched-test-reset-extra-cb-wt"

	ch, secondCallbackURL := newNexusCompletionHandler(s.T())

	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		sigCh := workflow.GetSignalChannel(ctx, "complete")
		var signal any
		sigCh.Receive(ctx, &signal)
		return nil
	}, workflow.RegisterOptions{Name: wt})

	ctx := routeToScheduler(testcontext.For(s.T()))

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(24 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	listEntry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.RecentActions) >= 1 &&
			ent.Info.RecentActions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	a1 := listEntry.Info.RecentActions[0]
	wfExec := &commonpb.WorkflowExecution{
		WorkflowId: a1.StartWorkflowResult.WorkflowId,
		RunId:      a1.StartWorkflowResult.RunId,
	}

	env.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	attachRequestID := uuid.NewString()
	attachResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                attachRequestID,
		Namespace:                env.Namespace().String(),
		WorkflowId:               wfExec.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: wt},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
		},
		CompletionCallbacks: []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: secondCallbackURL,
					},
				},
			},
		},
	})
	s.Require().NoError(err)
	env.False(attachResp.Started, "expected to attach to existing run, not start a new one")

	env.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted
		5 WorkflowExecutionOptionsUpdated`,
		env.GetHistoryFunc(env.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         wfExec,
		Reason:                    "reset-with-additional-callback-test",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.Require().NoError(err)
	resetRun := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resetResp.RunId,
	}

	var startRequestID string
	s.Await(func(suite *ScheduleCHASMSuite) {
		descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: resetRun,
		})
		suite.NoError(err)
		suite.Len(descResp.Callbacks, 2)
		reqIDs := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
		attachInfo, ok := reqIDs[attachRequestID]
		suite.True(ok, "attachRequestId not found in RequestIdInfos")
		suite.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, attachInfo.GetEventType())
		for reqID, info := range reqIDs {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				startRequestID = reqID
				break
			}
		}
		suite.NotEmpty(startRequestID, "no request ID found for WorkflowExecutionStarted")
		suite.NotEqual(startRequestID, attachRequestID,
			"schedule callback and manually-attached callback must have different request IDs")
	}, 10*time.Second, 100*time.Millisecond)

	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfExec.WorkflowId,
		},
		SignalName: "complete",
	})
	s.Require().NoError(err)

	getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		for _, action := range ent.Info.RecentActions {
			if action.GetStartWorkflowResult().GetRunId() == wfExec.RunId {
				return action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			}
		}
		return false
	})

	select {
	case completion := <-ch.requestCh:
		env.Equal(nexus.OperationStateSucceeded, completion.State)
		ch.requestCompleteCh <- nil
	case <-time.After(10 * time.Second):
		env.Fail("timeout waiting for second callback to be delivered")
	}
}

func (s *ScheduleCHASMSuite) TestResetWithAdditionalCallback_ChasmCallbacks() {
	routeToScheduler := routeToCHASMScheduler
	enableCHASMCallbacks := true
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, enableCHASMCallbacks)
	env.OverrideDynamicConfig(
		callback.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	sid := "sched-test-reset-extra-cb"
	wid := "sched-test-reset-extra-cb-wf"
	wt := "sched-test-reset-extra-cb-wt"

	ch, secondCallbackURL := newNexusCompletionHandler(s.T())

	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		sigCh := workflow.GetSignalChannel(ctx, "complete")
		var signal any
		sigCh.Receive(ctx, &signal)
		return nil
	}, workflow.RegisterOptions{Name: wt})

	ctx := routeToScheduler(testcontext.For(s.T()))

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(24 * time.Hour)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	listEntry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.RecentActions) >= 1 &&
			ent.Info.RecentActions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	a1 := listEntry.Info.RecentActions[0]
	wfExec := &commonpb.WorkflowExecution{
		WorkflowId: a1.StartWorkflowResult.WorkflowId,
		RunId:      a1.StartWorkflowResult.RunId,
	}

	env.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		env.GetHistoryFunc(env.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	attachRequestID := uuid.NewString()
	attachResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                attachRequestID,
		Namespace:                env.Namespace().String(),
		WorkflowId:               wfExec.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: wt},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
		},
		CompletionCallbacks: []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: secondCallbackURL,
					},
				},
			},
		},
	})
	s.Require().NoError(err)
	env.False(attachResp.Started, "expected to attach to existing run, not start a new one")

	env.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted
		5 WorkflowExecutionOptionsUpdated`,
		env.GetHistoryFunc(env.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         wfExec,
		Reason:                    "reset-with-additional-callback-test",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.Require().NoError(err)
	resetRun := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resetResp.RunId,
	}

	var startRequestID string
	s.Await(func(suite *ScheduleCHASMSuite) {
		descResp, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: resetRun,
		})
		suite.NoError(err)
		suite.Len(descResp.Callbacks, 2)
		reqIDs := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
		attachInfo, ok := reqIDs[attachRequestID]
		suite.True(ok, "attachRequestId not found in RequestIdInfos")
		suite.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, attachInfo.GetEventType())
		for reqID, info := range reqIDs {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				startRequestID = reqID
				break
			}
		}
		suite.NotEmpty(startRequestID, "no request ID found for WorkflowExecutionStarted")
		suite.NotEqual(startRequestID, attachRequestID,
			"schedule callback and manually-attached callback must have different request IDs")
	}, 10*time.Second, 100*time.Millisecond)

	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfExec.WorkflowId,
		},
		SignalName: "complete",
	})
	s.Require().NoError(err)

	getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(ent *schedulepb.ScheduleListEntry) bool {
		for _, action := range ent.Info.RecentActions {
			if action.GetStartWorkflowResult().GetRunId() == wfExec.RunId {
				return action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			}
		}
		return false
	})

	select {
	case completion := <-ch.requestCh:
		env.Equal(nexus.OperationStateSucceeded, completion.State)
		ch.requestCompleteCh <- nil
	case <-time.After(10 * time.Second):
		env.Fail("timeout waiting for second callback to be delivered")
	}
}

// testCreatesWorkflowSentinel tests that creating a CHASM schedule also starts a
// dummy workflow to reserve the schedule ID in the V1 workflow ID-space.
func (s *ScheduleCHASMSuite) TestCreatesWorkflowSentinel() {
	routeToScheduler := routeToCHASMScheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sid")
	wid := testcore.RandomizeStr("wid")
	wt := testcore.RandomizeStr("wt")

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   testcore.RandomizeStr("identity"),
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.Require().NoError(err)

	// Verify the dummy workflow was created to reserve the V1 workflow ID.
	sentinelWfID := scheduler.WorkflowIDPrefix + sid
	var descResp *workflowservice.DescribeWorkflowExecutionResponse
	env.Eventually(func() bool {
		descResp, err = env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: sentinelWfID},
		})
		return err == nil
	}, 15*time.Second, 500*time.Millisecond, "dummy sentinel workflow should exist")
	env.Equal(dummy.DummyWFTypeName, descResp.WorkflowExecutionInfo.Type.Name)
	env.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.WorkflowExecutionInfo.Status)

	// Verify visibility shows exactly one schedule (not the dummy workflow).
	getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, nil)
	listResp, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 5,
	})
	s.Require().NoError(err)
	env.Len(listResp.Schedules, 1)

	countResp, err := env.FrontendClient().CountSchedules(ctx, &workflowservice.CountSchedulesRequest{
		Namespace: env.Namespace().String(),
	})
	s.Require().NoError(err)
	env.Equal(int64(1), countResp.Count)
}

func (s *ScheduleCHASMSuite) TestStateSizeBytesReported() {
	routeToScheduler := routeToCHASMScheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sched-state-size")
	wid := testcore.RandomizeStr("sched-state-size-wf")
	wt := testcore.RandomizeStr("sched-state-size-wt")

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		State: &schedulepb.ScheduleState{Paused: true},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	env.Positive(desc.GetInfo().GetStateSizeBytes(), "Describe should report a non-zero StateSizeBytes")
}

// testCreatesCHASMSentinel tests that creating a V1 schedule also creates a
// CHASM sentinel to reserve the schedule ID in the CHASM execution space.
func (s *ScheduleV1Suite) TestCreatesCHASMSentinel() {
	routeToScheduler := routeToV1Scheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sid")
	wid := testcore.RandomizeStr("wid")
	wt := testcore.RandomizeStr("wt")

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   testcore.RandomizeStr("identity"),
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.Require().NoError(err)

	// Verify a CHASM sentinel was created to reserve the schedule ID.
	// DescribeSchedule should return NotFound, as well as CreateSentinel
	nsID := env.NamespaceID().String()
	env.Eventually(func() bool {
		_, descErr := env.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid},
			},
		)
		var notFoundErr *serviceerror.NotFound
		if !errors.As(descErr, &notFoundErr) {
			return false
		}

		// A CHASM CreateSchedule should also fail with NotFound because
		// the sentinel blocks it.
		_, createErr := env.GetTestCluster().SchedulerClient().CreateSchedule(
			ctx,
			&schedulerpb.CreateScheduleRequest{
				NamespaceId: nsID,
				FrontendRequest: &workflowservice.CreateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: sid,
					RequestId:  testcore.RandomizeStr("test-sentinel-check"),
					Schedule:   schedule,
				},
			},
		)
		return errors.As(createErr, &notFoundErr)
	}, 15*time.Second, 500*time.Millisecond, "CHASM sentinel should exist for V1 schedule")

	// Verify visibility shows exactly one schedule (not the sentinel).
	getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, nil)
	listResp, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
		Namespace:       env.Namespace().String(),
		MaximumPageSize: 5,
	})
	s.Require().NoError(err)
	env.Len(listResp.Schedules, 1)

	countResp, err := env.FrontendClient().CountSchedules(ctx, &workflowservice.CountSchedulesRequest{
		Namespace: env.Namespace().String(),
	})
	s.Require().NoError(err)
	env.Equal(int64(1), countResp.Count)
}

// testSkipsWorkflowSentinelWhenDisabled asserts that a CHASM CreateSchedule
// does not start the dummy V1 workflow when EnableCHASMSchedulerSentinels is off.
func (s *ScheduleCHASMSuite) TestSkipsWorkflowSentinelWhenDisabled() {
	routeToScheduler := routeToCHASMScheduler
	env := newScheduleEnv(s.T(), append(scheduleCommonOpts(s.T()),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerSentinels, false),
	)...)

	sid := testcore.RandomizeStr("sid")
	wid := testcore.RandomizeStr("wid")
	wt := testcore.RandomizeStr("wt")

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   testcore.RandomizeStr("identity"),
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.Require().NoError(err)

	// The dummy V1 workflow that reserves the schedule ID is gated on the
	// sentinel flag, so it must not exist.
	sentinelWfID := scheduler.WorkflowIDPrefix + sid
	_, descErr := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: sentinelWfID},
	})
	var notFoundErr *serviceerror.NotFound
	env.ErrorAs(descErr, &notFoundErr, "no dummy sentinel workflow should be created when sentinels are disabled")
}

// testSkipsCHASMSentinelWhenDisabled asserts that a V1 CreateSchedule does not
// create a CHASM sentinel when EnableCHASMSchedulerSentinels is off.
func (s *ScheduleV1Suite) TestSkipsCHASMSentinelWhenDisabled() {
	routeToScheduler := routeToV1Scheduler
	env := newScheduleEnv(s.T(), append(scheduleCommonOpts(s.T()),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerSentinels, false),
	)...)

	sid := testcore.RandomizeStr("sid")
	wid := testcore.RandomizeStr("wid")
	wt := testcore.RandomizeStr("wt")

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   testcore.RandomizeStr("identity"),
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.Require().NoError(err)

	// With no CHASM sentinel reserving the ID, a CHASM CreateSchedule for the
	// same ID must not be blocked by the NotFound (sentinel) signal.
	nsID := env.NamespaceID().String()
	_, createErr := env.GetTestCluster().SchedulerClient().CreateSchedule(
		ctx,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
				RequestId:  testcore.RandomizeStr("test-no-sentinel"),
				Schedule:   schedule,
			},
		},
	)
	s.Require().NoError(createErr, "no CHASM sentinel should block CreateSchedule when sentinels are disabled, got: %v", createErr)
}

func (s *ScheduleCHASMSuite) TestCreateScheduleAlreadyExists() {
	routeToScheduler := routeToCHASMScheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-already-exists"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-already-exists",
					WorkflowType: &commonpb.WorkflowType{Name: "action"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().NoError(err)

	// Try to create again with a different request ID - should fail with AlreadyExists
	req.RequestId = uuid.NewString()
	_, err = env.FrontendClient().CreateSchedule(ctx, req)
	s.Require().Error(err)

	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	env.ErrorAs(err, &alreadyStarted)
	env.Contains(err.Error(), sid)
}

// CreateSchedule is special-cased in the SDKs to translate
// serviceerror.WorkflowExecutionAlreadyStarted into
// temporal.ErrScheduleAlreadyRunning. This tests the SDK's behavior E2E against
// the handler. A similar test exists in the features repository.
func (s *ScheduleSuite) TestCreateScheduleDuplicateSdkError(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	opts := scheduleCommonOpts(s.T())
	if chasmEnabled {
		opts = append(opts, testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true))
	}
	env := newScheduleEnv(s.T(), opts...)

	sid := "sched-test-duplicate-sdk-" + uuid.NewString()[:8]
	schedOpts := sdkclient.ScheduleOptions{
		ID:   sid,
		Spec: sdkclient.ScheduleSpec{},
		Action: &sdkclient.ScheduleWorkflowAction{
			ID:        "wf-" + sid,
			Workflow:  "noop",
			TaskQueue: env.WorkerTaskQueue(),
		},
		Paused: true,
	}

	ctx := routeToScheduler(testcontext.For(s.T()))
	handle, err := env.SdkClient().ScheduleClient().Create(ctx, schedOpts)
	s.Require().NoError(err)
	defer func() { _ = handle.Delete(context.Background()) }()

	_, err = env.SdkClient().ScheduleClient().Create(ctx, schedOpts)
	env.ErrorIs(err, temporal.ErrScheduleAlreadyRunning)
}

func (s *ScheduleCHASMSuite) TestPatchRejectsExcessBackfillers() {
	routeToScheduler := routeToCHASMScheduler
	// Hold all 100 backfillers alive through legitimate buffer backpressure rather
	// than starvation. Backfillers are given a real (non-zero) share of an empty
	// buffer, but each is pointed at a range far larger than the buffer under
	// BUFFER_ALL with no worker registered: the first start runs forever (nothing
	// completes it) and the rest fill the shared buffer, so every backfiller stalls
	// with range still to process and none can finish and self-delete before the
	// 101st patch is rejected. (Before the capacity fix this test passed only
	// because all 100 backfillers were starved to zero capacity and never drained.)
	tweakables := chasmscheduler.DefaultTweakables
	tweakables.MaxBufferSize = 300
	tweakables.GeneratorBufferReserveSize = 25
	opts := append(scheduleCommonOpts(s.T()), testcore.WithDynamicConfig(chasmscheduler.CurrentTweakables, tweakables))
	env := newScheduleEnv(s.T(), opts...)
	sid := "sched-test-too-many-backfillers"
	wt := "sched-test-too-many-backfillers-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(fastInterval)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-too-many-backfillers",
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		State: &schedulepb.ScheduleState{Paused: true},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// Patch with 50 backfill requests at a time until we reach the limit of 100.
	now := time.Now()
	for i := 0; i < 100; i += 50 {
		backfills := make([]*schedulepb.BackfillRequest, 50)
		for j := range backfills {
			backfills[j] = &schedulepb.BackfillRequest{
				// A range far larger than the buffer (fastInterval fires over an hour)
				// so no backfiller can finish processing it and self-delete.
				StartTime:     timestamppb.New(now.Add(-time.Hour)),
				EndTime:       timestamppb.New(now),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			}
		}
		_, err = env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
			Patch: &schedulepb.SchedulePatch{
				BackfillRequest: backfills,
			},
			Identity:  "test",
			RequestId: uuid.NewString(),
		})
		s.Require().NoError(err)
	}

	// The next patch should be rejected.
	_, err = env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			BackfillRequest: []*schedulepb.BackfillRequest{
				{
					StartTime:     timestamppb.New(now.Add(-time.Hour)),
					EndTime:       timestamppb.New(now),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().Error(err)
	var failedPrecondition *serviceerror.FailedPrecondition
	env.ErrorAs(err, &failedPrecondition)
	env.Contains(err.Error(), "too many concurrent backfillers")
}

// createSchedulerFromMigrationState creates a V2 scheduler directly from a migration
// state carrying a single BufferedStart pointing at (wid, runID). That start is what
// arms the callback re-attach task: it is the only path that produces a start with a
// RunId but no callback attached, so it is the only way to reach
// SchedulerCallbacksTaskHandler in a running server.
func createSchedulerFromMigrationState(
	ctx context.Context,
	t *testing.T,
	s *testcore.TestEnv,
	sid, wid, wt, runID string,
) {
	t.Helper()

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(24 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	now := time.Now().UTC()
	nsID := s.NamespaceID().String()

	migrationState := &schedulerpb.SchedulerMigrationState{
		SchedulerState: &schedulerpb.SchedulerState{
			Namespace:     s.Namespace().String(),
			NamespaceId:   nsID,
			ScheduleId:    sid,
			Schedule:      schedule,
			Info:          &schedulepb.ScheduleInfo{},
			ConflictToken: 1,
		},
		GeneratorState: &schedulerpb.GeneratorState{},
		InvokerState: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{
				{
					NominalTime: timestamppb.New(now),
					ActualTime:  timestamppb.New(now),
					StartTime:   timestamppb.New(now),
					WorkflowId:  wid,
					RunId:       runID,
					RequestId:   uuid.NewString(),
					Attempt:     1,
					HasCallback: false,
				},
			},
		},
	}
	_, err := s.GetTestCluster().SchedulerClient().CreateFromMigrationState(
		ctx,
		&schedulerpb.CreateFromMigrationStateRequest{
			NamespaceId: nsID,
			State:       migrationState,
		},
	)
	require.NoError(t, err)
}

// awaitReattachMetric waits for a schedule_callback_reattach sample with the given
// outcome and reason. The counter is recorded only after the re-attach's component
// update commits, so it lags the state it describes.
func awaitReattachMetric(t *testing.T, capture *testcore.NamespaceMetricCapture, outcome, reason string) {
	t.Helper()
	want := map[string]string{"outcome": outcome, "reason": reason}
	await.RequireTruef(t, func() bool {
		return countMetric(capture, metrics.ScheduleCallbackReattach.Name(), want) >= 1
	}, awaitTimeout, pollInterval, "re-attach should record outcome=%s reason=%s", outcome, reason)
}

func (s *ScheduleCHASMSuite) TestMigrationCallbackAttach() {
	routeToScheduler := routeToCHASMScheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sid")
	wid := testcore.RandomizeStr("wid")
	wt := testcore.RandomizeStr("wt")

	resumeSignal := "resume"
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			workflow.GetSignalChannel(ctx, resumeSignal).Receive(ctx, nil)
			return nil
		},
		workflow.RegisterOptions{Name: wt},
	)

	ctx := routeToScheduler(testcontext.For(s.T()))
	metricCapture := env.StartNamespaceMetricCapture()
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    env.Namespace().String(),
		WorkflowId:   wid,
		WorkflowType: &commonpb.WorkflowType{Name: wt},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:     testcore.RandomizeStr("identity"),
		RequestId:    testcore.RandomizeStr("request-id"),
	})
	s.Require().NoError(err)

	nsID := env.NamespaceID().String()
	createSchedulerFromMigrationState(ctx, s.T(), env, sid, wid, wt, startResp.RunId)

	env.Eventually(func() bool {
		descResp, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid},
			},
		)
		if err != nil {
			return false
		}
		running := descResp.GetFrontendResponse().GetInfo().GetRunningWorkflows()
		return len(running) > 0 && running[0].WorkflowId == wid
	}, 15*time.Second, 500*time.Millisecond, "CHASM scheduler should show running workflow")

	// A genuine attach: the target was running when the re-attach task ran, so the
	// completion below is observed over a callback rather than synthesized from a
	// describe. reason=none is what separates it from the two synthesizing paths covered
	// by testMigrationCallbackReattachSynthesized.
	//
	// This must be waited on before signaling: RunningWorkflows is populated straight
	// from the migration state and says nothing about whether the callback was attached
	// yet, so completing the target first would let the re-attach describe an
	// already-closed workflow and take the already_closed path instead.
	awaitReattachMetric(s.T(), metricCapture, "attached", "none")

	_, err = env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      startResp.RunId,
		},
		SignalName: resumeSignal,
	})
	s.Require().NoError(err)

	env.Eventually(func() bool {
		descResp, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: sid},
			},
		)
		if err != nil {
			return false
		}
		recent := descResp.GetFrontendResponse().GetInfo().GetRecentActions()
		for _, action := range recent {
			if action.GetStartWorkflowResult().GetWorkflowId() == wid &&
				action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				return true
			}
		}
		return false
	}, 15*time.Second, 500*time.Millisecond, "CHASM scheduler should reflect workflow completion")

	// The re-attached callback delivers over the same internal path as a natively
	// started action, so the delivery must be instrumented here too.
	requireInternalCallbackDelivered(s.T(), metricCapture)
}

// requireInternalCallbackDelivered asserts that at least one completion callback was
// delivered over the internal (cross-shard) path and that the delivery is fully
// instrumented: a successful delivery sample, a committed invocation event, an attempt
// count, and no sample left at the "unknown" sentinel -- an outcome that would mean a
// return path recorded no outcome at all, which is indistinguishable from never having
// run.
func requireInternalCallbackDelivered(t *testing.T, capture *testcore.NamespaceMetricCapture) {
	t.Helper()

	// The event is recorded after the callback's own state transition commits,
	// which is strictly after the scheduler observed the completion, so poll.
	await.RequireTruef(t, func() bool {
		return countMetric(capture, callback.InvocationEventCounter.Name(),
			map[string]string{"outcome": "success", "destination": chasm.NexusCompletionHandlerURL}) >= 1
	}, awaitTimeout, pollInterval, "a committed callback event should be recorded")

	deliveries := capture.Metric(callback.InternalRequestCounter.Name())
	require.NotEmpty(t, deliveries, "internal callback delivery should be counted")
	for _, delivery := range deliveries {
		require.NotEqual(t, "unknown", delivery.Tags["outcome"],
			"a delivery returned without recording an outcome")
		require.Equal(t, chasm.NexusCompletionHandlerURL, delivery.Tags["destination"])
	}
	require.GreaterOrEqual(t,
		countMetric(capture, callback.InternalRequestCounter.Name(), map[string]string{"outcome": "success"}), 1,
		"a successful internal delivery should be counted")

	// Counter and latency are recorded together on every exit path, so a latency sample
	// missing against a counted request means a path skipped the deferred record.
	await.RequireTruef(t, func() bool {
		return len(capture.Metric(callback.InternalRequestLatencyHistogram.Name())) ==
			len(capture.Metric(callback.InternalRequestCounter.Name()))
	}, awaitTimeout, pollInterval, "every counted delivery should also record a latency sample")

	// Attempts are recorded only on a terminal event, where the total is final.
	attempts := capture.CollectMetric(callback.InvocationAttemptsHistogram.Name(),
		func(rec *metricstest.CapturedRecording) bool { return rec.Tags["outcome"] == "success" })
	require.NotEmpty(t, attempts, "a terminal event should record an attempt count")
	require.GreaterOrEqual(t, attempts[0].Value, int64(1))

	require.Zero(t, countMetric(capture, callback.InvocationEventCounter.Name(),
		map[string]string{"outcome": "nonretryable-error"}), "no callback should have been dropped permanently")
}

// testCallbackCompletionMetrics pins the instrumentation on the V2 completion-callback
// delivery path for a natively started action. The schedule learns its action finished
// only through that callback, so a delivery that is dropped leaves the schedule
// believing the action is still running -- and before this instrumentation that drop
// left no trace beyond a log line.
func (s *ScheduleCHASMSuite) TestCallbackCompletionMetrics() {
	routeToScheduler := routeToCHASMScheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	// The instrumented delivery path is the CHASM callback implementation. It is the
	// default, but pin it: under the HSM implementation these metrics never fire.
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true)

	sid := testcore.RandomizeStr("sched-callback-metrics")
	wid := testcore.RandomizeStr("sched-callback-metrics-wf")
	wt := testcore.RandomizeStr("sched-callback-metrics-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(s.T()))
	metricCapture := env.StartNamespaceMetricCapture()

	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(noOpInterval),
		Action: startWorkflowAction(env, wid, wt),
	})
	patchSchedule(ctx, s.T(), env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED))

	// A COMPLETED recent action is proof the callback landed: nothing else tells the
	// schedule the workflow finished.
	await.RequireTruef(s.T(), func() bool {
		desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if err != nil {
			return false
		}
		actions := desc.GetInfo().GetRecentActions()
		return len(actions) == 1 &&
			actions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, awaitTimeout, pollInterval, "the triggered action should be observed as completed")
	s.Require().Equal(int32(1), runs.Load())

	requireInternalCallbackDelivered(s.T(), metricCapture)

	// A natively started action attaches its callback at start time, so the re-attach
	// path -- and its counter -- must not be involved.
	s.Require().Empty(metricCapture.Metric(metrics.ScheduleCallbackReattach.Name()))
}

// testMigrationCallbackReattachSynthesized covers the two re-attach classifications
// that synthesize an action result instead of observing one. Both fabricate a
// completion for the schedule from a describe rather than from a callback, so they are
// the paths where a migrated schedule can silently record an outcome the workflow never
// had.
func (s *ScheduleCHASMSuite) TestMigrationCallbackReattachSynthesized() {
	t := s.T()
	routeToScheduler := routeToCHASMScheduler
	cases := []struct {
		name string
		// targetMissing points the buffered start at a workflow that never existed,
		// standing in for a target deleted before the migration's re-attach ran.
		targetMissing bool
		wantReason    string
		wantStatus    enumspb.WorkflowExecutionStatus
	}{
		{
			name:       "AlreadyClosed",
			wantReason: "already_closed",
			wantStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
		{
			// Target gone: recorded TERMINATED, because the real outcome is unknowable.
			name:          "TargetGone",
			targetMissing: true,
			wantReason:    "not_found",
			wantStatus:    enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newScheduleEnv(t, scheduleCommonOpts(t)...)

			sid := testcore.RandomizeStr("sched-reattach")
			wid := testcore.RandomizeStr("sched-reattach-wf")
			wt := testcore.RandomizeStr("sched-reattach-wt")

			ctx := routeToScheduler(testcontext.For(t))
			metricCapture := env.StartNamespaceMetricCapture()

			runID := uuid.NewString()
			if !tc.targetMissing {
				var runs atomic.Int32
				registerCountingWorkflow(env, wt, &runs)
				startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
					Namespace:    env.Namespace().String(),
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Identity:     testcore.RandomizeStr("identity"),
					RequestId:    testcore.RandomizeStr("request-id"),
				})
				require.NoError(t, err)
				runID = startResp.RunId

				// Close it before migrating, so the re-attach describes a workflow that is
				// already finished rather than racing one that is still running.
				await.RequireTruef(t, func() bool {
					desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: env.Namespace().String(),
						Execution: &commonpb.WorkflowExecution{WorkflowId: wid, RunId: runID},
					})
					return err == nil &&
						desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
				}, awaitTimeout, pollInterval, "target workflow should close before migration")
			}

			createSchedulerFromMigrationState(ctx, t, env, sid, wid, wt, runID)

			awaitReattachMetric(t, metricCapture, "completed", tc.wantReason)

			// The synthesized result must also reach the schedule's own view, not just
			// the counter.
			desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: sid,
			})
			require.NoError(t, err)
			require.Empty(t, desc.GetInfo().GetRunningWorkflows())
			actions := desc.GetInfo().GetRecentActions()
			require.Len(t, actions, 1)
			require.Equal(t, wid, actions[0].GetStartWorkflowResult().GetWorkflowId())
			require.Equal(t, tc.wantStatus, actions[0].GetStartWorkflowStatus())

			// Nothing was actually attached, so no delivery should have been attempted.
			require.Empty(t, metricCapture.Metric(callback.InternalRequestCounter.Name()))
		})
	}
}

// testCHASMCanListV1Schedules tests that a schedule created in the V1 stack
// will also be visible in the V2 stack.
func (s *ScheduleV1Suite) TestCHASMCanListV1Schedules() {
	routeToScheduler := routeToV1Scheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "schedule-created-on-v1"
	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-",
					WorkflowType: &commonpb.WorkflowType{Name: "action"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	// Create on V1 stack.
	_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), req)
	s.Require().NoError(err)

	// Pause so that `FutureActionTimes` doesn't change between calls.
	_, err = env.FrontendClient().PatchSchedule(routeToScheduler(s.Context()), &workflowservice.PatchScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			Pause: "halt",
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	// Sanity test, list with V1 handler.
	v1Entry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, func(sle *schedulepb.ScheduleListEntry) bool {
		return sle.GetInfo().Paused
	})
	env.NotNil(v1Entry.GetInfo())

	// Count with V1 handler.
	v1CountResp, err := env.FrontendClient().CountSchedules(routeToScheduler(s.Context()), &workflowservice.CountSchedulesRequest{
		Namespace: env.Namespace().String(),
	})
	s.Require().NoError(err)
	s.Require().GreaterOrEqual(v1CountResp.Count, int64(1), "Expected at least 1 schedule with V1 handler")

	// Flip on CHASM experiment and make sure we can still list.
	chasmEntry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToCHASMScheduler, nil)
	env.NotNil(chasmEntry.GetInfo())
	env.ProtoEqual(chasmEntry.GetInfo(), v1Entry.GetInfo())

	// Count with CHASM handler and verify it matches V1 count.
	chasmCountResp, err := env.FrontendClient().CountSchedules(routeToCHASMScheduler(s.Context()), &workflowservice.CountSchedulesRequest{
		Namespace: env.Namespace().String(),
	})
	s.Require().NoError(err)
	env.Equal(v1CountResp.Count, chasmCountResp.Count, "CHASM and V1 counts should match")
}

// testRefresh applies to V1 scheduler only; V2 does not support/need manual refresh.
func (s *ScheduleV1Suite) TestRefresh() {
	routeToScheduler := routeToV1Scheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-refresh"
	wid := "sched-test-refresh-wf"
	wt := "sched-test-refresh-wt"

	// Phase is computed so the first tick lands ~10s after this point. Under
	// parallel load CreateSchedule can take several seconds; if the first
	// tick passes before the server materializes the schedule we'd wait a
	// full 30s for the next one, exceeding the Eventually budget below.
	phaseOffset := 10 * time.Second
	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{
					Interval: durationpb.New(30 * time.Second),
					Phase:    durationpb.New(time.Duration((time.Now().Unix()+int64(phaseOffset/time.Second))%30) * time.Second),
				},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:               wid,
					WorkflowType:             &commonpb.WorkflowType{Name: wt},
					TaskQueue:                &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					WorkflowExecutionTimeout: durationpb.New(3 * time.Second),
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	var runs atomic.Int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs.Add(1)
			return 0
		})
		s.Require().NoError(workflow.Sleep(ctx, 10*time.Second)) // longer than execution timeout
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), req)
	s.Require().NoError(err)

	env.Eventually(func() bool { return runs.Load() == 1 }, 20*time.Second, 200*time.Millisecond)

	// workflow has started but is now sleeping. it will timeout in 2 seconds.

	describeResp, err := env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	env.Len(describeResp.Info.RunningWorkflows, 1)

	events1 := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	expectedHistory := `
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 MarkerRecorded
  6 MarkerRecorded
  7 UpsertWorkflowSearchAttributes
  8 TimerStarted
  9 TimerFired
 10 WorkflowTaskScheduled
 11 WorkflowTaskStarted
 12 WorkflowTaskCompleted
 13 MarkerRecorded
 14 MarkerRecorded
 15 WorkflowPropertiesModified
 16 TimerStarted`

	env.EqualHistoryEvents(expectedHistory, events1)

	time.Sleep(4 * time.Second) //nolint:forbidigo
	// now it has timed out, but the scheduler hasn't noticed yet. we can prove it by checking
	// its history.

	events2 := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	env.EqualHistoryEvents(expectedHistory, events2)

	// when we describe we'll force a refresh and see it timed out
	describeResp, err = env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	env.Empty(describeResp.Info.RunningWorkflows)

	// check scheduler has gotten the refresh and done some stuff. signal is sent without waiting so we need to wait.
	env.Eventually(func() bool {
		events3 := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
		return len(events3) > len(events2)
	}, 5*time.Second, 100*time.Millisecond)
}

// testListBeforeRun only applies to V1, as V2 scheduler does not involve the
// per-NS worker or workflow.
func (s *ScheduleV1Suite) TestListBeforeRun() {
	routeToScheduler := routeToV1Scheduler
	env := newScheduleEnv(s.T(), append(scheduleCommonOpts(s.T()),
		testcore.WithDynamicConfig(dynamicconfig.WorkerPerNamespaceWorkerCount, 0),
	)...)

	sid := "sched-test-list-before-run"
	wid := "sched-test-list-before-run-wf"
	wt := "sched-test-list-before-run-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(3 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	startTime := time.Now()

	_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), req)
	s.Require().NoError(err)

	entry := getScheduleEntryFromVisibility(s.Context(), env, sid, routeToScheduler, nil)
	env.NotNil(entry.Info)
	env.ProtoEqual(schedule.Spec, entry.Info.Spec)
	env.Equal(wt, entry.Info.WorkflowType.Name)
	env.False(entry.Info.Paused)
	env.Greater(len(entry.Info.FutureActionTimes), 1)
	env.True(entry.Info.FutureActionTimes[0].AsTime().After(startTime))
}

// testRateLimit applies only to V1, as V2 scheduler does not impose its own rate limiting.
func (s *ScheduleV1Suite) TestRateLimit() {
	routeToScheduler := routeToV1Scheduler
	env := newScheduleEnv(s.T(), append(scheduleCommonOpts(s.T()),
		testcore.WithDynamicConfig(dynamicconfig.SchedulerNamespaceStartWorkflowRPS, 1.0),
	)...)

	sid := "sched-test-rate-limit-%d"
	wid := "sched-test-rate-limit-wf-%d"
	wt := "sched-test-rate-limit-wt"

	var runs atomic.Int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs.Add(1)
			return 0
		})
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	// create 10 copies of the schedule
	for i := range 10 {
		schedule := &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Second)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   fmt.Sprintf(wid, i),
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), &workflowservice.CreateScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: fmt.Sprintf(sid, i),
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		s.Require().NoError(err)
	}

	time.Sleep(5 * time.Second) //nolint:forbidigo

	// With no rate limit, we'd see 10/second == 50 workflows run. With a limit of 1/sec, we
	// expect to see around 5.
	env.Less(runs.Load(), int32(10))
}

// testNextTimeCache only applies to V1.
func (s *ScheduleV1Suite) TestNextTimeCache() {
	routeToScheduler := routeToV1Scheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-next-time-cache"
	wid := "sched-test-next-time-cache-wf"
	wt := "sched-test-next-time-cache-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	var runs atomic.Int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs.Add(1)
			return 0
		})
		return nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), req)
	s.Require().NoError(err)

	// wait for at least 13 runs
	const count = 13
	env.Eventually(func() bool { return runs.Load() >= count }, (count+10)*time.Second, 500*time.Millisecond)

	// there should be only four side effects for 13 runs, and only two mentioning "Next"
	// (cache refills)
	events := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	var sideEffects, nextTimeSideEffects int
	for _, e := range events {
		if marker := e.GetMarkerRecordedEventAttributes(); marker.GetMarkerName() == "SideEffect" {
			sideEffects++
			if p, ok := marker.Details["data"]; ok && len(p.Payloads) == 1 {
				if string(p.Payloads[0].Metadata["messageType"]) == "temporal.server.api.schedule.v1.NextTimeCache" ||
					strings.Contains(payloads.ToString(p), `"Next"`) {
					nextTimeSideEffects++
				}
			}
		}
	}

	const (
		// These match the ones in the scheduler workflow, but they're not exported.
		// Change these if those change.
		FutureActionCountForList = 5
		NextTimeCacheV2Size      = 14

		// Calculate expected results
		expectedCacheSize = NextTimeCacheV2Size - FutureActionCountForList + 1
		expectedRefills   = (count + expectedCacheSize - 1) / expectedCacheSize
		uuidCacheRefills  = (count + 9) / 10
	)
	env.Equal(expectedRefills+uuidCacheRefills, sideEffects)
	env.Equal(expectedRefills, nextTimeSideEffects)
}

// getScheduleEntryFromVisibility polls visibility using ListSchedules until it finds a schedule
// with the given id and for which the optional predicate function returns true.
func getScheduleEntryFromVisibility(ctx context.Context, env testcore.Env, sid string, routeToScheduler schedulerRoute, predicate func(*schedulepb.ScheduleListEntry) bool) *schedulepb.ScheduleListEntry {
	env.T().Helper()
	var slEntry *schedulepb.ScheduleListEntry
	require.Eventually(env.T(), func() bool { // wait for visibility
		listResp, err := env.FrontendClient().ListSchedules(routeToScheduler(ctx), &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 5,
		})
		if err != nil {
			return false
		}
		for _, ent := range listResp.Schedules {
			if ent.ScheduleId == sid {
				if predicate != nil && !predicate(ent) {
					return false
				}
				slEntry = ent
				return true
			}
		}
		return false
	}, 15*time.Second, 1*time.Second)
	return slEntry
}

func durationNear(t *testing.T, value, target time.Duration) {
	t.Helper()
	const tolerance = 5 * time.Second
	require.Greater(t, value, target-tolerance)
	require.Less(t, value, target+tolerance)
}

func assertSameRecentActions(
	t *testing.T,
	expected *workflowservice.DescribeScheduleResponse, actual *schedulepb.ScheduleListEntry,
) {
	t.Helper()
	if len(expected.Info.RecentActions) != len(actual.Info.RecentActions) {
		t.Fatalf(
			"RecentActions have different length expected %d, got %d",
			len(expected.Info.RecentActions),
			len(actual.Info.RecentActions))
	}
	for i := range expected.Info.RecentActions {
		if !proto.Equal(expected.Info.RecentActions[i], actual.Info.RecentActions[i]) {
			t.Errorf(
				"RecentActions are differ at index %d. Expected %v, got %v",
				i,
				expected.Info.RecentActions[i],
				actual.Info.RecentActions[i],
			)
		}
	}
}

// assertRecentActionsNoDuplicateRunIDs verifies that no two entries in
// RecentActions refer to the same workflow run. Duplicates can occur if the
// migration between V1 and V2 schedulers doesn't properly deduplicate entries
// that appear in both RunningWorkflows and RecentActions.
func assertRecentActionsNoDuplicateRunIDs(t *testing.T, actions []*schedulepb.ScheduleActionResult) {
	t.Helper()
	seen := make(map[string]int) // runId -> index of first occurrence
	for i, action := range actions {
		runID := action.GetStartWorkflowResult().GetRunId()
		if runID == "" {
			continue
		}
		if firstIdx, ok := seen[runID]; ok {
			t.Errorf(
				"duplicate RunId %q in RecentActions at indices %d and %d (workflowId=%q)",
				runID, firstIdx, i, action.GetStartWorkflowResult().GetWorkflowId(),
			)
		}
		seen[runID] = i
	}
}
func (s *ScheduleCHASMSuite) TestUpdateScheduleMemo() {
	routeToScheduler := routeToCHASMScheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-update-memo"
	wid := "sched-test-update-memo-wf"
	wt := "sched-test-update-memo-wt"

	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	memo1 := payload.EncodeString("val1")
	memo2 := payload.EncodeString("val2")

	// Create schedule with initial memo.
	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"key1": memo1,
				"key2": memo2,
			},
		},
	})
	s.Require().NoError(err)

	// Verify initial memo.
	describeResp, err := env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	s.Require().Equal(memo1.Data, describeResp.Memo.Fields["key1"].Data)
	s.Require().Equal(memo2.Data, describeResp.Memo.Fields["key2"].Data)

	// Update: replace memo with only key3 (key1 and key2 should be gone).
	memo3 := payload.EncodeString("new")
	_, err = env.FrontendClient().UpdateSchedule(routeToScheduler(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"key3": memo3,
			},
		},
	})
	s.Require().NoError(err)

	// Verify replaced memo.
	describeResp, err = env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	s.Require().Nil(describeResp.Memo.Fields["key1"], "key1 should be gone after replace")
	s.Require().Nil(describeResp.Memo.Fields["key2"], "key2 should be gone after replace")
	s.Require().Equal(memo3.Data, describeResp.Memo.Fields["key3"].Data, "key3 should be set")

	// Update with nil memo (no change).
	_, err = env.FrontendClient().UpdateSchedule(routeToScheduler(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// Verify memo unchanged.
	describeResp, err = env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	s.Require().Equal(memo3.Data, describeResp.Memo.Fields["key3"].Data, "key3 should be unchanged")

	// Update with empty memo (clear all).
	_, err = env.FrontendClient().UpdateSchedule(routeToScheduler(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{},
		},
	})
	s.Require().NoError(err)

	// Verify memo cleared.
	describeResp, err = env.FrontendClient().DescribeSchedule(routeToScheduler(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	s.Require().Empty(describeResp.Memo.GetFields(), "memo should be empty after replace with empty map")
}

func (s *ScheduleV1Suite) TestUpdateScheduleMemoRejected() {
	routeToScheduler := routeToV1Scheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-update-memo-rejected"
	wid := "sched-test-update-memo-rejected-wf"
	wt := "sched-test-update-memo-rejected-wt"

	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create V1 schedule.
	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// Update with memo should be rejected.
	_, err = env.FrontendClient().UpdateSchedule(routeToScheduler(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{
				"key": payload.EncodeString("value"),
			},
		},
	})
	s.Require().Error(err)
	var failedPrecondition *serviceerror.FailedPrecondition
	s.Require().ErrorAs(err, &failedPrecondition)
	s.Require().Contains(err.Error(), "memo updates are not supported on workflow-backed schedules")
}

func (s *ScheduleSuite) TestUnpauseResumesProcessing(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-unpause-resumes"
	wid := "sched-test-unpause-resumes-wf"
	wt := "sched-test-unpause-resumes-wt"

	var runs atomic.Int32
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			workflow.SideEffect(ctx, func(ctx workflow.Context) any {
				runs.Add(1)
				return 0
			})
			return nil
		},
		workflow.RegisterOptions{Name: wt},
	)

	_, err := env.FrontendClient().CreateSchedule(routeToScheduler(s.Context()), &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Second)}},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err)

	// Wait for the schedule to fire at least once, confirming it's running.
	env.Eventually(func() bool { return runs.Load() >= 1 }, 15*time.Second, 500*time.Millisecond)

	// Pause.
	_, err = env.FrontendClient().PatchSchedule(routeToScheduler(s.Context()), &workflowservice.PatchScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Patch:      &schedulepb.SchedulePatch{Pause: "pausing for test"},
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// Wait for the already-queued generator task to run after pause. That task
	// observes paused state, performs no-op scheduling, and then the schedule
	// becomes quiescent (no new runs over a stability window).
	stableSamples := 0
	lastRuns := runs.Load()
	env.Eventually(func() bool {
		currentRuns := runs.Load()
		if currentRuns == lastRuns {
			stableSamples++
		} else {
			lastRuns = currentRuns
			stableSamples = 0
		}
		return stableSamples >= 6
	}, 15*time.Second, 500*time.Millisecond)
	runsBeforeUnpause := runs.Load()

	// Unpause.
	_, err = env.FrontendClient().PatchSchedule(routeToScheduler(s.Context()), &workflowservice.PatchScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Patch:      &schedulepb.SchedulePatch{Unpause: "resuming"},
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// The generator should be kicked immediately on unpause and new runs should follow.
	env.Eventually(
		func() bool { return runs.Load() > runsBeforeUnpause },
		15*time.Second,
		500*time.Millisecond,
		"schedule should resume processing after unpause",
	)
}

// testPausedDropsCatchup verifies that an action scheduled by the spec during
// a paused window is NOT invoked when the schedule is unpaused.
func testPausedDropsCatchup(t *testing.T, routeToScheduler schedulerRoute) {
	s := newEnvWithIdleTime(t, shortIdleTime)

	sid := testcore.RandomizeStr("sched-paused-drops-catchup")
	wid := testcore.RandomizeStr("sched-paused-drops-catchup-wf")
	wt := testcore.RandomizeStr("sched-paused-drops-catchup-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	// Single calendar entry a few seconds in the future. While paused, its
	// time will pass. Offset must exceed worst-case CreateSchedule latency
	// under parallel load so the entry is still genuinely in the future when
	// the server materializes the schedule - otherwise the test passes for
	// the wrong reason (HWM already past the entry at create time).
	fireAt := time.Now().Add(10 * time.Second).UTC()
	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{calendarSpec(fireAt)},
		},
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	await.RequireTruef(t, func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && len(desc.Info.FutureActionTimes) == 0
	}, awaitTimeout, pollInterval,
		"FutureActionTimes should empty out once the only calendar date passes (proves HWM advanced past it while paused)")

	patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{Unpause: "drops-catchup-test"})
	await.RequireTruef(t, func() bool { return scheduleClosed(ctx, s, sid) },
		awaitTimeout, pollInterval,
		"schedule should close from idle after unpause (no future actions, no replay)")
}

// testPausedScheduleNeverIdles verifies that a paused schedule is held open
// indefinitely on both backends, even past the configured idle window.
func (s *ScheduleSuite) TestPausedScheduleNeverIdles(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newEnvWithIdleTime(s.T(), shortIdleTime)

	sid := testcore.RandomizeStr("sched-paused-never-idles")
	wid := testcore.RandomizeStr("sched-paused-never-idles-wf")
	wt := testcore.RandomizeStr("sched-paused-never-idles-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(s.Context())
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(env, wid, wt),
	})

	await.RequireTruef(s.T(),
		func() bool { return runs.Load() >= 1 },
		awaitTimeout, pollInterval,
		"schedule should have fired at least once before pause",
	)

	patchSchedule(ctx, s.T(), env, sid, &schedulepb.SchedulePatch{Pause: "never-idles-test"})

	// Across a window well past IdleTime, the schedule must never idle-close.
	s.Require().Never(func() bool { return scheduleClosed(ctx, env, sid) },
		3*shortIdleTime, pollInterval,
		"paused schedule must not close from idle even past IdleTime")

	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	s.Require().True(desc.Schedule.State.Paused, "paused schedule must stay paused")

	// Also verify by unpausing and seeing actions resume.
	runsBeforeUnpause := runs.Load()
	patchSchedule(ctx, s.T(), env, sid, &schedulepb.SchedulePatch{Unpause: "never-idles-test-resume"})
	await.RequireTruef(s.T(),
		func() bool { return runs.Load() > runsBeforeUnpause },
		awaitTimeout, pollInterval,
		"paused-then-unpaused schedule should resume firing (it should not have closed)",
	)
}

// testPausedEmptySpecStaysOpen verifies that a schedule created with an empty
// spec (no Calendar / CronString / Interval) and Paused=true - the SDK
// manual-only pattern - can be created without timing out and remains
// describable.
func (s *ScheduleSuite) TestPausedEmptySpecStaysOpen(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sched-paused-empty-spec")
	wid := testcore.RandomizeStr("sched-paused-empty-spec-wf")
	wt := testcore.RandomizeStr("sched-paused-empty-spec-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	// Empty spec + paused: a manual-only schedule. Create must succeed without
	// timing out (the original regression was "context deadline exceeded").
	ctx := routeToScheduler(s.Context())
	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	createSchedule(createCtx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   &schedulepb.ScheduleSpec{},
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(env, wid, wt),
	})

	s.Require().Never(func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"empty paused spec must not fire any actions automatically")

	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	s.Require().NoError(err)
	s.Require().True(desc.Schedule.State.Paused, "schedule must still be paused")

	// Unpause + TriggerImmediately to sanity-check the schedule is functional,
	// not just open.
	patchSchedule(ctx, s.T(), env, sid, &schedulepb.SchedulePatch{
		Unpause:            "empty-spec-trigger",
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
	})

	await.RequireTruef(s.T(),
		func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"manual trigger after unpause should fire exactly one action",
	)
}

// testTriggerImmediatelyOnActiveSchedule verifies that a TriggerImmediately
// patch on a running schedule fires an extra action and leaves the schedule
// active.
func testTriggerImmediatelyOnActiveSchedule(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-trigger-on-active")
	wid := testcore.RandomizeStr("sched-trigger-on-active-wf")
	wt := testcore.RandomizeStr("sched-trigger-on-active-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		// Fire a year into the future, keeping the schedule active, but without firing
		// actions.
		Spec: &schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{calendarSpec(time.Now().AddDate(1, 0, 0).UTC())},
		},
		Action: startWorkflowAction(env, wid, wt),
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	})

	await.RequireTruef(t, func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && len(desc.Info.FutureActionTimes) > 0
	}, awaitTimeout, pollInterval, "schedule should reach active state with future actions planned")
	require.Zero(t, runs.Load(), "no automated action should fire before the trigger")

	patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
	await.RequireTruef(t,
		func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"TriggerImmediately should fire exactly one action on the active schedule",
	)

	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Info.RecentActions, "RecentActions should include the manual trigger")
	require.NotEmpty(t, desc.Info.FutureActionTimes, "schedule should still have future automated actions planned")
}

// testTriggerImmediatelyOnPausedSchedule verifies that TriggerImmediately fires
// an action even when the schedule is paused. Manual starts bypass the paused
// gate via useScheduledAction's Manual carve-out in processBuffer.
func testTriggerImmediatelyOnPausedSchedule(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-trigger-on-paused")
	wid := testcore.RandomizeStr("sched-trigger-on-paused-wf")
	wt := testcore.RandomizeStr("sched-trigger-on-paused-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(env, wid, wt),
	})

	// Paused suppresses firing even though the 1s interval would otherwise tick.
	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"paused schedule must not fire automated actions before the trigger")

	patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))

	await.RequireTruef(t, func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"TriggerImmediately must fire exactly one action despite the schedule being paused")

	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Info.RecentActions, "RecentActions should include the manual trigger")
	require.True(t, desc.Schedule.State.Paused, "schedule must still be paused after trigger fires")
}

// testTriggerImmediatelyAfterActionsExhausted verifies that TriggerImmediately
// fires an action even on a schedule that has no LimitedActions slots left.
func testTriggerImmediatelyAfterActionsExhausted(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-trigger-after-exhausted")
	wid := testcore.RandomizeStr("sched-trigger-after-exhausted-wf")
	wt := testcore.RandomizeStr("sched-trigger-after-exhausted-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		Spec: intervalSpec(fastInterval),
		// Start exhausted so no automated action fires.
		State:  &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 0},
		Action: startWorkflowAction(env, wid, wt),
	})

	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"exhausted schedule must not auto-fire before the trigger")

	patchSchedule(ctx, t, env, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
	await.RequireTruef(t, func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"TriggerImmediately must fire exactly once despite RemainingActions=0")

	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Info.RecentActions, "RecentActions should include the manual trigger")
	require.Equal(t, int64(0), desc.Schedule.State.RemainingActions,
		"manual trigger must not consume a LimitedActions slot")
}

func testBackfillReprocessesCompletedAction(
	t *testing.T,
	routeToScheduler schedulerRoute,
	paused bool,
	intervalsOnEachSide int,
) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-reprocess")
	wid := testcore.RandomizeStr("sched-backfill-reprocess-wf")
	wt := testcore.RandomizeStr("sched-backfill-reprocess-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec: intervalSpec(fastInterval),
		State: &schedulepb.ScheduleState{
			LimitedActions:   true,
			RemainingActions: 1,
		},
		Action: startWorkflowAction(s, wid, wt),
	})

	var completedTime time.Time
	await.RequireTruef(t, func() bool {
		desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		if err != nil {
			return false
		}
		for _, action := range desc.GetInfo().GetRecentActions() {
			if action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				completedTime = action.GetScheduleTime().AsTime()
				return true
			}
		}
		return false
	}, awaitTimeout, pollInterval, "the automatic action should complete")
	require.Equal(t, int32(1), runs.Load())

	if paused {
		patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{Pause: "test completed-action backfill"})
	}

	rangeOffset := time.Duration(intervalsOnEachSide) * fastInterval
	patchSchedule(ctx, t, s, sid, backfillPatch(
		completedTime.Add(-rangeOffset),
		completedTime.Add(rangeOffset),
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	))

	expectedRuns := int32(2 + 2*intervalsOnEachSide)
	await.RequireTruef(t, func() bool { return runs.Load() == expectedRuns }, neverWindow, pollInterval,
		"backfill should reprocess the completed action exactly once")

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Equal(t, int64(expectedRuns), desc.GetInfo().GetActionCount())
	require.Empty(t, desc.GetInfo().GetRunningWorkflows())
	require.Equal(t, paused, desc.GetSchedule().GetState().GetPaused())
}

// testBackfillWithBufferOneOverlap pins the expected behavior of BUFFER_ONE
// over a multi-tick backfill: the first start runs immediately, exactly one
// follow-up is buffered (Attempt=-1 deferred), the rest are dropped, and the
// deferred one runs once the first completes.
func testBackfillWithBufferOneOverlap(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-buffer-one")
	wid := testcore.RandomizeStr("sched-backfill-buffer-one-wf")
	wt := testcore.RandomizeStr("sched-backfill-buffer-one-wt")

	// Gate runs so the first-running / one-deferred / rest-dropped sequence is deterministic.
	var runs atomic.Int32
	registerGatedWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(env, wid, wt),
	})

	now := time.Now().UTC()
	patchSchedule(ctx, t, env, sid, backfillPatch(now.Add(-5*time.Second), now, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE))

	// First backfill start runs with exactly one buffered behind it (BUFFER_ONE drops the rest).
	await.RequireTruef(t, func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && desc.GetInfo().GetBufferSize() == 1 && len(desc.GetInfo().GetRunningWorkflows()) == 1
	}, awaitTimeout, pollInterval, "expected exactly one running backfill start with one deferred behind it")
	require.Equal(t, int32(1), runs.Load(), "only the first backfill start should have fired so far")

	// Releasing the running start must re-enable the deferred one (Attempt=-1 -> 0) so it fires.
	require.Equal(t, 1, completeRunningWorkflows(ctx, t, env, sid))
	await.RequireTruef(t, func() bool { return runs.Load() == 2 },
		awaitTimeout, pollInterval,
		"deferred backfill start must fire after the running one completes (Attempt=-1 -> 0 re-enable)")
	require.Never(t, func() bool { return runs.Load() > 2 },
		neverWindow, pollInterval,
		"BUFFER_ONE must collapse the rest of the backfill ticks - only the deferred start re-enables")
}

// testBackfillRangeSmallerThanInterval covers the edge where a backfill range
// is narrower than the spec interval - no spec tick lands inside it, so no
// actions fire and the backfiller still drains cleanly.
func testBackfillRangeSmallerThanInterval(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-narrow")
	wid := testcore.RandomizeStr("sched-backfill-narrow-wf")
	wt := testcore.RandomizeStr("sched-backfill-narrow-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		// noOpInterval (1h) ticks on the hour; the backfill window below is mid-hour.
		Spec:   intervalSpec(noOpInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(env, wid, wt),
	})

	// A 10s window (above the 1s resolution, below the 1h interval) anchored mid-hour
	// in the previous hour: always in the past and provably free of an on-the-hour tick.
	prevHour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	windowStart := prevHour.Add(20 * time.Minute)
	windowEnd := windowStart.Add(10 * time.Second)
	patchSchedule(ctx, t, env, sid, backfillPatch(windowStart, windowEnd, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))

	// No spec tick falls inside a sub-interval window, so no action fires.
	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"backfill range narrower than spec interval must produce no actions")

	// And the schedule must still be describable - the backfiller drained without
	// firing anything and got cleaned up.
	_, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err, "schedule must remain describable after empty backfill")
}

// testBackfillWithSkipOverlap verifies that SKIP overlap correctly collapses a
// multi-tick backfill range to a single workflow execution.
func testBackfillWithSkipOverlap(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-skip")
	wid := testcore.RandomizeStr("sched-backfill-skip-wf")
	wt := testcore.RandomizeStr("sched-backfill-skip-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(env, wid, wt),
	})

	now := time.Now().UTC()
	patchSchedule(ctx, t, env, sid, backfillPatch(now.Add(-5*time.Second), now, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))

	// SKIP collapses the 5-tick backfill to exactly one fire, and it stays there.
	await.RequireTruef(t, func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"SKIP backfill should fire exactly once")
	require.Never(t, func() bool { return runs.Load() > 1 },
		neverWindow, pollInterval,
		"SKIP backfill must collapse to a single execution, not fire all 5 ticks")
}

func (s *ScheduleSuite) TestUpdateScheduleRequestIDTooLong(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := "sched-test-update-reqid-too-long"
	wid := "sched-test-update-reqid-too-long-wf"
	wt := "sched-test-update-reqid-too-long-wt"

	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec:   intervalSpec(noOpInterval),
		Action: startWorkflowAction(env, wid, wt),
	}

	ctx := routeToScheduler(s.Context())
	createSchedule(ctx, s.T(), env, sid, schedule)

	// Update with an oversized request ID.
	_, err := env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  strings.Repeat("x", 1001),
	})
	var invalidArgReqID *serviceerror.InvalidArgument
	s.Require().ErrorAs(err, &invalidArgReqID)
}

func (s *ScheduleCHASMSuite) TestLargeScheduleID() {
	routeToScheduler := routeToCHASMScheduler
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	ctx := routeToScheduler(testcore.NewContext())

	// The V1 sentinel shares the SQL workflow ID limit with the schedule ID
	// prefix, so this is the largest schedule ID supported by every SQL backend.
	const workflowIDColumnLimit = 255
	scheduleIDLength := workflowIDColumnLimit - len(scheduler.WorkflowIDPrefix)
	sid := strings.Repeat("a", scheduleIDLength)
	wid := testcore.RandomizeStr("sched-large-id-wf")
	wt := testcore.RandomizeStr("sched-large-id-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(env, wid, wt),
	})

	await.RequireTruef(s.T(), func() bool { return runs.Load() > 0 }, awaitTimeout, pollInterval,
		"schedule ID of length %d should start a workflow", scheduleIDLength)
}

func (s *ScheduleSuite) TestUpdateScheduleBlobSizeLimit(chasmEnabled bool) {
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	env := newScheduleEnv(s.T(),
		append(scheduleCommonOpts(s.T()),
			testcore.WithDynamicConfig(dynamicconfig.BlobSizeLimitError, 1000),
			testcore.WithDynamicConfig(dynamicconfig.BlobSizeLimitWarn, 500),
		)...,
	)

	sid := "sched-test-update-blob-limit"
	wid := "sched-test-update-blob-limit-wf"
	wt := "sched-test-update-blob-limit-wt"

	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create schedule.
	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// Update with an oversized memo that exceeds the blob size limit.
	largeMemo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"key": {Data: make([]byte, 1001)},
		},
	}
	_, err = env.FrontendClient().UpdateSchedule(routeToScheduler(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo:       largeMemo,
	})
	var invalidArgBlob *serviceerror.InvalidArgument
	s.Require().ErrorAs(err, &invalidArgBlob)
}

// TestScheduleCreationRolloutPercent verifies that
// CHASMSchedulerCreationRolloutPercent acts as a per-schedule sampling gate
// after EnableCHASMSchedulerCreation is on: at 50%, two schedules whose IDs
// bucket on opposite sides of the rollout land on different stacks.
func (s *ScheduleCHASMSuite) TestScheduleCreationRolloutPercent() {
	opts := append(scheduleCommonOpts(s.T()),
		// V1 worker is needed because at 50% rollout some schedules land on V1.
		testcore.WithWorkerService("V1 scheduler"),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true),
		testcore.WithDynamicConfig(dynamicconfig.CHASMSchedulerCreationRolloutPercent, 50),
	)
	env := newScheduleEnv(s.T(), opts...)
	ctx := s.Context()
	nsName := env.Namespace().String()
	nsID := env.NamespaceID().String()

	chasmSID, v1SID := testcore.PickRolloutSplit(s.T(), nsName, 50)

	mkSchedule := func() *schedulepb.Schedule {
		return &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(1 * time.Hour)}},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   testcore.RandomizeStr("wid"),
						WorkflowType: &commonpb.WorkflowType{Name: testcore.RandomizeStr("wt")},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
	}

	for _, sid := range []string{chasmSID, v1SID} {
		_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
			Schedule:   mkSchedule(),
			Identity:   testcore.RandomizeStr("identity"),
			RequestId:  uuid.NewString(),
		})
		s.Require().NoError(err)
	}

	// A direct CHASM DescribeSchedule succeeds for CHASM-backed schedules and
	// returns NotFound for V1-backed schedules (whose CHASM key is a sentinel).
	describeOnCHASM := func(sid string) error {
		_, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(ctx, &schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		})
		return err
	}

	s.Require().Eventually(func() bool { return describeOnCHASM(chasmSID) == nil }, 15*time.Second, 250*time.Millisecond,
		"schedule %q bucketed into CHASM should be describable via the CHASM handler", chasmSID)

	var notFoundErr *serviceerror.NotFound
	s.Require().ErrorAs(describeOnCHASM(v1SID), &notFoundErr,
		"schedule %q bucketed into V1 should not be present on the CHASM handler", v1SID)

	// Both schedules must remain describable through the public frontend
	// regardless of which stack they live on — the V1 schedule in particular
	// must round-trip through the frontend's fallback path. The V1 workflow
	// takes a moment to be queryable after creation, so poll.
	for _, sid := range []string{chasmSID, v1SID} {
		s.Require().Eventually(func() bool {
			_, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
				Namespace:  nsName,
				ScheduleId: sid,
			})
			return err == nil
		}, 15*time.Second, 250*time.Millisecond, "frontend DescribeSchedule should succeed for %q", sid)
	}
}

// scheduleClosesCase parameterizes the testScheduleClosesFromIdle matrix.
type scheduleClosesCase struct {
	name         string
	prefix       string
	state        *schedulepb.ScheduleState
	policies     *schedulepb.SchedulePolicies
	expectedRuns int32

	// buildSpec receives the current time at the moment the schedule is created
	// so calendar/end-time-relative specs remain in the future even when test env
	// spinup is slow .
	buildSpec func(now time.Time) *schedulepb.ScheduleSpec

	// strictRunCount asserts that runs == expectedRuns at the end (used for
	// LimitedActions to verify the budget is not exceeded). For calendar/end-time
	// variants, the spec naturally bounds runs but a stray tick is possible
	// before close, so we only check >=.
	strictRunCount bool
}

// testScheduleClosesFromIdle runs the idle-close matrix as parallel subtests.
func testScheduleClosesFromIdle(t *testing.T, routeToScheduler schedulerRoute) {
	cases := []scheduleClosesCase{
		{
			name:         "SingleDate",
			prefix:       "sched-single-date-closes",
			expectedRuns: 1,
			buildSpec: func(now time.Time) *schedulepb.ScheduleSpec {
				return &schedulepb.ScheduleSpec{
					Calendar: []*schedulepb.CalendarSpec{calendarSpec(now.Add(5 * time.Second))},
				}
			},
		},
		{
			name:         "MultiDate",
			prefix:       "sched-multi-date-closes",
			expectedRuns: 2,
			buildSpec: func(now time.Time) *schedulepb.ScheduleSpec {
				return &schedulepb.ScheduleSpec{
					Calendar: []*schedulepb.CalendarSpec{
						calendarSpec(now.Add(5 * time.Second)),
						calendarSpec(now.Add(10 * time.Second)),
					},
				}
			},
		},
		{
			name:         "LimitedActions",
			prefix:       "sched-limited-actions-closes",
			expectedRuns: 2,
			buildSpec: func(_ time.Time) *schedulepb.ScheduleSpec {
				return intervalSpec(fastInterval)
			},
			state:          &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 2},
			strictRunCount: true,
		},
		{
			name:         "FinalAllowAllAction",
			prefix:       "sched-final-allow-all-closes",
			expectedRuns: 1,
			buildSpec: func(_ time.Time) *schedulepb.ScheduleSpec {
				return intervalSpec(fastInterval)
			},
			state:          &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 1},
			policies:       &schedulepb.SchedulePolicies{OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL},
			strictRunCount: true,
		},
		{
			name:         "IntervalEndTime",
			prefix:       "sched-interval-end-closes",
			expectedRuns: 1,
			buildSpec: func(now time.Time) *schedulepb.ScheduleSpec {
				spec := intervalSpec(fastInterval)
				spec.EndTime = timestamppb.New(now.Add(10 * time.Second))
				return spec
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { t.Parallel(); runScheduleClosesFromIdleCase(t, routeToScheduler, c) })
	}
}

// runScheduleClosesFromIdleCase drives a schedule whose spec exhausts itself,
// then asserts (1) the expected number of runs fire, (2) FutureActionTimes
// drains to zero, and (3) the schedule closes after IdleTime.
func runScheduleClosesFromIdleCase(t *testing.T, routeToScheduler schedulerRoute, c scheduleClosesCase) {
	s := newEnvWithIdleTime(t, shortIdleTime)

	sid := testcore.RandomizeStr(c.prefix)
	wid := testcore.RandomizeStr(c.prefix + "-wf")
	wt := testcore.RandomizeStr(c.prefix + "-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:     c.buildSpec(time.Now().UTC()),
		Policies: c.policies,
		State:    c.state,
		Action:   startWorkflowAction(s, wid, wt),
	})

	// A hard action budget must land on exactly expectedRuns; time-bounded specs
	// may emit a stray tick before close, so they only require the lower bound.
	await.RequireTruef(t, func() bool {
		if c.strictRunCount {
			return runs.Load() == c.expectedRuns
		}
		return runs.Load() >= c.expectedRuns
	}, awaitTimeout, pollInterval, "schedule should fire its expected actions")

	await.RequireTruef(t, func() bool {
		resp, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && len(resp.Info.FutureActionTimes) == 0
	}, awaitTimeout, pollInterval, "schedule should drain its future action times")

	await.RequireTruef(t, func() bool { return scheduleClosed(ctx, s, sid) },
		awaitTimeout, pollInterval, "schedule should idle-close after IdleTime")

	if c.strictRunCount {
		require.Equal(t, c.expectedRuns, runs.Load(), "schedule must not exceed its action budget")
	}
}

// testManualOnlyUnpausedClosesFromIdle verifies that an unpaused manual-only
// (empty-spec) schedule closes once its IdleTime elapses (as opposed to
// testPausedEmptySpecStaysOpen, where a *paused* empty-spec schedule stays
// open).
func testManualOnlyUnpausedClosesFromIdle(t *testing.T, routeToScheduler schedulerRoute) {
	s := newEnvWithIdleTime(t, shortIdleTime)

	sid := testcore.RandomizeStr("sched-manual-only-unpaused")
	wid := testcore.RandomizeStr("sched-manual-only-wf")
	wt := testcore.RandomizeStr("sched-manual-only-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		// Empty spec: a manual-only schedule, no automated actions.
		Spec:   &schedulepb.ScheduleSpec{},
		Action: startWorkflowAction(s, wid, wt),
	})

	// With no spec and no manual trigger, the schedule closes once its idle window elapses.
	await.RequireTruef(t, func() bool { return scheduleClosed(ctx, s, sid) },
		awaitTimeout, pollInterval, "manual-only schedule should close after idle window")
	require.Zero(t, runs.Load(), "a manual-only schedule must not fire any actions on its own")
}

// testPauseDuringIdleWindow covers setting pausing, and unpausing, while a
// schedule was idling.
func testPauseDuringIdleWindow(t *testing.T, routeToScheduler schedulerRoute) {
	// Deliberately longer than shortIdleTime: the pause/unpause RPCs must land
	// inside the idle window (before the deadline) even under slow CI.
	idleTime := 10 * time.Second
	s := newEnvWithIdleTime(t, idleTime)

	sid := testcore.RandomizeStr("sched-pause-during-idle")
	wid := testcore.RandomizeStr("sched-pause-during-idle-wf")
	wt := testcore.RandomizeStr("sched-pause-during-idle-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 1},
		Action: startWorkflowAction(s, wid, wt),
	})

	// The single allowed action fires, exhausting the budget and arming idle.
	await.RequireTruef(t, func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval, "the one allowed action must fire before pausing")

	patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{Pause: "pause-during-idle"})

	// Paused must hold the schedule open past the original idle deadline.
	require.Never(t, func() bool { return scheduleClosed(ctx, s, sid) },
		idleTime*2, pollInterval, "paused schedule must not close past original idle deadline")

	// Unpause: the Generator re-arms idle and the schedule finally closes.
	patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{Unpause: "resume-after-idle"})
	await.RequireTruef(t, func() bool { return scheduleClosed(ctx, s, sid) },
		awaitTimeout, pollInterval, "schedule must close after unpause via re-armed idle task")
	require.Equal(t, int32(1), runs.Load(), "no extra actions should fire across pause/unpause")
}

// testBackfillBlocksIdleClose verifies that a schedule with no remaining
// automated actions and a pending backfill is not closed by the idle path
// until it drains.
func testBackfillBlocksIdleClose(t *testing.T, routeToScheduler schedulerRoute) {
	s := newEnvWithIdleTime(t, shortIdleTime)

	sid := testcore.RandomizeStr("sched-backfill-blocks-idle")
	wid := testcore.RandomizeStr("sched-backfill-blocks-idle-wf")
	wt := testcore.RandomizeStr("sched-backfill-blocks-idle-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec: intervalSpec(fastInterval),
		State: &schedulepb.ScheduleState{
			LimitedActions:   true,
			RemainingActions: 1,
		},
		Action: startWorkflowAction(s, wid, wt),
	})

	// The single allowed automated action fires, leaving the scheduler heading to idle.
	await.RequireTruef(t, func() bool { return runs.Load() == 1 }, awaitTimeout, pollInterval,
		"the single allowed automated action should have fired")

	// BUFFER_ALL is used to force each to run sequentially (versus in parallel with
	// ALLOW_ALL), which is a better test to show the idle time is pushed back.
	now := time.Now().UTC()
	patchSchedule(ctx, t, s, sid, backfillPatch(now.Add(-10*time.Second), now, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL))

	// Backfill fires despite the scheduler heading to idle; tick count in the 5s
	// window varies with boundary alignment, so assert the lower bound.
	await.RequireTruef(t, func() bool { return runs.Load() >= 10 },
		awaitTimeout, pollInterval,
		"backfill should fire actions even though the scheduler was heading to idle")

	await.RequireTruef(t, func() bool { return scheduleClosed(ctx, s, sid) },
		awaitTimeout, pollInterval,
		"scheduler should close from idle once the backfill drains and IdleTime elapses")
}

// testMultiRangeBackfillCountedExactlyOnce asserts that the `ActionCount` is
// correctly counted when multiple backfillers are concurrently running.
func testMultiRangeBackfillCountedExactlyOnce(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-multi-range-backfill")
	wid := testcore.RandomizeStr("sched-multi-range-backfill-wf")
	wt := testcore.RandomizeStr("sched-multi-range-backfill-wt")

	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		// 1m interval; each 2m backfill range below covers a couple of ticks.
		Spec:   intervalSpec(time.Minute),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(env, wid, wt),
	})

	now := time.Now().UTC()
	threeYearsAgo := now.Add(-3 * 365 * 24 * time.Hour).Truncate(time.Minute)
	thirtyMinutesAgo := now.Add(-30 * time.Minute).Truncate(time.Minute)
	patchSchedule(ctx, t, env, sid, &schedulepb.SchedulePatch{
		BackfillRequest: []*schedulepb.BackfillRequest{
			{
				StartTime:     timestamppb.New(threeYearsAgo.Add(-2 * time.Minute)),
				EndTime:       timestamppb.New(threeYearsAgo),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
			{
				StartTime:     timestamppb.New(thirtyMinutesAgo.Add(-2 * time.Minute)),
				EndTime:       timestamppb.New(thirtyMinutesAgo),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			},
		},
	})

	await.RequireTruef(t, func() bool {
		desc, descErr := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		return desc.Info.ActionCount == 6 && len(desc.Info.RunningWorkflows) == 0
	}, awaitTimeout, pollInterval,
		"backfill should fire 6 actions and complete on a paused schedule")
}

// testBackfillOnPausedSchedule verifies that a paused schedule still processes
// a backfill request to completion, even though the schedule otherwise has no
// automated actions running.
func testBackfillOnPausedSchedule(t *testing.T, routeToScheduler schedulerRoute) {
	env := newScheduleEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-paused")
	wid := testcore.RandomizeStr("sched-backfill-paused-wf")
	wt := testcore.RandomizeStr("sched-backfill-paused-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	ctx := routeToScheduler(testcontext.For(t))
	createSchedule(ctx, t, env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(env, wid, wt),
	})

	// Paused suppresses firing even though the 1s interval would otherwise tick.
	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"paused schedule must not fire automated actions")

	now := time.Now().UTC()
	patchSchedule(ctx, t, env, sid, backfillPatch(now.Add(-5*time.Second), now, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL))

	await.RequireTruef(t, func() bool { return runs.Load() >= 5 },
		awaitTimeout, pollInterval,
		"backfill should fire actions on a paused schedule")
}

type ScheduleSuite struct {
	parallelsuite.Suite[*ScheduleSuite]
}

func TestScheduleV1SharedSuite(t *testing.T) {
	parallelsuite.Run(t, &ScheduleSuite{}, false)
}

func TestScheduleCHASMSharedSuite(t *testing.T) {
	parallelsuite.Run(t, &ScheduleSuite{}, true)
}

func (s *ScheduleSuite) TestTriggerImmediately(chasmEnabled bool) {
	t := s.T()
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	t.Run("OnActiveSchedule", func(t *testing.T) {
		testTriggerImmediatelyOnActiveSchedule(t, routeToScheduler)
	})
	t.Run("OnPausedSchedule", func(t *testing.T) {
		testTriggerImmediatelyOnPausedSchedule(t, routeToScheduler)
	})
	t.Run("AfterActionsExhausted", func(t *testing.T) {
		testTriggerImmediatelyAfterActionsExhausted(t, routeToScheduler)
	})
}

func (s *ScheduleSuite) TestBackfill(chasmEnabled bool) {
	t := s.T()
	routeToScheduler := schedulerRouteFor(chasmEnabled)
	t.Run("ReprocessCompletedActionExactTimePaused", func(t *testing.T) {
		testBackfillReprocessesCompletedAction(t, routeToScheduler, true, 0)
	})
	t.Run("ReprocessCompletedActionExactTimeActive", func(t *testing.T) {
		testBackfillReprocessesCompletedAction(t, routeToScheduler, false, 0)
	})
	t.Run("ReprocessCompletedActionInRange", func(t *testing.T) {
		testBackfillReprocessesCompletedAction(t, routeToScheduler, true, 1)
	})
	t.Run("SkipOverlap", func(t *testing.T) {
		testBackfillWithSkipOverlap(t, routeToScheduler)
	})
	t.Run("BufferOneOverlap", func(t *testing.T) {
		testBackfillWithBufferOneOverlap(t, routeToScheduler)
	})
	t.Run("MultiRangeCountedExactlyOnce", func(t *testing.T) {
		testMultiRangeBackfillCountedExactlyOnce(t, routeToScheduler)
	})
	t.Run("RangeSmallerThanInterval", func(t *testing.T) {
		testBackfillRangeSmallerThanInterval(t, routeToScheduler)
	})
}

type ScheduleCHASMSuite struct {
	parallelsuite.Suite[*ScheduleCHASMSuite]
}

func TestScheduleCHASMSuite(t *testing.T) {
	parallelsuite.Run(t, &ScheduleCHASMSuite{})
}

// TestScheduleNextActionTimeVisibility asserts that the CHASM scheduler's
// ScheduleNextActionTime search attribute is published to visibility and is
// queryable through the frontend ListSchedules API.
func (s *ScheduleCHASMSuite) TestScheduleNextActionTimeVisibility() {
	opts := scheduleCommonOpts(s.T())
	env := newScheduleEnv(s.T(), opts...)

	v2Sid := testcore.RandomizeStr("sched-next-action-v2")
	wid := testcore.RandomizeStr("sched-next-action-wf")
	wt := testcore.RandomizeStr("sched-next-action-wt")

	// Register a no-op workflow type so a stray fire doesn't generate worker
	// noise. The schedule is never paused: we want its next action time to stay
	// populated so it remains queryable.
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	mkSchedule := func() *schedulepb.Schedule {
		return &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Interval: []*schedulepb.IntervalSpec{
					{Interval: durationpb.New(1 * time.Hour), Phase: durationpb.New(23 * time.Minute)},
				},
			},
			Action: &schedulepb.ScheduleAction{
				Action: &schedulepb.ScheduleAction_StartWorkflow{
					StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
						WorkflowId:   wid,
						WorkflowType: &commonpb.WorkflowType{Name: wt},
						TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
	}

	routeToScheduler := routeToCHASMScheduler
	v2Ctx := routeToScheduler(s.Context())
	createTime := time.Now()

	_, err := env.FrontendClient().CreateSchedule(v2Ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: v2Sid,
		Schedule:   mkSchedule(),
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// The CHASM scheduler publishes its next action time to visibility as the
	// ScheduleNextActionTime search attribute. The schedule fires on an hourly
	// interval, so its next action time is always in the future; a query for
	// ScheduleNextActionTime > createTime must therefore eventually return this
	// schedule. (The SA is indexed/queryable but not surfaced on the list entry,
	// so we assert via the query rather than by reading the entry's SAs.)
	query := fmt.Sprintf(`%s > "%s"`,
		chasmscheduler.ScheduleNextActionTimeName,
		createTime.UTC().Format(time.RFC3339Nano),
	)

	s.Require().Eventually(func() bool {
		listResp, err := env.FrontendClient().ListSchedules(v2Ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 100,
			Query:           query,
		})
		if err != nil {
			return false
		}
		for _, ent := range listResp.Schedules {
			if ent.ScheduleId == v2Sid {
				return true
			}
		}
		return false
	}, 15*time.Second, 1*time.Second,
		"schedule %q must be returned by query %q (next action time published to visibility and in the future)",
		v2Sid, query)
}

func (s *ScheduleCHASMSuite) TestListSchedulesPreservesV2ScheduleIDWithV1Prefix() {
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	ctx := routeToCHASMScheduler(testcontext.For(s.T()))

	suffix := testcore.RandomizeStr("sched-list-v2-prefix")
	plainID := "foo-" + suffix
	prefixedID := scheduler.WorkflowIDPrefix + plainID
	for _, scheduleID := range []string{plainID, prefixedID} {
		createSchedule(ctx, s.T(), env, scheduleID, &schedulepb.Schedule{
			Spec:   intervalSpec(noOpInterval),
			State:  &schedulepb.ScheduleState{Paused: true},
			Action: startWorkflowAction(env, "wf-"+scheduleID, "wt-"+scheduleID),
		})
	}

	s.Require().Eventually(func() bool {
		response, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 10,
		})
		if err != nil {
			return false
		}
		ids := make(map[string]struct{}, len(response.Schedules))
		for _, schedule := range response.Schedules {
			ids[schedule.GetScheduleId()] = struct{}{}
		}
		_, foundPlainID := ids[plainID]
		_, foundPrefixedID := ids[prefixedID]
		return foundPlainID && foundPrefixedID
	}, awaitTimeout, pollInterval,
		"ListSchedules must preserve both V2 IDs %q and %q", plainID, prefixedID)
}

func (s *ScheduleCHASMSuite) TestScheduleRejectsInvalidRequests() {
	t := s.T()
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	ctx := routeToCHASMScheduler(testcontext.For(s.T()))
	invalidPolicy := enumspb.ScheduleOverlapPolicy(99)

	validSchedule := func(scheduleID string) *schedulepb.Schedule {
		return &schedulepb.Schedule{
			Spec:   intervalSpec(noOpInterval),
			State:  &schedulepb.ScheduleState{Paused: true},
			Action: startWorkflowAction(env, "wf-"+scheduleID, "wt-"+scheduleID),
		}
	}

	testCases := []struct {
		name             string
		requiresExisting bool
		errorMessage     string
		request          func(scheduleID string) error
	}{
		{
			name:         "CreateSchedule schedule policy",
			errorMessage: "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule: &schedulepb.Schedule{Policies: &schedulepb.SchedulePolicies{
						OverlapPolicy: invalidPolicy,
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:         "CreateSchedule initial patch",
			errorMessage: "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule:   &schedulepb.Schedule{},
					InitialPatch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						OverlapPolicy: invalidPolicy,
					}}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:         "CreateSchedule initial patch timestamp",
			errorMessage: "backfill request 0 start time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule:   &schedulepb.Schedule{},
					InitialPatch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						StartTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:         "CreateSchedule timestamp",
			errorMessage: "start time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{
						StartTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:         "CreateSchedule remaining actions",
			errorMessage: "remaining actions cannot be negative",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule:   &schedulepb.Schedule{State: &schedulepb.ScheduleState{RemainingActions: -1}},
					RequestId:  uuid.NewString(),
					Identity:   "test",
				})
				return err
			},
		},
		{
			name:             "UpdateSchedule schedule policy",
			requiresExisting: true,
			errorMessage:     "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule: &schedulepb.Schedule{Policies: &schedulepb.SchedulePolicies{
						OverlapPolicy: invalidPolicy,
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "UpdateSchedule timestamp",
			requiresExisting: true,
			errorMessage:     "start time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule: &schedulepb.Schedule{Spec: &schedulepb.ScheduleSpec{
						StartTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "UpdateSchedule remaining actions",
			requiresExisting: true,
			errorMessage:     "remaining actions cannot be negative",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Schedule:   &schedulepb.Schedule{State: &schedulepb.ScheduleState{RemainingActions: -1}},
					RequestId:  uuid.NewString(),
					Identity:   "test",
				})
				return err
			},
		},
		{
			name:             "PatchSchedule trigger",
			requiresExisting: true,
			errorMessage:     "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
						OverlapPolicy: invalidPolicy,
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "PatchSchedule backfill",
			requiresExisting: true,
			errorMessage:     "unsupported overlap policy",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						OverlapPolicy: invalidPolicy,
					}}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "PatchSchedule trigger timestamp",
			requiresExisting: true,
			errorMessage:     "trigger immediately request scheduled time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{
						ScheduledTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
		{
			name:             "PatchSchedule backfill timestamp",
			requiresExisting: true,
			errorMessage:     "backfill request 0 end time is not a valid timestamp",
			request: func(scheduleID string) error {
				_, err := env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
					Namespace:  env.Namespace().String(),
					ScheduleId: scheduleID,
					Patch: &schedulepb.SchedulePatch{BackfillRequest: []*schedulepb.BackfillRequest{{
						EndTime: &timestamppb.Timestamp{Nanos: 1_000_000_000},
					}}},
					RequestId: uuid.NewString(),
					Identity:  "test",
				})
				return err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			scheduleID := testcore.RandomizeStr("sched-invalid-request")
			if tc.requiresExisting {
				createSchedule(ctx, t, env, scheduleID, validSchedule(scheduleID))
			}

			var invalidArgument *serviceerror.InvalidArgument
			err := tc.request(scheduleID)
			require.ErrorAs(t, err, &invalidArgument)
			require.ErrorContains(t, err, tc.errorMessage)

			describeResponse, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: scheduleID,
			})
			if tc.requiresExisting {
				require.NoError(t, err)
				require.NotNil(t, describeResponse)
				return
			}
			var notFound *serviceerror.NotFound
			require.ErrorAs(t, err, &notFound)
		})
	}
}

// TestMirroredIncludeExcludeSpec sets identical interval and exclusion
// specifications that match every 1s, effectively cancelling each other out.
func (s *ScheduleCHASMSuite) TestMirroredIncludeExcludeSpec() {
	// A tiny compute bound trips the mirrored spec near-instantly; the default (~1.2M candidate
	// scans per GetNextTime) makes this test burn seconds of CPU on every scheduler code path.
	opts := append(scheduleCommonOpts(s.T()), testcore.WithDynamicConfig(dynamicconfig.SchedulerSpecMaxIterations, 1000))
	env := testcore.NewEnv(s.T(), opts...)

	sid := testcore.RandomizeStr("sched-cancelling-spec")
	wid := testcore.RandomizeStr("sched-cancelling-spec-wf")
	wt := testcore.RandomizeStr("sched-cancelling-spec-wt")

	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}

	ctx, cancel := context.WithTimeout(routeToCHASMScheduler(s.Context()), 10*time.Second)
	defer cancel()
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Calendar:        []*schedulepb.CalendarSpec{everySecond},
			ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond},
		},
		Action: startWorkflowAction(env, wid, wt),
	})

	// ListMatchingTimes must surface the compute limit as an error rather than hang.
	_, lmErr := env.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		StartTime:  timestamppb.New(time.Now().UTC()),
		EndTime:    timestamppb.New(time.Now().UTC().Add(time.Hour)),
	})
	s.Require().Error(lmErr, "ListMatchingTimes should error for a mirrored include/exclude spec")
}

// TestMirroredIncludeExcludeSpecOnUpdate is like TestMirroredIncludeExcludeSpec but reaches the
// mirrored spec via UpdateSchedule, exercising the spec-recompile path on an existing schedule.
func (s *ScheduleCHASMSuite) TestMirroredIncludeExcludeSpecOnUpdate() {
	// A tiny compute bound trips the mirrored spec near-instantly (see TestMirroredIncludeExcludeSpec).
	opts := append(scheduleCommonOpts(s.T()), testcore.WithDynamicConfig(dynamicconfig.SchedulerSpecMaxIterations, 1000))
	env := testcore.NewEnv(s.T(), opts...)

	sid := testcore.RandomizeStr("sched-cancelling-update")
	wid := testcore.RandomizeStr("sched-cancelling-update-wf")
	wt := testcore.RandomizeStr("sched-cancelling-update-wt")

	ctx := routeToCHASMScheduler(s.Context())
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(1 * time.Hour),
		Action: startWorkflowAction(env, wid, wt),
	})

	everySecond := &schedulepb.CalendarSpec{Second: "*", Minute: "*", Hour: "*"}
	updateCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := env.FrontendClient().UpdateSchedule(updateCtx, &workflowservice.UpdateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				Calendar:        []*schedulepb.CalendarSpec{everySecond},
				ExcludeCalendar: []*schedulepb.CalendarSpec{everySecond},
			},
			Action: startWorkflowAction(env, wid, wt),
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Require().NoError(err, "UpdateSchedule to a mirrored include/exclude spec should not hang")

	// Once the mirrored spec is applied, ListMatchingTimes must surface the compute limit.
	await.RequireTruef(s.T(), func() bool {
		_, lmErr := env.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
			StartTime:  timestamppb.New(time.Now().UTC()),
			EndTime:    timestamppb.New(time.Now().UTC().Add(time.Hour)),
		})
		return lmErr != nil
	}, awaitTimeout, pollInterval, "ListMatchingTimes should error once the mirrored spec is applied")
}

// TestScheduleFarFutureActionTimes verifies that a schedule firing far beyond the compute
// horizon (an interval 10x the horizon) still fills FutureActionTimes, since each far-future
// time is found in a single interval step rather than by scanning.
func (s *ScheduleCHASMSuite) TestScheduleFarFutureActionTimes() {
	env := testcore.NewEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sched-far-future")
	wid := testcore.RandomizeStr("sched-far-future-wf")
	wt := testcore.RandomizeStr("sched-far-future-wt")

	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	warn := time.Duration(scheduler.DefaultWarnIterations) * time.Second
	interval := 10 * warn

	ctx := routeToCHASMScheduler(s.Context())
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(interval),
		Action: startWorkflowAction(env, wid, wt),
	})

	var future []*timestamppb.Timestamp
	await.RequireTruef(s.T(), func() bool {
		resp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		if err != nil || len(resp.GetInfo().GetFutureActionTimes()) < 10 {
			return false
		}
		future = resp.GetInfo().GetFutureActionTimes()
		return true
	}, awaitTimeout, pollInterval, "FutureActionTimes should fill to 10 far-future entries")

	for i := 1; i < len(future); i++ {
		s.Require().Equal(interval, future[i].AsTime().Sub(future[i-1].AsTime()),
			"consecutive future action times should be exactly one interval apart")
	}
	s.Require().Greater(future[len(future)-1].AsTime().Sub(time.Now().UTC()), warn,
		"future action times should extend well past the two-week compute horizon")

	base := time.Now().UTC()
	resp, err := env.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		StartTime:  timestamppb.New(base),
		EndTime:    timestamppb.New(base.Add(11 * interval)),
	})
	s.Require().NoError(err)
	s.Require().GreaterOrEqual(len(resp.GetStartTime()), 10,
		"ListMatchingTimes should enumerate the far-future series")
}

// TestScheduleManyCalendars verifies that a spec with 50 calendars and 50 excludes still
// evaluates cheaply, so the schedule keeps dispatching actions while RecentActions and
// FutureActionTimes keep updating.
func (s *ScheduleCHASMSuite) TestScheduleManyCalendars() {
	env := testcore.NewEnv(s.T(), scheduleCommonOpts(s.T())...)

	sid := testcore.RandomizeStr("sched-many-calendars")
	wid := testcore.RandomizeStr("sched-many-calendars-wf")
	wt := testcore.RandomizeStr("sched-many-calendars-wt")

	var runs atomic.Int32
	registerCountingWorkflow(env, wt, &runs)

	// Calendars match seconds 0..49 every minute so the schedule fires often. The excludes cancel
	// every fire in one upcoming minute (computed from now) so they actually take effect, while
	// the schedule keeps firing every other minute.
	const n = 50
	excludeMinute := time.Now().UTC().Truncate(time.Minute).Add(2 * time.Minute)
	var calendars, excludes []*schedulepb.CalendarSpec
	for i := range n {
		calendars = append(calendars, &schedulepb.CalendarSpec{
			Second: fmt.Sprintf("%d", i), Minute: "*", Hour: "*",
		})
		excludes = append(excludes, calendarSpec(excludeMinute.Add(time.Duration(i)*time.Second)))
	}

	ctx := routeToCHASMScheduler(s.Context())
	createSchedule(ctx, s.T(), env, sid, &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Calendar:        calendars,
			ExcludeCalendar: excludes,
		},
		Action: startWorkflowAction(env, wid, wt),
	})

	await.RequireTruef(s.T(), func() bool { return runs.Load() >= 3 },
		awaitTimeout, pollInterval, "schedule with many calendars should keep dispatching actions")

	describe := func() *schedulepb.ScheduleInfo {
		resp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: sid,
		})
		s.Require().NoError(err)
		return resp.GetInfo()
	}

	var baseRecent, baseFuture time.Time
	await.RequireTruef(s.T(), func() bool {
		info := describe()
		if len(info.GetRecentActions()) == 0 || len(info.GetFutureActionTimes()) == 0 {
			return false
		}
		baseRecent = info.GetRecentActions()[len(info.GetRecentActions())-1].GetActualTime().AsTime()
		baseFuture = info.GetFutureActionTimes()[0].AsTime()
		return true
	}, awaitTimeout, pollInterval, "RecentActions and FutureActionTimes should be populated")

	await.RequireTruef(s.T(), func() bool {
		info := describe()
		if len(info.GetRecentActions()) == 0 || len(info.GetFutureActionTimes()) == 0 {
			return false
		}
		newerRecent := info.GetRecentActions()[len(info.GetRecentActions())-1].GetActualTime().AsTime().After(baseRecent)
		advancedFuture := info.GetFutureActionTimes()[0].AsTime().After(baseFuture)
		return newerRecent && advancedFuture
	}, awaitTimeout, pollInterval, "RecentActions and FutureActionTimes should keep updating as actions fire")

	// The excludes cancel every fire in excludeMinute, so ListMatchingTimes over it is empty
	// (the schedule keeps firing in other minutes, asserted above).
	resp, err := env.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		StartTime:  timestamppb.New(excludeMinute.Add(-time.Second)),
		EndTime:    timestamppb.New(excludeMinute.Add(50 * time.Second)),
	})
	s.Require().NoError(err)
	s.Require().Empty(resp.GetStartTime(), "the excluded minute must have no matches")
}

// TestScheduleCountsVisibility asserts that the CHASM scheduler's
// ScheduleRunningWorkflowCount and ScheduleBufferedStartsCount search attributes
// are published to visibility and queryable through the frontend ListSchedules
// API. A schedule that fires every second under BUFFER_ONE, started with a
// workflow that blocks, settles into one running workflow with one fire buffered
// behind it. The counts aren't surfaced on the list entry, so we assert via the
// query rather than by reading the entry's SAs.
func (s *ScheduleCHASMSuite) TestScheduleCountsVisibility() {
	env := newScheduleEnv(s.T(), scheduleCommonOpts(s.T())...)
	routeToScheduler := routeToCHASMScheduler

	sid := testcore.RandomizeStr("sched-counts-v2")
	wid := testcore.RandomizeStr("sched-counts-wf")
	wt := testcore.RandomizeStr("sched-counts-wt")

	// A workflow that holds open so a fire stays running while later fires buffer.
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			return workflow.Sleep(ctx, time.Hour)
		},
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	ctx := routeToScheduler(s.Context())
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.Require().NoError(err)

	// matchesQuery reports whether the schedule is returned when filtering on the
	// given search-attribute query.
	matchesQuery := func(query string) bool {
		listResp, listErr := env.FrontendClient().ListSchedules(routeToScheduler(s.Context()), &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 5,
			Query:           query,
		})
		if listErr != nil {
			return false
		}
		for _, ent := range listResp.Schedules {
			if ent.ScheduleId == sid {
				return true
			}
		}
		return false
	}

	// Starting the workflow and buffering the next fire takes a few seconds on top
	// of visibility propagation, so allow 30s for each count to become queryable.
	s.Require().Eventually(func() bool {
		return matchesQuery(fmt.Sprintf("%s >= 1", chasmscheduler.ScheduleRunningWorkflowCountName))
	}, 30*time.Second, 500*time.Millisecond,
		"schedule must be queryable by ScheduleRunningWorkflowCount >= 1")

	s.Require().Eventually(func() bool {
		return matchesQuery(fmt.Sprintf("%s >= 1", chasmscheduler.ScheduleBufferedStartsCountName))
	}, 30*time.Second, 500*time.Millisecond,
		"schedule must be queryable by ScheduleBufferedStartsCount >= 1")
}
