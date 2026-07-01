package tests

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
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
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/worker/dummy"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// contextFactory wraps a base context for CHASM vs V1 differences.
type contextFactory func(context.Context) context.Context

var (
	chasmContextFactory contextFactory = func(ctx context.Context) context.Context {
		return metadata.NewOutgoingContext(ctx, metadata.Pairs(
			headers.ExperimentHeaderName, "chasm-scheduler",
		))
	}
	v1ContextFactory contextFactory = func(ctx context.Context) context.Context {
		return ctx
	}
)

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
	return testcore.NewEnv(t, append(opts, extra...)...)
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

func TestScheduleCHASM(t *testing.T) {
	t.Parallel()
	runSharedScheduleTests(t, chasmContextFactory)

	// CHASM-only tests
	newContext := chasmContextFactory
	t.Run("TestCreateScheduleAlreadyExists", func(t *testing.T) { t.Parallel(); testCreateScheduleAlreadyExists(t, newContext) })
	t.Run("TestCreateScheduleDuplicateSdkError", func(t *testing.T) { t.Parallel(); testCreateScheduleDuplicateSdkError(t, true) })
	t.Run("TestPatchRejectsExcessBackfillers", func(t *testing.T) { t.Parallel(); testPatchRejectsExcessBackfillers(t, newContext) })
	t.Run("TestDoubleReset_HSMCallbacks", func(t *testing.T) { t.Parallel(); testScheduledWorkflowDoubleReset(t, newContext, false) })
	t.Run("TestDoubleReset_ChasmCallbacks", func(t *testing.T) { t.Parallel(); testScheduledWorkflowDoubleReset(t, newContext, true) })
	t.Run("TestResetWithAdditionalCallback_HSMCallbacks", func(t *testing.T) { t.Parallel(); testResetWithAdditionalCallback(t, newContext, false) })
	t.Run("TestResetWithAdditionalCallback_ChasmCallbacks", func(t *testing.T) { t.Parallel(); testResetWithAdditionalCallback(t, newContext, true) })
	t.Run("TestMigrationCallbackAttach", func(t *testing.T) { t.Parallel(); testMigrationCallbackAttach(t, newContext) })
	t.Run("TestCreatesWorkflowSentinel", func(t *testing.T) { t.Parallel(); testCreatesWorkflowSentinel(t, newContext) })
	t.Run("TestSkipsWorkflowSentinelWhenDisabled", func(t *testing.T) { t.Parallel(); testSkipsWorkflowSentinelWhenDisabled(t, newContext) })
	t.Run("TestUpdateScheduleMemo", func(t *testing.T) { t.Parallel(); testUpdateScheduleMemo(t, newContext) })
	t.Run("TestUpdateScheduleMemoOnly", func(t *testing.T) { t.Parallel(); testUpdateScheduleMemoOnly(t, newContext) })
	t.Run("TestStateSizeBytesReported", func(t *testing.T) { t.Parallel(); testStateSizeBytesReported(t, newContext) })
	t.Run("IdleClose", func(t *testing.T) {
		t.Parallel()
		testScheduleClosesFromIdle(t, newContext)
		t.Run("ManualOnly", func(t *testing.T) { t.Parallel(); testManualOnlyUnpausedClosesFromIdle(t, newContext) })
		t.Run("PauseDuringWindow", func(t *testing.T) { t.Parallel(); testPauseDuringIdleWindow(t, newContext) })
		t.Run("BackfillBlocks", func(t *testing.T) { t.Parallel(); testBackfillBlocksIdleClose(t, newContext) })
	})
	t.Run("PausedBehavior", func(t *testing.T) {
		t.Parallel()
		t.Run("DropsCatchup", func(t *testing.T) { t.Parallel(); testPausedDropsCatchup(t, newContext) })
		t.Run("RecentActionsAdvance", func(t *testing.T) { t.Parallel(); testRecentActionsAdvanceWhilePaused(t, newContext) })
		t.Run("FutureActionTimesAdvance", func(t *testing.T) { t.Parallel(); testFutureActionTimesAdvanceWhilePaused(t, newContext) })
		t.Run("BackfillDrains", func(t *testing.T) { t.Parallel(); testBackfillOnPausedSchedule(t, newContext) })
	})
	t.Run("TestScheduledWorkflowContinueAsNewCompletion", func(t *testing.T) { t.Parallel(); testScheduledWorkflowContinueAsNewCompletion(t, newContext) })
}

func TestScheduleV1(t *testing.T) {
	t.Parallel()
	runSharedScheduleTests(t, v1ContextFactory)

	// V1-only tests
	newContext := v1ContextFactory
	t.Run("TestCreateScheduleDuplicateSdkError", func(t *testing.T) { t.Parallel(); testCreateScheduleDuplicateSdkError(t, false) })
	t.Run("TestCHASMCanListV1Schedules", func(t *testing.T) { t.Parallel(); testCHASMCanListV1Schedules(t, newContext) })
	t.Run("TestRefresh", func(t *testing.T) { t.Parallel(); testRefresh(t, newContext) })
	t.Run("TestListBeforeRun", func(t *testing.T) { t.Parallel(); testListBeforeRun(t, newContext) })
	t.Run("TestRateLimit", func(t *testing.T) { t.Parallel(); testRateLimit(t, newContext) })
	t.Run("TestNextTimeCache", func(t *testing.T) { t.Parallel(); testNextTimeCache(t, newContext) })
	t.Run("TestCreatesCHASMSentinel", func(t *testing.T) { t.Parallel(); testCreatesCHASMSentinel(t, newContext) })
	t.Run("TestSkipsCHASMSentinelWhenDisabled", func(t *testing.T) { t.Parallel(); testSkipsCHASMSentinelWhenDisabled(t, newContext) })
	t.Run("TestUpdateScheduleMemoRejected", func(t *testing.T) { t.Parallel(); testUpdateScheduleMemoRejected(t, newContext) })
}

func runSharedScheduleTests(t *testing.T, newContext contextFactory) {
	t.Run("TestBasics", func(t *testing.T) { t.Parallel(); testBasics(t, newContext) })
	t.Run("TestInput", func(t *testing.T) { t.Parallel(); testInput(t, newContext) })
	t.Run("TestLastCompletionAndError", func(t *testing.T) { t.Parallel(); testLastCompletionAndError(t, newContext) })
	t.Run("TestScheduleContinuesAfterWorkflowRetryFailure", func(t *testing.T) { t.Parallel(); testScheduleContinuesAfterWorkflowRetryFailure(t, newContext) })
	t.Run("TestListSchedulesReturnsWorkflowStatus", func(t *testing.T) { t.Parallel(); testListSchedulesReturnsWorkflowStatus(t, newContext) })
	t.Run("TestUpdateIntervalTakesEffect", func(t *testing.T) { t.Parallel(); testUpdateIntervalTakesEffect(t, newContext) })
	t.Run("TestListScheduleMatchingTimes", func(t *testing.T) { t.Parallel(); testListScheduleMatchingTimes(t, newContext) })
	t.Run("TestLimitMemoSpecSize", func(t *testing.T) { t.Parallel(); testLimitMemoSpecSize(t, newContext) })
	t.Run("TestCountSchedules", func(t *testing.T) { t.Parallel(); testCountSchedules(t, newContext) })
	t.Run("TestSchedule_InternalTaskQueue", func(t *testing.T) { t.Parallel(); testScheduleInternalTaskQueue(t, newContext) })
	t.Run("TestDeletedScheduleOperations", func(t *testing.T) { t.Parallel(); testDeletedScheduleOperations(t, newContext) })
	t.Run("TestUnpauseResumesProcessing", func(t *testing.T) { t.Parallel(); testCHASMUnpauseResumesProcessing(t, newContext) })
	t.Run("TestPausedScheduleNeverIdles", func(t *testing.T) { t.Parallel(); testPausedScheduleNeverIdles(t, newContext) })
	t.Run("TestPausedEmptySpecStaysOpen", func(t *testing.T) { t.Parallel(); testPausedEmptySpecStaysOpen(t, newContext) })
	t.Run("TriggerImmediately", func(t *testing.T) {
		t.Parallel()
		t.Run("OnActiveSchedule", func(t *testing.T) { t.Parallel(); testTriggerImmediatelyOnActiveSchedule(t, newContext) })
		t.Run("OnPausedSchedule", func(t *testing.T) { t.Parallel(); testTriggerImmediatelyOnPausedSchedule(t, newContext) })
		t.Run("AfterActionsExhausted", func(t *testing.T) { t.Parallel(); testTriggerImmediatelyAfterActionsExhausted(t, newContext) })
	})
	t.Run("Backfill", func(t *testing.T) {
		t.Parallel()
		t.Run("SkipOverlap", func(t *testing.T) { t.Parallel(); testBackfillWithSkipOverlap(t, newContext) })
		t.Run("BufferOneOverlap", func(t *testing.T) { t.Parallel(); testBackfillWithBufferOneOverlap(t, newContext) })
		t.Run("MultiRangeCountedExactlyOnce", func(t *testing.T) { t.Parallel(); testMultiRangeBackfillCountedExactlyOnce(t, newContext) })
		t.Run("RangeSmallerThanInterval", func(t *testing.T) { t.Parallel(); testBackfillRangeSmallerThanInterval(t, newContext) })
	})
	t.Run("TestUpdateScheduleRequestIDTooLong", func(t *testing.T) { t.Parallel(); testUpdateScheduleRequestIDTooLong(t, newContext) })
	t.Run("TestUpdateScheduleBlobSizeLimit", func(t *testing.T) { t.Parallel(); testUpdateScheduleBlobSizeLimit(t, newContext) })
	t.Run("TestListSchedulesPagination", func(t *testing.T) { t.Parallel(); testListSchedulesPagination(t, newContext) })
	t.Run("TestListSchedulesFilterAndEntryFields", func(t *testing.T) { t.Parallel(); testListSchedulesFilterAndEntryFields(t, newContext) })
	t.Run("TestListSchedulesFilterByScheduleId", func(t *testing.T) { t.Parallel(); testListSchedulesFilterByScheduleID(t, newContext) })
	t.Run("TestBufferSizeReportedWhenBuffered", func(t *testing.T) { t.Parallel(); testBufferSizeReportedWhenBuffered(t, newContext) })
	t.Run("TestBufferOneDeferredFiresAfterCompletion", func(t *testing.T) { t.Parallel(); testBufferOneDeferredFiresAfterCompletion(t, newContext) })
}

// testBufferSizeReportedWhenBuffered verifies that ScheduleInfo.BufferSize is
// populated by both the V1 and V2 (CHASM) schedulers when at least one fire is
// queued behind a still-running workflow. A schedule with a 1s interval and
// BUFFER_ONE keeps exactly one start in the buffer while the first workflow
// is still running.
func testBufferSizeReportedWhenBuffered(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-buffer-size")
	wid := testcore.RandomizeStr("sched-buffer-size-wf")
	wt := testcore.RandomizeStr("sched-buffer-size-wt")

	s.SdkWorker().RegisterWorkflowWithOptions(
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	var lastDescribe *workflowservice.DescribeScheduleResponse
	s.Eventually(func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		lastDescribe = desc
		return desc.GetInfo().GetBufferSize() >= 1 && len(desc.GetInfo().GetRunningWorkflows()) >= 1
	}, 30*time.Second, 500*time.Millisecond, "DescribeSchedule should report BufferSize >= 1 with a running workflow blocking the buffer")

	s.GreaterOrEqual(lastDescribe.GetInfo().GetBufferSize(), int64(1), "BufferSize must reflect at least one buffered start")
	s.GreaterOrEqual(len(lastDescribe.GetInfo().GetRunningWorkflows()), 1, "expected the buffered fire to be queued behind a running workflow")
}

// testRecentActionsAdvanceWhilePaused verifies that an in-flight workflow's
// completion status surfaces in ListSchedules' RecentActions even while the
// schedule is paused. CHASM-only: the live Invoker updates BufferedStart on
// Nexus completion regardless of paused state, so the listed status moves
// from RUNNING to COMPLETED. V1's scheduler workflow does not advance its
// memo while paused, so the listed status stays at RUNNING until unpause.
func testRecentActionsAdvanceWhilePaused(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-recentactions-paused")
	wid := testcore.RandomizeStr("sched-recentactions-paused-wf")
	wt := testcore.RandomizeStr("sched-recentactions-paused-wt")

	// Gate the run so the RUNNING -> COMPLETED transition is driven explicitly.
	var runs atomic.Int32
	registerGatedWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(s, wid, wt),
	})

	// Wait for the first workflow to be reported as RUNNING in ListSchedules.
	running := getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.RecentActions) >= 1 &&
			ent.Info.RecentActions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	runningRunID := running.Info.RecentActions[0].GetStartWorkflowResult().GetRunId()
	require.NotEmpty(t, runningRunID)

	// Pause, then release the run: its COMPLETED status must surface while paused.
	patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{Pause: "pausing for the test"})

	signaled := completeRunningWorkflows(ctx, t, s, sid)
	require.Equal(t, 1, signaled, "exactly one run should be in flight under the default SKIP overlap policy")

	// While paused, the listed RecentActions entry transitions to COMPLETED.
	getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
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
func testFutureActionTimesAdvanceWhilePaused(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-future-actions-paused")
	wid := testcore.RandomizeStr("sched-future-actions-paused-wf")
	wt := testcore.RandomizeStr("sched-future-actions-paused-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	// Wait for visibility to surface an initial FutureActionTimes projection.
	initial := getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.FutureActionTimes) > 0
	})
	initialFirst := initial.Info.FutureActionTimes[0].AsTime()

	// While still paused, the earliest projected time must advance past the
	// initial value: the Generator keeps ticking and republishing the
	// projection, even though no workflows fire.
	getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.FutureActionTimes) > 0 &&
			ent.Info.FutureActionTimes[0].AsTime().After(initialFirst)
	})
	require.Zero(t, runs.Load(), "a paused schedule must not fire any workflows")
}

// testBufferOneDeferredFiresAfterCompletion exercises the BUFFER_ONE deferred
// lifecycle end-to-end: an action that gets buffered while a workflow is
// running must fire once that workflow completes. Without re-enabling the
// deferred start (Attempt=-1 -> 0 in recordCompletedAction), the buffered
// fire would be stranded.
func testBufferOneDeferredFiresAfterCompletion(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-buffer-one-deferred")
	wid := testcore.RandomizeStr("sched-buffer-one-deferred-wf")
	wt := testcore.RandomizeStr("sched-buffer-one-deferred-wt")

	// Gate runs so "first running, second buffered" and "deferred fires after
	// completion" are both reached deterministically.
	var runs atomic.Int32
	registerGatedWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(s, wid, wt),
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	})

	// Exactly one workflow runs with exactly one start buffered behind it (BUFFER_ONE caps the buffer at one).
	await.RequireTruef(t, func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && desc.GetInfo().GetBufferSize() == 1 && len(desc.GetInfo().GetRunningWorkflows()) == 1
	}, awaitTimeout, pollInterval, "expected exactly one running workflow with one deferred start buffered behind it")
	require.Equal(t, int32(1), runs.Load(), "only the first workflow should have fired before the running one completes")

	// Releasing the running workflow must re-enable the deferred start (Attempt=-1 -> 0) so it fires.
	require.Equal(t, 1, completeRunningWorkflows(ctx, t, s, sid))
	await.RequireTruef(t, func() bool { return runs.Load() == 2 },
		awaitTimeout, pollInterval,
		"deferred start must fire after the running workflow completes - regression for the Attempt=-1 -> 0 re-enable path")
}

func testDeletedScheduleOperations(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create a schedule.
	_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Delete the schedule.
	_, err = s.FrontendClient().DeleteSchedule(newContext(s.Context()), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)

	// Describe should return NotFound.
	var notFoundErr *serviceerror.NotFound
	s.Eventually(func() bool {
		_, descErr := s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		return errors.As(descErr, &notFoundErr)
	}, 10*time.Second, 200*time.Millisecond)

	// Update, Patch, and Delete behave differently across CHASM and V1,
	// so they are not tested here. See TestScheduleUpdateAfterDelete.
}

func testBasics(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
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
		Namespace:  s.Namespace().String(),
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
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})
	workflow2Fn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs2, 1)
			return 0
		})
		return nil
	}
	s.SdkWorker().RegisterWorkflowWithOptions(workflow2Fn, workflow.RegisterOptions{Name: wt2})

	// create

	ctx := newContext(s.Context())
	createTime := time.Now()
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)

	// describe immediately after create and verify FutureActionTimes
	describeRespAfterCreate, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.NotEmpty(describeRespAfterCreate.Info.FutureActionTimes, "FutureActionTimes should be set immediately after create")
	// FutureActionTimes should be in the future (after createTime) and aligned to 5-second intervals
	for i, fat := range describeRespAfterCreate.Info.FutureActionTimes {
		s.True(fat.AsTime().After(createTime) || fat.AsTime().Equal(createTime),
			"FutureActionTimes[%d] should be >= createTime", i)
		s.Equal(int64(0), fat.AsTime().UnixNano()%int64(5*time.Second),
			"FutureActionTimes[%d] should be aligned to 5-second intervals", i)
	}

	// sleep until we see two runs, plus a bit more to ensure that the second run has completed
	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 2 }, 15*time.Second, 500*time.Millisecond)
	time.Sleep(2 * time.Second) //nolint:forbidigo

	// wait for visibility to stabilize on completed before calling describe,
	// otherwise their recent actions may flake and differ

	visibilityResponse := getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		recentActions := ent.GetInfo().GetRecentActions()
		return len(recentActions) >= 2 && recentActions[1].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	})

	describeResp, err := s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)

	// validate describe response

	checkSpec := func(spec *schedulepb.ScheduleSpec) {
		protorequire.ProtoSliceEqual(s.T(), schedule.Spec.Interval, spec.Interval)
		s.Nil(spec.Calendar)
		s.Nil(spec.CronString)
		s.ProtoElementsMatch([]*schedulepb.StructuredCalendarSpec{
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

	s.Equal(enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, describeResp.Schedule.Policies.OverlapPolicy)     // set to default value
	s.EqualValues(365*24*3600, describeResp.Schedule.Policies.CatchupWindow.AsDuration().Seconds()) // set to default value

	s.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	s.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	s.Nil(describeResp.SearchAttributes.IndexedFields[sadefs.BinaryChecksums])
	s.Nil(describeResp.SearchAttributes.IndexedFields[sadefs.BuildIds])
	s.Nil(describeResp.SearchAttributes.IndexedFields[sadefs.TemporalNamespaceDivision])
	s.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	s.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	// GreaterOrEqual is used as we may have had other runs start while waiting for visibility
	durationNear(t, describeResp.Info.CreateTime.AsTime().Sub(createTime), 0)
	s.GreaterOrEqual(describeResp.Info.ActionCount, int64(2))
	s.EqualValues(0, describeResp.Info.MissedCatchupWindow)
	s.EqualValues(0, describeResp.Info.OverlapSkipped)
	s.GreaterOrEqual(len(describeResp.Info.RunningWorkflows), 0)
	s.GreaterOrEqual(len(describeResp.Info.RecentActions), 2)
	action0 := describeResp.Info.RecentActions[0]
	s.WithinRange(action0.ScheduleTime.AsTime(), createTime, time.Now())
	s.Equal(int64(0), action0.ScheduleTime.AsTime().UnixNano()%int64(5*time.Second))
	durationNear(t, action0.ActualTime.AsTime().Sub(action0.ScheduleTime.AsTime()), 0)

	// validate list response

	s.Equal(sid, visibilityResponse.ScheduleId)
	s.Equal(schSAValue.Data, visibilityResponse.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	s.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	s.Nil(visibilityResponse.SearchAttributes.IndexedFields[sadefs.BinaryChecksums])
	s.Nil(visibilityResponse.SearchAttributes.IndexedFields[sadefs.BuildIds])
	s.Nil(visibilityResponse.SearchAttributes.IndexedFields[sadefs.TemporalNamespaceDivision])
	s.Equal(schMemo.Data, visibilityResponse.Memo.Fields["schedmemo1"].Data)
	checkSpec(visibilityResponse.Info.Spec)
	s.Equal(wt, visibilityResponse.Info.WorkflowType.Name)
	s.False(visibilityResponse.Info.Paused)
	assertSameRecentActions(s.T(), describeResp, visibilityResponse)
	assertRecentActionsNoDuplicateRunIDs(s.T(), describeResp.Info.RecentActions)

	// list workflows

	wfResp, err := s.FrontendClient().ListWorkflowExecutions(newContext(s.Context()), &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: s.Namespace().String(),
		PageSize:  5,
		Query:     "",
	})
	s.NoError(err)
	s.GreaterOrEqual(len(wfResp.Executions), 2) // could have had a 3rd run while waiting for visibility
	for _, ex := range wfResp.Executions {
		s.Equal(wt, ex.Type.Name, "should only see started workflows")
	}
	ex0 := wfResp.Executions[0]
	s.True(strings.HasPrefix(ex0.Execution.WorkflowId, wid))
	matchingRunId := false
	for _, recentAction := range describeResp.GetInfo().GetRecentActions() {
		if ex0.GetExecution().GetRunId() == recentAction.GetStartWorkflowResult().GetRunId() {
			matchingRunId = true
			break
		}
	}
	s.True(matchingRunId, "ListWorkflowExecutions returned a run ID wasn't in the describe response")
	s.Equal(wt, ex0.Type.Name)
	s.Nil(ex0.ParentExecution) // not a child workflow
	s.Equal(wfMemo.Data, ex0.Memo.Fields["wfmemo1"].Data)
	s.Equal(wfSAValue.Data, ex0.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(payload.EncodeString(sid).Data, ex0.SearchAttributes.IndexedFields[sadefs.TemporalScheduledById].Data)
	var ex0StartTime time.Time
	s.NoError(payload.Decode(ex0.SearchAttributes.IndexedFields[sadefs.TemporalScheduledStartTime], &ex0StartTime))
	s.WithinRange(ex0StartTime, createTime, time.Now())
	s.Equal(int64(0), ex0StartTime.UnixNano()%int64(5*time.Second))

	// list schedules with search attribute filter

	listResp, err := s.FrontendClient().ListSchedules(newContext(s.Context()), &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 5,
		Query:           "CustomKeywordField = 'schedule sa value' AND TemporalSchedulePaused = false",
	})
	s.NoError(err)
	s.Len(listResp.Schedules, 1)
	entry := listResp.Schedules[0]
	s.Equal(sid, entry.ScheduleId)

	// list schedules with invalid search attribute filter

	_, err = s.FrontendClient().ListSchedules(newContext(s.Context()), &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 5,
		Query:           "ExecutionDuration > '1s'",
	})
	s.Error(err)

	// update schedule, no updates to search attributes

	schedule.Spec.Interval[0].Phase = durationpb.New(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	updateTime := time.Now()
	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// wait for one new run
	s.Eventually(
		func() bool { return atomic.LoadInt32(&runs2) == 1 },
		7*time.Second,
		500*time.Millisecond,
	)

	// describe again
	describeResp, err = s.FrontendClient().DescribeSchedule(
		newContext(s.Context()),
		&workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		},
	)
	s.NoError(err)

	s.Len(describeResp.SearchAttributes.GetIndexedFields(), 3)
	s.Equal(schSAValue.Data, describeResp.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(schSAIntValue.Data, describeResp.SearchAttributes.IndexedFields[csaInt].Data)
	s.Equal(schSABoolValue.Data, describeResp.SearchAttributes.IndexedFields[csaBool].Data)
	s.Equal(schMemo.Data, describeResp.Memo.Fields["schedmemo1"].Data)
	s.Equal(wfSAValue.Data, describeResp.Schedule.Action.GetStartWorkflow().SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(wfMemo.Data, describeResp.Schedule.Action.GetStartWorkflow().Memo.Fields["wfmemo1"].Data)

	durationNear(t, describeResp.Info.UpdateTime.AsTime().Sub(updateTime), 0)
	lastAction := describeResp.Info.RecentActions[len(describeResp.Info.RecentActions)-1]
	s.Equal(int64(1000000000), lastAction.ScheduleTime.AsTime().UnixNano()%int64(5*time.Second), lastAction.ScheduleTime.AsTime().UnixNano())

	// update schedule and search attributes

	schedule.Spec.Interval[0].Phase = durationpb.New(1 * time.Second)
	schedule.Action.GetStartWorkflow().WorkflowType.Name = wt2

	csaDouble := "CustomDoubleField"
	schSADoubleValue, _ := payload.Encode(3.14)
	schSAIntValue, _ = payload.Encode(321)
	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
	s.NoError(err)

	// wait until search attributes are updated
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			describeResp, err = s.FrontendClient().DescribeSchedule(
				newContext(s.Context()),
				&workflowservice.DescribeScheduleRequest{
					Namespace:  s.Namespace().String(),
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

	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:        s.Namespace().String(),
		ScheduleId:       sid,
		Schedule:         schedule,
		Identity:         "test",
		RequestId:        uuid.NewString(),
		SearchAttributes: &commonpb.SearchAttributes{},
	})
	s.NoError(err)

	// wait until search attributes are updated
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			describeResp, err = s.FrontendClient().DescribeSchedule(
				newContext(s.Context()),
				&workflowservice.DescribeScheduleRequest{
					Namespace:  s.Namespace().String(),
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

	_, err = s.FrontendClient().PatchSchedule(newContext(s.Context()), &workflowservice.PatchScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			Pause: "because I said so",
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.NoError(err)

	time.Sleep(7 * time.Second) //nolint:forbidigo
	s.EqualValues(1, atomic.LoadInt32(&runs2), "has not run again")

	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)

	s.True(describeResp.Schedule.State.Paused)
	s.Equal("because I said so", describeResp.Schedule.State.Notes)

	// don't loop to wait for visibility, we already waited 7s from the patch
	listResp, err = s.FrontendClient().ListSchedules(newContext(s.Context()), &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 5,
	})
	s.NoError(err)
	s.Len(listResp.Schedules, 1)
	entry = listResp.Schedules[0]
	s.Equal(sid, entry.ScheduleId)
	s.True(entry.Info.Paused)
	s.Equal("because I said so", entry.Info.Notes)

	// finally delete

	_, err = s.FrontendClient().DeleteSchedule(newContext(s.Context()), &workflowservice.DeleteScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Identity:   "test",
	})
	s.NoError(err)

	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	var notFoundErr *serviceerror.NotFound
	s.ErrorAs(err, &notFoundErr)

	s.Eventually(func() bool { // wait for visibility
		listResp, err := s.FrontendClient().ListSchedules(newContext(s.Context()), &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 5,
		})
		s.NoError(err)
		return len(listResp.Schedules) == 0
	}, 10*time.Second, 1*time.Second)
}

func testInput(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
	s.NoError(err)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					Input:        inputPayloads,
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	var runs int32
	workflowFn := func(ctx workflow.Context, arg1 *myData, arg2 map[int]float64) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			s.Equal(*input1, *arg1)
			s.Equal(input2, arg2)
			atomic.AddInt32(&runs, 1)
			return 0
		})
		return nil
	}
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	ctx := newContext(s.Context())
	_, err = s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)

	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 8*time.Second, 200*time.Millisecond)
}

func testLastCompletionAndError(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	runs := make(map[string]struct{})
	var testComplete int32

	workflowFn := func(ctx workflow.Context) (string, error) {
		var num int
		_ = workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			runs[workflow.GetInfo(ctx).WorkflowExecution.ID] = struct{}{}
			return len(runs)
		}).Get(&num)

		var lcr string
		if workflow.HasLastCompletionResult(ctx) {
			s.NoError(workflow.GetLastCompletionResult(ctx, &lcr))
		}

		lastErr := workflow.GetLastError(ctx)

		switch num {
		case 1:
			s.Empty(lcr)
			s.NoError(lastErr)
			return "this one succeeds", nil
		case 2:
			s.NoError(lastErr)
			s.Equal("this one succeeds", lcr)
			return "", errors.New("this one fails")
		case 3:
			s.Equal("this one succeeds", lcr)
			s.ErrorContains(lastErr, "this one fails")
			atomic.StoreInt32(&testComplete, 1)
			return "done", nil
		default:
			panic("shouldn't be running anymore")
		}
	}
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)

	s.Eventually(func() bool { return atomic.LoadInt32(&testComplete) == 1 }, 20*time.Second, 200*time.Millisecond)
}

// testScheduleContinuesAfterWorkflowRetryFailure verifies a schedule keeps firing actions
// after a scheduled workflow exhausts its retry policy and fails.
func testScheduleContinuesAfterWorkflowRetryFailure(t *testing.T, newContext contextFactory) {
	// Recording FAILED actions across the workflow's retry chain relies on the scheduler matching
	// completions by the request ID carried in the completion callback token (which survives the new
	// runs created by retries). That requires the envelope token format, which is gated off by default
	// for safe rollout, so enable it explicitly here.
	opts := append(scheduleCommonOpts(t), testcore.WithDynamicConfig(callback.EncodeInternalTokenWithEnvelope, true))
	s := testcore.NewEnv(t, opts...)

	sid := testcore.RandomizeStr("sched-retry-fail")
	wid := testcore.RandomizeStr("sched-retry-fail-wf")
	wt := testcore.RandomizeStr("sched-retry-fail-wt")

	var sawRetry int32
	workflowFn := func(ctx workflow.Context) error {
		if workflow.GetInfo(ctx).Attempt > 1 {
			atomic.StoreInt32(&sawRetry, 1)
		}
		return errors.New("intentional failure to force a retry")
	}
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					RetryPolicy: &commonpb.RetryPolicy{
						InitialInterval:    durationpb.New(1 * time.Second),
						BackoffCoefficient: 1.0,
						MaximumAttempts:    2,
					},
				},
			},
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Two FAILED actions proves the schedule kept firing past the first retry-failure.
	var failedActions int
	var lastDescribe *workflowservice.DescribeScheduleResponse
	s.Eventually(func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
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
		return atomic.LoadInt32(&sawRetry) == 1 && failedActions >= 2
	}, 30*time.Second, 500*time.Millisecond,
		"schedule should keep recording FAILED actions after the workflow retry-fails")

	s.Equal(int32(1), atomic.LoadInt32(&sawRetry), "scheduled workflow should have retried (attempt > 1)")
	s.GreaterOrEqual(failedActions, 2, "schedule should record multiple retry-failed actions")
	s.GreaterOrEqual(lastDescribe.GetInfo().GetActionCount(), int64(2))
	s.False(lastDescribe.GetSchedule().GetState().GetPaused(), "a retry-failed workflow must not pause the schedule")
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
func testScheduledWorkflowContinueAsNewCompletion(t *testing.T, newContext contextFactory) {
	// The scheduler matches the continued-as-new run's completion by the request ID carried in the
	// completion callback token, which only survives continue-as-new in the envelope token format.
	// That format is gated off by default for safe rollout, so enable it explicitly here.
	opts := append(scheduleCommonOpts(t), testcore.WithDynamicConfig(callback.EncodeInternalTokenWithEnvelope, true))
	s := testcore.NewEnv(t, opts...)

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
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	require.NoError(t, err)

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
	s.Eventually(func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
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
	s.NotEmpty(completedWFID)
	canHist := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: completedWFID})
	canCB := getCompletionCallback(canHist)
	s.NotNil(canCB, "continued-as-new run must carry the completion callback")

	var continuedFromRunID string
	for _, e := range canHist {
		if a := e.GetWorkflowExecutionStartedEventAttributes(); a != nil {
			continuedFromRunID = a.GetContinuedExecutionRunId()
			break
		}
	}
	s.NotEmpty(continuedFromRunID, "completed action should have continued-as-new")

	origHist := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: completedWFID, RunId: continuedFromRunID})
	origCB := getCompletionCallback(origHist)
	s.NotNil(origCB, "original run must carry the completion callback")
	s.Equal(origCB.GetNexus().GetHeader(), canCB.GetNexus().GetHeader(), "completion callback must be propagated intact across continue-as-new")
}

func testListSchedulesReturnsWorkflowStatus(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
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
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	req := &workflowservice.CreateScheduleRequest{
		Namespace:    s.Namespace().String(),
		ScheduleId:   sid,
		Schedule:     schedule,
		InitialPatch: patch,
		RequestId:    uuid.NewString(),
	}
	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)

	// validate RecentActions made it to visibility
	listResp := getScheduleEntryFromVisibility(s, sid, newContext, func(listResp *schedulepb.ScheduleListEntry) bool {
		return len(listResp.Info.RecentActions) >= 1
	})
	s.Len(listResp.Info.RecentActions, 1)

	a1 := listResp.Info.RecentActions[0]
	s.True(strings.HasPrefix(a1.StartWorkflowResult.WorkflowId, wid))
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, a1.StartWorkflowStatus)

	// let the started workflow complete
	_, err = s.FrontendClient().SignalWorkflowExecution(newContext(s.Context()), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: a1.StartWorkflowResult.WorkflowId,
			RunId:      a1.StartWorkflowResult.RunId,
		},
		SignalName: resumeSignal,
	})
	s.NoError(err)

	// now wait for second recent action to land in visbility
	listResp = getScheduleEntryFromVisibility(s, sid, newContext, func(listResp *schedulepb.ScheduleListEntry) bool {
		return len(listResp.Info.RecentActions) >= 2
	})

	a1 = listResp.Info.RecentActions[0]
	a2 := listResp.Info.RecentActions[1]
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, a1.StartWorkflowStatus)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, a2.StartWorkflowStatus)

	// Also verify that DescribeSchedule's output matches
	descResp, err := s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)
	assertSameRecentActions(s.T(), descResp, listResp)

	// Verify no duplicate RunIds in recent actions (regression for migration dedup bug).
	assertRecentActionsNoDuplicateRunIDs(s.T(), descResp.Info.RecentActions)
	assertRecentActionsNoDuplicateRunIDs(s.T(), listResp.Info.RecentActions)
}

func testUpdateIntervalTakesEffect(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := "sched-test-update-interval"
	wid := "sched-test-update-interval-wf"
	wt := "sched-test-update-interval-wt"

	var runs int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs, 1)
			return 0
		})
		return nil
	}
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Update the interval to be very short (1s).
	schedule.Spec.Interval[0].Interval = durationpb.New(1 * time.Second)
	_, err = s.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// After updating to 1s interval, we should see runs start within a few seconds.
	s.Eventually(
		func() bool { return atomic.LoadInt32(&runs) >= 2 },
		10*time.Second,
		500*time.Millisecond,
		"expected at least 2 runs within 10s after updating interval to 1s",
	)
}

func testListScheduleMatchingTimes(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := "sched-test-list-matching-times"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-list-matching-times",
					WorkflowType: &commonpb.WorkflowType{Name: "action"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)

	// Query for matching times over a 5-hour window.
	now := time.Now().UTC().Truncate(time.Hour).Add(time.Hour) // Start of next hour
	startTime := timestamppb.New(now)
	endTime := timestamppb.New(now.Add(5 * time.Hour))

	resp, err := s.FrontendClient().ListScheduleMatchingTimes(ctx, &workflowservice.ListScheduleMatchingTimesRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		StartTime:  startTime,
		EndTime:    endTime,
	})
	s.NoError(err)
	// With 1-hour interval over 5 hours, we expect 5 matching times.
	s.Len(resp.GetStartTime(), 5)
}

func testLimitMemoSpecSize(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
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
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)
	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)

	// Verify the memo field length limit was enforced.
	entry := getScheduleEntryFromVisibility(s, sid, newContext, nil)
	require.NotNil(t, entry)
	spec := entry.GetInfo().GetSpec()
	require.Len(t, spec.GetInterval(), expectedLimit)
	require.Len(t, spec.GetStructuredCalendar(), expectedLimit)
	require.Len(t, spec.GetExcludeStructuredCalendar(), expectedLimit)
}

func testCountSchedules(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
			State: &schedulepb.ScheduleState{
				Paused: paused,
			},
		}

		_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), &workflowservice.CreateScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		s.NoError(err)
	}

	// Test basic count (all schedules)
	s.Eventually(func() bool {
		countResp, err := s.FrontendClient().CountSchedules(newContext(s.Context()), &workflowservice.CountSchedulesRequest{
			Namespace: s.Namespace().String(),
		})
		return err == nil && countResp.Count >= 3
	}, 15*time.Second, 1*time.Second, "Expected at least 3 schedules")

	// Test count with query filter for paused schedules
	s.Eventually(func() bool {
		countResp, err := s.FrontendClient().CountSchedules(newContext(s.Context()), &workflowservice.CountSchedulesRequest{
			Namespace: s.Namespace().String(),
			Query:     fmt.Sprintf("%s = true", sadefs.TemporalSchedulePaused),
		})
		return err == nil && countResp.Count >= 1
	}, 15*time.Second, 1*time.Second, "Expected at least 1 paused schedule")

	// Test count with query filter for non-paused schedules
	s.Eventually(func() bool {
		countResp, err := s.FrontendClient().CountSchedules(newContext(s.Context()), &workflowservice.CountSchedulesRequest{
			Namespace: s.Namespace().String(),
			Query:     fmt.Sprintf("%s = false", sadefs.TemporalSchedulePaused),
		})
		return err == nil && countResp.Count >= 2
	}, 15*time.Second, 1*time.Second, "Expected at least 2 non-paused schedules")
}

func testListSchedulesPagination(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), &workflowservice.CreateScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		s.NoError(err)
	}

	// Wait for all schedules to be visible.
	s.Eventually(func() bool {
		countResp, err := s.FrontendClient().CountSchedules(newContext(s.Context()), &workflowservice.CountSchedulesRequest{
			Namespace: s.Namespace().String(),
		})
		return err == nil && countResp.Count >= numSchedules
	}, 15*time.Second, 1*time.Second, "Expected all schedules to be visible")

	// Paginate with page size 2 and collect all schedule IDs.
	ctx := newContext(s.Context())
	var allIDs []string
	var nextPageToken []byte
	for {
		resp, err := s.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 2,
			NextPageToken:   nextPageToken,
		})
		s.NoError(err)
		for _, entry := range resp.Schedules {
			allIDs = append(allIDs, entry.ScheduleId)
		}
		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			break
		}
		// Each page except possibly the last should have entries.
		s.NotEmpty(resp.Schedules)
	}

	// Verify we found all created schedules.
	for i := range numSchedules {
		sid := fmt.Sprintf("%s%d", sidPrefix, i)
		s.Contains(allIDs, sid, "Expected schedule %s in paginated results", sid)
	}
}

func testListSchedulesFilterAndEntryFields(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		State: &schedulepb.ScheduleState{
			Paused: true,
			Notes:  "paused for test",
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
	s.NoError(err)

	// Wait for the schedule to appear with correct paused state.
	entry := getScheduleEntryFromVisibility(s, sid, newContext, func(e *schedulepb.ScheduleListEntry) bool {
		return e.Info.Paused
	})

	// Verify entry-level fields.
	s.Equal(schMemo.Data, entry.Memo.Fields["schedmemo1"].Data)
	s.Equal(schSAValue.Data, entry.SearchAttributes.IndexedFields[csaKeyword].Data)
	s.Equal(wt, entry.Info.WorkflowType.Name)
	s.True(entry.Info.Paused)
	s.Equal("paused for test", entry.Info.Notes)

	// Filter by TemporalSchedulePaused should find this schedule.
	s.EventuallyWithT(func(c *assert.CollectT) {
		listResp, err := s.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 10,
			Query:           fmt.Sprintf("%s = true", sadefs.TemporalSchedulePaused),
		})
		require.NoError(c, err)
		var ids []string
		for _, e := range listResp.Schedules {
			ids = append(ids, e.ScheduleId)
		}
		require.Contains(c, ids, sid)
	}, 15*time.Second, 1*time.Second)

	// Filter for paused=false should not include this schedule.
	s.EventuallyWithT(func(c *assert.CollectT) {
		listResp, err := s.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 10,
			Query:           fmt.Sprintf("%s = false", sadefs.TemporalSchedulePaused),
		})
		require.NoError(c, err)
		for _, e := range listResp.Schedules {
			require.NotEqual(c, sid, e.ScheduleId)
		}
	}, 15*time.Second, 1*time.Second)
}

func testListSchedulesFilterByScheduleID(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
			State: &schedulepb.ScheduleState{Paused: true},
		}
	}

	ctx := newContext(s.Context())

	// Create two schedules.
	for _, sid := range []string{sid1, sid2} {
		_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule(sid),
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		s.NoError(err)
	}

	// Wait for both schedules to appear in visibility.
	getScheduleEntryFromVisibility(s, sid1, newContext, nil)
	getScheduleEntryFromVisibility(s, sid2, newContext, nil)

	listScheduleIDs := func(query string) []string {
		t.Helper()
		listResp, err := s.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
			MaximumPageSize: 10,
			Query:           query,
		})
		require.NoError(t, err)
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
			s.EventuallyWithT(func(c *assert.CollectT) {
				ids := listScheduleIDs(tc.query)
				require.Len(c, ids, len(tc.wantIDs))
				for _, want := range tc.wantIDs {
					require.Contains(c, ids, want)
				}
			}, 15*time.Second, 1*time.Second)
		})
	}
}

func testScheduleInternalTaskQueue(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)
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
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		}

		ctx := newContext(s.Context())
		_, err := s.FrontendClient().CreateSchedule(ctx, req)
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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		req := &workflowservice.CreateScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		}

		ctx := newContext(s.Context())
		_, err := s.FrontendClient().CreateSchedule(ctx, req)
		require.NoError(t, err)

		// Now try to update with internal task queue
		schedule.Action.GetStartWorkflow().TaskQueue = &taskqueuepb.TaskQueue{
			Name: primitives.PerNSWorkerTaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}
		updateReq := &workflowservice.UpdateScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		}

		_, err = s.FrontendClient().UpdateSchedule(ctx, updateReq)
		require.Error(t, err)
		var invalidArgument *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgument)
		require.Contains(t, err.Error(), errorMessageKeyword)
	})
}

func testScheduledWorkflowDoubleReset(t *testing.T, newContext contextFactory, enableCHASMCallbacks bool) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)
	s.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, enableCHASMCallbacks)

	sid := "sched-test-double-reset"
	wid := "sched-test-double-reset-wf"
	wt := "sched-test-double-reset-wt"

	s.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		ch := workflow.GetSignalChannel(ctx, "complete")
		var signal any
		ch.Receive(ctx, &signal)
		return nil
	}, workflow.RegisterOptions{Name: wt})

	ctx := newContext(s.Context())

	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		RequestId: uuid.NewString(),
	})
	s.NoError(err)

	// Wait for scheduler to start the workflow and show it as RUNNING.
	listEntry := getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.RecentActions) >= 1 &&
			ent.Info.RecentActions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	a1 := listEntry.Info.RecentActions[0]
	wfExec := &commonpb.WorkflowExecution{
		WorkflowId: a1.StartWorkflowResult.WorkflowId,
		RunId:      a1.StartWorkflowResult.RunId,
	}

	s.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		s.GetHistoryFunc(s.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	origDesc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: wfExec,
	})
	s.NoError(err)
	var originalStartReqID string
	for reqID, info := range origDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
		if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
			originalStartReqID = reqID
			break
		}
	}
	s.NotEmpty(originalStartReqID, "original run must have a request ID for WorkflowExecutionStarted")

	resp1, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         wfExec,
		Reason:                    "double-reset-test-first",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	resetRun1 := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resp1.RunId,
	}

	s.EventuallyWithT(func(col *assert.CollectT) {
		resetDesc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: resetRun1,
		})
		require.NoError(col, err)
		var resetStartReqID string
		for reqID, info := range resetDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				resetStartReqID = reqID
				break
			}
		}
		require.Equal(col, originalStartReqID, resetStartReqID,
			"start request ID must be preserved across first reset")
	}, 10*time.Second, 100*time.Millisecond)

	resp2, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         resetRun1,
		Reason:                    "double-reset-test-second",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	resetRun2 := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resp2.RunId,
	}

	s.EventuallyWithT(func(col *assert.CollectT) {
		resetDesc, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: resetRun2,
		})
		require.NoError(col, err)
		var resetStartReqID string
		for reqID, info := range resetDesc.GetWorkflowExtendedInfo().GetRequestIdInfos() {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				resetStartReqID = reqID
				break
			}
		}
		require.Equal(col, originalStartReqID, resetStartReqID,
			"start request ID must be preserved across double reset")
	}, 10*time.Second, 100*time.Millisecond)

	_, err = s.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfExec.WorkflowId,
		},
		SignalName: "complete",
	})
	s.NoError(err)

	getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		for _, action := range ent.Info.RecentActions {
			if action.GetStartWorkflowResult().GetRunId() == wfExec.RunId {
				return action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			}
		}
		return false
	})
}

func testResetWithAdditionalCallback(t *testing.T, newContext contextFactory, enableCHASMCallbacks bool) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)
	s.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, enableCHASMCallbacks)
	s.OverrideDynamicConfig(
		callback.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	sid := "sched-test-reset-extra-cb"
	wid := "sched-test-reset-extra-cb-wf"
	wt := "sched-test-reset-extra-cb-wt"

	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()
	secondCallbackURL := func() string {
		hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: ch})
		srv := httptest.NewServer(hh)
		t.Cleanup(func() { srv.Close() })
		return srv.URL + "/callback"
	}()

	s.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		sigCh := workflow.GetSignalChannel(ctx, "complete")
		var signal any
		sigCh.Receive(ctx, &signal)
		return nil
	}, workflow.RegisterOptions{Name: wt})

	ctx := newContext(s.Context())

	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		InitialPatch: &schedulepb.SchedulePatch{
			TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
		},
		RequestId: uuid.NewString(),
	})
	s.NoError(err)

	listEntry := getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		return len(ent.Info.RecentActions) >= 1 &&
			ent.Info.RecentActions[0].GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
	})
	a1 := listEntry.Info.RecentActions[0]
	wfExec := &commonpb.WorkflowExecution{
		WorkflowId: a1.StartWorkflowResult.WorkflowId,
		RunId:      a1.StartWorkflowResult.RunId,
	}

	s.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		s.GetHistoryFunc(s.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	attachRequestID := uuid.NewString()
	attachResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                attachRequestID,
		Namespace:                s.Namespace().String(),
		WorkflowId:               wfExec.WorkflowId,
		WorkflowType:             &commonpb.WorkflowType{Name: wt},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
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
	s.NoError(err)
	s.False(attachResp.Started, "expected to attach to existing run, not start a new one")

	s.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted
		5 WorkflowExecutionOptionsUpdated`,
		s.GetHistoryFunc(s.Namespace().String(), wfExec),
		5*time.Second,
		10*time.Millisecond,
	)

	resetResp, err := s.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 s.Namespace().String(),
		WorkflowExecution:         wfExec,
		Reason:                    "reset-with-additional-callback-test",
		WorkflowTaskFinishEventId: 3,
		RequestId:                 uuid.NewString(),
	})
	s.NoError(err)
	resetRun := &commonpb.WorkflowExecution{
		WorkflowId: wfExec.WorkflowId,
		RunId:      resetResp.RunId,
	}

	var startRequestID string
	s.EventuallyWithT(func(col *assert.CollectT) {
		descResp, err := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: resetRun,
		})
		require.NoError(col, err)
		require.Len(col, descResp.Callbacks, 2)
		reqIDs := descResp.GetWorkflowExtendedInfo().GetRequestIdInfos()
		attachInfo, ok := reqIDs[attachRequestID]
		require.True(col, ok, "attachRequestId not found in RequestIdInfos")
		require.Equal(col, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, attachInfo.GetEventType())
		for reqID, info := range reqIDs {
			if info.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
				startRequestID = reqID
				break
			}
		}
		require.NotEmpty(col, startRequestID, "no request ID found for WorkflowExecutionStarted")
		require.NotEqual(col, startRequestID, attachRequestID,
			"schedule callback and manually-attached callback must have different request IDs")
	}, 10*time.Second, 100*time.Millisecond)

	_, err = s.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wfExec.WorkflowId,
		},
		SignalName: "complete",
	})
	s.NoError(err)

	getScheduleEntryFromVisibility(s, sid, newContext, func(ent *schedulepb.ScheduleListEntry) bool {
		for _, action := range ent.Info.RecentActions {
			if action.GetStartWorkflowResult().GetRunId() == wfExec.RunId {
				return action.GetStartWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
			}
		}
		return false
	})

	select {
	case completion := <-ch.requestCh:
		s.Equal(nexus.OperationStateSucceeded, completion.State)
		ch.requestCompleteCh <- nil
	case <-time.After(10 * time.Second):
		s.Fail("timeout waiting for second callback to be delivered")
	}
}

// testCreatesWorkflowSentinel tests that creating a CHASM schedule also starts a
// dummy workflow to reserve the schedule ID in the V1 workflow ID-space.
func testCreatesWorkflowSentinel(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   testcore.RandomizeStr("identity"),
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.NoError(err)

	// Verify the dummy workflow was created to reserve the V1 workflow ID.
	sentinelWfID := scheduler.WorkflowIDPrefix + sid
	var descResp *workflowservice.DescribeWorkflowExecutionResponse
	s.Eventually(func() bool {
		descResp, err = s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: sentinelWfID},
		})
		return err == nil
	}, 15*time.Second, 500*time.Millisecond, "dummy sentinel workflow should exist")
	s.Equal(dummy.DummyWFTypeName, descResp.WorkflowExecutionInfo.Type.Name)
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, descResp.WorkflowExecutionInfo.Status)

	// Verify visibility shows exactly one schedule (not the dummy workflow).
	getScheduleEntryFromVisibility(s, sid, newContext, nil)
	listResp, err := s.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 5,
	})
	s.NoError(err)
	s.Len(listResp.Schedules, 1)

	countResp, err := s.FrontendClient().CountSchedules(ctx, &workflowservice.CountSchedulesRequest{
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
	s.Equal(int64(1), countResp.Count)
}

func testStateSizeBytesReported(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		State: &schedulepb.ScheduleState{Paused: true},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.Positive(desc.GetInfo().GetStateSizeBytes(), "Describe should report a non-zero StateSizeBytes")
}

// testCreatesCHASMSentinel tests that creating a V1 schedule also creates a
// CHASM sentinel to reserve the schedule ID in the CHASM execution space.
func testCreatesCHASMSentinel(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   testcore.RandomizeStr("identity"),
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.NoError(err)

	// Verify a CHASM sentinel was created to reserve the schedule ID.
	// DescribeSchedule should return NotFound, as well as CreateSentinel
	nsID := s.NamespaceID().String()
	s.Eventually(func() bool {
		_, descErr := s.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: s.Namespace().String(), ScheduleId: sid},
			},
		)
		var notFoundErr *serviceerror.NotFound
		if !errors.As(descErr, &notFoundErr) {
			return false
		}

		// A CHASM CreateSchedule should also fail with NotFound because
		// the sentinel blocks it.
		_, createErr := s.GetTestCluster().SchedulerClient().CreateSchedule(
			ctx,
			&schedulerpb.CreateScheduleRequest{
				NamespaceId: nsID,
				FrontendRequest: &workflowservice.CreateScheduleRequest{
					Namespace:  s.Namespace().String(),
					ScheduleId: sid,
					RequestId:  testcore.RandomizeStr("test-sentinel-check"),
					Schedule:   schedule,
				},
			},
		)
		return errors.As(createErr, &notFoundErr)
	}, 15*time.Second, 500*time.Millisecond, "CHASM sentinel should exist for V1 schedule")

	// Verify visibility shows exactly one schedule (not the sentinel).
	getScheduleEntryFromVisibility(s, sid, newContext, nil)
	listResp, err := s.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
		Namespace:       s.Namespace().String(),
		MaximumPageSize: 5,
	})
	s.NoError(err)
	s.Len(listResp.Schedules, 1)

	countResp, err := s.FrontendClient().CountSchedules(ctx, &workflowservice.CountSchedulesRequest{
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
	s.Equal(int64(1), countResp.Count)
}

// testSkipsWorkflowSentinelWhenDisabled asserts that a CHASM CreateSchedule
// does not start the dummy V1 workflow when EnableCHASMSchedulerSentinels is off.
func testSkipsWorkflowSentinelWhenDisabled(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, append(scheduleCommonOpts(t),
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   testcore.RandomizeStr("identity"),
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.NoError(err)

	// The dummy V1 workflow that reserves the schedule ID is gated on the
	// sentinel flag, so it must not exist.
	sentinelWfID := scheduler.WorkflowIDPrefix + sid
	_, descErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: sentinelWfID},
	})
	var notFoundErr *serviceerror.NotFound
	s.ErrorAs(descErr, &notFoundErr, "no dummy sentinel workflow should be created when sentinels are disabled")
}

// testSkipsCHASMSentinelWhenDisabled asserts that a V1 CreateSchedule does not
// create a CHASM sentinel when EnableCHASMSchedulerSentinels is off.
func testSkipsCHASMSentinelWhenDisabled(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, append(scheduleCommonOpts(t),
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   testcore.RandomizeStr("identity"),
		RequestId:  testcore.RandomizeStr("request-id"),
	})
	s.NoError(err)

	// With no CHASM sentinel reserving the ID, a CHASM CreateSchedule for the
	// same ID must not be blocked by the NotFound (sentinel) signal.
	nsID := s.NamespaceID().String()
	_, createErr := s.GetTestCluster().SchedulerClient().CreateSchedule(
		ctx,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: nsID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  s.Namespace().String(),
				ScheduleId: sid,
				RequestId:  testcore.RandomizeStr("test-no-sentinel"),
				Schedule:   schedule,
			},
		},
	)
	s.NoError(createErr, "no CHASM sentinel should block CreateSchedule when sentinels are disabled, got: %v", createErr)
}

func testCreateScheduleAlreadyExists(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, req)
	s.NoError(err)

	// Try to create again with a different request ID - should fail with AlreadyExists
	req.RequestId = uuid.NewString()
	_, err = s.FrontendClient().CreateSchedule(ctx, req)
	s.Error(err)

	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	s.ErrorAs(err, &alreadyStarted)
	s.Contains(err.Error(), sid)
}

// CreateSchedule is special-cased in the SDKs to translate
// serviceerror.WorkflowExecutionAlreadyStarted into
// temporal.ErrScheduleAlreadyRunning. This tests the SDK's behavior E2E against
// the handler. A similar test exists in the features repository.
func testCreateScheduleDuplicateSdkError(t *testing.T, useCHASM bool) {
	opts := scheduleCommonOpts(t)
	if useCHASM {
		opts = append(opts, testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true))
	}
	s := testcore.NewEnv(t, opts...)

	sid := "sched-test-duplicate-sdk-" + uuid.NewString()[:8]
	schedOpts := sdkclient.ScheduleOptions{
		ID:   sid,
		Spec: sdkclient.ScheduleSpec{},
		Action: &sdkclient.ScheduleWorkflowAction{
			ID:        "wf-" + sid,
			Workflow:  "noop",
			TaskQueue: s.WorkerTaskQueue(),
		},
		Paused: true,
	}

	ctx := s.Context()
	handle, err := s.SdkClient().ScheduleClient().Create(ctx, schedOpts)
	s.NoError(err)
	defer func() { _ = handle.Delete(context.Background()) }()

	_, err = s.SdkClient().ScheduleClient().Create(ctx, schedOpts)
	s.ErrorIs(err, temporal.ErrScheduleAlreadyRunning)
}

func testPatchRejectsExcessBackfillers(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)
	sid := "sched-test-too-many-backfillers"
	wt := "sched-test-too-many-backfillers-wt"

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "wf-too-many-backfillers",
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		State: &schedulepb.ScheduleState{Paused: true},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Patch with 50 backfill requests at a time until we reach the limit of 100.
	now := time.Now()
	for i := 0; i < 100; i += 50 {
		backfills := make([]*schedulepb.BackfillRequest, 50)
		for j := range backfills {
			backfills[j] = &schedulepb.BackfillRequest{
				StartTime:     timestamppb.New(now),
				EndTime:       timestamppb.New(now.Add(time.Minute)),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			}
		}
		_, err = s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
			Patch: &schedulepb.SchedulePatch{
				BackfillRequest: backfills,
			},
			Identity:  "test",
			RequestId: uuid.NewString(),
		})
		s.NoError(err)
	}

	// The next patch should be rejected.
	_, err = s.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			BackfillRequest: []*schedulepb.BackfillRequest{
				{
					StartTime:     timestamppb.New(now),
					EndTime:       timestamppb.New(now.Add(time.Minute)),
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.Error(err)
	var failedPrecondition *serviceerror.FailedPrecondition
	s.ErrorAs(err, &failedPrecondition)
	s.Contains(err.Error(), "too many concurrent backfillers")
}

func testMigrationCallbackAttach(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sid")
	wid := testcore.RandomizeStr("wid")
	wt := testcore.RandomizeStr("wt")

	resumeSignal := "resume"
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			workflow.GetSignalChannel(ctx, resumeSignal).Receive(ctx, nil)
			return nil
		},
		workflow.RegisterOptions{Name: wt},
	)

	ctx := newContext(s.Context())
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    s.Namespace().String(),
		WorkflowId:   wid,
		WorkflowType: &commonpb.WorkflowType{Name: wt},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:     testcore.RandomizeStr("identity"),
		RequestId:    testcore.RandomizeStr("request-id"),
	})
	s.NoError(err)

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
					RunId:       startResp.RunId,
					RequestId:   uuid.NewString(),
					Attempt:     1,
					HasCallback: false,
				},
			},
		},
	}
	_, err = s.GetTestCluster().SchedulerClient().CreateFromMigrationState(
		ctx,
		&schedulerpb.CreateFromMigrationStateRequest{
			NamespaceId: nsID,
			State:       migrationState,
		},
	)
	s.NoError(err)

	s.Eventually(func() bool {
		descResp, err := s.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: s.Namespace().String(), ScheduleId: sid},
			},
		)
		if err != nil {
			return false
		}
		running := descResp.GetFrontendResponse().GetInfo().GetRunningWorkflows()
		return len(running) > 0 && running[0].WorkflowId == wid
	}, 15*time.Second, 500*time.Millisecond, "CHASM scheduler should show running workflow")

	_, err = s.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      startResp.RunId,
		},
		SignalName: resumeSignal,
	})
	s.NoError(err)

	s.Eventually(func() bool {
		descResp, err := s.GetTestCluster().SchedulerClient().DescribeSchedule(
			ctx,
			&schedulerpb.DescribeScheduleRequest{
				NamespaceId:     nsID,
				FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: s.Namespace().String(), ScheduleId: sid},
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
}

// testCHASMCanListV1Schedules tests that a schedule created in the V1 stack
// will also be visible in the V2 stack.
func testCHASMCanListV1Schedules(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	// Create on V1 stack.
	_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), req)
	s.NoError(err)

	// Pause so that `FutureActionTimes` doesn't change between calls.
	_, err = s.FrontendClient().PatchSchedule(newContext(s.Context()), &workflowservice.PatchScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Patch: &schedulepb.SchedulePatch{
			Pause: "halt",
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.NoError(err)

	// Sanity test, list with V1 handler.
	v1Entry := getScheduleEntryFromVisibility(s, sid, newContext, func(sle *schedulepb.ScheduleListEntry) bool {
		return sle.GetInfo().Paused
	})
	s.NotNil(v1Entry.GetInfo())

	// Count with V1 handler.
	v1CountResp, err := s.FrontendClient().CountSchedules(newContext(s.Context()), &workflowservice.CountSchedulesRequest{
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
	s.GreaterOrEqual(v1CountResp.Count, int64(1), "Expected at least 1 schedule with V1 handler")

	// Flip on CHASM experiment and make sure we can still list.
	chasmEntry := getScheduleEntryFromVisibility(s, sid, chasmContextFactory, nil)
	s.NotNil(chasmEntry.GetInfo())
	s.ProtoEqual(chasmEntry.GetInfo(), v1Entry.GetInfo())

	// Count with CHASM handler and verify it matches V1 count.
	chasmCountResp, err := s.FrontendClient().CountSchedules(chasmContextFactory(s.Context()), &workflowservice.CountSchedulesRequest{
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)
	s.Equal(v1CountResp.Count, chasmCountResp.Count, "CHASM and V1 counts should match")
}

// testRefresh applies to V1 scheduler only; V2 does not support/need manual refresh.
func testRefresh(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:                &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					WorkflowExecutionTimeout: durationpb.New(3 * time.Second),
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	var runs int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs, 1)
			return 0
		})
		s.NoError(workflow.Sleep(ctx, 10*time.Second)) // longer than execution timeout
		return nil
	}
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), req)
	s.NoError(err)

	s.Eventually(func() bool { return atomic.LoadInt32(&runs) == 1 }, 20*time.Second, 200*time.Millisecond)

	// workflow has started but is now sleeping. it will timeout in 2 seconds.

	describeResp, err := s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.Len(describeResp.Info.RunningWorkflows, 1)

	events1 := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
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

	s.EqualHistoryEvents(expectedHistory, events1)

	time.Sleep(4 * time.Second) //nolint:forbidigo
	// now it has timed out, but the scheduler hasn't noticed yet. we can prove it by checking
	// its history.

	events2 := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
	s.EqualHistoryEvents(expectedHistory, events2)

	// when we describe we'll force a refresh and see it timed out
	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	s.NoError(err)
	s.Empty(describeResp.Info.RunningWorkflows)

	// check scheduler has gotten the refresh and done some stuff. signal is sent without waiting so we need to wait.
	s.Eventually(func() bool {
		events3 := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
		return len(events3) > len(events2)
	}, 5*time.Second, 100*time.Millisecond)
}

// testListBeforeRun only applies to V1, as V2 scheduler does not involve the
// per-NS worker or workflow.
func testListBeforeRun(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, append(scheduleCommonOpts(t),
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	}

	startTime := time.Now()

	_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), req)
	s.NoError(err)

	entry := getScheduleEntryFromVisibility(s, sid, newContext, nil)
	s.NotNil(entry.Info)
	s.ProtoEqual(schedule.Spec, entry.Info.Spec)
	s.Equal(wt, entry.Info.WorkflowType.Name)
	s.False(entry.Info.Paused)
	s.Greater(len(entry.Info.FutureActionTimes), 1)
	s.True(entry.Info.FutureActionTimes[0].AsTime().After(startTime))
}

// testRateLimit applies only to V1, as V2 scheduler does not impose its own rate limiting.
func testRateLimit(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, append(scheduleCommonOpts(t),
		testcore.WithDynamicConfig(dynamicconfig.SchedulerNamespaceStartWorkflowRPS, 1.0),
	)...)

	sid := "sched-test-rate-limit-%d"
	wid := "sched-test-rate-limit-wf-%d"
	wt := "sched-test-rate-limit-wt"

	var runs int32
	workflowFn := func(ctx workflow.Context) error {
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			atomic.AddInt32(&runs, 1)
			return 0
		})
		return nil
	}
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
		_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), &workflowservice.CreateScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: fmt.Sprintf(sid, i),
			Schedule:   schedule,
			Identity:   "test",
			RequestId:  uuid.NewString(),
		})
		s.NoError(err)
	}

	time.Sleep(5 * time.Second) //nolint:forbidigo

	// With no rate limit, we'd see 10/second == 50 workflows run. With a limit of 1/sec, we
	// expect to see around 5.
	s.Less(atomic.LoadInt32(&runs), int32(10))
}

// testNextTimeCache only applies to V1.
func testNextTimeCache(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}
	req := &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wt})

	_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), req)
	s.NoError(err)

	// wait for at least 13 runs
	const count = 13
	s.Eventually(func() bool { return runs.Load() >= count }, (count+10)*time.Second, 500*time.Millisecond)

	// there should be only four side effects for 13 runs, and only two mentioning "Next"
	// (cache refills)
	events := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: scheduler.WorkflowIDPrefix + sid})
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
	s.Equal(expectedRefills+uuidCacheRefills, sideEffects)
	s.Equal(expectedRefills, nextTimeSideEffects)
}

// getScheduleEntryFromVisibility polls visibility using ListSchedules until it finds a schedule
// with the given id and for which the optional predicate function returns true.
func getScheduleEntryFromVisibility(env testcore.Env, sid string, newContext contextFactory, predicate func(*schedulepb.ScheduleListEntry) bool) *schedulepb.ScheduleListEntry {
	env.T().Helper()
	var slEntry *schedulepb.ScheduleListEntry
	require.Eventually(env.T(), func() bool { // wait for visibility
		listResp, err := env.FrontendClient().ListSchedules(newContext(env.Context()), &workflowservice.ListSchedulesRequest{
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
func testUpdateScheduleMemo(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := "sched-test-update-memo"
	wid := "sched-test-update-memo-wf"
	wt := "sched-test-update-memo-wt"

	s.SdkWorker().RegisterWorkflowWithOptions(
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	memo1 := payload.EncodeString("val1")
	memo2 := payload.EncodeString("val2")

	// Create schedule with initial memo.
	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
	require.NoError(t, err)

	// Verify initial memo.
	describeResp, err := s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Equal(t, memo1.Data, describeResp.Memo.Fields["key1"].Data)
	require.Equal(t, memo2.Data, describeResp.Memo.Fields["key2"].Data)

	// Update: replace memo with only key3 (key1 and key2 should be gone).
	memo3 := payload.EncodeString("new")
	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
	require.NoError(t, err)

	// Verify replaced memo.
	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Nil(t, describeResp.Memo.Fields["key1"], "key1 should be gone after replace")
	require.Nil(t, describeResp.Memo.Fields["key2"], "key2 should be gone after replace")
	require.Equal(t, memo3.Data, describeResp.Memo.Fields["key3"].Data, "key3 should be set")

	// Update with nil memo (no change).
	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Verify memo unchanged.
	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Equal(t, memo3.Data, describeResp.Memo.Fields["key3"].Data, "key3 should be unchanged")

	// Update with empty memo (clear all).
	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{},
		},
	})
	require.NoError(t, err)

	// Verify memo cleared.
	describeResp, err = s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Empty(t, describeResp.Memo.GetFields(), "memo should be empty after replace with empty map")
}

func testUpdateScheduleMemoRejected(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := "sched-test-update-memo-rejected"
	wid := "sched-test-update-memo-rejected-wf"
	wt := "sched-test-update-memo-rejected-wt"

	s.SdkWorker().RegisterWorkflowWithOptions(
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create V1 schedule.
	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Update with memo should be rejected.
	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
	require.Error(t, err)
	var failedPrecondition *serviceerror.FailedPrecondition
	require.ErrorAs(t, err, &failedPrecondition)
	require.Contains(t, err.Error(), "memo updates are not supported on workflow-backed schedules")
}

func testUpdateScheduleMemoOnly(t *testing.T, newContext contextFactory) {
	// UpdateScheduleRequest uses replace semantics for the schedule field, so omitting it
	// causes the schedule to be unset. Memo-only updates require the server to skip replacing
	// the schedule when the field is nil, similar to how memo and search_attributes are handled.
	t.Skip("memo-only updates not yet supported: omitting the schedule field unsets the schedule")

	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := "sched-test-update-memo-only"
	wid := "sched-test-update-memo-only-wf"
	wt := "sched-test-update-memo-only-wt"

	s.SdkWorker().RegisterWorkflowWithOptions(
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create schedule with initial memo.
	memo1 := payload.EncodeString("val1")
	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{"key1": memo1},
		},
	})
	require.NoError(t, err)

	// Update only memo, without setting the schedule field.
	memo2 := payload.EncodeString("val2")
	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo: &commonpb.Memo{
			Fields: map[string]*commonpb.Payload{"key1": memo2},
		},
	})
	require.NoError(t, err)

	// Verify memo was updated and schedule is still intact.
	describeResp, err := s.FrontendClient().DescribeSchedule(newContext(s.Context()), &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.Equal(t, memo2.Data, describeResp.Memo.Fields["key1"].Data, "memo should be updated")
	require.NotNil(t, describeResp.Schedule.Spec, "schedule spec should not be nil")
	require.NotEmpty(t, describeResp.Schedule.Spec.Interval, "schedule spec intervals should be preserved")
	require.NotNil(t, describeResp.Schedule.Action, "schedule action should be preserved")
}

func testCHASMUnpauseResumesProcessing(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := "sched-test-unpause-resumes"
	wid := "sched-test-unpause-resumes-wf"
	wt := "sched-test-unpause-resumes-wt"

	var runs int32
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			workflow.SideEffect(ctx, func(ctx workflow.Context) any {
				atomic.AddInt32(&runs, 1)
				return 0
			})
			return nil
		},
		workflow.RegisterOptions{Name: wt},
	)

	_, err := s.FrontendClient().CreateSchedule(newContext(s.Context()), &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		},
		Identity:  "test",
		RequestId: uuid.NewString(),
	})
	s.NoError(err)

	// Wait for the schedule to fire at least once, confirming it's running.
	s.Eventually(func() bool { return atomic.LoadInt32(&runs) >= 1 }, 15*time.Second, 500*time.Millisecond)

	// Pause.
	_, err = s.FrontendClient().PatchSchedule(newContext(s.Context()), &workflowservice.PatchScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Patch:      &schedulepb.SchedulePatch{Pause: "pausing for test"},
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Wait for the already-queued generator task to run after pause. That task
	// observes paused state, performs no-op scheduling, and then the schedule
	// becomes quiescent (no new runs over a stability window).
	stableSamples := 0
	lastRuns := atomic.LoadInt32(&runs)
	s.Eventually(func() bool {
		currentRuns := atomic.LoadInt32(&runs)
		if currentRuns == lastRuns {
			stableSamples++
		} else {
			lastRuns = currentRuns
			stableSamples = 0
		}
		return stableSamples >= 6
	}, 15*time.Second, 500*time.Millisecond)
	runsBeforeUnpause := atomic.LoadInt32(&runs)

	// Unpause.
	_, err = s.FrontendClient().PatchSchedule(newContext(s.Context()), &workflowservice.PatchScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Patch:      &schedulepb.SchedulePatch{Unpause: "resuming"},
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// The generator should be kicked immediately on unpause and new runs should follow.
	s.Eventually(
		func() bool { return atomic.LoadInt32(&runs) > runsBeforeUnpause },
		15*time.Second,
		500*time.Millisecond,
		"schedule should resume processing after unpause",
	)
}

// testPausedDropsCatchup verifies that an action scheduled by the spec during
// a paused window is NOT invoked when the schedule is unpaused.
func testPausedDropsCatchup(t *testing.T, newContext contextFactory) {
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
	ctx := newContext(s.Context())
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
func testPausedScheduleNeverIdles(t *testing.T, newContext contextFactory) {
	s := newEnvWithIdleTime(t, shortIdleTime)

	sid := testcore.RandomizeStr("sched-paused-never-idles")
	wid := testcore.RandomizeStr("sched-paused-never-idles-wf")
	wt := testcore.RandomizeStr("sched-paused-never-idles-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		Action: startWorkflowAction(s, wid, wt),
	})

	await.RequireTruef(t,
		func() bool { return runs.Load() >= 1 },
		awaitTimeout, pollInterval,
		"schedule should have fired at least once before pause",
	)

	patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{Pause: "never-idles-test"})

	// Across a window well past IdleTime, the schedule must never idle-close.
	require.Never(t, func() bool { return scheduleClosed(ctx, s, sid) },
		3*shortIdleTime, pollInterval,
		"paused schedule must not close from idle even past IdleTime")

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.True(t, desc.Schedule.State.Paused, "paused schedule must stay paused")

	// Also verify by unpausing and seeing actions resume.
	runsBeforeUnpause := runs.Load()
	patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{Unpause: "never-idles-test-resume"})
	await.RequireTruef(t,
		func() bool { return runs.Load() > runsBeforeUnpause },
		awaitTimeout, pollInterval,
		"paused-then-unpaused schedule should resume firing (it should not have closed)",
	)
}

// testPausedEmptySpecStaysOpen verifies that a schedule created with an empty
// spec (no Calendar / CronString / Interval) and Paused=true - the SDK
// manual-only pattern - can be created without timing out and remains
// describable.
func testPausedEmptySpecStaysOpen(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-paused-empty-spec")
	wid := testcore.RandomizeStr("sched-paused-empty-spec-wf")
	wt := testcore.RandomizeStr("sched-paused-empty-spec-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	// Empty spec + paused: a manual-only schedule. Create must succeed without
	// timing out (the original regression was "context deadline exceeded").
	ctx := newContext(s.Context())
	createCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	createSchedule(createCtx, t, s, sid, &schedulepb.Schedule{
		Spec:   &schedulepb.ScheduleSpec{},
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"empty paused spec must not fire any actions automatically")

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.True(t, desc.Schedule.State.Paused, "schedule must still be paused")

	// Unpause + TriggerImmediately to sanity-check the schedule is functional,
	// not just open.
	patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{
		Unpause:            "empty-spec-trigger",
		TriggerImmediately: &schedulepb.TriggerImmediatelyRequest{},
	})

	await.RequireTruef(t,
		func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"manual trigger after unpause should fire exactly one action",
	)
}

// testTriggerImmediatelyOnActiveSchedule verifies that a TriggerImmediately
// patch on a running schedule fires an extra action and leaves the schedule
// active.
func testTriggerImmediatelyOnActiveSchedule(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-trigger-on-active")
	wid := testcore.RandomizeStr("sched-trigger-on-active-wf")
	wt := testcore.RandomizeStr("sched-trigger-on-active-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		// Fire a year into the future, keeping the schedule active, but without firing
		// actions.
		Spec: &schedulepb.ScheduleSpec{
			Calendar: []*schedulepb.CalendarSpec{calendarSpec(time.Now().AddDate(1, 0, 0).UTC())},
		},
		Action: startWorkflowAction(s, wid, wt),
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	})

	await.RequireTruef(t, func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && len(desc.Info.FutureActionTimes) > 0
	}, awaitTimeout, pollInterval, "schedule should reach active state with future actions planned")
	require.Zero(t, runs.Load(), "no automated action should fire before the trigger")

	patchSchedule(ctx, t, s, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
	await.RequireTruef(t,
		func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"TriggerImmediately should fire exactly one action on the active schedule",
	)

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Info.RecentActions, "RecentActions should include the manual trigger")
	require.NotEmpty(t, desc.Info.FutureActionTimes, "schedule should still have future automated actions planned")
}

// testTriggerImmediatelyOnPausedSchedule verifies that TriggerImmediately fires
// an action even when the schedule is paused. Manual starts bypass the paused
// gate via useScheduledAction's Manual carve-out in processBuffer.
func testTriggerImmediatelyOnPausedSchedule(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-trigger-on-paused")
	wid := testcore.RandomizeStr("sched-trigger-on-paused-wf")
	wt := testcore.RandomizeStr("sched-trigger-on-paused-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	// Paused suppresses firing even though the 1s interval would otherwise tick.
	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"paused schedule must not fire automated actions before the trigger")

	patchSchedule(ctx, t, s, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))

	await.RequireTruef(t, func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"TriggerImmediately must fire exactly one action despite the schedule being paused")

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Info.RecentActions, "RecentActions should include the manual trigger")
	require.True(t, desc.Schedule.State.Paused, "schedule must still be paused after trigger fires")
}

// testTriggerImmediatelyAfterActionsExhausted verifies that TriggerImmediately
// fires an action even on a schedule that has no LimitedActions slots left.
func testTriggerImmediatelyAfterActionsExhausted(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-trigger-after-exhausted")
	wid := testcore.RandomizeStr("sched-trigger-after-exhausted-wf")
	wt := testcore.RandomizeStr("sched-trigger-after-exhausted-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec: intervalSpec(fastInterval),
		// Start exhausted so no automated action fires.
		State:  &schedulepb.ScheduleState{LimitedActions: true, RemainingActions: 0},
		Action: startWorkflowAction(s, wid, wt),
	})

	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"exhausted schedule must not auto-fire before the trigger")

	patchSchedule(ctx, t, s, sid, triggerPatch(enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))
	await.RequireTruef(t, func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"TriggerImmediately must fire exactly once despite RemainingActions=0")

	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.Info.RecentActions, "RecentActions should include the manual trigger")
	require.Equal(t, int64(0), desc.Schedule.State.RemainingActions,
		"manual trigger must not consume a LimitedActions slot")
}

// testBackfillWithBufferOneOverlap pins the expected behavior of BUFFER_ONE
// over a multi-tick backfill: the first start runs immediately, exactly one
// follow-up is buffered (Attempt=-1 deferred), the rest are dropped, and the
// deferred one runs once the first completes. Currently SKIPPED: fails on
// both V1 and CHASM because the deferred start never gets re-enabled after
// the running workflow completes. The first start fires, the rest never run.
// Likely a real bug in the BUFFER_ONE + backfill (Manual=true) interaction -
// recordCompletedAction's re-enable loop on Attempt==-1 may not be running
// against backfill-buffered starts. Worth a separate investigation.
func testBackfillWithBufferOneOverlap(t *testing.T, newContext contextFactory) {
	// TODO(temporalio/temporal): track removing this skip once the BUFFER_ONE
	// backfill deferred re-enable path is fixed. Verify by running:
	//   go test ./tests/ -run 'TestScheduleCHASM/Backfill/BufferOneOverlap' -v
	t.Skip("BUFFER_ONE backfill deferred re-enable is broken on both V1 and CHASM; see test doc")

	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-buffer-one")
	wid := testcore.RandomizeStr("sched-backfill-buffer-one-wf")
	wt := testcore.RandomizeStr("sched-backfill-buffer-one-wt")

	// Gate runs so the first-running / one-deferred / rest-dropped sequence is deterministic.
	var runs atomic.Int32
	registerGatedWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	now := time.Now().UTC()
	patchSchedule(ctx, t, s, sid, backfillPatch(now.Add(-5*time.Second), now, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE))

	// First backfill start runs with exactly one buffered behind it (BUFFER_ONE drops the rest).
	await.RequireTruef(t, func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		return descErr == nil && desc.GetInfo().GetBufferSize() == 1 && len(desc.GetInfo().GetRunningWorkflows()) == 1
	}, awaitTimeout, pollInterval, "expected exactly one running backfill start with one deferred behind it")
	require.Equal(t, int32(1), runs.Load(), "only the first backfill start should have fired so far")

	// Releasing the running start must re-enable the deferred one (Attempt=-1 -> 0) so it fires.
	require.Equal(t, 1, completeRunningWorkflows(ctx, t, s, sid))
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
func testBackfillRangeSmallerThanInterval(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-narrow")
	wid := testcore.RandomizeStr("sched-backfill-narrow-wf")
	wt := testcore.RandomizeStr("sched-backfill-narrow-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		// noOpInterval (1h) ticks on the hour; the backfill window below is mid-hour.
		Spec:   intervalSpec(noOpInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	// A 10s window (above the 1s resolution, below the 1h interval) anchored mid-hour
	// in the previous hour: always in the past and provably free of an on-the-hour tick.
	prevHour := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	windowStart := prevHour.Add(20 * time.Minute)
	windowEnd := windowStart.Add(10 * time.Second)
	patchSchedule(ctx, t, s, sid, backfillPatch(windowStart, windowEnd, enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL))

	// No spec tick falls inside a sub-interval window, so no action fires.
	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"backfill range narrower than spec interval must produce no actions")

	// And the schedule must still be describable - the backfiller drained without
	// firing anything and got cleaned up.
	_, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err, "schedule must remain describable after empty backfill")
}

// testBackfillWithSkipOverlap verifies that SKIP overlap correctly collapses a
// multi-tick backfill range to a single workflow execution.
func testBackfillWithSkipOverlap(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-skip")
	wid := testcore.RandomizeStr("sched-backfill-skip-wf")
	wt := testcore.RandomizeStr("sched-backfill-skip-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	now := time.Now().UTC()
	patchSchedule(ctx, t, s, sid, backfillPatch(now.Add(-5*time.Second), now, enumspb.SCHEDULE_OVERLAP_POLICY_SKIP))

	// SKIP collapses the 5-tick backfill to exactly one fire, and it stays there.
	await.RequireTruef(t, func() bool { return runs.Load() == 1 },
		awaitTimeout, pollInterval,
		"SKIP backfill should fire exactly once")
	require.Never(t, func() bool { return runs.Load() > 1 },
		neverWindow, pollInterval,
		"SKIP backfill must collapse to a single execution, not fire all 5 ticks")
}

func testUpdateScheduleRequestIDTooLong(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := "sched-test-update-reqid-too-long"
	wid := "sched-test-update-reqid-too-long-wf"
	wt := "sched-test-update-reqid-too-long-wt"

	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec:   intervalSpec(noOpInterval),
		Action: startWorkflowAction(s, wid, wt),
	}

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, schedule)

	// Update with an oversized request ID.
	_, err := s.FrontendClient().UpdateSchedule(ctx, &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  strings.Repeat("x", 1001),
	})
	var invalidArgReqID *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgReqID)
}

func testUpdateScheduleBlobSizeLimit(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t,
		append(scheduleCommonOpts(t),
			testcore.WithDynamicConfig(dynamicconfig.BlobSizeLimitError, 1000),
			testcore.WithDynamicConfig(dynamicconfig.BlobSizeLimitWarn, 500),
		)...,
	)

	sid := "sched-test-update-blob-limit"
	wid := "sched-test-update-blob-limit-wf"
	wt := "sched-test-update-blob-limit-wt"

	s.SdkWorker().RegisterWorkflowWithOptions(
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	// Create schedule.
	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Update with an oversized memo that exceeds the blob size limit.
	largeMemo := &commonpb.Memo{
		Fields: map[string]*commonpb.Payload{
			"key": {Data: make([]byte, 1001)},
		},
	}
	_, err = s.FrontendClient().UpdateSchedule(newContext(s.Context()), &workflowservice.UpdateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
		Memo:       largeMemo,
	})
	var invalidArgBlob *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalidArgBlob)
}

// TestScheduleCreationRolloutPercent verifies that
// CHASMSchedulerCreationRolloutPercent acts as a per-schedule sampling gate
// after EnableCHASMSchedulerCreation is on: at 50%, two schedules whose IDs
// bucket on opposite sides of the rollout land on different stacks.
func TestScheduleCreationRolloutPercent(t *testing.T) {
	opts := append(scheduleCommonOpts(t),
		// V1 worker is needed because at 50% rollout some schedules land on V1.
		testcore.WithWorkerService("V1 scheduler"),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMSchedulerCreation, true),
		testcore.WithDynamicConfig(dynamicconfig.CHASMSchedulerCreationRolloutPercent, 50),
	)
	s := testcore.NewEnv(t, opts...)
	ctx := s.Context()
	nsName := s.Namespace().String()
	nsID := s.NamespaceID().String()

	chasmSID, v1SID := testcore.PickRolloutSplit(t, nsName, 50)

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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
	}

	for _, sid := range []string{chasmSID, v1SID} {
		_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
			Namespace:  nsName,
			ScheduleId: sid,
			Schedule:   mkSchedule(),
			Identity:   testcore.RandomizeStr("identity"),
			RequestId:  uuid.NewString(),
		})
		require.NoError(t, err)
	}

	// A direct CHASM DescribeSchedule succeeds for CHASM-backed schedules and
	// returns NotFound for V1-backed schedules (whose CHASM key is a sentinel).
	describeOnCHASM := func(sid string) error {
		_, err := s.GetTestCluster().SchedulerClient().DescribeSchedule(ctx, &schedulerpb.DescribeScheduleRequest{
			NamespaceId:     nsID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: nsName, ScheduleId: sid},
		})
		return err
	}

	require.Eventually(t, func() bool { return describeOnCHASM(chasmSID) == nil }, 15*time.Second, 250*time.Millisecond,
		"schedule %q bucketed into CHASM should be describable via the CHASM handler", chasmSID)

	var notFoundErr *serviceerror.NotFound
	require.ErrorAs(t, describeOnCHASM(v1SID), &notFoundErr,
		"schedule %q bucketed into V1 should not be present on the CHASM handler", v1SID)

	// Both schedules must remain describable through the public frontend
	// regardless of which stack they live on — the V1 schedule in particular
	// must round-trip through the frontend's fallback path. The V1 workflow
	// takes a moment to be queryable after creation, so poll.
	for _, sid := range []string{chasmSID, v1SID} {
		require.Eventually(t, func() bool {
			_, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
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
func testScheduleClosesFromIdle(t *testing.T, newContext contextFactory) {
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
		t.Run(c.name, func(t *testing.T) { t.Parallel(); runScheduleClosesFromIdleCase(t, newContext, c) })
	}
}

// runScheduleClosesFromIdleCase drives a schedule whose spec exhausts itself,
// then asserts (1) the expected number of runs fire, (2) FutureActionTimes
// drains to zero, and (3) the schedule closes after IdleTime.
func runScheduleClosesFromIdleCase(t *testing.T, newContext contextFactory, c scheduleClosesCase) {
	s := newEnvWithIdleTime(t, shortIdleTime)

	sid := testcore.RandomizeStr(c.prefix)
	wid := testcore.RandomizeStr(c.prefix + "-wf")
	wt := testcore.RandomizeStr(c.prefix + "-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   c.buildSpec(time.Now().UTC()),
		State:  c.state,
		Action: startWorkflowAction(s, wid, wt),
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
func testManualOnlyUnpausedClosesFromIdle(t *testing.T, newContext contextFactory) {
	s := newEnvWithIdleTime(t, shortIdleTime)

	sid := testcore.RandomizeStr("sched-manual-only-unpaused")
	wid := testcore.RandomizeStr("sched-manual-only-wf")
	wt := testcore.RandomizeStr("sched-manual-only-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
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
func testPauseDuringIdleWindow(t *testing.T, newContext contextFactory) {
	// Deliberately longer than shortIdleTime: the pause/unpause RPCs must land
	// inside the idle window (before the deadline) even under slow CI.
	idleTime := 10 * time.Second
	s := newEnvWithIdleTime(t, idleTime)

	sid := testcore.RandomizeStr("sched-pause-during-idle")
	wid := testcore.RandomizeStr("sched-pause-during-idle-wf")
	wt := testcore.RandomizeStr("sched-pause-during-idle-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
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
func testBackfillBlocksIdleClose(t *testing.T, newContext contextFactory) {
	s := newEnvWithIdleTime(t, shortIdleTime)

	sid := testcore.RandomizeStr("sched-backfill-blocks-idle")
	wid := testcore.RandomizeStr("sched-backfill-blocks-idle-wf")
	wt := testcore.RandomizeStr("sched-backfill-blocks-idle-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
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
func testMultiRangeBackfillCountedExactlyOnce(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-multi-range-backfill")
	wid := testcore.RandomizeStr("sched-multi-range-backfill-wf")
	wt := testcore.RandomizeStr("sched-multi-range-backfill-wt")

	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: wt},
	)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		// 1m interval; each 2m backfill range below covers a couple of ticks.
		Spec:   intervalSpec(time.Minute),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	now := time.Now().UTC()
	threeYearsAgo := now.Add(-3 * 365 * 24 * time.Hour).Truncate(time.Minute)
	thirtyMinutesAgo := now.Add(-30 * time.Minute).Truncate(time.Minute)
	patchSchedule(ctx, t, s, sid, &schedulepb.SchedulePatch{
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
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
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
func testBackfillOnPausedSchedule(t *testing.T, newContext contextFactory) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)

	sid := testcore.RandomizeStr("sched-backfill-paused")
	wid := testcore.RandomizeStr("sched-backfill-paused-wf")
	wt := testcore.RandomizeStr("sched-backfill-paused-wt")

	var runs atomic.Int32
	registerCountingWorkflow(s, wt, &runs)

	ctx := newContext(s.Context())
	createSchedule(ctx, t, s, sid, &schedulepb.Schedule{
		Spec:   intervalSpec(fastInterval),
		State:  &schedulepb.ScheduleState{Paused: true},
		Action: startWorkflowAction(s, wid, wt),
	})

	// Paused suppresses firing even though the 1s interval would otherwise tick.
	require.Never(t, func() bool { return runs.Load() > 0 },
		neverWindow, pollInterval,
		"paused schedule must not fire automated actions")

	now := time.Now().UTC()
	patchSchedule(ctx, t, s, sid, backfillPatch(now.Add(-5*time.Second), now, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL))

	await.RequireTruef(t, func() bool { return runs.Load() >= 5 },
		awaitTimeout, pollInterval,
		"backfill should fire actions on a paused schedule")
}

// TestScheduleNextActionTimeVisibility asserts that the CHASM scheduler's
// ScheduleNextActionTime search attribute is published to visibility and is
// queryable through the frontend ListSchedules API.
func TestScheduleNextActionTimeVisibility(t *testing.T) {
	opts := scheduleCommonOpts(t)
	s := testcore.NewEnv(t, opts...)

	v2Sid := testcore.RandomizeStr("sched-next-action-v2")
	wid := testcore.RandomizeStr("sched-next-action-wf")
	wt := testcore.RandomizeStr("sched-next-action-wt")

	// Register a no-op workflow type so a stray fire doesn't generate worker
	// noise. The schedule is never paused: we want its next action time to stay
	// populated so it remains queryable.
	s.SdkWorker().RegisterWorkflowWithOptions(
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
						TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
					},
				},
			},
		}
	}

	newContext := chasmContextFactory
	v2Ctx := newContext(s.Context())
	createTime := time.Now()

	_, err := s.FrontendClient().CreateSchedule(v2Ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: v2Sid,
		Schedule:   mkSchedule(),
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

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

	require.Eventually(t, func() bool {
		listResp, err := s.FrontendClient().ListSchedules(v2Ctx, &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
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

// TestScheduleCountsVisibility asserts that the CHASM scheduler's
// ScheduleRunningWorkflowCount and ScheduleBufferedStartsCount search attributes
// are published to visibility and queryable through the frontend ListSchedules
// API. A schedule that fires every second under BUFFER_ONE, started with a
// workflow that blocks, settles into one running workflow with one fire buffered
// behind it. The counts aren't surfaced on the list entry, so we assert via the
// query rather than by reading the entry's SAs.
func TestScheduleCountsVisibility(t *testing.T) {
	s := testcore.NewEnv(t, scheduleCommonOpts(t)...)
	newContext := chasmContextFactory

	sid := testcore.RandomizeStr("sched-counts-v2")
	wid := testcore.RandomizeStr("sched-counts-wf")
	wt := testcore.RandomizeStr("sched-counts-wt")

	// A workflow that holds open so a fire stays running while later fires buffer.
	s.SdkWorker().RegisterWorkflowWithOptions(
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
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// matchesQuery reports whether the schedule is returned when filtering on the
	// given search-attribute query.
	matchesQuery := func(query string) bool {
		listResp, listErr := s.FrontendClient().ListSchedules(newContext(s.Context()), &workflowservice.ListSchedulesRequest{
			Namespace:       s.Namespace().String(),
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
	require.Eventually(t, func() bool {
		return matchesQuery(fmt.Sprintf("%s >= 1", chasmscheduler.ScheduleRunningWorkflowCountName))
	}, 30*time.Second, 500*time.Millisecond,
		"schedule must be queryable by ScheduleRunningWorkflowCount >= 1")

	require.Eventually(t, func() bool {
		return matchesQuery(fmt.Sprintf("%s >= 1", chasmscheduler.ScheduleBufferedStartsCountName))
	}, 30*time.Second, 500*time.Millisecond,
		"schedule must be queryable by ScheduleBufferedStartsCount >= 1")
}
