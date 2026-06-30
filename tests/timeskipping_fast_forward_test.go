package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdktemporal "go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TimeSkippingFastForwardFunctionalSuite struct {
	parallelsuite.Suite[*TimeSkippingFastForwardFunctionalSuite]
}

func TestTimeSkippingFastForwardFunctionalSuite(t *testing.T) {
	parallelsuite.Run(t, &TimeSkippingFastForwardFunctionalSuite{})
}

func (s *TimeSkippingFastForwardFunctionalSuite) getMutableState(env *testcore.TestEnv, workflowID, runID string) *persistence.GetWorkflowExecutionResponse {
	shardID := common.WorkflowIDToHistoryShard(
		env.NamespaceID().String(),
		workflowID,
		env.GetTestClusterConfig().HistoryConfig.NumHistoryShards,
	)
	ms, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(testcore.NewContext(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	return ms
}

func (s *TimeSkippingFastForwardFunctionalSuite) findTransitionedEvents(history []*historypb.HistoryEvent) []*historypb.HistoryEvent {
	var out []*historypb.HistoryEvent
	for _, e := range history {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED {
			out = append(out, e)
		}
	}
	return out
}

func fastForwardStartReq(env *testcore.TestEnv, tv *testvars.TestVars, runTimeout time.Duration, cfg *commonpb.TimeSkippingConfig) *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(runTimeout),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  cfg,
	}
}

func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_WithActivity() {
	// B3 not fixed: fast-forward disable fires regardless of in-flight activity. Update this
	// test when B3 lands so it asserts the disable is deferred to the next idle moment.
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		fastForward = 30 * time.Minute
		timer1Dur   = 29*time.Minute + 58*time.Second
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:     true,
		FastForward: durationpb.New(fastForward)}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, fastForwardStartReq(env, tv, 24*time.Hour, cfg))
	s.NoError(err)
	runID := startResp.RunId

	// WT1: schedule timer1.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("t1", timer1Dur)},
		}, nil
	})
	s.NoError(err)

	// WT2 (timer1 fired): schedule activity1.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{scheduleActivityCmd(tv)},
		}, nil
	})
	s.NoError(err)

	// Wait for the fast-forward's TimeSkippingTimerTask to fire while the activity is still in
	// flight. With timer1=29:58 and fast-forward=30m, the regenerated fast-forward task's wall
	// VisibilityTime is at startTime+2s; the executor hits this within seconds of WT2
	// closing. B3 not fixed: the executor emits the disable transition regardless of
	// the in-flight activity, flipping Enabled=false / HasReached=true on the fast-forward.
	s.Eventually(func() bool {
		ms := s.getMutableState(env, tv.WorkflowID(), runID)
		tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
		ff := tsi.GetFastForwardInfo()
		return ff != nil && ff.GetHasReached()
	}, 30*time.Second, 200*time.Millisecond, "expected fastForward timer task to fire while activity is in-flight (B3 path)")

	// AT1: complete the activity.
	_, err = env.TaskPoller().PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT3 (activity completed): complete workflow.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	transitions := s.findTransitionedEvents(hist)
	s.Len(transitions, 2)

	first := transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.False(first.GetDisabledAfterFastForward())
	s.NotNil(first.GetTargetTime())
	firstSkip := first.GetTargetTime().AsTime().Sub(transitions[0].GetEventTime().AsTime())
	s.InDelta(float64(timer1Dur), float64(firstSkip), float64(accumTol))

	second := transitions[1].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.True(second.GetDisabledAfterFastForward())

	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	s.NoError(err)
	startTime := desc.WorkflowExecutionInfo.GetStartTime().AsTime()

	secondVirtual := transitions[1].GetEventTime().AsTime().Sub(startTime)
	s.InDelta(float64(fastForward), float64(secondVirtual), float64(minuteToler))

	var activityCompleted int
	for _, e := range hist {
		if e.GetEventType() == enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED {
			activityCompleted++
		}
	}
	s.Equal(1, activityCompleted)

	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled())
	ff := tsi.GetFastForwardInfo()
	s.NotNil(ff)
	s.True(ff.GetHasReached())
}

// TestFastForward_PauseLifecycle exercises the full paused-workflow
// time-skipping lifecycle and verifies three invariants together:
//
//	(1) Pause blocks close-transaction skipping. While paused, no new
//	    TimeSkippingTransitioned event is added on close-tx — both
//	    closeTransactionHandleTimeSkipping and shouldExecuteTimeSkipping
//	    short-circuit on IsWorkflowExecutionStatusPaused.
//	(2) The fast-forward's TimeSkippingTimerTask still fires while paused.
//	    executeTimeSkippingTimerTask only checks IsWorkflowExecutionRunning
//	    (paused workflows are State=RUNNING), so the disable transition is
//	    written through pause — analogous to user-timer-fired events firing
//	    through pause.
//	(3) The unpause transaction does not trigger an extra skip. Unpause sets
//	    CreateWorkflowTask=true; the new WFT is scheduled in the same
//	    transaction, and hasInflightWorkToPreventTimeSkipping returns true
//	    on the pending WFT, blocking shouldExecuteTimeSkipping.
//
// Sequence:
//
//	WT1 → start timer1 (29:50). Close-tx fires transition 1 (skip-to-timer1)
//	      and timer1 fires. Fast-forward wake-up timer task is wall-anchored at
//	      ~10s real-time from now.
//	WT2 → schedule activity1. Close-tx: pending activity → no skip.
//	Pause → activity1 stamp bumped (dispatched task now invalid). Pause
//	        close-tx is blocked by IsWorkflowExecutionStatusPaused.
//	(wait) Fast-forward timer task fires while paused → transition 2
//	       (DisabledAfterBound=true); Config.Enabled becomes false.
//	Unpause → activity1 re-dispatched, WT3 scheduled in the same tx;
//	          close-tx pending WFT → no extra transition.
//	Poll & complete activity1.
//	WT3 → completeWorkflowCmd.
//
// Final history must contain exactly two transitions, in order:
// (a) skip-to-timer1 (DisabledAfterBound=false), (b) fast-forward-disable.
func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_PauseLifecycle() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	env.OverrideDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		fastForward = 30 * time.Minute
		timer1Dur   = 29*time.Minute + 50*time.Second
		minuteToler = time.Minute
	)

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:     true,
		FastForward: durationpb.New(fastForward)}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, fastForwardStartReq(env, tv, 24*time.Hour, cfg))
	s.NoError(err)
	runID := startResp.RunId

	// WT1: start timer1. Close-tx skips to timer1 and timer1 fires.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("t1", timer1Dur)},
		}, nil
	})
	s.NoError(err)

	// WT2: timer1 fired → schedule activity1. The fast-forward wake-up timer is wall-anchored
	// at start+fast-forward (~10s real time from this point).
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{scheduleActivityCmd(tv)},
		}, nil
	})
	s.NoError(err)

	// Pause. Activity1's stamp is bumped: the dispatched matching task becomes
	// undeliverable until unpause re-generates it. The pause close-tx is blocked
	// by IsWorkflowExecutionStatusPaused.
	_, err = env.FrontendClient().PauseWorkflowExecution(ctx, &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: tv.WorkflowID(),
		RunId:      runID,
		Identity:   "test",
		Reason:     "pause lifecycle test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Wait for the fast-forward TimeSkippingTimerTask to fire while paused. The executor
	// writes the disable event regardless of pause status: HasReached becomes true
	// and Config.Enabled becomes false.
	s.AwaitTruef(func() bool {
		ms := s.getMutableState(env, tv.WorkflowID(), runID)
		tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
		ff := tsi.GetFastForwardInfo()
		return ff != nil && ff.GetHasReached()
	}, 30*time.Second, 200*time.Millisecond, "expected fastForward timer task to fire while paused")

	// Snapshot history while still paused: exactly two transitions — skip-to-timer1
	// (from WT1 close-tx) and fast-forward-disable (from the timer task that just fired).
	// No spurious third transition from any close-tx evaluated under pause.
	histDuringPause := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(), RunId: runID,
	})
	s.Len(s.findTransitionedEvents(histDuringPause), 2,
		"pause must not produce a spurious close-tx transition")

	// Unpause. ApplyUnpaused re-dispatches activity1 (new stamp), the unpause
	// action sets CreateWorkflowTask=true so a new WFT is scheduled in the same
	// transaction. The pending WFT keeps hasInflightWorkToPreventTimeSkipping
	// true through close-tx, so no extra transition fires here either.
	_, err = env.FrontendClient().UnpauseWorkflowExecution(ctx, &workflowservice.UnpauseWorkflowExecutionRequest{
		Namespace:  env.Namespace().String(),
		WorkflowId: tv.WorkflowID(),
		RunId:      runID,
		Identity:   "test",
		Reason:     "unpause lifecycle test",
		RequestId:  uuid.NewString(),
	})
	s.NoError(err)

	// Activity1 is dispatchable again after unpause: poll and complete it.
	_, err = env.TaskPoller().PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT3: complete the workflow.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	transitions := s.findTransitionedEvents(hist)
	s.Len(transitions, 2, "expected exactly two transitions across the entire lifecycle")

	first := transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.False(first.GetDisabledAfterFastForward())
	s.NotNil(first.GetTargetTime())

	second := transitions[1].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.True(second.GetDisabledAfterFastForward(), "second transition must be the fast-forward-disable event")

	s.True(hasEventType(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED), "pause event must be in history")
	s.True(hasEventType(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED), "unpause event must be in history")
	s.True(hasEventType(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must complete")

	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	s.NoError(err)
	startTime := desc.WorkflowExecutionInfo.GetStartTime().AsTime()
	secondVirtual := transitions[1].GetEventTime().AsTime().Sub(startTime)
	s.InDelta(float64(fastForward), float64(secondVirtual), float64(minuteToler))

	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled(), "Config.Enabled must be false after fastForward reached")
	ff := tsi.GetFastForwardInfo()
	s.NotNil(ff)
	s.True(ff.GetHasReached(), "HasReached must be true after fastForward timer fired")
}

func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_NoUserTimer() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		fastForward = 30 * time.Minute
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:     true,
		FastForward: durationpb.New(fastForward)}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, fastForwardStartReq(env, tv, 24*time.Hour, cfg))
	s.NoError(err)
	runID := startResp.RunId

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	s.NoError(err)

	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	s.NoError(err)
	startTime := desc.WorkflowExecutionInfo.GetStartTime().AsTime()

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	transitions := s.findTransitionedEvents(hist)
	s.Len(transitions, 1)
	attrs := transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.True(attrs.GetDisabledAfterFastForward())
	s.WithinDuration(startTime, transitions[0].GetEventTime().AsTime(), minuteToler)

	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled())
	s.InDelta(float64(fastForward), float64(tsi.GetAccumulatedSkippedDuration().AsDuration()), float64(accumTol))
	ff := tsi.GetFastForwardInfo()
	s.NotNil(ff)
	s.True(ff.GetHasReached())

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "test cleanup",
	})
}

func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_EqualsRunTimeout() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	ctx := testcore.NewContext()

	const (
		runTimeout  = 5 * time.Minute
		fastForward = runTimeout // fastForward == runTimeout
	)

	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, runTimeout)
	}, workflow.RegisterOptions{Name: "sleepEqualsTimeoutWorkflow"})

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:     true,
		FastForward: durationpb.New(fastForward),
	}
	workflowID := uuid.NewString()
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: "sleepEqualsTimeoutWorkflow"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue()},
		WorkflowRunTimeout:  durationpb.New(runTimeout),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  cfg,
	})
	s.NoError(err)
	runID := startResp.RunId

	run := env.SdkClient().GetWorkflow(ctx, workflowID, runID)
	err = run.Get(ctx, nil)
	var timeoutErr *sdktemporal.TimeoutError
	s.ErrorAs(err, &timeoutErr, "expected TimeoutError, got: %v", err)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: workflowID, RunId: runID})
	indexOfEventType := func(history []*historypb.HistoryEvent, t enumspb.EventType) int {
		for i, e := range history {
			if e.GetEventType() == t {
				return i
			}
		}
		return -1
	}
	transitionIdx := indexOfEventType(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED)
	timedOutIdx := indexOfEventType(hist, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT)
	s.GreaterOrEqual(transitionIdx, 0, "expected a time-skipping transition event")
	s.GreaterOrEqual(timedOutIdx, 0, "expected the workflow to time out")
	s.Less(transitionIdx, timedOutIdx, "the time-skipping transition must be recorded before the timeout")
}
