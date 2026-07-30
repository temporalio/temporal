package tests

import (
	"context"
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
	ms, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(s.Context(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  workflowID,
		RunID:       runID,
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	s.NoError(err)
	return ms
}

// getMutableStateByWorkflowID resolves the current run of a workflow (the newest run of a
// continue-as-new / retry / cron chain) and returns its persistence-layer mutable state.
func (s *TimeSkippingFastForwardFunctionalSuite) getMutableStateByWorkflowID(ctx context.Context, env *testcore.TestEnv, workflowID string) *persistence.GetWorkflowExecutionResponse {
	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	s.NoError(err)
	return s.getMutableState(env, workflowID, desc.WorkflowExecutionInfo.Execution.RunId)
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
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	const (
		fastForward = 30 * time.Minute
		timer1Dur   = 29*time.Minute + 58*time.Second
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: "ff-id"}}
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
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	env.OverrideDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	const (
		fastForward = 30 * time.Minute
		timer1Dur   = 29*time.Minute + 50*time.Second
		minuteToler = time.Minute
	)

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: "ff-id"}}
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

// TestFastForward_PollWakesOnCompletion exercises the PollWorkflowExecutionTimeSkipping
// long poll end-to-end through the notifier: the poll must NOT be answered by the
// initial short-circuit read, it must park in the notifier wait, and it must be woken
// when the fast-forward completes.
//
// Setup keeps the workflow non-idle (a pending activity) so close-tx time skipping
// cannot fire and the fast-forward stays pending when the poll starts — forcing the
// wait path. Completing the activity and its follow-up WFT drives the workflow idle,
// so the idle close-tx skips to the fast-forward target and disables it. That persisted
// mutation fires the notifier, which wakes the blocked poll with FAST_FORWARD_COMPLETED.
func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_PollWakesOnCompletion() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	// Large fast-forward: the wall-anchored wake-up timer will not fire during the test,
	// so the only way the fast-forward completes is the idle close-tx skip we drive below.
	const fastForward = 30 * time.Minute

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: "ff-id"}}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, fastForwardStartReq(env, tv, 24*time.Hour, cfg))
	s.NoError(err)
	runID := startResp.RunId

	// WT1: schedule an activity. The pending, dispatchable activity keeps the workflow
	// non-idle, so no close-tx skip can fire and the fast-forward stays pending.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{scheduleActivityCmd(tv)},
		}, nil
	})
	s.NoError(err)

	// Sanity: the fast-forward is pending (not reached) while the activity is in flight,
	// so the poll's initial read cannot short-circuit.
	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	ff := ms.State.ExecutionInfo.GetTimeSkippingInfo().GetFastForwardInfo()
	s.NotNil(ff)
	if ff != nil {
		s.False(ff.GetHasReached())
	}

	type pollResult struct {
		resp *workflowservice.PollWorkflowExecutionTimeSkippingResponse
		err  error
	}
	resultCh := make(chan pollResult, 1)
	go func() {
		resp, pollErr := env.FrontendClient().PollWorkflowExecutionTimeSkipping(ctx, &workflowservice.PollWorkflowExecutionTimeSkippingRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
			FastForwardId:     "ff-id",
		})
		resultCh <- pollResult{resp, pollErr}
	}()

	// The poll must block in the notifier wait: the fast-forward is pending, so the initial
	// read returns "keep waiting" rather than a terminal result. A prompt return here would
	// mean it short-circuited, which is exactly what this test guards against.
	select {
	case r := <-resultCh:
		s.Failf("poll returned before the fast-forward completed",
			"result=%v err=%v", r.resp.GetFastForwardPollingResult(), r.err)
	case <-time.After(2 * time.Second):
	}

	// Drive the workflow idle so the close-tx skip fires. Complete the activity, then
	// complete the follow-up WFT with no commands: the idle close-tx skips to the
	// fast-forward target and disables it (HasReached=true), waking the poll.
	_, err = env.TaskPoller().PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	s.NoError(err)

	// The notifier must wake the poll with FAST_FORWARD_COMPLETED.
	select {
	case r := <-resultCh:
		s.NoError(r.err)
		s.Equal(
			enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_COMPLETED,
			r.resp.GetFastForwardPollingResult())
		s.NotNil(r.resp.GetFastForwardInfo())
		s.True(r.resp.GetFastForwardInfo().GetHasCompleted())
		s.Equal("ff-id", r.resp.GetFastForwardInfo().GetFastForwardId())
	case <-time.After(30 * time.Second):
		s.Fail("poll did not wake after the fast-forward completed")
	}

	// A subsequent poll for the same fast-forward returns the same result immediately:
	// the fast-forward has completed, so the initial read short-circuits without ever
	// entering the notifier wait.
	resp2, err := env.FrontendClient().PollWorkflowExecutionTimeSkipping(ctx, &workflowservice.PollWorkflowExecutionTimeSkippingRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		FastForwardId:     "ff-id",
	})
	s.NoError(err)
	s.Equal(
		enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_COMPLETED,
		resp2.GetFastForwardPollingResult())
	s.NotNil(resp2.GetFastForwardInfo())
	s.True(resp2.GetFastForwardInfo().GetHasCompleted())
	s.Equal("ff-id", resp2.GetFastForwardInfo().GetFastForwardId())

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "test cleanup",
	})
}

// TestFastForward_PollWakesAcrossContinueAsNew proves the subscription really spans a chain of
// runs. The notification key carries no RunID, so a poll that parks while run 1 is alive must be
// woken by run 2's fast-forward completion after continue-as-new — even though the poll named
// run 1 explicitly. This exercises two things together: the propagated fast-forward target keeps
// the same id across the chain, and the publisher on run 2 lands on the key run 1's waiter holds.
func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_PollWakesAcrossContinueAsNew() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	// Large enough that the wall-anchored wake-up timer cannot fire during the test: the only way
	// the fast-forward completes is the idle close-tx skip we drive on run 2.
	const fastForward = 30 * time.Minute
	const ffID = "ff-across-can"

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: ffID}}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, fastForwardStartReq(env, tv, 24*time.Hour, cfg))
	s.NoError(err)
	run1ID := startResp.RunId

	// WT1 schedules an activity, keeping run 1 non-idle so no close-tx skip can fire. The
	// fast-forward is therefore still pending when the poll parks below.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return cmdsResponse(scheduleActivityCmd(tv)), nil
	})
	s.NoError(err)

	run1FF := s.getMutableState(env, tv.WorkflowID(), run1ID).State.ExecutionInfo.GetTimeSkippingInfo().GetFastForwardInfo()
	s.NotNil(run1FF)
	if run1FF != nil {
		s.False(run1FF.GetHasReached(), "fast-forward must still be pending on run 1")
	}

	type pollResult struct {
		resp *workflowservice.PollWorkflowExecutionTimeSkippingResponse
		err  error
	}
	resultCh := make(chan pollResult, 1)
	go func() {
		resp, pollErr := env.FrontendClient().PollWorkflowExecutionTimeSkipping(ctx, &workflowservice.PollWorkflowExecutionTimeSkippingRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: run1ID},
			FastForwardId:     ffID,
		})
		resultCh <- pollResult{resp, pollErr}
	}()

	// The poll must park in the notifier wait rather than short-circuit on its initial read.
	select {
	case r := <-resultCh:
		s.Failf("poll returned before continue-as-new",
			"result=%v err=%v", r.resp.GetFastForwardPollingResult(), r.err)
	case <-time.After(2 * time.Second):
	}

	// Complete the activity, then continue-as-new on the follow-up WFT. Run 1 is closing, so it
	// cannot skip; the still-pending fast-forward target propagates to run 2 instead.
	_, err = env.TaskPoller().PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return cmdsResponse(continueAsNewCmd(tv.WorkflowType(), tv.TaskQueue())), nil
	})
	s.NoError(err)

	// Run 2 must be a different run that inherited the pending fast-forward under the same id,
	// otherwise the wake-up below would not prove anything about crossing the chain.
	run2MS := s.getMutableStateByWorkflowID(ctx, env, tv.WorkflowID())
	run2ID := run2MS.State.ExecutionState.RunId
	s.NotEqual(run1ID, run2ID, "continue-as-new should have produced a new run")
	run2TSI := run2MS.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(run2TSI)
	if run2TSI != nil {
		s.Equal(ffID, run2TSI.GetConfig().GetFastForwardConfig().GetId(),
			"run 2 should inherit the same fast-forward id")
		s.NotNil(run2TSI.GetFastForwardInfo())
		if run2TSI.GetFastForwardInfo() != nil {
			s.False(run2TSI.GetFastForwardInfo().GetHasReached(),
				"run 2 should still have the fast-forward pending")
		}
	}

	// An empty WFT drives run 2 idle, so its close-tx skip advances to the inherited target and
	// completes the fast-forward. The publisher is run 2; the waiter subscribed naming run 1.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	})
	s.NoError(err)

	select {
	case r := <-resultCh:
		s.NoError(r.err)
		s.Equal(
			enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_COMPLETED,
			r.resp.GetFastForwardPollingResult())
		s.NotNil(r.resp.GetFastForwardInfo())
		if r.resp.GetFastForwardInfo() != nil {
			s.True(r.resp.GetFastForwardInfo().GetHasCompleted())
			s.Equal(ffID, r.resp.GetFastForwardInfo().GetFastForwardId())
		}
	case <-time.After(30 * time.Second):
		s.Fail("poll did not wake when the fast-forward completed on the next run of the chain")
	}

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
		Reason:            "test cleanup",
	})
}

// TestFastForward_DescribeFollowsChain verifies DescribeWorkflowExecution keeps answering for the
// current run after a continue-as-new, and reports the *virtual* clock inherited across the chain
// rather than wall clock.
func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_DescribeFollowsChain() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	const timer1 = time.Hour
	const timer2 = 2 * time.Hour

	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx,
		fastForwardStartReq(env, tv, 24*time.Hour, &commonpb.TimeSkippingConfig{Enabled: true}))
	s.NoError(err)
	run1ID := startResp.RunId

	// Run 1 skips timer1 then continues-as-new; run 2 skips timer2 then completes. The chain
	// accumulates timer1+timer2 of virtual time.
	state := 0
	handler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		fired := firedTimers(task)
		switch {
		case state == 0:
			state = 1
			return cmdsResponse(timerCmd("t1", timer1)), nil
		case state == 1 && fired["t1"]:
			state = 2
			return cmdsResponse(continueAsNewCmd(tv.WorkflowType(), tv.TaskQueue())), nil
		case state == 2:
			state = 3
			return cmdsResponse(timerCmd("t2", timer2)), nil
		case state == 3 && fired["t2"]:
			state = 4
			return cmdsResponse(completeCmd()), nil
		}
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}
	for range 20 {
		if _, err := env.TaskPoller().PollAndHandleWorkflowTask(tv, handler); err != nil {
			continue // long-poll emptiness between skips is expected
		}
		if state == 4 {
			break
		}
	}
	s.Equal(4, state, "the continue-as-new chain should have run to completion")

	// Describe with no RunID must resolve to run 2, not the run the chain started on.
	wallBeforeDescribe := time.Now()
	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
	})
	s.NoError(err)
	s.NotEqual(run1ID, desc.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
		"describe should follow the chain to the current run")

	tsInfo := desc.GetWorkflowExtendedInfo().GetTimeSkippingInfo()
	s.NotNil(tsInfo, "describe must report time-skipping info for the current run of the chain")
	if tsInfo == nil {
		return
	}
	s.True(tsInfo.GetEffectiveConfig().GetEnabled(),
		"the propagated config should still be enabled on the current run")

	// CurrentTime is virtual, so it runs ahead of wall clock by everything the chain skipped.
	// Comparing against the persisted accumulation keeps this independent of test duration.
	accumulated := s.getMutableStateByWorkflowID(ctx, env, tv.WorkflowID()).
		State.ExecutionInfo.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
	s.InDelta(float64(timer1+timer2), float64(accumulated), float64(time.Minute),
		"the chain should have accumulated both timers of virtual time")
	s.InDelta(float64(accumulated), float64(tsInfo.GetCurrentTime().AsTime().Sub(wallBeforeDescribe)),
		float64(time.Minute), "describe's CurrentTime should be wall clock plus the accumulated skip")
}

// TestFastForward_PollWakesOnWorkflowEnd verifies that a PollWorkflowExecutionTimeSkipping long
// poll parked on a still-pending fast-forward is woken — instantly and with the correct terminal
// result — when the whole workflow execution ends before the fast-forward completes. Terminating
// the run (state COMPLETED, status TERMINATED, no continuation) is the whole-execution-end signal;
// canceling is analogous.
func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_PollWakesOnWorkflowEnd() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	// Large fast-forward so it cannot complete on its own during the test; the only terminal
	// event will be the termination we drive below.
	const fastForward = 30 * time.Minute

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: "ff-id"}}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, fastForwardStartReq(env, tv, 24*time.Hour, cfg))
	s.NoError(err)
	runID := startResp.RunId

	// WT1: schedule an activity. The pending, dispatchable activity keeps the workflow non-idle,
	// so no close-tx skip fires and the fast-forward stays pending while the poll is parked.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{scheduleActivityCmd(tv)},
		}, nil
	})
	s.NoError(err)

	// Sanity: the fast-forward is pending, so the poll's initial read cannot short-circuit.
	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	ff := ms.State.ExecutionInfo.GetTimeSkippingInfo().GetFastForwardInfo()
	s.NotNil(ff)
	if ff != nil {
		s.False(ff.GetHasReached())
	}

	type pollResult struct {
		resp *workflowservice.PollWorkflowExecutionTimeSkippingResponse
		err  error
	}
	resultCh := make(chan pollResult, 1)
	go func() {
		resp, pollErr := env.FrontendClient().PollWorkflowExecutionTimeSkipping(ctx, &workflowservice.PollWorkflowExecutionTimeSkippingRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
			FastForwardId:     "ff-id",
		})
		resultCh <- pollResult{resp, pollErr}
	}()

	// The poll must park in the notifier wait: the fast-forward is pending, so a prompt return
	// here would mean it short-circuited.
	select {
	case r := <-resultCh:
		s.Failf("poll returned before the workflow ended", "result=%v err=%v", r.resp.GetFastForwardPollingResult(), r.err)
	case <-time.After(2 * time.Second):
	}

	// End the whole execution while the poll is parked. Termination closes the run with no
	// continuation, which fires the notifier with WorkflowExecutionCompleted=true.
	_, err = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "end while polling",
	})
	s.NoError(err)

	// The notifier must wake the poll with a failure whose reason names the ended execution. The
	// fast-forward info is still the pending (never-completed) one.
	select {
	case r := <-resultCh:
		s.NoError(r.err)
		s.Equal(
			enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED,
			r.resp.GetFastForwardPollingResult())
		s.NotEmpty(r.resp.GetFailedReason(), "a failed poll must explain why")
		s.NotNil(r.resp.GetFastForwardInfo())
		s.False(r.resp.GetFastForwardInfo().GetHasCompleted())
		s.Equal("ff-id", r.resp.GetFastForwardInfo().GetFastForwardId())
	case <-time.After(30 * time.Second):
		s.Fail("poll did not wake after the workflow ended")
	}

	// A subsequent poll returns the same terminal result immediately via the initial-read
	// short-circuit (the run is closed with no continuation).
	resp2, err := env.FrontendClient().PollWorkflowExecutionTimeSkipping(ctx, &workflowservice.PollWorkflowExecutionTimeSkippingRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		FastForwardId:     "ff-id",
	})
	s.NoError(err)
	s.Equal(
		enumspb.FAST_FORWARD_POLLING_RESULT_FAST_FORWARD_FAILED,
		resp2.GetFastForwardPollingResult())
	s.NotEmpty(resp2.GetFailedReason())
}

func (s *TimeSkippingFastForwardFunctionalSuite) TestFastForward_NoUserTimer() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	const (
		fastForward = 30 * time.Minute
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: "ff-id"}}
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
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	ctx := s.Context()

	const (
		runTimeout  = 5 * time.Minute
		fastForward = runTimeout // fastForward == runTimeout
	)

	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, runTimeout)
	}, workflow.RegisterOptions{Name: "sleepEqualsTimeoutWorkflow"})

	cfg := &commonpb.TimeSkippingConfig{
		Enabled:           true,
		FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: "ff-id"},
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

func (s *TimeSkippingFastForwardFunctionalSuite) TestEventsOrderOfFastForwardTimerFiredDuringStartedWorkflowTask() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.WorkflowTimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := s.Context()

	const (
		fastForward = time.Minute
		timer1Dur   = 45 * time.Second
	)

	cfg := &commonpb.TimeSkippingConfig{Enabled: true, FastForwardConfig: &commonpb.FastForwardConfig{Duration: durationpb.New(fastForward), Id: "ff-id"}}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(90 * time.Second),
		TimeSkippingConfig:  cfg,
	})
	s.NoError(err)
	runID := startResp.RunId

	// WT1: start timer1. Close-tx skips to timer1; timer1 fires; WT2 is scheduled.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("t1", timer1Dur)},
		}, nil
	})
	s.NoError(err)

	// WT2: poll (marks it STARTED), then hold it open until the fast-forward disable fires, so the
	// transition is emitted while this workflow task is in flight. The eventual complete response is
	// expected to be rejected (UnhandledCommand) because the buffered transition invalidated the WT.
	_, _ = env.TaskPoller().PollWorkflowTask(&workflowservice.PollWorkflowTaskQueueRequest{}).
		HandleTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			s.AwaitTruef(func() bool {
				ms := s.getMutableState(env, tv.WorkflowID(), runID)
				return ms.State.ExecutionInfo.GetTimeSkippingInfo().GetFastForwardInfo().GetHasReached()
			}, 75*time.Second, 200*time.Millisecond, "fast-forward disable did not fire while the workflow task was started")

			// The disable transition mutates state synchronously in the close-transaction,
			// even though its history event is still buffered while this WT is in flight:
			// time skipping is already disabled here, before the event is flushed to history.
			ms := s.getMutableState(env, tv.WorkflowID(), runID)
			s.False(ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig().GetEnabled(),
				"time skipping must be disabled in mutable state once the fast-forward timer fired")
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{completeWorkflowCmd()},
			}, nil
		}, taskpoller.WithTimeout(90*time.Second))

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})

	// A fast-forward disable transition (DisabledAfterFastForward) must be present.
	sawDisableTransition := false
	for _, e := range s.findTransitionedEvents(hist) {
		if e.GetWorkflowExecutionTimeSkippingTransitionedEventAttributes().GetDisabledAfterFastForward() {
			sawDisableTransition = true
		}
	}
	s.True(sawDisableTransition, "expected a fast-forward disable transition")

	// No transition may sit inside an open WorkflowTaskStarted->Completed/Failed/TimedOut window.
	insideStartedWT := false
	for _, e := range hist {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED:
			insideStartedWT = true
		case enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
			enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT:
			insideStartedWT = false
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED:
			s.False(insideStartedWT,
				"time-skipping transition (event %d) must not be wedged inside an open workflow-task window",
				e.GetEventId())
		default:
			// other event types are irrelevant to this ordering check
		}
	}

	// Event IDs must be strictly increasing across the whole history.
	for i := 1; i < len(hist); i++ {
		s.Greater(hist[i].GetEventId(), hist[i-1].GetEventId(),
			"event IDs must be strictly increasing: event %d follows event %d",
			hist[i].GetEventId(), hist[i-1].GetEventId())
	}
}
