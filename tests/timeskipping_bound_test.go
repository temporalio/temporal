package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type TimeSkippingBoundFunctionalSuite struct {
	parallelsuite.Suite[*TimeSkippingBoundFunctionalSuite]
}

func TestTimeSkippingBoundFunctionalSuite(t *testing.T) {
	parallelsuite.Run(t, &TimeSkippingBoundFunctionalSuite{})
}

func (s *TimeSkippingBoundFunctionalSuite) getMutableState(env *testcore.TestEnv, workflowID, runID string) *persistence.GetWorkflowExecutionResponse {
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

func (s *TimeSkippingBoundFunctionalSuite) getBackoffTimerTasks(env *testcore.TestEnv, workflowID, runID string) []*historytasks.WorkflowBackoffTimerTask {
	recorder := env.GetTestCluster().GetTaskQueueRecorder()
	s.NotNil(recorder)
	recorded := recorder.GetRecordedTasksByCategoryFiltered(historytasks.CategoryTimer, testcore.TaskFilter{
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	})
	var out []*historytasks.WorkflowBackoffTimerTask
	for _, rec := range recorded {
		if t, ok := rec.Task.(*historytasks.WorkflowBackoffTimerTask); ok {
			out = append(out, t)
		}
	}
	return out
}

func (s *TimeSkippingBoundFunctionalSuite) findTransitionedEvents(history []*historypb.HistoryEvent) []*historypb.HistoryEvent {
	var out []*historypb.HistoryEvent
	for _, e := range history {
		if e.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED {
			out = append(out, e)
		}
	}
	return out
}

func boundStartReq(env *testcore.TestEnv, tv *testvars.TestVars, runTimeout time.Duration, cfg *workflowpb.TimeSkippingConfig) *workflowservice.StartWorkflowExecutionRequest {
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

func (s *TimeSkippingBoundFunctionalSuite) TestBound_MaxSkip_LongUserTimer() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		maxSkip     = 30 * time.Minute
		userTimer   = 1 * time.Hour
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(maxSkip)},
	}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, boundStartReq(env, tv, 24*time.Hour, cfg))
	s.NoError(err)
	runID := startResp.RunId

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("t1", userTimer)},
		}, nil
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
	s.True(attrs.GetDisabledAfterBound())

	eventTime := transitions[0].GetEventTime().AsTime()
	s.WithinDuration(startTime, eventTime, minuteToler)

	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled())
	s.InDelta(float64(maxSkip), float64(tsi.GetAccumulatedSkippedDuration().AsDuration()), float64(accumTol))

	foundTimer := false
	for _, ti := range ms.State.TimerInfos {
		if ti.GetTimerId() == "t1" {
			foundTimer = true
			s.WithinDuration(startTime.Add(userTimer), ti.GetExpiryTime().AsTime(), minuteToler)
		}
	}
	s.True(foundTimer, "t1 user timer must be persisted on MS")

	// Load-bearing skip-happened check: regenerated user-timer task's wall-clock fire time
	// is virtual_expiry - accumulated = startTime + (userTimer - maxSkip). Without skip
	// it would be startTime + userTimer, so this catches a regression where the
	// task is not re-emitted after the bound transition.
	recorder := env.GetTestCluster().GetTaskQueueRecorder()
	s.NotNil(recorder)
	recorded := recorder.GetRecordedTasksByCategoryFiltered(historytasks.CategoryTimer, testcore.TaskFilter{
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		RunID:       runID,
	})
	var t1Task *historytasks.UserTimerTask
	for _, rec := range recorded {
		if ut, ok := rec.Task.(*historytasks.UserTimerTask); ok {
			t1Task = ut
		}
	}
	s.NotNil(t1Task, "t1 user-timer task must be recorded in the timer queue")
	if t1Task != nil {
		s.WithinDuration(startTime.Add(userTimer-maxSkip), t1Task.VisibilityTimestamp, minuteToler)
	}

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "test cleanup",
	})
}

func (s *TimeSkippingBoundFunctionalSuite) TestBound_MaxSkip_TwoShortUserTimers() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		maxSkip     = time.Hour
		timer1Dur   = 30 * time.Minute
		timer2Dur   = 40 * time.Minute
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(maxSkip)},
	}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, boundStartReq(env, tv, 24*time.Hour, cfg))
	s.NoError(err)
	runID := startResp.RunId

	state := 0
	handler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		switch state {
		case 0:
			state = 1
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{startTimerCmd("t1", timer1Dur)},
			}, nil
		case 1:
			state = 2
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{scheduleActivityCmd(tv)},
			}, nil
		case 2:
			state = 3
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{startTimerCmd("t2", timer2Dur)},
			}, nil
		default:
			return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
		}
	}

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, handler)
	s.NoError(err)
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, handler)
	s.NoError(err)
	_, err = env.TaskPoller().PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, handler)
	s.NoError(err)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	transitions := s.findTransitionedEvents(hist)
	s.Len(transitions, 2)

	first := transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.False(first.GetDisabledAfterBound())
	s.NotNil(first.GetTargetTime())
	firstSkip := first.GetTargetTime().AsTime().Sub(transitions[0].GetEventTime().AsTime())
	s.InDelta(float64(timer1Dur), float64(firstSkip), float64(accumTol))

	second := transitions[1].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.True(second.GetDisabledAfterBound())
	s.NotNil(second.GetTargetTime())
	secondSkip := second.GetTargetTime().AsTime().Sub(transitions[1].GetEventTime().AsTime())
	s.InDelta(float64(timer1Dur), float64(secondSkip), float64(accumTol))

	monotone := transitions[1].GetEventTime().AsTime().Sub(transitions[0].GetEventTime().AsTime())
	s.InDelta(float64(timer1Dur), float64(monotone), float64(minuteToler))

	var activityCompleted, activityScheduled int
	for _, e := range hist {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			activityCompleted++
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			activityScheduled++
		default:
			// other event types not asserted
		}
	}
	s.Equal(1, activityCompleted)
	s.Equal(1, activityScheduled)

	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled())
	s.InDelta(float64(maxSkip), float64(tsi.GetAccumulatedSkippedDuration().AsDuration()), float64(accumTol))

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "test cleanup",
	})
}

func (s *TimeSkippingBoundFunctionalSuite) TestBound_MaxSkip_NoUserTimer() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		maxSkip     = 30 * time.Minute
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(maxSkip)},
	}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, boundStartReq(env, tv, 24*time.Hour, cfg))
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
	s.True(attrs.GetDisabledAfterBound())
	s.WithinDuration(startTime, transitions[0].GetEventTime().AsTime(), minuteToler)

	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled())
	s.InDelta(float64(maxSkip), float64(tsi.GetAccumulatedSkippedDuration().AsDuration()), float64(accumTol))

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "test cleanup",
	})
}

func (s *TimeSkippingBoundFunctionalSuite) TestBound_MaxSkip_StartBackoffCron() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	// Cron expression chosen so the first run's backoff is comfortably greater than the
	// (server-enforced 1m minimum) MaxSkippedDuration: hourly cron yields a 0–60min gap.
	// Skip the rare window where now is in the last minute of the hour.
	now := time.Now()
	nextHour := now.Truncate(time.Hour).Add(time.Hour)
	cronGap := nextHour.Sub(now)
	if cronGap < 90*time.Second {
		s.T().Skipf("cron gap (%v) too small for stable test; rerun", cronGap)
	}

	const boundDuration = time.Minute
	startWall := time.Now()

	cfg := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(boundDuration)},
	}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  cfg,
		CronSchedule:        "0 * * * *",
	})
	s.NoError(err)
	run1ID := startResp.RunId

	// The bound disable fires during WorkflowExecutionStarted's closeTransaction. We do
	// NOT wait for the first WT — that would require waiting wall-clock (cronGap - bound),
	// up to ~59 minutes. The TSI / history state is observable as soon as the start
	// transaction commits.
	s.Eventually(func() bool {
		ms := s.getMutableState(env, tv.WorkflowID(), run1ID)
		tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
		return tsi != nil && tsi.GetAccumulatedSkippedDuration().AsDuration() > 0
	}, 30*time.Second, 200*time.Millisecond, "expected MS to record a skip after workflow start")

	elapsed := time.Since(startWall)
	s.Less(elapsed, 30*time.Second)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: run1ID})
	transitions := s.findTransitionedEvents(hist)
	s.Len(transitions, 1)
	attrs := transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.True(attrs.GetDisabledAfterBound())
	s.NotNil(attrs.GetTargetTime())
	skip := attrs.GetTargetTime().AsTime().Sub(transitions[0].GetEventTime().AsTime())
	s.InDelta(float64(boundDuration), float64(skip), float64(15*time.Second))

	ms := s.getMutableState(env, tv.WorkflowID(), run1ID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled())
	s.InDelta(float64(boundDuration), float64(tsi.GetAccumulatedSkippedDuration().AsDuration()), float64(15*time.Second))

	backoffTasks := s.getBackoffTimerTasks(env, tv.WorkflowID(), run1ID)
	s.NotEmpty(backoffTasks)
	for _, t := range backoffTasks {
		s.Equal(enumsspb.WORKFLOW_BACKOFF_TYPE_CRON, t.WorkflowBackoffType)
	}

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
		Reason:            "test cleanup: stop cron chain",
	})
}

func (s *TimeSkippingBoundFunctionalSuite) TestBound_MaxSkip_StartWithDelay() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		delay         = 5 * time.Minute
		boundDuration = time.Minute
	)

	startWall := time.Now()

	cfg := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(boundDuration)},
	}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(24 * time.Hour),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  cfg,
		WorkflowStartDelay:  durationpb.New(delay),
	})
	s.NoError(err)
	runID := startResp.RunId

	// Bound disable fires during WorkflowExecutionStarted's closeTransaction. Don't wait
	// for the first WT — that requires waiting wall-clock (delay - bound), which would
	// exceed the default poll timeout and may de-subscribe matching pollers.
	s.Eventually(func() bool {
		ms := s.getMutableState(env, tv.WorkflowID(), runID)
		tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
		return tsi != nil && tsi.GetAccumulatedSkippedDuration().AsDuration() > 0
	}, 30*time.Second, 200*time.Millisecond, "expected MS to record a skip after workflow start")

	elapsed := time.Since(startWall)
	s.Less(elapsed, 30*time.Second)

	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	transitions := s.findTransitionedEvents(hist)
	s.Len(transitions, 1)
	attrs := transitions[0].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.True(attrs.GetDisabledAfterBound())
	s.NotNil(attrs.GetTargetTime())
	skip := attrs.GetTargetTime().AsTime().Sub(transitions[0].GetEventTime().AsTime())
	s.InDelta(float64(boundDuration), float64(skip), float64(15*time.Second))

	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled())
	s.InDelta(float64(boundDuration), float64(tsi.GetAccumulatedSkippedDuration().AsDuration()), float64(15*time.Second))

	backoffTasks := s.getBackoffTimerTasks(env, tv.WorkflowID(), runID)
	s.NotEmpty(backoffTasks)
	for _, t := range backoffTasks {
		s.Equal(enumsspb.WORKFLOW_BACKOFF_TYPE_DELAY_START, t.WorkflowBackoffType)
	}

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "test cleanup",
	})
}

func (s *TimeSkippingBoundFunctionalSuite) TestBound_MaxElapsed_WithActivity() {
	// B3 not fixed: bound disable fires regardless of in-flight activity. Update this
	// test when B3 lands so it asserts the disable is deferred to the next idle moment.
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		bound       = 30 * time.Minute
		timer1Dur   = 29*time.Minute + 58*time.Second
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxElapsedDuration{MaxElapsedDuration: durationpb.New(bound)},
	}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, boundStartReq(env, tv, 24*time.Hour, cfg))
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

	// Wait for the bound's TimeSkippingTimerTask to fire while the activity is still in
	// flight. With timer1=29:58 and bound=30m, the regenerated bound task's wall
	// VisibilityTime is at startTime+2s; the executor hits this within seconds of WT2
	// closing. B3 not fixed: the executor emits the disable transition regardless of
	// the in-flight activity, flipping Enabled=false / HasReached=true on the bound.
	s.Eventually(func() bool {
		ms := s.getMutableState(env, tv.WorkflowID(), runID)
		tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
		bi := tsi.GetCurrentElapsedDurationBound()
		return bi != nil && bi.GetHasReached()
	}, 30*time.Second, 200*time.Millisecond, "expected bound timer task to fire while activity is in-flight (B3 path)")

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
	s.False(first.GetDisabledAfterBound())
	s.NotNil(first.GetTargetTime())
	firstSkip := first.GetTargetTime().AsTime().Sub(transitions[0].GetEventTime().AsTime())
	s.InDelta(float64(timer1Dur), float64(firstSkip), float64(accumTol))

	second := transitions[1].GetWorkflowExecutionTimeSkippingTransitionedEventAttributes()
	s.True(second.GetDisabledAfterBound())

	desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	s.NoError(err)
	startTime := desc.WorkflowExecutionInfo.GetStartTime().AsTime()

	secondVirtual := transitions[1].GetEventTime().AsTime().Sub(startTime)
	s.InDelta(float64(bound), float64(secondVirtual), float64(minuteToler))

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
	bi := tsi.GetCurrentElapsedDurationBound()
	s.NotNil(bi)
	s.True(bi.GetHasReached())
}

func (s *TimeSkippingBoundFunctionalSuite) TestBound_MaxElapsed_NoUserTimer() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	const (
		bound       = 30 * time.Minute
		minuteToler = time.Minute
		accumTol    = 30 * time.Second
	)

	cfg := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxElapsedDuration{MaxElapsedDuration: durationpb.New(bound)},
	}
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, boundStartReq(env, tv, 24*time.Hour, cfg))
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
	s.True(attrs.GetDisabledAfterBound())
	s.WithinDuration(startTime, transitions[0].GetEventTime().AsTime(), minuteToler)

	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	tsi := ms.State.ExecutionInfo.GetTimeSkippingInfo()
	s.NotNil(tsi)
	s.False(tsi.GetConfig().GetEnabled())
	s.InDelta(float64(bound), float64(tsi.GetAccumulatedSkippedDuration().AsDuration()), float64(accumTol))
	bi := tsi.GetCurrentElapsedDurationBound()
	s.NotNil(bi)
	s.True(bi.GetHasReached())

	_, _ = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:            "test cleanup",
	})
}
