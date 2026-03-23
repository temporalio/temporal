package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func updateTimeSkipping(
	env testcore.Env,
	workflowExecution *commonpb.WorkflowExecution,
	identity string,
	enabled bool,
) *workflowservice.UpdateWorkflowExecutionOptionsResponse {
	resp, err := env.FrontendClient().UpdateWorkflowExecutionOptions(env.Context(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: workflowExecution,
		WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
			TimeSkippingConfig: &workflowpb.TimeSkippingConfig{Enabled: enabled},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
		Identity:   identity,
	})
	require.NoError(env.T(), err)
	return resp
}

// TestTimeSkipping_EnabledToDisabled starts a workflow with time skipping enabled,
// then disables it via UpdateWorkflowExecutionOptions. Verifies a
// WorkflowExecutionOptionsUpdated event is written with Enabled=false.
func TestTimeSkipping_EnabledToDisabled(t *testing.T) {
	s := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.TimeSkippingEnabled, true))

	id := "functional-timeskipping-enabled-to-disabled"
	tl := "functional-timeskipping-enabled-to-disabled-tq"
	tv := testvars.New(t).WithTaskQueue(tl)

	startResp, err := s.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: startResp.GetRunId()}

	// Disable time skipping — a real change that must produce an options-updated event.
	updateResp := updateTimeSkipping(s, workflowExecution, tv.WorkerIdentity(), false)
	s.False(updateResp.GetWorkflowExecutionOptions().GetTimeSkippingConfig().GetEnabled())

	poller := taskpoller.New(t, s.FrontendClient(), s.Namespace().String())
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		},
	).HandleTask(tv, taskpoller.CompleteWorkflowHandler)
	s.NoError(err)

	// todo: @feiyang, will change with a new event type after data plane is added
	historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCompleted
`, historyEvents)

	for _, event := range historyEvents {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			s.False(event.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig().GetEnabled())
		}
	}
}

// TestTimeSkipping_DisabledToEnabled starts a workflow without time skipping,
// then enables it via UpdateWorkflowExecutionOptions. Verifies a
// WorkflowExecutionOptionsUpdated event is written with Enabled=true.
func TestTimeSkipping_DisabledToEnabled(t *testing.T) {
	s := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.TimeSkippingEnabled, true))

	id := "functional-timeskipping-disabled-to-enabled"
	tl := "functional-timeskipping-disabled-to-enabled-tq"
	tv := testvars.New(t).WithTaskQueue(tl)

	startResp, err := s.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: startResp.GetRunId()}

	// Enable time skipping — a real change that must produce an options-updated event.
	updateResp := updateTimeSkipping(s, workflowExecution, tv.WorkerIdentity(), true)
	s.True(updateResp.GetWorkflowExecutionOptions().GetTimeSkippingConfig().GetEnabled())

	poller := taskpoller.New(t, s.FrontendClient(), s.Namespace().String())
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		},
	).HandleTask(tv, taskpoller.CompleteWorkflowHandler)
	s.NoError(err)

	// todo: @feiyang, will change with a new event type after data plane is added
	historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCompleted
`, historyEvents)

	for _, event := range historyEvents {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			s.True(event.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig().GetEnabled())
		}
	}
}

// TestTimeSkipping_DisabledToDisabled starts a workflow with time skipping enabled,
// disables it, then attempts to disable it again. The second update is a no-op
// and must not produce a second WorkflowExecutionOptionsUpdated event.
func TestTimeSkipping_DisabledToDisabled(t *testing.T) {
	s := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.TimeSkippingEnabled, true))

	id := "functional-timeskipping-disabled-to-disabled"
	tl := "functional-timeskipping-disabled-to-disabled-tq"
	tv := testvars.New(t).WithTaskQueue(tl)

	startResp, err := s.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: startResp.GetRunId()}

	// First update: enabled → disabled. This is a real change that produces an event.
	updateTimeSkipping(s, workflowExecution, tv.WorkerIdentity(), false)

	// Second update: disabled → disabled again. Must be a no-op with no additional event.
	updateResp := updateTimeSkipping(s, workflowExecution, tv.WorkerIdentity(), false)
	s.False(updateResp.GetWorkflowExecutionOptions().GetTimeSkippingConfig().GetEnabled())

	poller := taskpoller.New(t, s.FrontendClient(), s.Namespace().String())
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{
			TaskQueue: &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		},
	).HandleTask(tv, taskpoller.CompleteWorkflowHandler)
	s.NoError(err)

	// Only one WorkflowExecutionOptionsUpdated event (from the first update).
	historyEvents := s.GetHistory(s.Namespace().String(), workflowExecution)
	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowExecutionOptionsUpdated
  4 WorkflowTaskStarted
  5 WorkflowTaskCompleted
  6 WorkflowExecutionCompleted
`, historyEvents)
}

// TestTimeSkipping_FeatureDisabled verifies that starting a workflow with time skipping
// returns an error when the feature flag is off for the namespace.
func TestTimeSkipping_FeatureDisabled(t *testing.T) {
	// TimeSkippingEnabled defaults to false; no override needed.
	s := testcore.NewEnv(t)

	id := "functional-timeskipping-feature-disabled"
	tl := "functional-timeskipping-feature-disabled-tq"

	_, err := s.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.Error(err, "expected error when time skipping is disabled for namespace")
}

// TestTimeSkipping_Automatic_Server starts a workflow with time skipping enabled
// that mimics: sleep(1h) → check time → sleep(1h) → check time → complete.
// Each sleep is a separate timer scheduled sequentially. The time-skipping mechanism
// advances virtual time once per timer, completing the workflow in wall-clock seconds.
// Virtual time is verified at each checkpoint via WorkflowTaskStarted event timestamps.
func TestTimeSkipping_Automatic_Server(t *testing.T) {
	s := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.TimeSkippingEnabled, true))

	id := "functional-timeskipping-timer-fires-after-skip"
	tl := "functional-timeskipping-timer-fires-after-skip-tq"
	tv := testvars.New(t).WithTaskQueue(tl)

	startResp, err := s.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(3 * time.Hour), // must exceed total timer duration (2h)
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	workflowExecution := &commonpb.WorkflowExecution{WorkflowId: id, RunId: startResp.RunId}

	poller := taskpoller.New(t, s.FrontendClient(), s.Namespace().String())
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	startTimer := func(id string, d time.Duration) *commandpb.Command {
		return &commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_START_TIMER,
			Attributes: &commandpb.Command_StartTimerCommandAttributes{
				StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
					TimerId:            id,
					StartToFireTimeout: durationpb.New(d),
				},
			},
		}
	}

	virtualNow := func(task *workflowservice.PollWorkflowTaskQueueResponse) time.Time {
		events := task.GetHistory().GetEvents()
		return events[len(events)-1].GetEventTime().AsTime()
	}

	// WT1: sleep(1h) — schedule the first 1-hour timer. Capture workflow start time.
	var startTime time.Time
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: taskQueue},
	).HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		startTime = task.GetHistory().GetEvents()[0].GetEventTime().AsTime()
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimer("sleep-1", time.Hour)},
		}, nil
	})
	s.NoError(err)

	// WT2: fires after the first 1h skip. Check virtual time ≈ start+1h, then sleep(1h) again.
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: taskQueue},
	).HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		s.GreaterOrEqual(
			virtualNow(task).Sub(startTime), time.Hour,
			"expected virtual time ≥ start+1h after first skip, got %v", virtualNow(task).Sub(startTime),
		)
		s.LessOrEqual(
			virtualNow(task).Sub(startTime), time.Hour+5*time.Second,
			"expected virtual time ≤ start+1h+5s after first skip, got %v", virtualNow(task).Sub(startTime),
		)
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimer("sleep-2", time.Hour)},
		}, nil
	})
	s.NoError(err)

	// WT3: fires after the second 1h skip. Check virtual time ≈ start+2h, then complete.
	_, err = poller.PollWorkflowTask(
		&workflowservice.PollWorkflowTaskQueueRequest{TaskQueue: taskQueue},
	).HandleTask(tv, func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		s.GreaterOrEqual(
			virtualNow(task).Sub(startTime), 2*time.Hour,
			"expected virtual time ≥ start+2h after second skip, got %v", virtualNow(task).Sub(startTime),
		)
		s.LessOrEqual(
			virtualNow(task).Sub(startTime), 2*time.Hour+5*time.Second,
			"expected virtual time ≤ start+2h+5s after second skip, got %v", virtualNow(task).Sub(startTime),
		)
		return taskpoller.CompleteWorkflowHandler(task)
	})
	s.NoError(err)

	s.EqualHistoryEvents(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 TimerStarted
  6 WorkflowExecutionTimeSkipped
  7 TimerFired
  8 WorkflowTaskScheduled
  9 WorkflowTaskStarted
 10 WorkflowTaskCompleted
 11 TimerStarted
 12 WorkflowExecutionTimeSkipped
 13 TimerFired
 14 WorkflowTaskScheduled
 15 WorkflowTaskStarted
 16 WorkflowTaskCompleted
 17 WorkflowExecutionCompleted
`, s.GetHistory(s.Namespace().String(), workflowExecution))
}

// TestTimeSkipping_EnableMidFlight starts a workflow without time skipping, waits for
// it to reach a 1h timer, then enables time skipping via UpdateWorkflowExecutionOptions.
// Verifies the workflow completes quickly and workflow.Now() reflects virtual time ≈ start+1h.
func TestTimeSkipping_EnableMidFlight(t *testing.T) {
	s := testcore.NewEnv(t, testcore.WithSdkWorker(), testcore.WithDynamicConfig(dynamicconfig.TimeSkippingEnabled, true))

	type result struct {
		StartTime      time.Time
		TimeAfterSleep time.Time
	}

	workflowFn := func(ctx workflow.Context) (result, error) {
		startTime := workflow.Now(ctx)
		if err := workflow.Sleep(ctx, time.Hour); err != nil {
			return result{}, err
		}
		return result{startTime, workflow.Now(ctx)}, nil
	}

	const wfType = "timeskipping-enable-mid-flight"
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wfType})

	const wfID = "functional-timeskipping-enable-mid-flight"
	startResp, err := s.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         wfID,
		WorkflowType:       &commonpb.WorkflowType{Name: wfType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout: durationpb.New(3 * time.Hour),
		// No TimeSkippingConfig — time skipping disabled at start.
	})
	s.NoError(err)

	// Wait for the SDK worker to execute the first WorkflowTask and schedule the timer.
	wfExec := &commonpb.WorkflowExecution{WorkflowId: wfID, RunId: startResp.RunId}
	s.Eventually(func() bool {
		history := s.GetHistory(s.Namespace().String(), wfExec)
		for _, e := range history {
			if e.GetEventType() == enumspb.EVENT_TYPE_TIMER_STARTED {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "expected timer to be scheduled before enabling time skipping")

	// Enable time skipping mid-flight — the already-scheduled 1h timer should fire virtually.
	updateTimeSkipping(s, wfExec, "test", true)

	var res result
	s.NoError(s.SdkClient().GetWorkflow(s.Context(), wfID, startResp.RunId).Get(s.Context(), &res))

	elapsed := res.TimeAfterSleep.Sub(res.StartTime)
	s.GreaterOrEqual(elapsed, time.Hour,
		"expected virtual time ≥ start+1h after skip, got %v", elapsed)
	fmt.Println("time elapsed: ", elapsed.Minutes())
	s.LessOrEqual(elapsed, time.Hour+10*time.Second,
		"expected virtual time ≤ start+1h+5s after skip, got %v", elapsed)
}

// TestTimeSkipping_ParentChild tests that a parent workflow correctly receives results
// from a child workflow that contains a time-skipped sleep.
// The parent starts a child with a fixed WorkflowID. Once the child's timer is
// scheduled, time skipping is enabled on the child mid-flight. The parent waits
// for the child to complete and verifies virtual time advanced by ~1h.
func TestTimeSkipping_ParentChild(t *testing.T) {
	s := testcore.NewEnv(t, testcore.WithSdkWorker(), testcore.WithDynamicConfig(dynamicconfig.TimeSkippingEnabled, true))

	type childResult struct {
		StartTime      time.Time
		TimeAfterSleep time.Time
	}

	const childWfType = "timeskipping-parent-child-child"
	const childWfID = "functional-timeskipping-parent-child-child"
	wallClockStartTime := time.Now()

	childWorkflowFn := func(ctx workflow.Context) (childResult, error) {
		start := workflow.Now(ctx)
		if err := workflow.Sleep(ctx, time.Hour); err != nil {
			return childResult{}, err
		}
		return childResult{start, workflow.Now(ctx)}, nil
	}

	parentWorkflowFn := func(ctx workflow.Context) (childResult, error) {
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowID:               childWfID,
			WorkflowExecutionTimeout: 2 * time.Hour,
		})
		var res childResult
		if err := workflow.ExecuteChildWorkflow(childCtx, childWfType).Get(ctx, &res); err != nil {
			return childResult{}, err
		}
		return res, nil
	}

	const parentWfType = "timeskipping-parent-child-parent"
	s.SdkWorker().RegisterWorkflowWithOptions(childWorkflowFn, workflow.RegisterOptions{Name: childWfType})
	s.SdkWorker().RegisterWorkflowWithOptions(parentWorkflowFn, workflow.RegisterOptions{Name: parentWfType})

	const parentWfID = "functional-timeskipping-parent-child-parent"
	startResp, err := s.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         parentWfID,
		WorkflowType:       &commonpb.WorkflowType{Name: parentWfType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout: durationpb.New(3 * time.Hour),
	})
	s.NoError(err)

	// Wait for the child workflow's timer to be scheduled.
	childExec := &commonpb.WorkflowExecution{WorkflowId: childWfID}
	s.Eventually(func() bool {
		resp, err := s.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.Namespace().String(),
			Execution: childExec,
		})
		if err != nil {
			return false
		}
		for _, e := range resp.History.GetEvents() {
			if e.GetEventType() == enumspb.EVENT_TYPE_TIMER_STARTED {
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "expected child timer to be scheduled before enabling time skipping")

	// Enable time skipping on the child mid-flight so its 1h timer fires virtually.
	// todo: @feiyang - this can be discussed as to be a default behavior,
	// right now child doesn't inherit the time skipping config from the parent,
	updateTimeSkipping(s, childExec, "test", true)

	// Parent should complete once the child's timer fires and the child returns its result.
	var res childResult
	s.NoError(s.SdkClient().GetWorkflow(s.Context(), parentWfID, startResp.RunId).Get(s.Context(), &res))

	elapsed := res.TimeAfterSleep.Sub(res.StartTime)
	s.GreaterOrEqual(elapsed, time.Hour,
		"expected child virtual time >= start+1h after skip, got %v", elapsed)
	s.LessOrEqual(elapsed, time.Hour+5*time.Second,
		"expected child virtual time <= start+1h+5s after skip, got %v", elapsed)
	// check this testing finishes in 1 minute
	s.LessOrEqual(time.Since(wallClockStartTime), 1*time.Minute,
		"expected child virtual time <= 1 minute after skip, got %v", time.Since(wallClockStartTime))

}

// TestTimeSkipping_Automatic_SDKIntegration uses a real SDK worker to run a workflow that:
//  1. sleep(1h)  — skipped virtually
//  2. run a dummy activity — must complete without timeout
//  3. sleep(1h)  — skipped virtually
//
// Verifies that workflow.Now() reflects virtual time at each checkpoint, and that
// the activity completes successfully (time skipping does not cause spurious timeouts).
func TestTimeSkipping_Automatic_SDKIntegration(t *testing.T) {
	s := testcore.NewEnv(t, testcore.WithSdkWorker(), testcore.WithDynamicConfig(dynamicconfig.TimeSkippingEnabled, true))

	// Dummy activity — returns immediately with the real wall-clock time it ran at,
	// so the test can confirm it executed well within its timeout window.
	activityFn := func(ctx context.Context) (time.Time, error) {
		return time.Now(), nil
	}
	s.SdkWorker().RegisterActivity(activityFn)

	type result struct {
		StartTime            time.Time
		TimeAfterFirstSleep  time.Time
		ActivityRealTime     time.Time // real wall-clock time the activity body ran
		TimeAfterSecondSleep time.Time
	}

	workflowFn := func(ctx workflow.Context) (result, error) {
		startTime := workflow.Now(ctx)

		if err := workflow.Sleep(ctx, time.Hour); err != nil {
			return result{}, err
		}
		t1 := workflow.Now(ctx)

		// Run the dummy activity between the two sleeps.
		// A 10-second start-to-close timeout is generous for a no-op activity.
		var activityRealTime time.Time
		actCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 10 * time.Second,
		})
		if err := workflow.ExecuteActivity(actCtx, activityFn).Get(ctx, &activityRealTime); err != nil {
			return result{}, err
		}

		if err := workflow.Sleep(ctx, time.Hour); err != nil {
			return result{}, err
		}
		t2 := workflow.Now(ctx)

		return result{startTime, t1, activityRealTime, t2}, nil
	}

	// Register with an explicit name so we can pass it to the gRPC start request.
	const wfType = "timeskipping-sdk-sleep-workflow"
	s.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: wfType})

	// The SDK's StartWorkflowOptions does not expose TimeSkippingConfig yet, so start
	// via the gRPC frontend directly, then obtain the run handle from SdkClient.
	const wfID = "functional-timeskipping-sdk-user-timers"
	startResp, err := s.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.Namespace().String(),
		WorkflowId:         wfID,
		WorkflowType:       &commonpb.WorkflowType{Name: wfType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout: durationpb.New(3 * time.Hour),
		TimeSkippingConfig: &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)

	run := s.SdkClient().GetWorkflow(s.Context(), wfID, startResp.RunId)

	var res result
	s.NoError(run.Get(s.Context(), &res))

	// workflow.Now() after first sleep should be ~1h after start
	s.GreaterOrEqual(res.TimeAfterFirstSleep.Sub(res.StartTime), time.Hour,
		"expected virtual time ≥ start+1h after first sleep, got %v", res.TimeAfterFirstSleep.Sub(res.StartTime))
	s.LessOrEqual(res.TimeAfterFirstSleep.Sub(res.StartTime), time.Hour+3*time.Second,
		"expected virtual time ≤ start+1h+3s after first sleep, got %v", res.TimeAfterFirstSleep.Sub(res.StartTime))

	// The activity ran at a real wall-clock time after the first skip, so its real
	// execution time must be within the 10-second start-to-close timeout window.
	s.True(res.ActivityRealTime.After(res.StartTime),
		"activity should have run after workflow start")
	s.Less(time.Since(res.ActivityRealTime), 30*time.Second,
		"activity real execution time should be recent (within test wall-clock), got %v ago", time.Since(res.ActivityRealTime))

	// workflow.Now() after second sleep should be ~2h after start
	s.GreaterOrEqual(res.TimeAfterSecondSleep.Sub(res.StartTime), 2*time.Hour,
		"expected virtual time ≥ start+2h after second sleep, got %v", res.TimeAfterSecondSleep.Sub(res.StartTime))
	s.LessOrEqual(res.TimeAfterSecondSleep.Sub(res.StartTime), 2*time.Hour+3*time.Second,
		"expected virtual time ≤ start+2h+3s after second sleep, got %v", res.TimeAfterSecondSleep.Sub(res.StartTime))
}
