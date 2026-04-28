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
	updatepb "go.temporal.io/api/update/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type TimeSkippingTestSuite struct {
	parallelsuite.Suite[*TimeSkippingTestSuite]
}

func TestTimeSkippingTestSuite(t *testing.T) {
	parallelsuite.Run(t, &TimeSkippingTestSuite{})
}

// TestTimeSkipping_FeatureDisabled verifies that starting a workflow with time skipping
// returns an error when the feature flag is off for the namespace.
func (s *TimeSkippingTestSuite) TestTimeSkipping_FeatureDisabled() {
	env := testcore.NewEnv(s.T())
	// TimeSkippingEnabled defaults to false; no override needed.
	id := "functional-timeskipping-feature-disabled"
	tl := "functional-timeskipping-feature-disabled-tq"

	_, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: id + "-type"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.Error(err, "expected error when time skipping is disabled for namespace")
}

// TestTimeSkipping_StartWorkflow_DCEnabled verifies that StartWorkflowExecution with
// TimeSkippingConfig persists the config in mutable state when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_StartWorkflow_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	inputBound := &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
		MaxSkippedDuration: durationpb.New(time.Hour),
	}

	resp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true, Bound: inputBound},
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig().GetEnabled())
	s.True(proto.Equal(&workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   inputBound,
	}, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_SignalWithStart_DCEnabled verifies that SignalWithStartWorkflowExecution
// with TimeSkippingConfig persists the config in mutable state when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_SignalWithStart_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	inputBound := &workflowpb.TimeSkippingConfig_MaxElapsedDuration{
		MaxElapsedDuration: durationpb.New(time.Hour),
	}

	resp, err := env.FrontendClient().SignalWithStartWorkflowExecution(testcore.NewContext(), &workflowservice.SignalWithStartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		SignalName:          tv.SignalName(),
		TimeSkippingConfig: &workflowpb.TimeSkippingConfig{
			Enabled: true,
			Bound:   inputBound,
		},
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(proto.Equal(&workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   inputBound,
	}, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_ExecuteMultiOperation_DCEnabled verifies that a StartWorkflow inside
// ExecuteMultiOperation with TimeSkippingConfig persists the config in mutable state
// when the feature flag is on.
func (s *TimeSkippingTestSuite) TestTimeSkipping_ExecuteMultiOperation_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	maxSkippedDuration := time.Hour

	inputConfig := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound: &workflowpb.TimeSkippingConfig_MaxSkippedDuration{
			MaxSkippedDuration: durationpb.New(maxSkippedDuration),
		},
	}

	resp, err := env.FrontendClient().ExecuteMultiOperation(testcore.NewContext(), &workflowservice.ExecuteMultiOperationRequest{
		Namespace: env.Namespace().String(),
		Operations: []*workflowservice.ExecuteMultiOperationRequest_Operation{
			{
				Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_StartWorkflow{
					StartWorkflow: &workflowservice.StartWorkflowExecutionRequest{
						RequestId:           uuid.NewString(),
						Namespace:           env.Namespace().String(),
						WorkflowId:          tv.WorkflowID(),
						WorkflowType:        tv.WorkflowType(),
						TaskQueue:           tv.TaskQueue(),
						WorkflowRunTimeout:  durationpb.New(100 * time.Second),
						WorkflowTaskTimeout: durationpb.New(10 * time.Second),
						TimeSkippingConfig:  inputConfig,
					},
				},
			},
			{
				Operation: &workflowservice.ExecuteMultiOperationRequest_Operation_UpdateWorkflow{
					UpdateWorkflow: &workflowservice.UpdateWorkflowExecutionRequest{
						Namespace:         env.Namespace().String(),
						WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID()},
						Request: &updatepb.Request{
							Meta:  &updatepb.Meta{UpdateId: uuid.NewString()},
							Input: &updatepb.Input{Name: "my-update"},
						},
					},
				},
			},
		},
	})
	s.NoError(err)

	runID := resp.GetResponses()[0].GetStartWorkflow().GetRunId()
	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(inputConfig, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
}

// TestTimeSkipping_UpdateWorkflowOptions_DCEnabled exercises the full UpdateWorkflowExecutionOptions
// lifecycle for TimeSkippingConfig:
//  1. Start workflow with no time-skipping — assert mutable state has no config.
//  2. First update: enable with MaxSkippedDuration bound — check MS and event 1 attributes.
//  3. Second update: change bound to MaxElapsedDuration, add DisablePropagation — check MS and event 2 attributes.
//  4. Third update: disable (Enabled=false) — check MS and event 3 attributes.
//  5. Assert exactly 3 WorkflowExecutionOptionsUpdated events appear in history.
func (s *TimeSkippingTestSuite) TestTimeSkipping_UpdateWorkflowOptions_DCEnabled() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	// Start a workflow without any time-skipping config.
	startResp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	runID := startResp.RunId

	// collectOptionsEvents returns all WorkflowExecutionOptionsUpdated events in history order.
	collectOptionsEvents := func() []*historypb.HistoryEvent {
		histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(testcore.NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		})
		s.NoError(err)
		var events []*historypb.HistoryEvent
		for _, e := range histResp.History.Events {
			if e.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
				events = append(events, e)
			}
		}
		return events
	}
	updateOptions := func(cfg *workflowpb.TimeSkippingConfig) {
		_, err := env.FrontendClient().UpdateWorkflowExecutionOptions(testcore.NewContext(), &workflowservice.UpdateWorkflowExecutionOptionsRequest{
			Namespace:                env.Namespace().String(),
			WorkflowExecution:        &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
			WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: cfg},
			UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
		})
		s.NoError(err)
	}

	// No time-skipping config before any update.
	ms := s.getMutableState(env, tv.WorkflowID(), runID)
	s.Nil(ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig())

	// First update: enable with a bound.
	config1 := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(time.Hour)},
	}
	updateOptions(config1)

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config1, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	events := collectOptionsEvents()
	s.Len(events, 1)
	s.True(proto.Equal(config1, events[0].GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))

	// Second update: change bound type, add DisablePropagation.
	config2 := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxElapsedDuration{MaxElapsedDuration: durationpb.New(2 * time.Hour)},
	}
	updateOptions(config2)

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config2, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	events = collectOptionsEvents()
	s.Len(events, 2)
	s.True(proto.Equal(config2, events[1].GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))

	// Third update: disable time-skipping.
	config3 := &workflowpb.TimeSkippingConfig{Enabled: false}
	updateOptions(config3)

	ms = s.getMutableState(env, tv.WorkflowID(), runID)
	s.True(proto.Equal(config3, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))
	events = collectOptionsEvents()
	s.Len(events, 3)
	s.True(proto.Equal(config3, events[2].GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))
}

// TestTimeSkipping_ResetWithUpdateOptions verifies that resetting a workflow with a
// PostResetOperation that sets TimeSkippingConfig persists the config in the new run's
// mutable state and emits a WorkflowExecutionOptionsUpdated history event whose
// attributes carry the full config.
func (s *TimeSkippingTestSuite) TestTimeSkipping_ResetWithUpdateOptions() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())
	ctx := testcore.NewContext()

	// Start a workflow and drain the first workflow task to establish a reset point.
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	runID := startResp.RunId

	_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv, taskpoller.DrainWorkflowTask)
	s.NoError(err)

	// Find the WorkflowTaskCompleted event ID to use as the reset point.
	histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	s.NoError(err)
	var wftCompletedEventID int64
	for _, e := range histResp.History.Events {
		if e.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wftCompletedEventID = e.EventId
			break
		}
	}
	s.NotZero(wftCompletedEventID)

	// Reset with PostResetOperations that sets TimeSkippingConfig.
	inputConfig := &workflowpb.TimeSkippingConfig{
		Enabled: true,
		Bound:   &workflowpb.TimeSkippingConfig_MaxSkippedDuration{MaxSkippedDuration: durationpb.New(time.Hour)},
	}
	resetResp, err := env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
		Reason:                    "test-timeskipping-reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
		PostResetOperations: []*workflowpb.PostResetOperation{
			{
				Variant: &workflowpb.PostResetOperation_UpdateWorkflowOptions_{
					UpdateWorkflowOptions: &workflowpb.PostResetOperation_UpdateWorkflowOptions{
						WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{TimeSkippingConfig: inputConfig},
						UpdateMask:               &fieldmaskpb.FieldMask{Paths: []string{"time_skipping_config"}},
					},
				},
			},
		},
	})
	s.NoError(err)
	newRunID := resetResp.RunId

	// New run's mutable state must have the config.
	ms := s.getMutableState(env, tv.WorkflowID(), newRunID)
	s.True(proto.Equal(inputConfig, ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig()))

	// New run's history must contain a WorkflowExecutionOptionsUpdated event with the config.
	histResp, err = env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: newRunID},
	})
	s.NoError(err)
	var optionsUpdatedEvent *historypb.HistoryEvent
	for _, e := range histResp.History.Events {
		if e.EventType == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED {
			optionsUpdatedEvent = e
			break
		}
	}
	s.NotNil(optionsUpdatedEvent, "expected WorkflowExecutionOptionsUpdated event in new run history")
	s.True(proto.Equal(inputConfig, optionsUpdatedEvent.GetWorkflowExecutionOptionsUpdatedEventAttributes().GetTimeSkippingConfig()))
}

func (s *TimeSkippingTestSuite) getMutableState(env *testcore.TestEnv, workflowID, runID string) *persistence.GetWorkflowExecutionResponse {
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

// startWorkflowWithTimeSkipping starts a workflow with time-skipping enabled
// and a caller-specified run timeout. Used by tests that need the run timeout
// to be long enough to fit a virtual-time skip.
func (s *TimeSkippingTestSuite) startWorkflowWithTimeSkipping(env *testcore.TestEnv, tv *testvars.TestVars, runTimeout time.Duration) string {
	resp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(runTimeout),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true},
	})
	s.NoError(err)
	return resp.GetRunId()
}

// scheduleActivityCmd returns a ScheduleActivityTask command that uses tv for all names /
// queue / timeout values.
func scheduleActivityCmd(tv *testvars.TestVars) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:             tv.ActivityID(),
				ActivityType:           tv.ActivityType(),
				TaskQueue:              tv.TaskQueue(),
				ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
			},
		},
	}
}

// startTimerCmd returns a StartTimer command with the given duration and timer ID.
func startTimerCmd(timerID string, d time.Duration) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_TIMER,
		Attributes: &commandpb.Command_StartTimerCommandAttributes{
			StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
				TimerId:            timerID,
				StartToFireTimeout: durationpb.New(d),
			},
		},
	}
}

// completeWorkflowCmd returns a CompleteWorkflowExecution command.
func completeWorkflowCmd() *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	}
}

// hasEventType returns true if any event in the slice has the given type.
func hasEventType(events []*historypb.HistoryEvent, t enumspb.EventType) bool {
	for _, e := range events {
		if e.GetEventType() == t {
			return true
		}
	}
	return false
}

// TestTimeSkipping_TimerAndActivity verifies that when a workflow has both a long user
// timer and a pending activity, time-skipping is blocked until the activity completes.
// Once the activity is done and the workflow task is drained, time-skipping fires and
// moves the timer's visibility timestamp to near-now, so the timer fires quickly.
//
// Sequence:
//
//	WT1 → schedule activity + start 1-hour timer
//	AT1 → complete activity
//	WT2 → drain (return no commands; triggers time-skipping on close)
//	WT3 → complete workflow (timer has fired)
func (s *TimeSkippingTestSuite) TestTimeSkipping_TimerAndActivity() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	// Run timeout must exceed the 1h timer; otherwise skip shifts the run-timeout
	// task into the past and the workflow times out before WT3 can fire.
	runID := s.startWorkflowWithTimeSkipping(env, tv, 2*time.Hour)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT 1: simultaneously schedule an activity and start a 1-hour timer.
	// Time-skipping cannot fire while both are pending.
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				scheduleActivityCmd(tv),
				startTimerCmd("timer-1", time.Hour),
			},
		}, nil
	})
	s.NoError(err)

	// Activity: complete it.  After this, only the 1-hour timer is pending.
	_, err = poller.PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT 2: drain (return no commands).  closeTransaction fires time-skipping here because
	// the workflow is now idle with a pending timer → regenerates the timer task at near-now.
	_, err = poller.PollAndHandleWorkflowTask(tv, taskpoller.DrainWorkflowTask)
	s.NoError(err)

	// WT 3: timer has fired (due to time-skipping); complete the workflow.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	// Verify history.
	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	s.True(hasEventType(history, enumspb.EVENT_TYPE_TIMER_FIRED), "timer must have fired via time-skipping")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED),
		"time-skipping transitioned event expected")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must complete")
}

// TestWorkflowLifecycle_VirtualTimeContract is an end-to-end regression test
// that exercises the virtual-time contract across a full workflow lifecycle:
// activity execution before and after skip, skip transition, workflow close,
// close-time semantics, run-timeout task regeneration, activity-timeout task
// shifting, and retention task scheduling.
//
// Scenario:
//
//	Start workflow with runTimeout = 4h, time-skipping enabled, namespace
//	retention = 1 day.
//	WT1  → schedule activity-1 + 1h user timer (activity blocks skip).
//	AT1  → complete activity-1 (virtual time barely advances — activity is fast).
//	WT2  → drain → closeTransaction fires skip transition (accumulated ≈ 1h).
//	WT3  → timer-1 has fired; schedule activity-2 with 5min ScheduleToClose
//	        (this is when ActivityTimeoutTask for activity-2 is written, with
//	        virtual ScheduledTime ≈ wallStart + 1h).
//	AT2  → complete activity-2.
//	WT4  → complete workflow.
//
// Assertions:
//
//  1. Activity-1 events: ActivityTaskCompleted for activity-1 appears in history.
//  2. Skip: history has WorkflowExecutionTimeSkippingTransitioned event.
//  3. Workflow close: history has WorkflowExecutionCompleted.
//  4. Describe semantics:
//     4a. StartTime ≈ wallBeforeStart (wall frame, admission anchor).
//     4b. ExecutionTime == StartTime (no backoff configured).
//     4c. CloseTime − StartTime ≈ skip (virtual frame).
//     4d. CloseTime − ExecutionTime ≈ skip (reported duration).
//  5. Run-timeout task regenerated: two WorkflowRunTimeoutTask writes; the
//     second has VisibilityTimestamp ≈ wallStart + runTimeout − skip (earlier
//     by ≈1h than the first).
//  6. Retention task: DeleteHistoryEventTask has VisibilityTimestamp ≈
//     wallClose + retention. The retention fires at real wall time
//     "retention after close," not virtual time "retention after virtual close."
//     Concretely: virtual deleteTime = virtualCloseTime + retention; after
//     toRealTime this is (wallClose + skip) + retention − skip =
//     wallClose + retention.
//  7. Activity-2 events: ActivityTaskScheduled for activity-2 has EventTime in
//     virtual frame (EventTime − workflow StartTime ≥ ~skip). Completed event
//     for activity-2 also appears.
//  8. Activity-2 timeout task: the ActivityTimeoutTask written when activity-2
//     is scheduled has VisibilityTimestamp ≈ wallAtActivity2Schedule +
//     scheduleToClose. Principle 2: even though virtual time is ~1h ahead when
//     the activity is scheduled, the wall-clock VisibilityTimestamp must be
//     anchored to real wall time (test allows ±3min tolerance).
//
// This is the single most comprehensive e2e test for the virtual-time system.
// If any of the four principles regresses, at least one of assertions 4–8 will
// fail.
func (s *TimeSkippingTestSuite) TestWorkflowLifecycle_VirtualTimeContract() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	const (
		runTimeout    = 4 * time.Hour
		timerDuration = 1 * time.Hour // determines the amount of skip
		// Namespace retention is 1 day per testcore.NewEnv (see test_env.go:165).
		// Any future change to that default will require updating this constant.
		namespaceRetention = 24 * time.Hour
		// Margin for timing-related assertions: scheduling jitter between our
		// wall-time samples and the server's, plus any clock drift. Must be
		// less than timerDuration so the shift assertion differentiates the
		// virtual-frame and wall-frame cases.
		assertionMargin = 5 * time.Minute
		// Second activity (scheduled AFTER skip) uses a 5-minute
		// ScheduleToClose timeout. With accumulated skip = timerDuration (~1h),
		// virtualToRealTime at the task-generator boundary should produce an
		// ActivityTimeoutTask VisibilityTimestamp ≈ wallAtActivity2Schedule +
		// activity2ScheduleToClose. Delta within activity2TimerMargin is
		// allowed.
		activity2ScheduleToClose = 5 * time.Minute
		activity2TimerMargin     = 3 * time.Minute
	)

	wallBeforeStart := time.Now()
	runID := s.startWorkflowWithTimeSkipping(env, tv, runTimeout)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT1: schedule activity-1 + 1h user timer. Activity blocks skip.
	tvActivity1 := tv.WithActivityIDNumber(1)
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				scheduleActivityCmd(tvActivity1),
				startTimerCmd("lifecycle-timer", timerDuration),
			},
		}, nil
	})
	s.NoError(err)

	// AT1: complete activity-1. After this the workflow has only the pending
	// user timer → skip becomes eligible.
	_, err = poller.PollAndHandleActivityTask(tvActivity1, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT2: drain → closeTransaction fires the skip transition.
	_, err = poller.PollAndHandleWorkflowTask(tv, taskpoller.DrainWorkflowTask)
	s.NoError(err)

	// WT3: timer-1 has fired via skip. Schedule activity-2 with a 5-minute
	// ScheduleToClose timeout. Record the wall time so we can verify that the
	// ActivityTimeoutTask's VisibilityTimestamp is anchored to wall clock
	// (principle 2) and NOT to virtual time (which would put it ~1h+5min out).
	wallAtActivity2Schedule := time.Now()
	tvActivity2 := tv.WithActivityIDNumber(2)
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{
				{
					CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
					Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
						ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
							ActivityId:             tvActivity2.ActivityID(),
							ActivityType:           tvActivity2.ActivityType(),
							TaskQueue:              tv.TaskQueue(),
							ScheduleToCloseTimeout: durationpb.New(activity2ScheduleToClose),
						},
					},
				},
			},
		}, nil
	})
	s.NoError(err)

	// AT2: complete activity-2.
	_, err = poller.PollAndHandleActivityTask(tvActivity2, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT4: complete workflow.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)
	wallAfterClose := time.Now()

	// ── Assertion 1/2/3: the three history events are present. ────────────────
	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	s.True(hasEventType(history, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED), "activity must have completed")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED), "skip transition must have happened")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must have completed")

	// ── Assertion 4: StartTime, ExecutionTime, CloseTime have the right frames. ─
	//
	//   StartTime     = caller-provided admission time → WALL clock,
	//                   ≈ wallBeforeStart (± scheduling jitter).
	//   ExecutionTime = StartTime + FirstWorkflowTaskBackoff.
	//                   With no cron / WorkflowStartDelay / ContinueAsNew-backoff,
	//                   FirstWorkflowTaskBackoff = 0 → ExecutionTime == StartTime.
	//   CloseTime     = VIRTUAL time at WorkflowExecutionCompleted event.
	//                   = wallClose + accumulatedSkip ≈ wallStart + skip.
	//
	// Consequences:
	//   - CloseTime − StartTime ≈ skip (≈ timerDuration).  (this is virtualDuration)
	//   - CloseTime − ExecutionTime ≈ skip.  (public reported duration)
	desc, err := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID},
	})
	s.NoError(err)
	execInfo := desc.GetWorkflowExecutionInfo()
	s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, execInfo.GetStatus())
	s.NotNil(execInfo.GetStartTime(), "StartTime must be set")
	s.NotNil(execInfo.GetExecutionTime(), "ExecutionTime must be set")
	s.NotNil(execInfo.GetCloseTime(), "CloseTime must be set")

	// 4a. StartTime ≈ wallBeforeStart (wall frame, admission anchor).
	startTime := execInfo.GetStartTime().AsTime()
	s.GreaterOrEqual(startTime, wallBeforeStart.Add(-assertionMargin),
		"StartTime %v is earlier than wallBeforeStart %v − margin; StartTime should be wall admission time",
		startTime, wallBeforeStart)
	s.LessOrEqual(startTime, wallBeforeStart.Add(assertionMargin),
		"StartTime %v is later than wallBeforeStart %v + margin; StartTime should be ≈ wall admission moment, NOT virtual-shifted",
		startTime, wallBeforeStart)

	// 4b. ExecutionTime == StartTime (no backoff configured).
	executionTime := execInfo.GetExecutionTime().AsTime()
	s.Equal(startTime, executionTime,
		"ExecutionTime %v should equal StartTime %v because FirstWorkflowTaskBackoff = 0",
		executionTime, startTime)

	// 4c. CloseTime − StartTime ≈ skip (virtual frame).
	virtualDuration := execInfo.GetCloseTime().AsTime().Sub(startTime)
	s.GreaterOrEqual(
		virtualDuration, timerDuration-assertionMargin,
		"Describe.CloseTime − Describe.StartTime = %v is shorter than the accumulated skip (~%v); CloseTime may not be written in virtual frame",
		virtualDuration, timerDuration,
	)
	// Upper bound: skip + actual wall time spent (very small in this test).
	// assertionMargin (5min) is comfortably above any realistic wall elapsed.
	s.LessOrEqual(
		virtualDuration, timerDuration+assertionMargin,
		"Describe.CloseTime − Describe.StartTime = %v exceeds accumulated skip + test wall time; something is wrong with the close-time computation",
		virtualDuration,
	)

	// 4d. CloseTime − ExecutionTime ≈ skip (the "reported duration").
	reportedDuration := execInfo.GetCloseTime().AsTime().Sub(executionTime)
	s.GreaterOrEqual(
		reportedDuration, timerDuration-assertionMargin,
		"Reported duration (CloseTime − ExecutionTime) = %v is shorter than accumulated skip (~%v)",
		reportedDuration, timerDuration,
	)
	s.LessOrEqual(
		reportedDuration, timerDuration+assertionMargin,
		"Reported duration (CloseTime − ExecutionTime) = %v exceeds accumulated skip + test wall time",
		reportedDuration,
	)

	// ── Assertion 5: WorkflowRunTimeoutTask regenerated with shifted timestamp. ─
	recorder := env.GetTestCluster().GetTaskQueueRecorder()
	s.NotNil(recorder)
	recorded := recorder.GetRecordedTasksByCategoryFiltered(historytasks.CategoryTimer, testcore.TaskFilter{
		NamespaceID: env.NamespaceID().String(),
		WorkflowID:  tv.WorkflowID(),
		RunID:       runID,
	})

	var runTimeoutTasks []*historytasks.WorkflowRunTimeoutTask
	var deleteHistoryTasks []*historytasks.DeleteHistoryEventTask
	var activityTimeoutTasks []*historytasks.ActivityTimeoutTask
	for _, rec := range recorded {
		switch t := rec.Task.(type) {
		case *historytasks.WorkflowRunTimeoutTask:
			runTimeoutTasks = append(runTimeoutTasks, t)
		case *historytasks.DeleteHistoryEventTask:
			deleteHistoryTasks = append(deleteHistoryTasks, t)
		case *historytasks.ActivityTimeoutTask:
			activityTimeoutTasks = append(activityTimeoutTasks, t)
		default:
			// other task types are not relevant to this assertion
		}
	}

	s.GreaterOrEqual(
		len(runTimeoutTasks), 2,
		"expected initial + regenerated WorkflowRunTimeoutTask (two writes)",
	)

	// Initial task at workflow start: VisibilityTimestamp ≈ wallStart + runTimeout.
	firstRTT := runTimeoutTasks[0]
	initialExpected := wallBeforeStart.Add(runTimeout)
	s.GreaterOrEqual(firstRTT.VisibilityTimestamp, initialExpected.Add(-assertionMargin),
		"initial WorkflowRunTimeoutTask %v is earlier than expected %v",
		firstRTT.VisibilityTimestamp, initialExpected)
	s.LessOrEqual(firstRTT.VisibilityTimestamp, initialExpected.Add(assertionMargin),
		"initial WorkflowRunTimeoutTask %v is later than expected %v",
		firstRTT.VisibilityTimestamp, initialExpected)

	// Regenerated task after skip: VisibilityTimestamp ≈ wallStart + runTimeout − skip.
	latestRTT := runTimeoutTasks[len(runTimeoutTasks)-1]
	shiftedExpected := wallBeforeStart.Add(runTimeout).Add(-timerDuration)
	s.GreaterOrEqual(latestRTT.VisibilityTimestamp, shiftedExpected.Add(-assertionMargin),
		"regenerated WorkflowRunTimeoutTask %v is earlier than expected %v",
		latestRTT.VisibilityTimestamp, shiftedExpected)
	s.LessOrEqual(latestRTT.VisibilityTimestamp, shiftedExpected.Add(assertionMargin),
		"regenerated WorkflowRunTimeoutTask %v is later than expected %v (principle 3: skip must shift outstanding tasks earlier by accumulated duration)",
		latestRTT.VisibilityTimestamp, shiftedExpected)

	// ── Assertion 6: DeleteHistoryEventTask fires at wallClose + retention. ───
	//
	// virtual deleteTime = virtualCloseTime + retention + tiny-jitter
	//                    = (wallClose + skip) + retention + tiny-jitter
	// VisibilityTimestamp = toRealTime(deleteTime)
	//                     = deleteTime − skip
	//                     = wallClose + retention + tiny-jitter
	//
	// RetentionTimerJitterDuration is overridden to 1s in functional tests
	// (testcore/dynamic_config_overrides.go:54), so the jitter is negligible.
	s.GreaterOrEqual(len(deleteHistoryTasks), 1, "expected a DeleteHistoryEventTask for retention")
	deleteTask := deleteHistoryTasks[len(deleteHistoryTasks)-1]
	retentionExpected := wallAfterClose.Add(namespaceRetention)
	s.GreaterOrEqual(deleteTask.VisibilityTimestamp, retentionExpected.Add(-assertionMargin),
		"DeleteHistoryEventTask VisibilityTimestamp %v is earlier than expected ~wallClose+retention %v (principle 2: retention should anchor on wall close, not virtual close)",
		deleteTask.VisibilityTimestamp, retentionExpected)
	s.LessOrEqual(deleteTask.VisibilityTimestamp, retentionExpected.Add(assertionMargin),
		"DeleteHistoryEventTask VisibilityTimestamp %v is later than expected ~wallClose+retention %v",
		deleteTask.VisibilityTimestamp, retentionExpected)

	// ── Assertion 7: activity-2 events are stamped with virtual time. ─────────
	//
	// activity-2 was scheduled AFTER the skip fired, so by the time WT3's
	// closeTransaction runs, ms.timeSource is ~1h ahead of wall clock. The
	// ActivityTaskScheduled event's EventTime is stamped from hBuilder which
	// uses ms.timeSource, so it must be in virtual frame — specifically at
	// least (workflow StartTime + timerDuration − margin) into the "virtual
	// future." A wall-frame write would produce EventTime ≈ wallStart +
	// small_delta, which would be ~1h earlier than the expected virtual
	// timestamp.
	var activity2ScheduledEvent, activity2CompletedEvent *historypb.HistoryEvent
	for _, e := range history {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED:
			if attrs := e.GetActivityTaskScheduledEventAttributes(); attrs != nil && attrs.GetActivityId() == tvActivity2.ActivityID() {
				activity2ScheduledEvent = e
			}
		case enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:
			// Completion event doesn't carry ActivityId directly; match by
			// ScheduledEventId pointing at activity-2's Scheduled event.
			if activity2ScheduledEvent != nil &&
				e.GetActivityTaskCompletedEventAttributes().GetScheduledEventId() == activity2ScheduledEvent.GetEventId() {
				activity2CompletedEvent = e
			}
		default:
			// other event types are not relevant to this assertion
		}
	}
	s.NotNil(activity2ScheduledEvent, "ActivityTaskScheduled event for activity-2 must exist")
	s.NotNil(activity2CompletedEvent, "ActivityTaskCompleted event for activity-2 must exist")

	virtualScheduledDelta := activity2ScheduledEvent.GetEventTime().AsTime().Sub(execInfo.GetStartTime().AsTime())
	s.GreaterOrEqual(
		virtualScheduledDelta, timerDuration-assertionMargin,
		"activity-2 ActivityTaskScheduled EventTime − StartTime = %v is shorter than accumulated skip (~%v); event may not be stamped with virtual time",
		virtualScheduledDelta, timerDuration,
	)

	// ── Assertion 8: activity-2 timeout task is anchored to wall clock. ──────
	//
	// When activity-2 is scheduled (WT3 close), timer_sequence.CreateNextActivityTimer
	// writes an ActivityTimeoutTask with:
	//   VisibilityTimestamp = toRealTime(virtualScheduledTime + activity2ScheduleToClose)
	//                       = (wallAtActivity2Schedule + skip + timeout) − skip
	//                       = wallAtActivity2Schedule + activity2ScheduleToClose
	//
	// Principle 2: regardless of virtual time's offset, the wall-clock
	// VisibilityTimestamp must be ≈ wallAtActivity2Schedule + 5min. The test
	// tolerates ±3min drift (covers scheduling jitter between our wall-time
	// sample and the server's write, plus any internal processing delay).
	s.GreaterOrEqual(len(activityTimeoutTasks), 1, "expected at least one ActivityTimeoutTask written for activity-2")
	// Scan all recorded ActivityTimeoutTasks; find the one referencing activity-2's
	// ScheduledEventId. Each activity may generate multiple timeout tasks
	// (ScheduleToStart, ScheduleToClose, etc.), but in this test only
	// ScheduleToClose was configured.
	var activity2TimeoutTask *historytasks.ActivityTimeoutTask
	for _, t := range activityTimeoutTasks {
		if t.EventID == activity2ScheduledEvent.GetEventId() {
			activity2TimeoutTask = t
			break
		}
	}
	s.NotNil(activity2TimeoutTask, "expected an ActivityTimeoutTask for activity-2 (EventID=%d)", activity2ScheduledEvent.GetEventId())

	activity2TimerExpected := wallAtActivity2Schedule.Add(activity2ScheduleToClose)
	s.False(
		activity2TimeoutTask.VisibilityTimestamp.Before(activity2TimerExpected.Add(-activity2TimerMargin)),
		"activity-2 ActivityTimeoutTask VisibilityTimestamp %v is earlier than expected ~wallAtSchedule+5min %v (principle 2: task must anchor to wall clock, not virtual)",
		activity2TimeoutTask.VisibilityTimestamp, activity2TimerExpected,
	)
	s.False(
		activity2TimeoutTask.VisibilityTimestamp.After(activity2TimerExpected.Add(activity2TimerMargin)),
		"activity-2 ActivityTimeoutTask VisibilityTimestamp %v is later than expected ~wallAtSchedule+5min %v; this would happen if the virtual ScheduledTime leaked into VisibilityTimestamp (would show ≈ wallAtSchedule + 1h + 5min)",
		activity2TimeoutTask.VisibilityTimestamp, activity2TimerExpected,
	)
}
