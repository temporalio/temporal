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
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
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
		TimeSkippingConfig:  &workflowpb.TimeSkippingConfig{Enabled: true, DisablePropagation: false, Bound: inputBound},
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(ms.State.ExecutionInfo.GetTimeSkippingInfo().GetConfig().GetEnabled())
	s.True(proto.Equal(&workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: false,
		Bound:              inputBound,
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
			Enabled:            true,
			DisablePropagation: true,
			Bound:              inputBound,
		},
	})
	s.NoError(err)

	ms := s.getMutableState(env, tv.WorkflowID(), resp.RunId)
	s.True(proto.Equal(&workflowpb.TimeSkippingConfig{
		Enabled:            true,
		DisablePropagation: true,
		Bound:              inputBound,
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
		Enabled:            true,
		DisablePropagation: true,
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

// startTimeSkippingWorkflow is a small helper that starts a workflow with time-skipping
// enabled and returns the run ID.
func (s *TimeSkippingTestSuite) startTimeSkippingWorkflow(env *testcore.TestEnv, tv *testvars.TestVars) string {
	resp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(300 * time.Second),
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

// TestTimeSkipping_ActivityOnly verifies that a workflow with time-skipping enabled but no
// user timer runs to completion normally (time-skipping never triggers because
// ShouldExecuteTimeSkipping requires a pending timer).
func (s *TimeSkippingTestSuite) TestTimeSkipping_ActivityOnly() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	runID := s.startTimeSkippingWorkflow(env, tv)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT 1: schedule the activity.
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{scheduleActivityCmd(tv)},
		}, nil
	})
	s.NoError(err)

	// Activity: complete it immediately.
	_, err = poller.PollAndHandleActivityTask(tv, taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT 2: activity has completed; complete the workflow.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	// Verify: workflow completed; no time-skipping transitioned event (no timer was ever started).
	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must complete")
	s.False(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED),
		"no time-skipping event expected when there is never a pending user timer")
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

	runID := s.startTimeSkippingWorkflow(env, tv)
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

// TestTimeSkipping_ActivityTimerActivityTimer exercises two full time-skipping cycles:
//
//	activity → timer → activity → timer → complete
//
// Each timer is set to 1 hour; time-skipping moves both to fire immediately so the
// workflow completes in seconds rather than hours.
//
// Sequence:
//
//	WT1  → schedule activity 1
//	AT1  → complete activity 1
//	WT2  → start 1-hour timer 1  (time-skipping fires on close)
//	WT3  → schedule activity 2  (timer 1 has fired)
//	AT2  → complete activity 2
//	WT4  → start 1-hour timer 2  (time-skipping fires on close)
//	WT5  → complete workflow      (timer 2 has fired)
func (s *TimeSkippingTestSuite) TestTimeSkipping_ActivityTimerActivityTimer() {
	env := testcore.NewEnv(s.T())
	env.OverrideDynamicConfig(dynamicconfig.TimeSkippingEnabled, true)
	tv := testvars.New(s.T())

	runID := s.startTimeSkippingWorkflow(env, tv)
	poller := taskpoller.New(s.T(), env.FrontendClient(), env.Namespace().String())

	// WT 1: schedule first activity.
	_, err := poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{scheduleActivityCmd(tv.WithActivityIDNumber(1))},
		}, nil
	})
	s.NoError(err)

	// AT 1: complete first activity.
	_, err = poller.PollAndHandleActivityTask(tv.WithActivityIDNumber(1), taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT 2: start a 1-hour timer.  No pending activity → time-skipping fires on closeTransaction.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("timer-1", time.Hour)},
		}, nil
	})
	s.NoError(err)

	// WT 3: timer 1 has fired; schedule second activity.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{scheduleActivityCmd(tv.WithActivityIDNumber(2))},
		}, nil
	})
	s.NoError(err)

	// AT 2: complete second activity.
	_, err = poller.PollAndHandleActivityTask(tv.WithActivityIDNumber(2), taskpoller.CompleteActivityTask(tv))
	s.NoError(err)

	// WT 4: start a second 1-hour timer.  No pending activity → time-skipping fires on closeTransaction.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{startTimerCmd("timer-2", time.Hour)},
		}, nil
	})
	s.NoError(err)

	// WT 5: timer 2 has fired; complete the workflow.
	_, err = poller.PollAndHandleWorkflowTask(tv, func(_ *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{completeWorkflowCmd()},
		}, nil
	})
	s.NoError(err)

	// Verify history: two timer-fired events and two time-skipping events.
	history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: tv.WorkflowID(), RunId: runID})

	var timerFiredCount, timeSkippingCount int
	for _, e := range history {
		switch e.GetEventType() {
		case enumspb.EVENT_TYPE_TIMER_FIRED:
			timerFiredCount++
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED:
			timeSkippingCount++
		}
	}
	s.Equal(2, timerFiredCount, "both timers must fire via time-skipping")
	s.Equal(2, timeSkippingCount, "two time-skipping transitioned events expected")
	s.True(hasEventType(history, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED), "workflow must complete")
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
