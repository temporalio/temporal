package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	activityconfig "go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// ChasmActivitySuite is a functional integration test suite that verifies the CHASM
// activity prototype codepath end-to-end with a real Temporal server and real worker
// poll operations (no SDK activity worker; raw gRPC via FrontendClient/TaskPoller).
type ChasmActivitySuite struct {
	parallelsuite.Suite[*ChasmActivitySuite]
}

func TestChasmActivitySuite(t *testing.T) {
	parallelsuite.Run(t, &ChasmActivitySuite{})
}

// TestChasmActivity_WorkflowIdRunIdFix verifies Fix 1:
//
// When a workflow-embedded CHASM activity is scheduled via handleCommandScheduleActivityTask
// (CHASM branch), the matching service receives a correctly-populated AddActivityTaskRequest
// that includes the workflow's WorkflowId and RunId (and a valid ScheduledEventId > 0).
// Without Fix 1, createAddActivityTaskRequest left these fields empty, so the matching
// service could not build a valid PollActivityTaskQueueResponse for the worker.
//
// Steps:
//  1. Start a workflow via StartWorkflowExecution.
//  2. Poll for a WFT and complete it with a SCHEDULE_ACTIVITY_TASK command.
//  3. Poll for the resulting activity task.
//  4. Assert that the activity poll response carries non-empty WorkflowId/RunId,
//     a valid ScheduledEventId > 0, and a non-empty ActivityId.
//  5. Complete the activity.
func (s *ChasmActivitySuite) TestChasmActivity_WorkflowIdRunIdFix() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMActivityPrototype, true),
		testcore.WithDynamicConfig(activityconfig.Enabled, true),
	)
	tv := testvars.New(s.T())

	// Start a workflow execution.
	startResp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(60 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.NotEmpty(startResp.RunId)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      startResp.RunId,
	}

	// Poll for the first workflow task and complete it with a schedule-activity command.
	// We capture the scheduledEventId from the WFT task so we can use it later.
	var scheduledEventID int64
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			// In the normal history shape:
			//   event 1: WorkflowExecutionStarted
			//   event 2: WorkflowTaskScheduled
			//   event 3: WorkflowTaskStarted  ← task.StartedEventId
			// After RespondWorkflowTaskCompleted:
			//   event 4: WorkflowTaskCompleted
			//   event 5: ActivityTaskScheduled  ← scheduledEventID = StartedEventId + 2
			scheduledEventID = task.StartedEventId + 2

			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             "chasm-test-activity-1",
								ActivityType:           &commonpb.ActivityType{Name: "MyActivityType"},
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
								ScheduleToStartTimeout: durationpb.New(10 * time.Second),
								StartToCloseTimeout:    durationpb.New(20 * time.Second),
								HeartbeatTimeout:       durationpb.New(0 * time.Second),
							},
						},
					},
				},
			}, nil
		},
	)
	s.NoError(err)
	s.Positive(scheduledEventID, "scheduledEventID should be positive")

	// Poll for the activity task.
	// This exercises the CHASM dispatch path: the server must build a valid
	// PollActivityTaskQueueResponse with WorkflowId, RunId, and ScheduledEventId
	// populated (Fix 1 in chasm/lib/activity/activity.go).
	actTask, err := env.FrontendClient().PollActivityTaskQueue(testcore.NewContext(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace: env.Namespace().String(),
		TaskQueue: tv.TaskQueue(),
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)

	// If the server times out (returns empty task token), it means the dispatch failed.
	s.NotEmpty(actTask.TaskToken,
		"CHASM activity dispatch failed: PollActivityTaskQueue returned empty task token. "+
			"This indicates Fix 1 (WorkflowId/RunId in createAddActivityTaskRequest) may not be effective on this path.")

	// Assert Fix 1: WorkflowId and RunId must be set on the poll response.
	s.Equal(tv.WorkflowID(), actTask.WorkflowExecution.GetWorkflowId(),
		"PollActivityTaskQueueResponse.WorkflowExecution.WorkflowId must match the started workflow")
	s.Equal(startResp.RunId, actTask.WorkflowExecution.GetRunId(),
		"PollActivityTaskQueueResponse.WorkflowExecution.RunId must match the started workflow run")

	// The ScheduledEventId is embedded in the task token.
	// Decode it to verify it was set correctly (Fix 1 ensures WorkflowId/RunId/ScheduledEventId
	// are propagated into the AddActivityTask matching request).
	tokenSerializer := tasktoken.NewSerializer()
	decodedToken, err := tokenSerializer.Deserialize(actTask.TaskToken)
	s.NoError(err)
	s.Equal(scheduledEventID, decodedToken.GetScheduledEventId(),
		"decoded task token ScheduledEventId must match the history event ID")
	s.Positive(decodedToken.GetScheduledEventId(),
		"decoded task token ScheduledEventId must be > 0")

	// Assert ActivityId is present.
	s.NotEmpty(actTask.ActivityId,
		"PollActivityTaskQueueResponse.ActivityId must be non-empty")

	// Complete the activity task so the server can proceed.
	_, err = env.FrontendClient().RespondActivityTaskCompleted(testcore.NewContext(), &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: actTask.TaskToken,
		Identity:  tv.WorkerIdentity(),
	})
	s.NoError(err)

	// Verify the history contains the expected activity events.
	events := env.GetHistory(env.Namespace().String(), workflowExecution)
	s.EqualHistoryEventsPrefix(`
  1 WorkflowExecutionStarted
  2 WorkflowTaskScheduled
  3 WorkflowTaskStarted
  4 WorkflowTaskCompleted
  5 ActivityTaskScheduled`, events)

	// Cleanup: complete the workflow.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(
		tv,
		taskpoller.CompleteWorkflowHandler,
	)
	s.NoError(err)
}

// TestChasmActivity_CancellationFix verifies Fix 2:
//
// When a CHASM-scheduled activity is cancelled via COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
// AddActivityTaskCancelRequestedEventCHASM is called (instead of the legacy path which would
// fail to find the activity in the ActivityInfo map), and the resulting history contains
// EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED followed by EVENT_TYPE_ACTIVITY_TASK_CANCELED
// (since the activity was never started).
//
// Steps:
//  1. Start a workflow.
//  2. Complete WFT with SCHEDULE_ACTIVITY_TASK command → captures scheduledEventId.
//  3. Complete another WFT with REQUEST_CANCEL_ACTIVITY_TASK for that scheduledEventId.
//  4. Verify the history contains ActivityTaskCancelRequested + ActivityTaskCanceled.
func (s *ChasmActivitySuite) TestChasmActivity_CancellationFix() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMActivityPrototype, true),
		testcore.WithDynamicConfig(activityconfig.Enabled, true),
	)
	tv := testvars.New(s.T())

	// Start a workflow execution.
	startResp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(60 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.NotEmpty(startResp.RunId)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: tv.WorkflowID(),
		RunId:      startResp.RunId,
	}

	// Step 2: Complete first WFT with a schedule-activity command.
	var scheduledEventID int64
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			// event 3: WorkflowTaskStarted → after complete:
			// event 4: WorkflowTaskCompleted, event 5: ActivityTaskScheduled
			scheduledEventID = task.StartedEventId + 2

			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             "chasm-cancel-activity-1",
								ActivityType:           &commonpb.ActivityType{Name: "MyActivityType"},
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
								ScheduleToStartTimeout: durationpb.New(10 * time.Second),
								StartToCloseTimeout:    durationpb.New(20 * time.Second),
								HeartbeatTimeout:       durationpb.New(0 * time.Second),
							},
						},
					},
				},
				// Force a new WFT so we get a second WFT right away to send the cancel command.
				ForceCreateNewWorkflowTask: true,
			}, nil
		},
	)
	s.NoError(err)
	s.Positive(scheduledEventID)

	// Step 3: Complete second WFT with REQUEST_CANCEL_ACTIVITY_TASK.
	// This exercises AddActivityTaskCancelRequestedEventCHASM (Fix 2).
	// Since the activity was never polled/started, wasNotStarted=true and
	// AddActivityTaskCanceledEvent is also called in the same WFT.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_ACTIVITY_TASK,
						Attributes: &commandpb.Command_RequestCancelActivityTaskCommandAttributes{
							RequestCancelActivityTaskCommandAttributes: &commandpb.RequestCancelActivityTaskCommandAttributes{
								ScheduledEventId: scheduledEventID,
							},
						},
					},
				},
			}, nil
		},
	)
	s.NoError(err)

	// Step 4: Verify the history contains the cancel events.
	events := env.GetHistory(env.Namespace().String(), workflowExecution)
	s.assertContainsEventType(events, enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCEL_REQUESTED,
		"history must contain ActivityTaskCancelRequested event (Fix 2 ensures CHASM branch is taken)")
	s.assertContainsEventType(events, enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		"history must contain ActivityTaskCanceled event (activity was never started, so immediate cancel)")
}

// TestChasmActivity_PendingActivityDescribe verifies Gap 1:
//
// When a CHASM-scheduled activity is pending, DescribeWorkflowExecution must include it
// in PendingActivities. Without the fix, CHASM activities are invisible to Describe
// because only the legacy pendingActivityInfoIDs map is read.
//
// Steps:
//  1. Start a workflow.
//  2. Complete the first WFT with a SCHEDULE_ACTIVITY_TASK command.
//  3. Call DescribeWorkflowExecution.
//  4. Assert PendingActivities has exactly 1 entry.
//  5. Assert the entry has the correct ActivityType name.
//  6. Assert the entry is in SCHEDULED state.
func (s *ChasmActivitySuite) TestChasmActivity_PendingActivityDescribe() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMActivityPrototype, true),
		testcore.WithDynamicConfig(activityconfig.Enabled, true),
	)
	tv := testvars.New(s.T())

	// Step 1: Start a workflow execution.
	startResp, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          tv.WorkflowID(),
		WorkflowType:        tv.WorkflowType(),
		TaskQueue:           tv.TaskQueue(),
		WorkflowRunTimeout:  durationpb.New(60 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            tv.WorkerIdentity(),
	})
	s.NoError(err)
	s.NotEmpty(startResp.RunId)

	// Step 2: Complete first WFT with a schedule-activity command.
	_, err = env.TaskPoller().PollAndHandleWorkflowTask(
		tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             "describe-test-activity-1",
								ActivityType:           &commonpb.ActivityType{Name: "MyActivityType"},
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(30 * time.Second),
								ScheduleToStartTimeout: durationpb.New(10 * time.Second),
								StartToCloseTimeout:    durationpb.New(20 * time.Second),
								HeartbeatTimeout:       durationpb.New(0 * time.Second),
							},
						},
					},
				},
			}, nil
		},
	)
	s.NoError(err)

	// Step 3: Describe the workflow execution.
	descResp, err := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
	})
	s.NoError(err)

	// Step 4: Assert exactly one pending activity is visible.
	s.Len(descResp.PendingActivities, 1,
		"DescribeWorkflowExecution must report the CHASM-scheduled activity as pending")

	// Step 5: Assert the ActivityType name is correct.
	s.Equal("MyActivityType", descResp.PendingActivities[0].GetActivityType().GetName(),
		"PendingActivities[0].ActivityType.Name must match the scheduled activity type")

	// Step 6: Assert the activity is in SCHEDULED state.
	s.Equal(enumspb.PENDING_ACTIVITY_STATE_SCHEDULED, descResp.PendingActivities[0].GetState(),
		"PendingActivities[0].State must be SCHEDULED")
}

// assertContainsEventType is a helper that asserts the given event type appears at least once
// in the history events slice.
func (s *ChasmActivitySuite) assertContainsEventType(events []*historypb.HistoryEvent, eventType enumspb.EventType, msgAndArgs ...interface{}) {
	s.T().Helper()
	for _, e := range events {
		if e.EventType == eventType {
			return
		}
	}
	s.Failf("event type not found in history",
		"expected event type %v in history; %v\nhistory events: %v",
		eventType, msgAndArgs, eventTypesOf(events))
}

// eventTypesOf returns a slice of event type names for diagnostic output.
func eventTypesOf(events []*historypb.HistoryEvent) []string {
	names := make([]string, len(events))
	for i, e := range events {
		names[i] = e.EventType.String()
	}
	return names
}
