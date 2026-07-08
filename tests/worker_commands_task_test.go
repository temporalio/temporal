package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type WorkerCommandsTaskSuite struct {
	parallelsuite.Suite[*WorkerCommandsTaskSuite]
}

func TestWorkerCommandsTaskSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkerCommandsTaskSuite{})
}

// TestDispatchCancelToWorker tests that when an activity cancellation is requested,
// the server dispatches an ActivityCommandTask to the worker's control queue via Nexus.
func (s *WorkerCommandsTaskSuite) TestDispatchCancelToWorker() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))
	tv := env.Tv()
	poller := env.TaskPoller()

	// Get the control queue name from test vars
	controlQueueName := tv.ControlQueueName(env.Namespace().String())
	s.T().Logf("WorkerInstanceKey: %s", tv.WorkerInstanceKey())
	s.T().Logf("ControlQueueName: %s", controlQueueName)

	// Start the workflow
	startResp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		WorkflowExecutionTimeout: durationpb.New(60 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	s.NoError(err)
	s.T().Logf("Started workflow: %s/%s", tv.WorkflowID(), startResp.RunId)

	// Poll and complete first workflow task - schedule the activity
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             tv.ActivityID(),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(60 * time.Second),
								StartToCloseTimeout:    durationpb.New(60 * time.Second),
							},
						},
					},
				},
			}, nil
		})
	s.NoError(err)
	s.T().Log("Scheduled activity")

	// Poll for activity task and start running the activity.
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	s.NoError(err)
	s.NotNil(activityPollResp)
	s.NotEmpty(activityPollResp.TaskToken)
	s.T().Log("Activity started with WorkerInstanceKey")

	// Request workflow cancellation
	s.T().Log("Requesting workflow cancellation...")
	_, err = env.FrontendClient().RequestCancelWorkflowExecution(s.Context(), &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
	})
	s.NoError(err)

	// Simulate what the SDK does when a workflow is cancelled.
	// Poll and complete the workflow task with RequestCancelActivityTask command.
	// This sets CancelRequested=true and triggers the dispatch of ActivityCommandTask.
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			// Find the scheduled event ID
			var scheduledEventID int64
			for _, event := range task.History.Events {
				if event.EventType == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
					scheduledEventID = event.EventId
					break
				}
			}
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
		})
	s.NoError(err)
	s.T().Log("Workflow task completed with RequestCancelActivityTask command")

	// Poll Nexus control queue until we receive the notification request
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	s.Awaitf(func(s *WorkerCommandsTaskSuite) {
		pollCtx, pollCancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer pollCancel()
		resp, err := env.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueueName, Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
			Identity:  tv.WorkerIdentity(),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.Request)
		nexusPollResp = resp
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for Nexus task")

	// Verify we received the notification request on the control queue
	s.NotNil(nexusPollResp.Request, "Expected to receive Nexus request on control queue")

	startOp := nexusPollResp.Request.GetStartOperation()
	s.NotNil(startOp, "Expected StartOperation in Nexus request")
	s.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service, "Expected WorkerService")
	s.Equal("ExecuteCommands", startOp.Operation, "Expected ExecuteCommands operation")

	// Verify the payload contains the expected CancelActivity command with a task token
	// that matches the one sent to the worker when the activity started.
	s.NotNil(startOp.Payload, "Expected payload in StartOperation request")
	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	s.NoError(err, "Failed to decode ExecuteCommandsRequest from payload")
	s.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	s.NotNil(cancelCmd, "Expected CancelActivity command")
	s.NotEmpty(cancelCmd.TaskToken, "Expected non-empty task token in CancelActivity command")
	// The cancel command's task token must match the one from the activity poll response.
	// The SDK uses exact task token bytes to look up and cancel running activities.
	s.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	s.T().Log("SUCCESS: Received ExecuteCommands Nexus request on control queue with matching CancelActivity task token")
}

// TestDispatchCancelOnWorkflowTermination tests that when a workflow is terminated,
// the server proactively dispatches cancel commands for in-flight activities
// that have a worker control queue. The activity is given a long timeout to
// ensure it is still in-flight when the workflow is terminated.
func (s *WorkerCommandsTaskSuite) TestDispatchCancelOnWorkflowTermination() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))
	tv := env.Tv()
	poller := env.TaskPoller()

	controlQueueName := tv.ControlQueueName(env.Namespace().String())

	// Start the workflow
	_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		WorkflowExecutionTimeout: durationpb.New(60 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	s.NoError(err)

	// Schedule the activity
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:   tv.ActivityID(),
								ActivityType: tv.ActivityType(),
								TaskQueue:    tv.TaskQueue(),
								// Long timeout to ensure the activity is still in-flight when the workflow is terminated.
								ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
								StartToCloseTimeout:    durationpb.New(5 * time.Minute),
							},
						},
					},
				},
			}, nil
		})
	s.NoError(err)

	// Poll for activity task and start it with a worker control queue
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	s.NoError(err)
	s.NotNil(activityPollResp)
	s.NotEmpty(activityPollResp.TaskToken)

	// Terminate the workflow directly (no cancellation request, no workflow task)
	_, err = env.FrontendClient().TerminateWorkflowExecution(s.Context(), &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: activityPollResp.WorkflowExecution,
		Reason:            "test termination",
	})
	s.NoError(err)

	// Poll Nexus control queue - should receive cancel command for the in-flight activity
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	s.Awaitf(func(s *WorkerCommandsTaskSuite) {
		pollCtx, pollCancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer pollCancel()
		resp, err := env.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueueName, Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
			Identity:  tv.WorkerIdentity(),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.Request)
		nexusPollResp = resp
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for cancel command on control queue after termination")

	// Verify the cancel command
	startOp := nexusPollResp.Request.GetStartOperation()
	s.NotNil(startOp, "Expected StartOperation in Nexus request")
	s.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service)
	s.Equal("ExecuteCommands", startOp.Operation)

	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	s.NoError(err)
	s.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	s.NotNil(cancelCmd, "Expected CancelActivity command")
	s.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	s.T().Log("SUCCESS: Received cancel command on control queue after workflow termination")
}

// TestDispatchCancelOnWorkflowReset tests that when a workflow is reset,
// the old run is terminated and cancel commands are dispatched for its in-flight activities.
// The activity is given a long timeout to ensure it is still in-flight when the workflow is reset.
// Note: reset internally calls TerminateWorkflow, so this exercises the same code path as
// TestDispatchCancelOnWorkflowTermination — it verifies the plumbing rather than a distinct branch.
func (s *WorkerCommandsTaskSuite) TestDispatchCancelOnWorkflowReset() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))
	tv := env.Tv()
	poller := env.TaskPoller()

	controlQueueName := tv.ControlQueueName(env.Namespace().String())

	// Start the workflow
	startResp, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		WorkflowExecutionTimeout: durationpb.New(60 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	s.NoError(err)

	// Schedule the activity
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:   tv.ActivityID(),
								ActivityType: tv.ActivityType(),
								TaskQueue:    tv.TaskQueue(),
								// Long timeout to ensure the activity is still in-flight when the workflow is reset.
								ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
								StartToCloseTimeout:    durationpb.New(5 * time.Minute),
							},
						},
					},
				},
			}, nil
		})
	s.NoError(err)

	// Poll for activity task and start it with a worker control queue
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	s.NoError(err)
	s.NotNil(activityPollResp)
	s.NotEmpty(activityPollResp.TaskToken)

	// Find the WFT completed event ID for the reset point
	histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(s.Context(), &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
	})
	s.NoError(err)
	var wftCompletedEventID int64
	for _, event := range histResp.History.Events {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wftCompletedEventID = event.EventId
			break
		}
	}
	s.NotZero(wftCompletedEventID)

	// Reset the workflow — this terminates the current run
	_, err = env.FrontendClient().ResetWorkflowExecution(s.Context(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
		Reason:                    "test reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	s.NoError(err)

	// Poll Nexus control queue - should receive cancel command for the in-flight activity
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	s.Awaitf(func(s *WorkerCommandsTaskSuite) {
		pollCtx, pollCancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer pollCancel()
		resp, err := env.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueueName, Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
			Identity:  tv.WorkerIdentity(),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.Request)
		nexusPollResp = resp
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for cancel command on control queue after reset")

	// Verify the cancel command
	startOp := nexusPollResp.Request.GetStartOperation()
	s.NotNil(startOp, "Expected StartOperation in Nexus request")
	s.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service)
	s.Equal("ExecuteCommands", startOp.Operation)

	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	s.NoError(err)
	s.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	s.NotNil(cancelCmd, "Expected CancelActivity command")
	s.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	s.T().Log("SUCCESS: Received cancel command on control queue after workflow reset")
}

// TestDispatchCancelOnWorkflowTimeout tests that when a workflow times out,
// cancel commands are dispatched for in-flight activities with a worker control queue.
// The activity is given a long timeout to ensure it is still in-flight when the workflow times out.
func (s *WorkerCommandsTaskSuite) TestDispatchCancelOnWorkflowTimeout() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))
	tv := env.Tv()
	poller := env.TaskPoller()

	controlQueueName := tv.ControlQueueName(env.Namespace().String())

	// Start the workflow with a short execution timeout
	_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		WorkflowExecutionTimeout: durationpb.New(3 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	s.NoError(err)

	// Schedule the activity
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:   tv.ActivityID(),
								ActivityType: tv.ActivityType(),
								TaskQueue:    tv.TaskQueue(),
								// Long timeout to ensure the activity is still in-flight when the workflow times out.
								ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
								StartToCloseTimeout:    durationpb.New(5 * time.Minute),
							},
						},
					},
				},
			}, nil
		})
	s.NoError(err)

	// Poll for activity task and start it with a worker control queue
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	s.NoError(err)
	s.NotNil(activityPollResp)
	s.NotEmpty(activityPollResp.TaskToken)

	// Wait for the workflow to time out (3s timeout set above)
	// Poll Nexus control queue - should receive cancel command after timeout
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	s.Awaitf(func(s *WorkerCommandsTaskSuite) {
		pollCtx, pollCancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer pollCancel()
		resp, err := env.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueueName, Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
			Identity:  tv.WorkerIdentity(),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.Request)
		nexusPollResp = resp
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for cancel command on control queue after workflow timeout")

	// Verify the cancel command
	startOp := nexusPollResp.Request.GetStartOperation()
	s.NotNil(startOp, "Expected StartOperation in Nexus request")
	s.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service)
	s.Equal("ExecuteCommands", startOp.Operation)

	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	s.NoError(err)
	s.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	s.NotNil(cancelCmd, "Expected CancelActivity command")
	s.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	s.T().Log("SUCCESS: Received cancel command on control queue after workflow timeout")
}

// TestDispatchCancelOnContinueAsNew tests that when a workflow continues-as-new,
// cancel commands are dispatched for in-flight activities from the old run.
// CaN creates a fresh mutable state — pending activities are abandoned, same as terminate.
func (s *WorkerCommandsTaskSuite) TestDispatchCancelOnContinueAsNew() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))
	tv := env.Tv()
	poller := env.TaskPoller()

	controlQueueName := tv.ControlQueueName(env.Namespace().String())

	// Start the workflow
	_, err := env.FrontendClient().StartWorkflowExecution(s.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		WorkflowExecutionTimeout: durationpb.New(60 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	s.NoError(err)

	// Schedule the activity
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:   tv.ActivityID(),
								ActivityType: tv.ActivityType(),
								TaskQueue:    tv.TaskQueue(),
								// Long timeout to ensure the activity is still in-flight when CaN happens.
								ScheduleToCloseTimeout: durationpb.New(5 * time.Minute),
								StartToCloseTimeout:    durationpb.New(5 * time.Minute),
							},
						},
					},
				},
			}, nil
		})
	s.NoError(err)

	// Poll for activity task and start it with a worker control queue
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(s.Context(), &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	s.NoError(err)
	s.NotNil(activityPollResp)
	s.NotEmpty(activityPollResp.TaskToken)

	// Signal the workflow to trigger a new workflow task
	_, err = env.FrontendClient().SignalWorkflowExecution(s.Context(), &workflowservice.SignalWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: activityPollResp.WorkflowExecution,
		SignalName:        "trigger-can",
		Identity:          tv.WorkerIdentity(),
	})
	s.NoError(err)

	// Handle the workflow task by issuing ContinueAsNew
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
						Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
							ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
								WorkflowType:        tv.WorkflowType(),
								TaskQueue:           tv.TaskQueue(),
								WorkflowTaskTimeout: durationpb.New(10 * time.Second),
								WorkflowRunTimeout:  durationpb.New(60 * time.Second),
							},
						},
					},
				},
			}, nil
		})
	s.NoError(err)

	// Poll Nexus control queue - should receive cancel command for the in-flight activity
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	s.Awaitf(func(s *WorkerCommandsTaskSuite) {
		pollCtx, pollCancel := context.WithTimeout(s.Context(), 5*time.Second)
		defer pollCancel()
		resp, err := env.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueueName, Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
			Identity:  tv.WorkerIdentity(),
		})
		s.NoError(err)
		s.NotNil(resp)
		s.NotNil(resp.Request)
		nexusPollResp = resp
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for cancel command on control queue after continue-as-new")

	// Verify the cancel command
	startOp := nexusPollResp.Request.GetStartOperation()
	s.NotNil(startOp, "Expected StartOperation in Nexus request")
	s.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service)
	s.Equal("ExecuteCommands", startOp.Operation)

	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	s.NoError(err)
	s.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	s.NotNil(cancelCmd, "Expected CancelActivity command")
	s.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	s.T().Log("SUCCESS: Received cancel command on control queue after continue-as-new")
}

// TestDispatchCancelToWorkerWithEagerActivity tests that worker commands work correctly
// for eagerly dispatched activities. Eager activities are started inline during
// RespondWorkflowTaskCompleted (bypassing matching/RecordActivityTaskStarted), so the
// server must set StartedClock in the eager path for worker commands to be dispatched.
func TestDispatchCancelToWorkerWithEagerActivity(t *testing.T) {
	env := testcore.NewEnv(t,
		testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableActivityEagerExecution, true),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tv := env.Tv()
	poller := env.TaskPoller()

	controlQueueName := tv.ControlQueueName(env.Namespace().String())

	// Start the workflow
	startResp, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		WorkflowExecutionTimeout: durationpb.New(60 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	require.NoError(t, err)

	// Poll and complete first workflow task - schedule an eager activity.
	// WorkerControlTaskQueue is set on the WFT completion request (not the activity poll)
	// because for eager dispatch the activity is returned inline.
	wftResp, err := poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				WorkerControlTaskQueue: controlQueueName,
				Commands: []*commandpb.Command{
					{
						CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
						Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
							ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
								ActivityId:             tv.ActivityID(),
								ActivityType:           tv.ActivityType(),
								TaskQueue:              tv.TaskQueue(),
								ScheduleToCloseTimeout: durationpb.New(60 * time.Second),
								StartToCloseTimeout:    durationpb.New(60 * time.Second),
								RequestEagerExecution:  true,
							},
						},
					},
				},
			}, nil
		},
	)
	require.NoError(t, err)
	env.NotEmpty(wftResp.GetActivityTasks(), "Expected eager activity task in WFT completion response")
	eagerActivityTaskToken := wftResp.GetActivityTasks()[0].TaskToken
	env.NotEmpty(eagerActivityTaskToken, "Expected task token from eager activity")
	t.Log("Activity eagerly dispatched")

	// Request workflow cancellation
	_, err = env.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
	})
	require.NoError(t, err)

	// Poll and complete the workflow task with RequestCancelActivityTask command.
	_, err = poller.PollAndHandleWorkflowTask(tv,
		func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			var scheduledEventID int64
			for _, event := range task.History.Events {
				if event.EventType == enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED {
					scheduledEventID = event.EventId
					break
				}
			}
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
		})
	require.NoError(t, err)
	t.Log("Workflow task completed with RequestCancelActivityTask command")

	// Poll Nexus control queue - cancel command should be dispatched because
	// StartedClock is set for the eager activity.
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	await.RequireTruef(t, func() bool {
		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		defer pollCancel()
		resp, err := env.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueueName, Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS},
			Identity:  tv.WorkerIdentity(),
		})
		if err == nil && resp != nil && resp.Request != nil {
			nexusPollResp = resp
			return true
		}
		return false
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for cancel command on control queue")

	// Verify the cancel command
	startOp := nexusPollResp.Request.GetStartOperation()
	env.NotNil(startOp)
	env.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service)
	env.Equal("ExecuteCommands", startOp.Operation)

	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	require.NoError(t, err)
	env.Len(executeReq.Commands, 1)
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	env.NotNil(cancelCmd, "Expected CancelActivity command")
	env.Equal(eagerActivityTaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the eager activity's task token")
	t.Log("SUCCESS: Received cancel command on control queue for eagerly dispatched activity")
}
