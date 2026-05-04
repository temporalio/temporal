package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestDispatchCancelToWorker tests that when an activity cancellation is requested,
// the server dispatches an ActivityCommandTask to the worker's control queue via Nexus.
func TestDispatchCancelToWorker(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tv := env.Tv()
	poller := env.TaskPoller()

	// Get the control queue name from test vars
	controlQueueName := tv.ControlQueueName(env.Namespace().String())
	t.Logf("WorkerInstanceKey: %s", tv.WorkerInstanceKey())
	t.Logf("ControlQueueName: %s", controlQueueName)

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
	env.NoError(err)
	t.Logf("Started workflow: %s/%s", tv.WorkflowID(), startResp.RunId)

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
	env.NoError(err)
	t.Log("Scheduled activity")

	// Poll for activity task and start running the activity.
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	env.NoError(err)
	env.NotNil(activityPollResp)
	env.NotEmpty(activityPollResp.TaskToken)
	t.Log("Activity started with WorkerInstanceKey")

	// Request workflow cancellation
	t.Log("Requesting workflow cancellation...")
	_, err = env.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
	})
	env.NoError(err)

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
	env.NoError(err)
	t.Log("Workflow task completed with RequestCancelActivityTask command")

	// Poll Nexus control queue until we receive the notification request
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	env.Eventually(func() bool {
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
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for Nexus task")

	// Verify we received the notification request on the control queue
	env.NotNil(nexusPollResp.Request, "Expected to receive Nexus request on control queue")

	startOp := nexusPollResp.Request.GetStartOperation()
	env.NotNil(startOp, "Expected StartOperation in Nexus request")
	env.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service, "Expected WorkerService")
	env.Equal("ExecuteCommands", startOp.Operation, "Expected ExecuteCommands operation")

	// Verify the payload contains the expected CancelActivity command with a task token
	// that matches the one sent to the worker when the activity started.
	env.NotNil(startOp.Payload, "Expected payload in StartOperation request")
	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	env.NoError(err, "Failed to decode ExecuteCommandsRequest from payload")
	env.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	env.NotNil(cancelCmd, "Expected CancelActivity command")
	env.NotEmpty(cancelCmd.TaskToken, "Expected non-empty task token in CancelActivity command")
	// The cancel command's task token must match the one from the activity poll response.
	// The SDK uses exact task token bytes to look up and cancel running activities.
	env.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	t.Log("SUCCESS: Received ExecuteCommands Nexus request on control queue with matching CancelActivity task token")
}

// TestDispatchCancelOnWorkflowTermination tests that when a workflow is terminated,
// the server proactively dispatches cancel commands for in-flight activities
// that have a worker control queue. The activity is given a long timeout to
// ensure it is still in-flight when the workflow is terminated.
func TestDispatchCancelOnWorkflowTermination(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tv := env.Tv()
	poller := env.TaskPoller()

	controlQueueName := tv.ControlQueueName(env.Namespace().String())

	// Start the workflow
	_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		WorkflowExecutionTimeout: durationpb.New(60 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	env.NoError(err)

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
	env.NoError(err)

	// Poll for activity task and start it with a worker control queue
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	env.NoError(err)
	env.NotNil(activityPollResp)
	env.NotEmpty(activityPollResp.TaskToken)

	// Terminate the workflow directly (no cancellation request, no workflow task)
	_, err = env.FrontendClient().TerminateWorkflowExecution(ctx, &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: activityPollResp.WorkflowExecution,
		Reason:            "test termination",
	})
	env.NoError(err)

	// Poll Nexus control queue - should receive cancel command for the in-flight activity
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	env.Eventually(func() bool {
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
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for cancel command on control queue after termination")

	// Verify the cancel command
	startOp := nexusPollResp.Request.GetStartOperation()
	env.NotNil(startOp, "Expected StartOperation in Nexus request")
	env.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service)
	env.Equal("ExecuteCommands", startOp.Operation)

	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	env.NoError(err)
	env.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	env.NotNil(cancelCmd, "Expected CancelActivity command")
	env.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	t.Log("SUCCESS: Received cancel command on control queue after workflow termination")
}

// TestDispatchCancelOnWorkflowReset tests that when a workflow is reset,
// the old run is terminated and cancel commands are dispatched for its in-flight activities.
// The activity is given a long timeout to ensure it is still in-flight when the workflow is reset.
// Note: reset internally calls TerminateWorkflow, so this exercises the same code path as
// TestDispatchCancelOnWorkflowTermination — it verifies the plumbing rather than a distinct branch.
func TestDispatchCancelOnWorkflowReset(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))

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
	env.NoError(err)

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
	env.NoError(err)

	// Poll for activity task and start it with a worker control queue
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	env.NoError(err)
	env.NotNil(activityPollResp)
	env.NotEmpty(activityPollResp.TaskToken)

	// Find the WFT completed event ID for the reset point
	histResp, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
	})
	env.NoError(err)
	var wftCompletedEventID int64
	for _, event := range histResp.History.Events {
		if event.EventType == enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			wftCompletedEventID = event.EventId
			break
		}
	}
	env.NotZero(wftCompletedEventID)

	// Reset the workflow — this terminates the current run
	_, err = env.FrontendClient().ResetWorkflowExecution(ctx, &workflowservice.ResetWorkflowExecutionRequest{
		Namespace: env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
		Reason:                    "test reset",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	env.NoError(err)

	// Poll Nexus control queue - should receive cancel command for the in-flight activity
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	env.Eventually(func() bool {
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
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for cancel command on control queue after reset")

	// Verify the cancel command
	startOp := nexusPollResp.Request.GetStartOperation()
	env.NotNil(startOp, "Expected StartOperation in Nexus request")
	env.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service)
	env.Equal("ExecuteCommands", startOp.Operation)

	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	env.NoError(err)
	env.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	env.NotNil(cancelCmd, "Expected CancelActivity command")
	env.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	t.Log("SUCCESS: Received cancel command on control queue after workflow reset")
}

// TestDispatchCancelOnWorkflowTimeout tests that when a workflow times out,
// cancel commands are dispatched for in-flight activities with a worker control queue.
// The activity is given a long timeout to ensure it is still in-flight when the workflow times out.
func TestDispatchCancelOnWorkflowTimeout(t *testing.T) {
	env := testcore.NewEnv(t, testcore.WithDynamicConfig(dynamicconfig.EnableCancelActivityWorkerCommand, true))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tv := env.Tv()
	poller := env.TaskPoller()

	controlQueueName := tv.ControlQueueName(env.Namespace().String())

	// Start the workflow with a short execution timeout
	_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		WorkflowExecutionTimeout: durationpb.New(3 * time.Second),
		WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
	})
	env.NoError(err)

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
	env.NoError(err)

	// Poll for activity task and start it with a worker control queue
	activityPollResp, err := env.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:              env.Namespace().String(),
		TaskQueue:              tv.TaskQueue(),
		Identity:               tv.WorkerIdentity(),
		WorkerInstanceKey:      tv.WorkerInstanceKey(),
		WorkerControlTaskQueue: controlQueueName,
	})
	env.NoError(err)
	env.NotNil(activityPollResp)
	env.NotEmpty(activityPollResp.TaskToken)

	// Wait for the workflow to time out (3s timeout set above)
	// Poll Nexus control queue - should receive cancel command after timeout
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	env.Eventually(func() bool {
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
	}, 120*time.Second, 100*time.Millisecond, "Timed out waiting for cancel command on control queue after workflow timeout")

	// Verify the cancel command
	startOp := nexusPollResp.Request.GetStartOperation()
	env.NotNil(startOp, "Expected StartOperation in Nexus request")
	env.Equal("temporal.api.nexusservices.workerservice.v1.WorkerService", startOp.Service)
	env.Equal("ExecuteCommands", startOp.Operation)

	var executeReq workerservicepb.ExecuteCommandsRequest
	err = payload.Decode(startOp.Payload, &executeReq)
	env.NoError(err)
	env.Len(executeReq.Commands, 1, "Expected exactly 1 command")
	cancelCmd := executeReq.Commands[0].GetCancelActivity()
	env.NotNil(cancelCmd, "Expected CancelActivity command")
	env.Equal(activityPollResp.TaskToken, cancelCmd.TaskToken,
		"Cancel command task token must match the activity's original task token")
	t.Log("SUCCESS: Received cancel command on control queue after workflow timeout")
}
