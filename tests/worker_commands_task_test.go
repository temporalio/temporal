package tests

import (
	"context"
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
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
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
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
	env.Equal(workerservicepb.WorkerService.ServiceName, startOp.Service, "Expected WorkerService")
	env.Equal(workerservicepb.WorkerService.ExecuteCommands.Name(), startOp.Operation, "Expected ExecuteCommands operation")
	t.Log("SUCCESS: Received ExecuteCommands Nexus request on control queue")
}
