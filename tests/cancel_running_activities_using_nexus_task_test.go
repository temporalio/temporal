package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type CancelRunningActivitiesUsingNexusTaskSuite struct {
	testcore.FunctionalTestBase
}

func TestCancelRunningActivitiesUsingNexusTaskSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CancelRunningActivitiesUsingNexusTaskSuite))
}

// Tests that when a workflow is cancelled, all running activities are also cancelled.
// The cancellation request should be delivered to the worker's control queue via the Nexus service.
//
// - Record worker heartbeat with WorkerGroupingKey so the server can look it up to get the control queue name.
// - Start a workflow that schedules a long running activity.
// - Poll the activity task queue and start running the activity.
// - Poll the control task queue and wait for the cancel request to be delivered.
// - Request the workflow to be cancelled.
// - Verify that the cancel request was delivered to the control queue.
func (s *CancelRunningActivitiesUsingNexusTaskSuite) TestDispatchCancelToWorker() {
	// Enable the feature
	cleanup := s.OverrideDynamicConfig(dynamicconfig.EnableActivityCancellationNexusTask, true)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tv := testvars.New(s.T())
	poller := taskpoller.New(s.T(), s.FrontendClient(), s.Namespace().String())

	// Get the control queue name from test vars
	controlQueueName := tv.ControlQueueName()
	s.T().Logf("WorkerInstanceKey: %s", tv.WorkerInstanceKey())
	s.T().Logf("WorkerGroupingKey: %s", tv.WorkerGroupingKey())
	s.T().Logf("ControlQueueName: %s", controlQueueName)

	// Record worker heartbeat with WorkerGroupingKey so the server can look it up
	_, err := s.FrontendClient().RecordWorkerHeartbeat(ctx, &workflowservice.RecordWorkerHeartbeatRequest{
		Namespace: s.Namespace().String(),
		WorkerHeartbeat: []*workerpb.WorkerHeartbeat{
			{
				WorkerInstanceKey: tv.WorkerInstanceKey(),
				TaskQueue:         tv.TaskQueue().Name,
				HostInfo: &workerpb.WorkerHostInfo{
					WorkerGroupingKey: tv.WorkerGroupingKey(),
				},
			},
		},
	})
	s.NoError(err)
	s.T().Log("Recorded worker heartbeat with WorkerGroupingKey")

	// Start the workflow
	startResp, err := s.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.Any().String(),
		Namespace:                s.Namespace().String(),
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
	activityPollResp, err := s.FrontendClient().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace:         s.Namespace().String(),
		TaskQueue:         tv.TaskQueue(),
		Identity:          tv.WorkerIdentity(),
		WorkerInstanceKey: tv.WorkerInstanceKey(),
	})
	s.NoError(err)
	s.NotNil(activityPollResp)
	s.NotEmpty(activityPollResp.TaskToken)
	s.T().Log("Activity started with WorkerInstanceKey")

	// Request workflow cancellation
	s.T().Log("Requesting workflow cancellation...")
	_, err = s.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace: s.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: tv.WorkflowID(),
			RunId:      startResp.RunId,
		},
	})
	s.NoError(err)

	// Simulate what the SDK does when a workflow is cancelled.
	// - Poll and complete the workflow task with RequestCancelActivityTask command
	// - This sets CancelRequested=true and triggers the dispatch of the cancel task to the worker's control queue.
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

	// Poll Nexus control queue in a loop until we receive the cancel request
	var nexusPollResp *workflowservice.PollNexusTaskQueueResponse
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		pollCtx, pollCancel := context.WithTimeout(ctx, 5*time.Second)
		nexusPollResp, err = s.FrontendClient().PollNexusTaskQueue(pollCtx, &workflowservice.PollNexusTaskQueueRequest{
			Namespace: s.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: controlQueueName, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  tv.WorkerIdentity(),
		})
		pollCancel()
		if nexusPollResp != nil && nexusPollResp.Request != nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Verify we received the cancel request on the control queue
	s.Require().NotNil(nexusPollResp, "Timed out waiting for Nexus task")
	s.Require().NotNil(nexusPollResp.Request, "Expected to receive Nexus request on control queue")

	startOp := nexusPollResp.Request.GetStartOperation()
	s.NotNil(startOp, "Expected StartOperation in Nexus request")
	s.Equal("cancel-activities", startOp.Operation, "Expected cancel-activities operation")
	s.T().Logf("SUCCESS: Received cancel-activities Nexus request on control queue")
}
