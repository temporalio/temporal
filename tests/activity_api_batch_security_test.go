package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// TestScheduleActivityOnPerNSTQ_Blocked verifies that a normal workflow
// running in a task queue that is not the internal per-namespace task queue
// cannot schedule an activity on the internal per-namespace task queue.
func TestScheduleActivityOnPerNSTQ_Blocked(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t)

	id := testcore.RandomizeStr(t.Name())
	wt := "test-schedule-activity-per-ns-tq-type"
	tl := "test-schedule-activity-per-ns-tq"
	identity := "worker1"

	workflowType := &commonpb.WorkflowType{Name: wt}
	taskQueue := &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}

	we, err := env.FrontendClient().StartWorkflowExecution(testcore.NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          id,
		WorkflowType:        workflowType,
		TaskQueue:           taskQueue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(100 * time.Second),
		WorkflowTaskTimeout: durationpb.New(10 * time.Second),
		Identity:            identity,
	})
	env.NoError(err)

	tv := testvars.New(t).WithTaskQueue(tl)

	// Workflow task handler that tries to schedule an activity on the internal per-ns task queue.
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
				Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
					ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
						ActivityId:   "activity1",
						ActivityType: &commonpb.ActivityType{Name: "test-activity"},
						TaskQueue: &taskqueuepb.TaskQueue{
							Name: primitives.PerNSWorkerTaskQueue,
							Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
						},
						StartToCloseTimeout: durationpb.New(10 * time.Second),
					},
				},
			}},
		}, nil
	}

	poller := taskpoller.New(t, env.FrontendClient(), env.Namespace().String())

	_, err = poller.PollAndHandleWorkflowTask(tv, wtHandler)
	env.Error(err, "Expected error when scheduling activity on internal per-namespace task queue")
	var invalidArgument *serviceerror.InvalidArgument
	env.ErrorAs(err, &invalidArgument)
	env.Contains(err.Error(), "internal per-namespace task queue")

	// Verify a WorkflowTaskFailed event was recorded with the correct cause.
	historyEvents := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: id,
		RunId:      we.RunId,
	})
	var foundTaskFailed bool
	for _, event := range historyEvents {
		if event.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED {
			foundTaskFailed = true
			attrs := event.GetWorkflowTaskFailedEventAttributes()
			env.Equal(enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, attrs.GetCause())
			env.Contains(attrs.GetFailure().GetMessage(), "internal per-namespace task queue")
			break
		}
	}
	env.True(foundTaskFailed, "WorkflowTaskFailed event should be recorded")
}
