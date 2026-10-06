package localserver

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	testNamespace = "default"
	taskQueue     = "tq"
)

func TestWorkflowWithActivityAndTimer(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	runIDs := 0
	s, err := New(now, func() string { runIDs++; return fmt.Sprintf("run-%d", runIDs) })
	require.NoError(t, err)

	started, err := s.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    testNamespace,
		WorkflowId:   "wf",
		WorkflowType: &commonpb.WorkflowType{Name: "Greet"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
		Input:        payloads("world"),
	})
	require.NoError(t, err)

	// First workflow task: schedule an activity and a timer.
	wft := pollWorkflowTask(t, s)
	require.Equal(t, int64(3), wft.GetStartedEventId())
	require.Equal(t, int64(0), wft.GetPreviousStartedEventId())
	requireEventTypes(t, wft.GetHistory().GetEvents(),
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
	)
	completeWorkflowTask(t, s, wft,
		&commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:          "1",
				ActivityType:        &commonpb.ActivityType{Name: "Hello"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
				Input:               payloads("world"),
				StartToCloseTimeout: durationpb.New(10 * time.Second),
			}},
		},
		&commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_START_TIMER,
			Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
				TimerId:            "t1",
				StartToFireTimeout: durationpb.New(5 * time.Second),
			}},
		},
	)

	// The activity runs and completes.
	act, err := s.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: testNamespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  "worker",
	})
	require.NoError(t, err)
	require.NotEmpty(t, act.GetTaskToken())
	require.Equal(t, "1", act.GetActivityId())
	require.Equal(t, "Hello", act.GetActivityType().GetName())
	require.Equal(t, payloads("world").GetPayloads()[0].GetData(), act.GetInput().GetPayloads()[0].GetData())
	_, err = s.RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: testNamespace,
		TaskToken: act.GetTaskToken(),
		Result:    payloads("hello world"),
	})
	require.NoError(t, err)

	// Second workflow task sees the activity result. While it is started the timer fires; the
	// TimerFired event is buffered until the task completes.
	wft = pollWorkflowTask(t, s)
	require.Equal(t, int64(3), wft.GetPreviousStartedEventId())
	events := wft.GetHistory().GetEvents()
	requireEventTypes(t, events,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_TIMER_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
	)
	completed := events[7].GetActivityTaskCompletedEventAttributes()
	require.Equal(t, int64(5), completed.GetScheduledEventId())
	require.Equal(t, int64(7), completed.GetStartedEventId())
	require.Equal(t, payloads("hello world").GetPayloads()[0].GetData(), completed.GetResult().GetPayloads()[0].GetData())

	require.NoError(t, s.AdvanceTime(ctx, now.Add(5*time.Second)))
	completeWorkflowTask(t, s, wft)

	// Third workflow task sees the buffered TimerFired event and completes the workflow.
	wft = pollWorkflowTask(t, s)
	events = wft.GetHistory().GetEvents()
	requireEventTypes(t, events[10:],
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_TIMER_FIRED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
	)
	require.Equal(t, int64(6), events[11].GetTimerFiredEventAttributes().GetStartedEventId())
	completeWorkflowTask(t, s, wft, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
			Result: payloads("done"),
		}},
	})

	history, err := s.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: testNamespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: started.GetRunId()},
	})
	require.NoError(t, err)
	events = history.GetHistory().GetEvents()
	requireEventTypes(t, events[14:],
		enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
	)
	for i, event := range events {
		require.Equal(t, int64(i+1), event.GetEventId())
	}
}

func pollWorkflowTask(t testing.TB, s *Server) *workflowservice.PollWorkflowTaskQueueResponse {
	t.Helper()
	response, err := s.PollWorkflowTaskQueue(context.Background(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: testNamespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
		Identity:  "worker",
	})
	require.NoError(t, err)
	require.NotEmpty(t, response.GetTaskToken())
	return response
}

func completeWorkflowTask(t testing.TB, s *Server, wft *workflowservice.PollWorkflowTaskQueueResponse, commands ...*commandpb.Command) {
	t.Helper()
	_, err := s.RespondWorkflowTaskCompleted(context.Background(), &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		TaskToken: wft.GetTaskToken(),
		Commands:  commands,
		Identity:  "worker",
	})
	require.NoError(t, err)
}

func requireEventTypes(t *testing.T, events []*historypb.HistoryEvent, expected ...enumspb.EventType) {
	t.Helper()
	actual := make([]enumspb.EventType, len(events))
	for i, event := range events {
		actual[i] = event.GetEventType()
	}
	require.Equal(t, expected, actual)
}

func payloads(s string) *commonpb.Payloads {
	return &commonpb.Payloads{Payloads: []*commonpb.Payload{{
		Metadata: map[string][]byte{"encoding": []byte("json/plain")},
		Data:     fmt.Appendf(nil, "%q", s),
	}}}
}

func TestDeleteClosedWorkflow(t *testing.T) {
	ctx := context.Background()
	s, err := New(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC), func() string { return "run-1" })
	require.NoError(t, err)
	runAgentLoop(t, s, "wf", 1)

	_, err = s.DeleteWorkflowExecution(ctx, &workflowservice.DeleteWorkflowExecutionRequest{
		Namespace:         testNamespace,
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "wf"},
	})
	require.NoError(t, err)

	_, err = s.GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: testNamespace,
		Execution: &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run-1"},
	})
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
	require.Empty(t, s.engine.executions)
}

func TestInvalidCommandFailsWorkflowTask(t *testing.T) {
	ctx := context.Background()
	s, err := New(time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC), func() string { return "run-1" })
	require.NoError(t, err)
	_, err = s.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    testNamespace,
		WorkflowId:   "wf",
		WorkflowType: &commonpb.WorkflowType{Name: "Greet"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
	})
	require.NoError(t, err)

	// The timer command is valid; the activity command has no timeout.
	completeWorkflowTask(t, s, pollWorkflowTask(t, s),
		&commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_START_TIMER,
			Attributes: &commandpb.Command_StartTimerCommandAttributes{StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
				TimerId:            "t1",
				StartToFireTimeout: durationpb.New(5 * time.Second),
			}},
		},
		&commandpb.Command{
			CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
			Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:   "1",
				ActivityType: &commonpb.ActivityType{Name: "Hello"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
			}},
		},
	)

	// As in the server, none of the task's commands take effect.
	wft := pollWorkflowTask(t, s)
	require.Equal(t, int32(2), wft.GetAttempt())
	events := wft.GetHistory().GetEvents()
	requireEventTypes(t, events,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
		enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
	)
	failed := events[3].GetWorkflowTaskFailedEventAttributes()
	require.Equal(t, enumspb.WORKFLOW_TASK_FAILED_CAUSE_BAD_SCHEDULE_ACTIVITY_ATTRIBUTES, failed.GetCause())
	_, ok := s.NextDeadline()
	require.False(t, ok, "the timer was not started")
}
