package localserver

import (
	"context"
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
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var importedExecution = &commonpb.WorkflowExecution{WorkflowId: "child", RunId: "upstream-run"}

func TestImportedWorkflowRunsLocally(t *testing.T) {
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	s, err := New(now, func() string { return "local-run" })
	require.NoError(t, err)
	baseline := upstreamBaseline(now)
	importBaseline(t, s, baseline)

	wft := pollWorkflowTask(t, s)
	require.Equal(t, importedExecution.GetRunId(), wft.GetWorkflowExecution().GetRunId())
	require.Equal(t, int64(3), wft.GetStartedEventId())
	for i, event := range baseline {
		require.True(t, proto.Equal(event, wft.GetHistory().GetEvents()[i]), "event %d", event.GetEventId())
	}
	completeWorkflowTask(t, s, wft, scheduleActivityCommand("1"))

	batches, lastEventID := rawHistoryAfter(t, s, 2)
	require.Equal(t, int64(5), lastEventID)
	requireBatches(t, batches,
		[]enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
		[]enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED},
	)

	completeActivity(t, s)
	completeWorkflowTask(t, s, pollWorkflowTask(t, s), &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
	})

	batches, lastEventID = rawHistoryAfter(t, s, 5)
	require.Equal(t, int64(11), lastEventID)
	requireBatches(t, batches,
		// As in the server, the activity's started event is written with its outcome.
		[]enumspb.EventType{enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
		[]enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
		[]enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED},
	)
}

func TestRawHistoryOfLocallyStartedWorkflowIsBatchedByTransaction(t *testing.T) {
	ctx := context.Background()
	s, err := New(time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC), func() string { return "run-1" })
	require.NoError(t, err)
	_, err = s.StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		Namespace:    testNamespace,
		WorkflowId:   importedExecution.GetWorkflowId(),
		WorkflowType: &commonpb.WorkflowType{Name: "Greet"},
		TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue},
	})
	require.NoError(t, err)
	pollWorkflowTask(t, s)

	response, err := s.Admin().GetWorkflowExecutionRawHistoryV2(ctx, &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: testNamespace,
		Execution:   &commonpb.WorkflowExecution{WorkflowId: importedExecution.GetWorkflowId(), RunId: "run-1"},
	})
	require.NoError(t, err)
	requireBatches(t, decodeBatches(t, response.GetHistoryBatches()),
		[]enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED},
		[]enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED},
	)
}

func TestAdminDeleteRemovesRunningWorkflow(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)
	s, err := New(now, func() string { return "local-run" })
	require.NoError(t, err)
	importBaseline(t, s, upstreamBaseline(now))
	wft := pollWorkflowTask(t, s)

	_, err = s.Admin().DeleteWorkflowExecution(ctx, &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: testNamespace,
		Execution: importedExecution,
	})
	require.NoError(t, err)

	_, err = s.RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: testNamespace,
		TaskToken: wft.GetTaskToken(),
	})
	var notFound *serviceerror.NotFound
	require.ErrorAs(t, err, &notFound)
	require.Empty(t, s.engine.executions)
}

// upstreamBaseline returns the history a server returns when a local server acquires a new workflow
// run: its started event and its first scheduled workflow task.
func upstreamBaseline(now time.Time) []*historypb.HistoryEvent {
	return []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventTime: timestamppb.New(now.Add(-time.Second)),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			TaskId:    1048577,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
				WorkflowType:        &commonpb.WorkflowType{Name: "Turn"},
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				Input:               payloads("question"),
				WorkflowTaskTimeout: durationpb.New(10 * time.Second),
				ParentWorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: "agent",
					RunId:      "agent-run",
				},
				ParentInitiatedEventId:   5,
				Attempt:                  1,
				OriginalExecutionRunId:   importedExecution.GetRunId(),
				FirstExecutionRunId:      importedExecution.GetRunId(),
				FirstWorkflowTaskBackoff: durationpb.New(0),
				WorkflowId:               importedExecution.GetWorkflowId(),
			}},
		},
		{
			EventId:   2,
			EventTime: timestamppb.New(now.Add(-time.Second)),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			TaskId:    1048578,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
				TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				StartToCloseTimeout: durationpb.New(10 * time.Second),
				Attempt:             1,
			}},
		},
	}
}

func importBaseline(t *testing.T, s *Server, events []*historypb.HistoryEvent) {
	t.Helper()
	data, err := proto.Marshal(&historypb.History{Events: events})
	require.NoError(t, err)
	_, err = s.Admin().ImportWorkflowExecution(context.Background(), &adminservice.ImportWorkflowExecutionRequest{
		Namespace:      testNamespace,
		Execution:      importedExecution,
		HistoryBatches: []*commonpb.DataBlob{{EncodingType: enumspb.ENCODING_TYPE_PROTO3, Data: data}},
		VersionHistory: &historyspb.VersionHistory{Items: []*historyspb.VersionHistoryItem{{EventId: 2}}},
	})
	require.NoError(t, err)
}

// rawHistoryAfter returns the history batches after an event and the ID of the last event.
func rawHistoryAfter(t *testing.T, s *Server, eventID int64) ([][]*historypb.HistoryEvent, int64) {
	t.Helper()
	response, err := s.Admin().GetWorkflowExecutionRawHistoryV2(context.Background(), &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId:  testNamespace,
		Execution:    importedExecution,
		StartEventId: eventID,
	})
	require.NoError(t, err)
	items := response.GetVersionHistory().GetItems()
	require.Len(t, items, 1)
	require.Equal(t, int64(0), items[0].GetVersion())
	return decodeBatches(t, response.GetHistoryBatches()), items[0].GetEventId()
}

func decodeBatches(t *testing.T, blobs []*commonpb.DataBlob) [][]*historypb.HistoryEvent {
	t.Helper()
	batches := make([][]*historypb.HistoryEvent, len(blobs))
	for i, blob := range blobs {
		require.Equal(t, enumspb.ENCODING_TYPE_PROTO3, blob.GetEncodingType())
		history := &historypb.History{}
		require.NoError(t, proto.Unmarshal(blob.GetData(), history))
		batches[i] = history.GetEvents()
	}
	return batches
}

func requireBatches(t *testing.T, batches [][]*historypb.HistoryEvent, expected ...[]enumspb.EventType) {
	t.Helper()
	actual := make([][]enumspb.EventType, len(batches))
	for i, batch := range batches {
		for _, event := range batch {
			actual[i] = append(actual[i], event.GetEventType())
		}
	}
	require.Equal(t, expected, actual)
}

func scheduleActivityCommand(activityID string) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
			ActivityId:          activityID,
			ActivityType:        &commonpb.ActivityType{Name: "Step"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
			StartToCloseTimeout: durationpb.New(10 * time.Second),
		}},
	}
}

func completeActivity(t *testing.T, s *Server) {
	t.Helper()
	ctx := context.Background()
	act, err := s.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
		Namespace: testNamespace,
		TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue},
	})
	require.NoError(t, err)
	_, err = s.RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: testNamespace,
		TaskToken: act.GetTaskToken(),
	})
	require.NoError(t, err)
}
