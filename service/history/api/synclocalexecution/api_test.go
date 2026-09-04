package synclocalexecution

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/serialization"
)

func TestDecodeAndValidateHistoryRejectsV1MultiRunAndChildEvents(t *testing.T) {
	serializer := serialization.NewSerializer()
	tests := []struct {
		name  string
		event *historypb.HistoryEvent
	}{
		{
			name: "continue as new",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
			},
		},
		{
			name: "retry successor",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						NewExecutionRunId: "next-run",
					},
				},
			},
		},
		{
			name: "cron successor",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
					WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
						NewExecutionRunId: "next-run",
					},
				},
			},
		},
		{
			name: "child initiated",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
			},
		},
		{
			name: "child dependency",
			event: &historypb.HistoryEvent{
				EventType: enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.event.EventId = 3
			test.event.Version = 1
			request := singleEventRequest(t, serializer, test.event)
			_, err := decodeAndValidateHistory(request, serializer, 1024, 10, 10)
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}

func TestDecodeAndValidateHistoryEnforcesDynamicBounds(t *testing.T) {
	serializer := serialization.NewSerializer()
	request := singleEventRequest(t, serializer, &historypb.HistoryEvent{
		EventId:   3,
		Version:   1,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
	})

	for _, test := range []struct {
		name       string
		maxBytes   int
		maxEvents  int
		maxBatches int
	}{
		{name: "bytes", maxBytes: 0, maxEvents: 10, maxBatches: 10},
		{name: "events", maxBytes: 1024, maxEvents: 0, maxBatches: 10},
		{name: "batches", maxBytes: 1024, maxEvents: 10, maxBatches: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := decodeAndValidateHistory(
				request,
				serializer,
				test.maxBytes,
				test.maxEvents,
				test.maxBatches,
			)
			require.ErrorAs(t, err, new(*serviceerror.ResourceExhausted))
		})
	}
}

func TestRepeatedReleasedSyncIsIdempotentAndFingerprintProtected(t *testing.T) {
	request := &adminservice.SyncLocalExecutionRequest{
		SyncId:          "sync-id",
		NewEventId:      23,
		NewEventVersion: 1,
		Release:         true,
	}
	requestHash := sha256.Sum256([]byte("request"))
	localInfo := &persistencespb.LocalExecutionInfo{
		State:                        persistencespb.LocalExecutionInfo_STATE_UNOWNED,
		LastSynchronizedEventId:      request.GetNewEventId(),
		LastSynchronizedEventVersion: request.GetNewEventVersion(),
		LastSyncId:                   request.GetSyncId(),
		LastSyncRequestHash:          requestHash[:],
	}

	response, handled, err := repeatedSync(localInfo, request, requestHash[:], time.Now())
	require.NoError(t, err)
	require.True(t, handled)
	require.Equal(t, request.GetNewEventId(), response.GetResponse().GetAcknowledgedEventId())

	conflictingHash := sha256.Sum256([]byte("different request"))
	_, handled, err = repeatedSync(localInfo, request, conflictingHash[:], time.Now())
	require.True(t, handled)
	require.ErrorAs(t, err, new(*serviceerror.FailedPrecondition))
}

func singleEventRequest(
	t *testing.T,
	serializer serialization.Serializer,
	event *historypb.HistoryEvent,
) *adminservice.SyncLocalExecutionRequest {
	t.Helper()
	batch, err := serializer.SerializeEvents([]*historypb.HistoryEvent{event})
	require.NoError(t, err)
	return &adminservice.SyncLocalExecutionRequest{
		Namespace: "namespace",
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflow-id",
			RunId:      "01a06e74-95f9-77ae-891b-0e6a9ff6b2aa",
		},
		ProtocolVersion:      protocolVersion,
		LocalServerId:        "local-server",
		SyncId:               "sync-id",
		PreviousEventId:      2,
		PreviousEventVersion: 1,
		NewEventId:           3,
		NewEventVersion:      1,
		HistoryBatches:       []*commonpb.DataBlob{batch},
		VersionHistory: &historyspb.VersionHistory{
			Items: []*historyspb.VersionHistoryItem{{EventId: 3, Version: 1}},
		},
	}
}
