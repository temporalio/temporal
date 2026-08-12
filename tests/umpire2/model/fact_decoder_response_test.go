package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/umpire2/fact"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestImportHistoryResponseCapturesNexusTimeoutSemantics(t *testing.T) {
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id"},
	}
	response := &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventId:   5,
			EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED,
			Attributes: &historypb.HistoryEvent_NexusOperationScheduledEventAttributes{
				NexusOperationScheduledEventAttributes: &historypb.NexusOperationScheduledEventAttributes{
					StartToCloseTimeout: durationpb.New(2 * time.Second),
				},
			},
		},
		{
			EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
			Attributes: &historypb.HistoryEvent_NexusOperationTimedOutEventAttributes{
				NexusOperationTimedOutEventAttributes: &historypb.NexusOperationTimedOutEventAttributes{
					ScheduledEventId: 5,
					Failure: &failurepb.Failure{Cause: &failurepb.Failure{
						Message: "operation timed out",
						FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
							TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
						}},
					}},
				},
			},
		},
	}}}

	decoded := fromResponse(request, response, "namespace-id")
	snapshot, ok := decoded.(*fact.NexusOperationHistorySnapshot)
	require.True(t, ok)
	require.Equal(t, &fact.NexusOperationHistorySnapshot{
		NamespaceID:         "namespace-id",
		WorkflowID:          "workflow-id",
		ScheduledEventID:    "5",
		StartToCloseTimeout: 2 * time.Second,
		TimeoutType:         enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		TimeoutMessage:      "operation timed out",
		EntityPath:          snapshot.EntityPath,
	}, snapshot)
}
