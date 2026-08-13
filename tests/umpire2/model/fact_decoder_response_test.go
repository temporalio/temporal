package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/tests/umpire2/fact"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestImportPollNexusResponseCapturesCallbackTarget(t *testing.T) {
	token, err := (&commonnexus.CallbackTokenGenerator{}).Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: "namespace-id",
		WorkflowId:  "operation-id",
		RunId:       "run-id",
		Ref:         &persistencespb.StateMachineRef{},
		RequestId:   "operation-request-id",
	})
	require.NoError(t, err)
	response := &workflowservice.PollNexusTaskQueueResponse{Request: &nexuspb.Request{
		Variant: &nexuspb.Request_StartOperation{StartOperation: &nexuspb.StartOperationRequest{
			RequestId:      "handler-request-id",
			Callback:       "https://callback",
			CallbackHeader: map[string]string{commonnexus.CallbackTokenHeader: token},
		}},
	}}

	decoded := fromResponses(&workflowservice.PollNexusTaskQueueRequest{}, response, "namespace-id")
	require.Len(t, decoded, 1)
	observed, ok := decoded[0].(*fact.NexusCallbackObservation)
	require.True(t, ok)
	require.Equal(t, "operation-id", observed.OperationID)
}

func TestImportStartWorkflowResponseReturnsEveryCallbackAttachment(t *testing.T) {
	callback := func(url string) *commonpb.Callback {
		return &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url}}}
	}
	request := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowId:          "handler-id",
		RequestId:           "request-id",
		CompletionCallbacks: []*commonpb.Callback{callback("https://first"), callback("https://second")},
	}
	response := &workflowservice.StartWorkflowExecutionResponse{RunId: "handler-run-id"}

	decoded := fromResponses(request, response, "namespace-id")
	require.Len(t, decoded, 2)
	first, ok := decoded[0].(*fact.WorkflowCallbackAttachment)
	require.True(t, ok)
	second, ok := decoded[1].(*fact.WorkflowCallbackAttachment)
	require.True(t, ok)
	require.NotEqual(t, first.CallbackID, second.CallbackID)
	require.Equal(t, "handler-run-id", first.HandlerRunID)
}

func TestImportHistoryResponseReturnsExistingAndTerminalFacts(t *testing.T) {
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id"}}
	response := &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED,
			Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestFailedEventAttributes{
				NexusOperationCancelRequestFailedEventAttributes: &historypb.NexusOperationCancelRequestFailedEventAttributes{ScheduledEventId: 5},
			},
		},
		{
			EventId:   9,
			EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
			Attributes: &historypb.HistoryEvent_NexusOperationCompletedEventAttributes{
				NexusOperationCompletedEventAttributes: &historypb.NexusOperationCompletedEventAttributes{ScheduledEventId: 5},
			},
		},
	}}}

	decoded := fromResponses(request, response, "namespace-id")
	require.Len(t, decoded, 2)
	_, ok := decoded[0].(*fact.NexusOperationCancelRequestFailed)
	require.True(t, ok)
	_, ok = decoded[1].(*fact.NexusOperationTerminal)
	require.True(t, ok)
}

func TestImportDescribeMutableStateReturnsExplicitEmptyStorageSnapshot(t *testing.T) {
	request := &adminservice.DescribeMutableStateRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id"}}
	response := &adminservice.DescribeMutableStateResponse{DatabaseMutableState: &persistencespb.WorkflowMutableState{}}

	decoded := fromResponses(request, response, "namespace-id")
	require.Len(t, decoded, 1)
	observed, ok := decoded[0].(*fact.WorkflowNexusStorageSnapshot)
	require.True(t, ok)
	require.Empty(t, observed.OperationIDs)
}

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

func TestImportHistoryResponseCapturesNexusCancelRequestFailure(t *testing.T) {
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id"},
	}
	response := &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED,
			Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestFailedEventAttributes{
				NexusOperationCancelRequestFailedEventAttributes: &historypb.NexusOperationCancelRequestFailedEventAttributes{
					ScheduledEventId: 5,
					RequestedEventId: 7,
					Failure:          &failurepb.Failure{Message: "cancel failed"},
				},
			},
		},
	}}}

	decoded := fromResponse(request, response, "namespace-id")
	observed, ok := decoded.(*fact.NexusOperationCancelRequestFailed)
	require.True(t, ok)
	require.Equal(t, &fact.NexusOperationCancelRequestFailed{
		NamespaceID:      "namespace-id",
		WorkflowID:       "workflow-id",
		ScheduledEventID: "5",
		RequestedEventID: "7",
		FailureMessage:   "cancel failed",
		EntityPath:       observed.EntityPath,
	}, observed)
}

func TestImportDescribeResponseCapturesStandaloneNexusCancelRequestFailure(t *testing.T) {
	request := &workflowservice.DescribeNexusOperationExecutionRequest{OperationId: "operation-id"}
	response := &workflowservice.DescribeNexusOperationExecutionResponse{Info: &nexuspb.NexusOperationExecutionInfo{
		CancellationInfo: &nexuspb.NexusOperationExecutionCancellationInfo{
			State:              enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED,
			LastAttemptFailure: &failurepb.Failure{Message: "cancel failed"},
		},
	}}

	decoded := fromResponse(request, response, "namespace-id")
	snapshot, ok := decoded.(*fact.NexusOperationExecutionSnapshot)
	require.True(t, ok)
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, snapshot.CancellationState)
	require.Equal(t, "cancel failed", snapshot.CancellationFailure)
}
