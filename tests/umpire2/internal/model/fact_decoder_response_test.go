package model

import (
	"sync"
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
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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

	decoded := NewFactDecoder().ImportResponses(&workflowservice.PollNexusTaskQueueRequest{}, response, "namespace-id")
	require.Len(t, decoded, 1)
	observed, ok := decoded[0].(*fact.NexusCallbackObservation)
	require.True(t, ok)
	require.Equal(t, "operation-id", observed.OperationID)
}

func TestFactDecoderCorrelatesSuccessfulNexusStartResponseAndPurgesNamespace(t *testing.T) {
	token, err := (&commonnexus.CallbackTokenGenerator{}).Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: "namespace-id",
		WorkflowId:  "operation-id",
		RunId:       "run-id",
		Ref:         &persistencespb.StateMachineRef{},
		RequestId:   "operation-request-id",
	})
	require.NoError(t, err)
	taskToken := []byte("secret-task-token")
	pollResponse := &workflowservice.PollNexusTaskQueueResponse{
		TaskToken: taskToken,
		Request: &nexuspb.Request{Variant: &nexuspb.Request_StartOperation{StartOperation: &nexuspb.StartOperationRequest{
			Callback:       "https://callback",
			CallbackHeader: map[string]string{commonnexus.CallbackTokenHeader: token},
		}}},
	}
	decoder := NewFactDecoder()
	pollFacts := decoder.ImportResponses(&workflowservice.PollNexusTaskQueueRequest{}, pollResponse, "namespace-id")
	require.Len(t, pollFacts, 1)
	callback := pollFacts[0].(*fact.NexusCallbackObservation)
	request := &workflowservice.RespondNexusTaskCompletedRequest{
		TaskToken: taskToken,
		Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_AsyncSuccess{AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "operation-token"}},
		}}},
	}

	decoded := decoder.ImportResponses(request, &workflowservice.RespondNexusTaskCompletedResponse{}, "namespace-id")
	require.Len(t, decoded, 1)
	response, ok := decoded[0].(*fact.NexusStartResponse)
	require.True(t, ok)
	require.Equal(t, callback.CallbackID, response.CallbackID)
	require.Equal(t, "async_success", response.ResponseKind)
	require.NotEmpty(t, response.DeliveryID)
	require.NotEmpty(t, response.ResponseFingerprint)
	require.NotZero(t, response.ObservedAt)

	decoder.PurgeNamespace("namespace-id")
	require.Empty(t, decoder.ImportResponses(request, &workflowservice.RespondNexusTaskCompletedResponse{}, "namespace-id"))
}

func TestFactDecoderCorrelatesConcurrentNexusStartResponses(t *testing.T) {
	token, err := (&commonnexus.CallbackTokenGenerator{}).Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId: "namespace-id",
		WorkflowId:  "operation-id",
		RunId:       "run-id",
		Ref:         &persistencespb.StateMachineRef{},
		RequestId:   "operation-request-id",
	})
	require.NoError(t, err)
	taskToken := []byte("secret-task-token")
	decoder := NewFactDecoder()
	decoder.ImportResponses(&workflowservice.PollNexusTaskQueueRequest{}, &workflowservice.PollNexusTaskQueueResponse{
		TaskToken: taskToken,
		Request: &nexuspb.Request{Variant: &nexuspb.Request_StartOperation{StartOperation: &nexuspb.StartOperationRequest{
			CallbackHeader: map[string]string{commonnexus.CallbackTokenHeader: token},
		}}},
	}, "namespace-id")
	request := &workflowservice.RespondNexusTaskCompletedRequest{
		TaskToken: taskToken,
		Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_AsyncSuccess{AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "operation-token"}},
		}}},
	}

	const workers = 32
	var waitGroup sync.WaitGroup
	waitGroup.Add(workers)
	results := make(chan []umpire.Fact, workers)
	for range workers {
		go func() {
			defer waitGroup.Done()
			results <- decoder.ImportResponses(request, &workflowservice.RespondNexusTaskCompletedResponse{}, "namespace-id")
		}()
	}
	waitGroup.Wait()
	close(results)
	for decoded := range results {
		require.Len(t, decoded, 1)
		_, ok := decoded[0].(*fact.NexusStartResponse)
		require.True(t, ok)
	}
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

	decoded := NewFactDecoder().ImportResponses(request, response, "namespace-id")
	require.Len(t, decoded, 2)
	first, ok := decoded[0].(*fact.WorkflowCallbackAttachment)
	require.True(t, ok)
	second, ok := decoded[1].(*fact.WorkflowCallbackAttachment)
	require.True(t, ok)
	require.NotEqual(t, first.CallbackID, second.CallbackID)
	require.Equal(t, "handler-run-id", first.HandlerRunID)
}

func TestImportHistoryResponseCapturesCallbackAttachmentReferences(t *testing.T) {
	startedAt := time.Date(2026, time.August, 12, 16, 0, 0, 0, time.UTC)
	attachedAt := startedAt.Add(time.Minute)
	callback := func(url string) *commonpb.Callback {
		return &commonpb.Callback{Variant: &commonpb.Callback_Nexus_{Nexus: &commonpb.Callback_Nexus{Url: url}}}
	}
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{Execution: &commonpb.WorkflowExecution{
		WorkflowId: "handler-id",
		RunId:      "handler-run-id",
	}}
	response := &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventTime: timestamppb.New(startedAt),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{CompletionCallbacks: []*commonpb.Callback{callback("https://start")}},
			},
		},
		{
			EventId:   7,
			EventTime: timestamppb.New(attachedAt),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionOptionsUpdatedEventAttributes{
				WorkflowExecutionOptionsUpdatedEventAttributes: &historypb.WorkflowExecutionOptionsUpdatedEventAttributes{
					AttachedRequestId:           "attach-request-id",
					AttachedCompletionCallbacks: []*commonpb.Callback{callback("https://attached")},
				},
			},
		},
	}}}

	decoded := NewFactDecoder().ImportResponses(request, response, "namespace-id")
	require.Len(t, decoded, 2)
	startAttachment, ok := decoded[0].(*fact.WorkflowCallbackAttachment)
	require.True(t, ok)
	require.Equal(t, startedAt, startAttachment.HandlerWorkflowStartTime)
	require.Equal(t, startedAt, startAttachment.AttachmentEventTime)
	require.Equal(t, int64(1), startAttachment.AttachmentEventID)
	require.Equal(t, "event", startAttachment.ReferenceKind)
	require.Equal(t, "1", startAttachment.ReferenceValue)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, startAttachment.ReferencedEventType)
	require.Empty(t, startAttachment.RequestID)

	requestAttachment, ok := decoded[1].(*fact.WorkflowCallbackAttachment)
	require.True(t, ok)
	require.Equal(t, startedAt, requestAttachment.HandlerWorkflowStartTime)
	require.Equal(t, attachedAt, requestAttachment.AttachmentEventTime)
	require.Equal(t, int64(7), requestAttachment.AttachmentEventID)
	require.Equal(t, "request", requestAttachment.ReferenceKind)
	require.Equal(t, "attach-request-id", requestAttachment.ReferenceValue)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, requestAttachment.ReferencedEventType)
	require.Equal(t, "attach-request-id", requestAttachment.RequestID)
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

	decoded := NewFactDecoder().ImportResponses(request, response, "namespace-id")
	require.Len(t, decoded, 2)
	_, ok := decoded[0].(*fact.NexusOperationCancelRequestFailed)
	require.True(t, ok)
	_, ok = decoded[1].(*fact.NexusOperationTerminal)
	require.True(t, ok)
}

func TestImportDescribeMutableStateReturnsExplicitEmptyStorageSnapshot(t *testing.T) {
	request := &adminservice.DescribeMutableStateRequest{Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id"}}
	response := &adminservice.DescribeMutableStateResponse{DatabaseMutableState: &persistencespb.WorkflowMutableState{}}

	decoded := NewFactDecoder().ImportResponses(request, response, "namespace-id")
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

	decoded := NewFactDecoder().ImportResponse(request, response, "namespace-id")
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

func TestImportHistoryResponseCapturesNexusStartedWorkflowReference(t *testing.T) {
	startedAt := time.Date(2026, time.August, 12, 17, 0, 0, 0, time.UTC)
	request := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Execution: &commonpb.WorkflowExecution{WorkflowId: "caller-id", RunId: "caller-run-id"},
	}
	response := &workflowservice.GetWorkflowExecutionHistoryResponse{History: &historypb.History{Events: []*historypb.HistoryEvent{{
		EventId:   8,
		EventTime: timestamppb.New(startedAt),
		EventType: enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		Attributes: &historypb.HistoryEvent_NexusOperationStartedEventAttributes{
			NexusOperationStartedEventAttributes: &historypb.NexusOperationStartedEventAttributes{ScheduledEventId: 5},
		},
		Links: []*commonpb.Link{{Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: &commonpb.Link_WorkflowEvent{
			WorkflowId: "handler-id",
			RunId:      "handler-run-id",
			Reference: &commonpb.Link_WorkflowEvent_EventRef{EventRef: &commonpb.Link_WorkflowEvent_EventReference{
				EventId:   1,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			}},
		}}}},
	}}}}

	decoded := NewFactDecoder().ImportResponses(request, response, "namespace-id")
	require.Len(t, decoded, 1)
	observed, ok := decoded[0].(*fact.NexusOperationStartedHistory)
	require.True(t, ok)
	require.Equal(t, startedAt, observed.EventTime())
	require.Equal(t, "caller-id", observed.WorkflowID)
	require.Equal(t, "5", observed.ScheduledEventID)
	require.Equal(t, "handler-id", observed.HandlerWorkflowID)
	require.Equal(t, "handler-run-id", observed.HandlerRunID)
	require.Equal(t, "event", observed.ReferenceKind)
	require.Equal(t, "1", observed.ReferenceValue)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, observed.ReferencedEventType)
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

	decoded := NewFactDecoder().ImportResponse(request, response, "namespace-id")
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

	decoded := NewFactDecoder().ImportResponse(request, response, "namespace-id")
	snapshot, ok := decoded.(*fact.NexusOperationExecutionSnapshot)
	require.True(t, ok)
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED, snapshot.CancellationState)
	require.Equal(t, "cancel failed", snapshot.CancellationFailure)
}
