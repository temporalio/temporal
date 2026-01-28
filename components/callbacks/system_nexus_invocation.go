package callbacks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/components/nexusoperations"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// systemNexusInvocation handles completion callbacks for system Nexus operations.
// When a target workflow closes, this invocation type completes the caller's
// Nexus operation by calling CompleteNexusOperation with the workflow's completion result
// (including the result payload for successful completions, or failure details for failures).
//
// This is used internally by system operations like WaitExternalWorkflowCompletion
// to deliver the target workflow's result back to the caller's Nexus operation.
type systemNexusInvocation struct {
	nexus      *persistencespb.Callback_Nexus
	completion nexusrpc.OperationCompletion
	attempt    int32
	requestID  string
}

// waitExternalWorkflowCompletionOutput mirrors the output format expected by the SDK.
// This must match the format in sdk-go/internal/wait_external_workflow.go.
type waitExternalWorkflowCompletionOutput struct {
	Status  enumspb.WorkflowExecutionStatus `json:"status"`
	Result  *commonpb.Payload               `json:"result,omitempty"`
	Failure *failurepb.Failure              `json:"failure,omitempty"`
}

func (h systemNexusInvocation) WrapError(result invocationResult, err error) error {
	return result.error()
}

func (h systemNexusInvocation) Invoke(ctx context.Context, ns *namespace.Namespace, e taskExecutor, task InvocationTask) invocationResult {
	// Get the callback token from the header - this identifies the caller's Nexus operation
	header := nexus.Header(h.nexus.GetHeader())
	if header == nil {
		header = nexus.Header{}
	}

	encodedToken := header.Get(commonnexus.CallbackTokenHeader)
	if encodedToken == "" {
		return invocationResultFail{logInternalError(e.Logger, "callback missing token", nil)}
	}

	// Decode the callback token to get the caller's operation reference
	token, err := commonnexus.DecodeCallbackToken(encodedToken)
	if err != nil {
		return invocationResultFail{logInternalError(e.Logger, "failed to decode callback token", err)}
	}

	tokenGenerator := commonnexus.NewCallbackTokenGenerator()
	completionToken, err := tokenGenerator.DecodeCompletion(token)
	if err != nil {
		return invocationResultFail{logInternalError(e.Logger, "failed to decode callback completion", err)}
	}

	// Build the CompleteNexusOperation request based on the completion type
	request, err := h.buildCompleteRequest(completionToken)
	if err != nil {
		return invocationResultFail{logInternalError(e.Logger, "failed to build completion request", err)}
	}

	// Call CompleteNexusOperation to complete the caller's Nexus operation
	_, err = e.HistoryClient.CompleteNexusOperation(ctx, request)
	if err != nil {
		redactedErr := logInternalError(e.Logger, "failed to complete Nexus operation", err)
		if isRetryableRPCResponse(err) {
			return invocationResultRetry{redactedErr}
		}
		return invocationResultFail{redactedErr}
	}

	return invocationResultOK{}
}

func (h systemNexusInvocation) buildCompleteRequest(
	completionToken *tokenspb.NexusOperationCompletion,
) (*historyservice.CompleteNexusOperationRequest, error) {
	completionToken.RequestId = h.requestID

	switch op := h.completion.(type) {
	case *nexusrpc.OperationCompletionSuccessful:
		// Read payload from the completion
		payloadBody, err := io.ReadAll(op.Reader)
		if err != nil {
			return nil, fmt.Errorf("failed to read payload: %w", err)
		}

		var resultPayload *commonpb.Payload
		if payloadBody != nil {
			content := &nexus.Content{
				Header: op.Reader.Header,
				Data:   payloadBody,
			}
			err := commonnexus.PayloadSerializer.Deserialize(content, &resultPayload)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize payload: %w", err)
			}
		}

		// Build the WaitExternalWorkflowCompletionOutput
		output := &waitExternalWorkflowCompletionOutput{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			Result: resultPayload,
		}

		// Serialize the output to a payload
		outputPayload, err := h.serializeOutput(output)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize output: %w", err)
		}

		return &historyservice.CompleteNexusOperationRequest{
			Completion:     completionToken,
			State:          string(nexus.OperationStateSucceeded),
			OperationToken: op.OperationToken,
			StartTime:      timestamppb.New(op.StartTime),
			Links:          convertNexusLinksToProtos(op.Links),
			Outcome: &historyservice.CompleteNexusOperationRequest_Success{
				Success: outputPayload,
			},
		}, nil

	case *nexusrpc.OperationCompletionUnsuccessful:
		// Map the nexus state to workflow execution status
		status := h.mapNexusStateToWorkflowStatus(op.State, op.Failure)

		// Build the WaitExternalWorkflowCompletionOutput with failure
		output := &waitExternalWorkflowCompletionOutput{
			Status:  status,
			Failure: h.convertNexusFailureToProto(op.Failure),
		}

		// Serialize the output to a payload
		outputPayload, err := h.serializeOutput(output)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize output: %w", err)
		}

		// Even though the workflow failed/was canceled/etc., we complete the Nexus operation
		// successfully with the failure info in the output payload.
		return &historyservice.CompleteNexusOperationRequest{
			Completion:     completionToken,
			State:          string(nexus.OperationStateSucceeded),
			OperationToken: op.OperationToken,
			StartTime:      timestamppb.New(op.StartTime),
			Links:          convertNexusLinksToProtos(op.Links),
			Outcome: &historyservice.CompleteNexusOperationRequest_Success{
				Success: outputPayload,
			},
		}, nil

	default:
		return nil, fmt.Errorf("unexpected nexus.OperationCompletion: %T", h.completion)
	}
}

// serializeOutput serializes the output to a Payload with JSON encoding.
// Uses protojson for proto message fields to ensure proper cross-language compatibility.
func (h systemNexusInvocation) serializeOutput(output *waitExternalWorkflowCompletionOutput) (*commonpb.Payload, error) {
	// Use a struct with json.RawMessage for proto fields so we can encode them with protojson
	type outputJSON struct {
		Status  enumspb.WorkflowExecutionStatus `json:"status"`
		Result  json.RawMessage                 `json:"result,omitempty"`
		Failure json.RawMessage                 `json:"failure,omitempty"`
	}

	out := outputJSON{
		Status: output.Status,
	}

	// Encode Result with protojson if present
	if output.Result != nil {
		resultBytes, err := protojson.Marshal(output.Result)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal result: %w", err)
		}
		out.Result = resultBytes
	}

	// Encode Failure with protojson if present
	if output.Failure != nil {
		failureBytes, err := protojson.Marshal(output.Failure)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal failure: %w", err)
		}
		out.Failure = failureBytes
	}

	data, err := json.Marshal(out)
	if err != nil {
		return nil, err
	}
	return &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding": []byte("json/plain"),
		},
		Data: data,
	}, nil
}

// mapNexusStateToWorkflowStatus maps a Nexus operation state and failure to a workflow execution status.
func (h systemNexusInvocation) mapNexusStateToWorkflowStatus(state nexus.OperationState, failure nexus.Failure) enumspb.WorkflowExecutionStatus {
	// Check failure metadata for workflow status hints
	if statusStr, ok := failure.Metadata["workflow_status"]; ok {
		switch statusStr {
		case "failed":
			return enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
		case "canceled":
			return enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
		case "terminated":
			return enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
		case "timed_out":
			return enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
		}
	}

	// Default mapping based on state
	switch state {
	case nexus.OperationStateFailed:
		return enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	case nexus.OperationStateCanceled:
		return enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
	default:
		return enumspb.WORKFLOW_EXECUTION_STATUS_FAILED
	}
}

// convertNexusFailureToProto converts a nexus.Failure to failurepb.Failure.
func (h systemNexusInvocation) convertNexusFailureToProto(failure nexus.Failure) *failurepb.Failure {
	return &failurepb.Failure{
		Message: failure.Message,
		// Note: We don't have access to the original FailureInfo here,
		// so we create a generic ApplicationFailureInfo
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         "WorkflowExecutionFailure",
				NonRetryable: true,
			},
		},
	}
}

// convertNexusLinksToProtos converts nexus.Link slice to []*commonpb.Link.
// Invalid links are silently skipped (non-essential for operation completion).
func convertNexusLinksToProtos(nexusLinks []nexus.Link) []*commonpb.Link {
	if nexusLinks == nil {
		return nil
	}
	var links []*commonpb.Link
	for _, nexusLink := range nexusLinks {
		linkWorkflowEventType := string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName())
		if nexusLink.Type == linkWorkflowEventType {
			link, err := nexusoperations.ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
			if err != nil {
				// Links are non-essential, skip on error
				continue
			}
			links = append(links, &commonpb.Link{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: link,
				},
			})
		}
		// Skip unknown link types
	}
	return links
}
