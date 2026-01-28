package system

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	commonnexus "go.temporal.io/server/common/nexus"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	historyi "go.temporal.io/server/service/history/interfaces"
)

// WaitExternalWorkflowCompletionInput is the input for the WaitExternalWorkflowCompletion operation.
type WaitExternalWorkflowCompletionInput struct {
	// WorkflowID is the ID of the workflow to wait for.
	WorkflowID string `json:"workflow_id"`
	// RunID is the optional run ID. If empty, waits for the current run.
	RunID string `json:"run_id,omitempty"`
}

// WaitExternalWorkflowCompletionOutput is the output of the WaitExternalWorkflowCompletion operation.
type WaitExternalWorkflowCompletionOutput struct {
	// Status is the final status of the workflow.
	Status enumspb.WorkflowExecutionStatus `json:"status"`
	// Result is the result payload if the workflow completed successfully.
	Result *commonpb.Payload `json:"result,omitempty"`
	// Failure is the failure if the workflow failed, was canceled, terminated, or timed out.
	Failure *failurepb.Failure `json:"failure,omitempty"`
}

// WaitExternalWorkflowCompletionHandler handles the WaitExternalWorkflowCompletion system operation.
type WaitExternalWorkflowCompletionHandler struct{}

// NewWaitExternalWorkflowCompletionHandler creates a new handler.
func NewWaitExternalWorkflowCompletionHandler() *WaitExternalWorkflowCompletionHandler {
	return &WaitExternalWorkflowCompletionHandler{}
}

// Name returns the operation name.
func (h *WaitExternalWorkflowCompletionHandler) Name() string {
	return commonnexus.WaitExternalWorkflowCompletionOperationName
}

// Execute runs the WaitExternalWorkflowCompletion operation.
// This implementation uses the history client to route requests to the correct shard,
// enabling cross-shard workflow lookups.
func (h *WaitExternalWorkflowCompletionHandler) Execute(
	ctx context.Context,
	execCtx ExecutionContext,
	args OperationArgs,
) (OperationResult, error) {
	// Parse input
	input, err := h.parseInput(args.Input)
	if err != nil {
		return OperationResult{}, err
	}

	// Validate input
	if input.WorkflowID == "" {
		return OperationResult{}, serviceerror.NewInvalidArgument("workflow_id is required")
	}

	// Type-assert the shard context to get the history client for cross-shard routing
	shardCtx, ok := execCtx.ShardContext.(historyi.ShardContext)
	if !ok || shardCtx == nil {
		// ShardContext not available - return async result
		return OperationResult{
			OperationToken: fmt.Sprintf("%s:%s", input.WorkflowID, input.RunID),
		}, nil
	}

	// Get the history client for cross-shard routing
	historyClient := shardCtx.GetHistoryClient()
	if historyClient == nil {
		// History client not available - return async result
		return OperationResult{
			OperationToken: fmt.Sprintf("%s:%s", input.WorkflowID, input.RunID),
		}, nil
	}

	// Fetch the last history event to check if workflow is closed.
	// Using reverse history with page size 1 is efficient for this check.
	histResp, err := historyClient.GetWorkflowExecutionHistoryReverse(ctx, &historyservice.GetWorkflowExecutionHistoryReverseRequest{
		NamespaceId: args.NamespaceID.String(),
		Request: &workflowservice.GetWorkflowExecutionHistoryReverseRequest{
			Namespace: args.NamespaceName.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: input.WorkflowID,
				RunId:      input.RunID,
			},
			MaximumPageSize: 1,
		},
	})
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			// Workflow not found - return sync failure
			return OperationResult{
				Failure: &failurepb.Failure{
					Message: fmt.Sprintf("workflow not found: %s", input.WorkflowID),
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
							Type:         "WorkflowNotFound",
							NonRetryable: true,
						},
					},
				},
			}, nil
		}
		return OperationResult{}, err
	}

	// Check if we got any events
	events := histResp.GetResponse().GetHistory().GetEvents()
	if len(events) == 0 {
		// No events - workflow might be in an invalid state, return async
		return OperationResult{
			OperationToken: fmt.Sprintf("%s:%s", input.WorkflowID, input.RunID),
		}, nil
	}

	// Check if the last event is a close event
	lastEvent := events[0]
	status, isCloseEvent := h.getCloseEventStatus(lastEvent)

	if isCloseEvent {
		// Workflow is already closed - build result from the close event
		output := &WaitExternalWorkflowCompletionOutput{
			Status: status,
		}
		h.populateOutputFromCompletionEvent(output, status, lastEvent)
		return h.buildResultFromOutput(output)
	}

	// Workflow is still running - attach a completion callback to be notified when it closes.
	runID := input.RunID
	if runID == "" {
		// RunID not provided - we need to get it via DescribeWorkflowExecution
		descResp, err := historyClient.DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
			NamespaceId: args.NamespaceID.String(),
			Request: &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: args.NamespaceName.String(),
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: input.WorkflowID,
				},
			},
		})
		if err != nil {
			return OperationResult{}, fmt.Errorf("failed to get workflow execution: %w", err)
		}
		runID = descResp.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	}
	return h.attachCallbackAndReturnAsync(ctx, execCtx, historyClient, args, input, runID)
}

// getCloseEventStatus returns the workflow status and true if the event is a close event.
func (h *WaitExternalWorkflowCompletionHandler) getCloseEventStatus(event *historypb.HistoryEvent) (enumspb.WorkflowExecutionStatus, bool) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		return enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, true
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW, true
	default:
		return enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, false
	}
}

// attachCallbackAndReturnAsync attaches a completion callback to the target workflow
// and returns an async result. When the target workflow closes, the callback will
// fire and complete the caller's Nexus operation.
func (h *WaitExternalWorkflowCompletionHandler) attachCallbackAndReturnAsync(
	ctx context.Context,
	execCtx ExecutionContext,
	historyClient historyservice.HistoryServiceClient,
	args OperationArgs,
	input *WaitExternalWorkflowCompletionInput,
	runID string,
) (OperationResult, error) {
	// Generate the callback token - this is done lazily only when going async
	callbackToken, err := execCtx.CallbackTokenGenerator()
	if err != nil {
		return OperationResult{}, fmt.Errorf("failed to generate callback token: %w", err)
	}

	// Create a completion callback that will invoke the system Nexus completion handler
	// when the target workflow closes.
	// The callback uses the system Nexus completion URL which routes to CompleteNexusOperation.
	callback := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: commonnexus.SystemNexusCompletionURL,
				Header: map[string]string{
					// The callback token identifies the caller's Nexus operation.
					// When the callback fires, this token is used to complete the operation.
					// Use lowercase key for compatibility with nexus.Header.Get() which does
					// case-insensitive lookup by converting to lowercase.
					strings.ToLower(commonnexus.CallbackTokenHeader): callbackToken,
				},
			},
		},
	}

	// Attach the callback to the target workflow via UpdateWorkflowExecutionOptions.
	// This will add a WorkflowExecutionOptionsUpdated event with the callback attached.
	// When the target workflow closes, the callback will fire.
	_, err = historyClient.UpdateWorkflowExecutionOptions(ctx, &historyservice.UpdateWorkflowExecutionOptionsRequest{
		NamespaceId: args.NamespaceID.String(),
		UpdateRequest: &workflowservice.UpdateWorkflowExecutionOptionsRequest{
			Namespace: args.NamespaceName.String(),
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: input.WorkflowID,
				RunId:      runID,
			},
			// Use an empty field mask since we're only attaching callbacks, not updating options
			UpdateMask: &fieldmaskpb.FieldMask{},
			Identity:   "system-nexus-wait-external-workflow",
		},
		CompletionCallbacks: []*commonpb.Callback{callback},
		AttachRequestId:     args.RequestID,
	})
	if err != nil {
		// If we fail to attach the callback, return the error
		// The Nexus operation will fail and the caller can retry
		return OperationResult{}, fmt.Errorf("failed to attach completion callback: %w", err)
	}

	// Return async result - the operation will complete when the target workflow closes
	// and the callback fires
	return OperationResult{
		OperationToken: fmt.Sprintf("%s:%s", input.WorkflowID, runID),
	}, nil
}

func (h *WaitExternalWorkflowCompletionHandler) parseInput(payload *commonpb.Payload) (*WaitExternalWorkflowCompletionInput, error) {
	if payload == nil {
		return nil, serviceerror.NewInvalidArgument("input payload is required")
	}

	var input WaitExternalWorkflowCompletionInput
	if err := json.Unmarshal(payload.GetData(), &input); err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("failed to parse input: %v", err))
	}

	return &input, nil
}

func (h *WaitExternalWorkflowCompletionHandler) populateOutputFromCompletionEvent(
	output *WaitExternalWorkflowCompletionOutput,
	status enumspb.WorkflowExecutionStatus,
	completionEvent *historypb.HistoryEvent,
) {
	switch status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		attrs := completionEvent.GetWorkflowExecutionCompletedEventAttributes()
		if attrs != nil && attrs.Result != nil && len(attrs.Result.Payloads) > 0 {
			output.Result = attrs.Result.Payloads[0]
		}

	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		attrs := completionEvent.GetWorkflowExecutionFailedEventAttributes()
		if attrs != nil {
			output.Failure = attrs.Failure
		}

	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		attrs := completionEvent.GetWorkflowExecutionCanceledEventAttributes()
		output.Failure = &failurepb.Failure{
			Message: "workflow was canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{
					Details: attrs.GetDetails(),
				},
			},
		}

	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		attrs := completionEvent.GetWorkflowExecutionTerminatedEventAttributes()
		output.Failure = &failurepb.Failure{
			Message: fmt.Sprintf("workflow was terminated: %s", attrs.GetReason()),
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
			},
		}

	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		output.Failure = &failurepb.Failure{
			Message: "workflow timed out",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				},
			},
		}
	}
}

func (h *WaitExternalWorkflowCompletionHandler) buildResultFromOutput(output *WaitExternalWorkflowCompletionOutput) (OperationResult, error) {
	// Serialize output to payload
	data, err := json.Marshal(output)
	if err != nil {
		return OperationResult{}, fmt.Errorf("failed to serialize output: %w", err)
	}

	payload := &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding": []byte("json/plain"),
		},
		Data: data,
	}

	// Return success with the output payload
	// The output contains the workflow status which the caller can inspect
	return OperationResult{
		Output: payload,
	}, nil
}

// RegisterWaitExternalWorkflowCompletion registers the handler with the registry.
func RegisterWaitExternalWorkflowCompletion(registry *Registry) {
	registry.Register(NewWaitExternalWorkflowCompletionHandler())
}

// Ensure WaitExternalWorkflowCompletionHandler implements OperationHandler
var _ OperationHandler = (*WaitExternalWorkflowCompletionHandler)(nil)
