package getworkflowexecutionresult

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionResultRequest,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	maxCANChainDepth int,
) (*historyservice.GetWorkflowExecutionResultResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	req := request.GetRequest()
	workflowID := req.GetExecution().GetWorkflowId()
	runID := req.GetExecution().GetRunId()
	hasCallbacks := len(req.GetCallbacks()) > 0 || len(req.GetLinks()) > 0

	// Walk the CAN chain until we reach the head. At head, we will return the result of the head's workflow run,
	// and optionally attach callbacks if hasCallbacks.
	for range maxCANChainDepth {
		result, err := getWorkflowResultForExecution(
			ctx, namespaceID, workflowID, runID, req, hasCallbacks, shardCtx, workflowConsistencyChecker,
		)
		if err != nil {
			return nil, err
		}
		if result.nextRunID != "" {
			runID = result.nextRunID
			continue
		}
		return result.resp, nil
	}
	return nil, serviceerror.NewInternal("exceeded maximum continue-as-new chain depth")
}

func getWorkflowResultForExecution(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	req *workflowservice.GetWorkflowExecutionResultRequest,
	hasCallbacks bool,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (wfRunResult, error) {
	var result wfRunResult
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()

			// Read-only path: if there are no callbacks to be attached or no running workflow to attach to,
			// classifyWorkflowRunResult and return the response.
			if !hasCallbacks || !mutableState.IsWorkflowExecutionRunning() {
				r, err := classifyWorkflowRunResult(ctx, mutableState)
				if err != nil {
					return nil, err
				}
				result = r
				return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
			}

			// Handle case when there are callbacks and this workflow execution is running:
			// 1. Append AddWorkflowExecutionOptionsUpdatedEvent
			// 2. Set response for caller with links
			// 3. Return with Noop=false so the callbacks get persisted.
			optionsEvent, err := mutableState.AddWorkflowExecutionOptionsUpdatedEvent(
				nil,
				false,
				req.GetRequestId(),
				req.GetCallbacks(),
				req.GetLinks(),
				req.GetIdentity(),
				nil,
				nil,
			)
			if err != nil {
				return nil, err
			}
			result.resp = wfNotCompletedResponse(mutableState)
			namespaceName := mutableState.GetNamespaceEntry().Name().String()
			result.resp.Response.Links = makeEventLink(
				namespaceName, result.resp.Response.GetNotCompleted().GetExecution(), optionsEvent,
			)
			return &api.UpdateWorkflowAction{Noop: false, CreateWorkflowTask: false}, nil
		},
		nil,
		shardCtx,
		workflowConsistencyChecker,
	)
	if err != nil {
		return wfRunResult{}, err
	}
	return result, nil
}

// wfRunResult describes the outcome of inspecting one run in the chain.
// - if nextRunID set: this run continued-as-new; caller should follow.
// - if resp set:      this run is the head of the workflow run.
type wfRunResult struct {
	nextRunID string
	resp      *historyservice.GetWorkflowExecutionResultResponse
}

func classifyWorkflowRunResult(ctx context.Context, mutableState historyi.MutableState) (wfRunResult, error) {
	if mutableState.IsWorkflowExecutionRunning() {
		return wfRunResult{resp: wfNotCompletedResponse(mutableState)}, nil
	}
	executionState := mutableState.GetExecutionState()
	// Guard against zombie/corrupted states - only COMPLETED state has a completion event.
	if executionState.State != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		return wfRunResult{}, consts.ErrWorkflowNotReady
	}

	// Handle CONTINUED_AS_NEW: the logical workflow chain is still running, set the nextRunID for caller
	// to follow the CAN chain.
	if executionState.GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW {
		completionEvent, err := mutableState.GetCompletionEvent(ctx)
		if err != nil {
			return wfRunResult{}, err
		}
		return wfRunResult{
			nextRunID: completionEvent.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId(),
		}, nil
	}

	// Return resp for workflow in terminal state.
	resp, err := wfCompletedResponse(ctx, mutableState)
	if err != nil {
		return wfRunResult{}, err
	}
	return wfRunResult{resp: resp}, nil
}

func wfNotCompletedResponse(mutableState historyi.MutableState) *historyservice.GetWorkflowExecutionResultResponse {
	executionState := mutableState.GetExecutionState()
	executionInfo := mutableState.GetExecutionInfo()
	return &historyservice.GetWorkflowExecutionResultResponse{
		Response: &workflowservice.GetWorkflowExecutionResultResponse{
			Completion: &workflowservice.GetWorkflowExecutionResultResponse_NotCompleted_{
				NotCompleted: &workflowservice.GetWorkflowExecutionResultResponse_NotCompleted{
					Execution: &commonpb.WorkflowExecution{
						WorkflowId: executionInfo.GetWorkflowId(),
						RunId:      executionState.GetRunId(),
					},
					Status: executionState.GetStatus(),
				},
			},
		},
	}
}

func wfCompletedResponse(ctx context.Context, mutableState historyi.MutableState) (*historyservice.GetWorkflowExecutionResultResponse, error) {
	executionState := mutableState.GetExecutionState()
	executionInfo := mutableState.GetExecutionInfo()
	namespaceName := mutableState.GetNamespaceEntry().Name().String()
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: executionInfo.GetWorkflowId(),
		RunId:      executionState.GetRunId(),
	}

	completionEvent, err := mutableState.GetCompletionEvent(ctx)
	if err != nil {
		return nil, err
	}
	result, failure, err := resultFromCompletionEvent(completionEvent)
	if err != nil {
		return nil, err
	}
	return &historyservice.GetWorkflowExecutionResultResponse{
		Response: &workflowservice.GetWorkflowExecutionResultResponse{
			Completion: &workflowservice.GetWorkflowExecutionResultResponse_Completed_{
				Completed: &workflowservice.GetWorkflowExecutionResultResponse_Completed{
					Execution: currentExecution,
					Status:    executionState.GetStatus(),
					Result:    result,
					Failure:   failure,
				},
			},
			Links: makeEventLink(namespaceName, currentExecution, completionEvent),
		},
	}, nil
}

// resultFromCompletionEvent extracts the result or failure from a workflow completion event.
// This follows the same conversion logic used when invoking workflow callbacks
// (see MutableStateImpl.GetNexusCompletion).
func resultFromCompletionEvent(event *historypb.HistoryEvent) (*commonpb.Payloads, *failurepb.Failure, error) {
	switch event.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		return event.GetWorkflowExecutionCompletedEventAttributes().GetResult(), nil, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		return nil, event.GetWorkflowExecutionFailedEventAttributes().GetFailure(), nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return nil, &failurepb.Failure{
			Message: "workflow execution was canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{
					Details: event.GetWorkflowExecutionCanceledEventAttributes().GetDetails(),
				},
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return nil, &failurepb.Failure{
			Message: "workflow execution was terminated",
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return nil, &failurepb.Failure{
			Message: "workflow execution timed out",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				},
			},
		}, nil
	default:
		return nil, nil, serviceerror.NewInternal(
			fmt.Sprintf("unexpected completion event type %v", event.GetEventType()),
		)
	}
}

func makeEventLink(namespaceName string, execution *commonpb.WorkflowExecution, event *historypb.HistoryEvent) []*commonpb.Link {
	return []*commonpb.Link{
		{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: &commonpb.Link_WorkflowEvent{
					Namespace:  namespaceName,
					WorkflowId: execution.GetWorkflowId(),
					RunId:      execution.GetRunId(),
					Reference: &commonpb.Link_WorkflowEvent_EventRef{
						EventRef: &commonpb.Link_WorkflowEvent_EventReference{
							EventId:   event.GetEventId(),
							EventType: event.GetEventType(),
						},
					},
				},
			},
		},
	}
}
