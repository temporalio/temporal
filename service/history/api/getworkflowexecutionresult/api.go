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
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	request *historyservice.GetWorkflowExecutionResultRequest,
	shardCtx historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.GetWorkflowExecutionResultResponse, error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	req := request.GetRequest()
	workflowID := req.GetExecution().GetWorkflowId()
	shouldAttachEvents := len(req.GetCompletionCallbacks()) > 0 || len(req.GetLinks()) > 0

	var resp *historyservice.GetWorkflowExecutionResultResponse
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(namespaceID.String(), workflowID, ""),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()

			// Read-only: if workflow execution is completed, retrieve the result and return.
			// No need to attach any completion callbacks.
			if !mutableState.IsWorkflowExecutionRunning() {
				r, err := wfCompletedResponse(ctx, mutableState)
				if err != nil {
					return nil, err
				}
				resp = r
				return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
			}

			// Read-only: if there are no completion callbacks or links that needs to be attached,
			// report the not-completed status and return.
			if !shouldAttachEvents {
				resp = wfRunningResponse(mutableState)
				return &api.UpdateWorkflowAction{Noop: true, CreateWorkflowTask: false}, nil
			}

			// Write path: there are completion callbacks or links, and this workflow execution is running:
			// First we attach callbacks and links via AddWorkflowExecutionOptionsUpdatedEvent
			optionsEvent, err := mutableState.AddWorkflowExecutionOptionsUpdatedEvent(
				nil,
				false,
				req.GetRequestId(),
				req.GetCompletionCallbacks(),
				req.GetLinks(),
				req.GetIdentity(),
				nil,
				nil,
			)
			if err != nil {
				return nil, err
			}

			// Then set response for caller with backlinks so UI can render these.
			resp = wfRunningResponse(mutableState)
			namespaceName := mutableState.GetNamespaceEntry().Name().String()
			resp.Response.Links = makeEventLink(
				namespaceName, resp.Response.GetExecution(), optionsEvent,
			)

			// Then we return from the closure with Noop=false so the attached callbacks and links
			// get persisted.
			return &api.UpdateWorkflowAction{Noop: false, CreateWorkflowTask: false}, nil
		},
		nil,
		shardCtx,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func wfRunningResponse(mutableState historyi.MutableState) *historyservice.GetWorkflowExecutionResultResponse {
	executionState := mutableState.GetExecutionState()
	executionInfo := mutableState.GetExecutionInfo()
	return &historyservice.GetWorkflowExecutionResultResponse{
		Response: &workflowservice.GetWorkflowExecutionResultResponse{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: executionInfo.GetWorkflowId(),
				RunId:      executionState.GetRunId(),
			},
			CompletionStatus: &workflowservice.GetWorkflowExecutionResultResponse_NotCompleted_{
				NotCompleted: &workflowservice.GetWorkflowExecutionResultResponse_NotCompleted{},
			},
		},
	}
}

func wfCompletedResponse(
	ctx context.Context, mutableState historyi.MutableState,
) (*historyservice.GetWorkflowExecutionResultResponse, error) {
	completionEvent, err := mutableState.GetCompletionEvent(ctx)
	if err != nil {
		return nil, err
	}

	executionState := mutableState.GetExecutionState()
	executionInfo := mutableState.GetExecutionInfo()
	currentExecution := &commonpb.WorkflowExecution{
		WorkflowId: executionInfo.GetWorkflowId(),
		RunId:      executionState.GetRunId(),
	}
	namespaceName := mutableState.GetNamespaceEntry().Name().String()
	resp := &historyservice.GetWorkflowExecutionResultResponse{
		Response: &workflowservice.GetWorkflowExecutionResultResponse{
			Execution: currentExecution,
			Links:     makeEventLink(namespaceName, currentExecution, completionEvent),
		},
	}
	switch completionEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		resp.Response.CompletionStatus = &workflowservice.GetWorkflowExecutionResultResponse_Success_{
			Success: &workflowservice.GetWorkflowExecutionResultResponse_Success{
				Result: completionEvent.GetWorkflowExecutionCompletedEventAttributes().GetResult(),
			},
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		resp.Response.CompletionStatus = &workflowservice.GetWorkflowExecutionResultResponse_Failure_{
			Failure: &workflowservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: completionEvent.GetWorkflowExecutionFailedEventAttributes().GetFailure(),
			},
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		resp.Response.CompletionStatus = &workflowservice.GetWorkflowExecutionResultResponse_Failure_{
			Failure: &workflowservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow execution was canceled",
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{
							Details: completionEvent.GetWorkflowExecutionCanceledEventAttributes().GetDetails(),
						},
					},
				},
			},
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		resp.Response.CompletionStatus = &workflowservice.GetWorkflowExecutionResultResponse_Failure_{
			Failure: &workflowservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow execution was terminated",
					FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
						TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{
							Identity: completionEvent.GetWorkflowExecutionTerminatedEventAttributes().GetIdentity(),
						},
					},
				},
			},
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		resp.Response.CompletionStatus = &workflowservice.GetWorkflowExecutionResultResponse_Failure_{
			Failure: &workflowservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow execution timed out",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{},
					},
				},
			},
		}
	default:
		// EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW is not listed because this function is
		// only reached via the current execution pointer, which is atomically updated to the new
		// run during CAN, so we should never enter that state here.
		return nil, serviceerror.NewInternal(
			fmt.Sprintf("unexpected completion event type %v", completionEvent.GetEventType()),
		)
	}
	return resp, nil
}

func makeEventLink(
	namespaceName string, execution *commonpb.WorkflowExecution, event *historypb.HistoryEvent,
) []*commonpb.Link {
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
