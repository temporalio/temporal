package workflow

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/applicationservice/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	workflowpb "go.temporal.io/server/chasm/lib/workflow/gen/workflowpb/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/searchattribute"
)

var (
	ErrSignalWithStartOperationDisabled            = serviceerror.NewUnimplemented("SignalWithStart operation is disabled")
	ErrGetWorkflowExecutionResultOperationDisabled = serviceerror.NewUnimplemented("GetWorkflowExecutionResult operation is disabled")
)

type nexusNamespaceIDCtxKey struct{}

// WithNexusNamespaceID returns a context carrying the namespace ID of the Nexus operation. The
// history handler sets this before dispatching to a Nexus operation handler, because the operation
// request no longer carries the namespace itself.
func WithNexusNamespaceID(ctx context.Context, namespaceID string) context.Context {
	return context.WithValue(ctx, nexusNamespaceIDCtxKey{}, namespaceID)
}

// nexusNamespaceIDFromContext returns the namespace ID set by [WithNexusNamespaceID], or "" if none.
func nexusNamespaceIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(nexusNamespaceIDCtxKey{}).(string)
	return id
}

type workflowServiceNexusHandler struct {
	config            Config
	namespaceRegistry namespace.Registry
	historyHandler    historyservice.HistoryServiceServer
}

// signalWithStartHandler implements the SignalWithStartWorkflowExecution Nexus operation as a
// sync op: Start does the work inline and returns a synchronous result.
type signalWithStartHandler struct {
	nexus.UnimplementedOperation[
		*workflowservice.SignalWithStartWorkflowExecutionRequest,
		*workflowservice.SignalWithStartWorkflowExecutionResponse,
	]
	h *workflowServiceNexusHandler
}

func (*signalWithStartHandler) Name() string {
	return workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution.Name()
}

func (s *signalWithStartHandler) Start(
	ctx context.Context,
	req *workflowservice.SignalWithStartWorkflowExecutionRequest,
	_ nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[*workflowservice.SignalWithStartWorkflowExecutionResponse], error) {
	if !s.h.config.enableSignalWithStartFromWorkflow(req.GetNamespace()) {
		return nil, ErrSignalWithStartOperationDisabled
	}
	nsID, err := s.h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid namespace %q: %v", req.GetNamespace(), err)
	}
	res, err := s.h.historyHandler.SignalWithStartWorkflowExecution(ctx, &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId:            nsID.String(),
		SignalWithStartRequest: req,
	})
	if err != nil {
		return nil, err
	}

	// Persist the link from the signaling workflow to its target workflow.
	// The backlink is already taken care of within the historyHandler.
	signalLink := res.GetSignalLink()
	link := commonnexus.ConvertLinkWorkflowEventToNexusLink(signalLink.GetWorkflowEvent())
	nexus.AddHandlerLinks(ctx, link)

	return &nexus.HandlerStartOperationResultSync[*workflowservice.SignalWithStartWorkflowExecutionResponse]{
		Value: &workflowservice.SignalWithStartWorkflowExecutionResponse{
			RunId:      res.GetRunId(),
			Started:    res.GetStarted(),
			SignalLink: signalLink,
		},
	}, nil
}

type getWorkflowExecutionResultHandler struct {
	nexus.UnimplementedOperation[
		*applicationservice.GetWorkflowExecutionResultRequest,
		*applicationservice.GetWorkflowExecutionResultResponse,
	]
	h *workflowServiceNexusHandler
}

// workflowResultFromCloseEvent maps a workflow's close (completion) event to the synchronous
// GetWorkflowExecutionResult response.
func workflowResultFromCloseEvent(
	closeEvent *historypb.HistoryEvent,
	req *applicationservice.GetWorkflowExecutionResultRequest,
) (*applicationservice.GetWorkflowExecutionResultResponse, error) {
	switch closeEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attrs := closeEvent.GetWorkflowExecutionCompletedEventAttributes()
		payloads := attrs.GetResult().GetPayloads()
		var result *commonpb.Payload
		if len(payloads) > 0 {
			result = payloads[0]
		}
		return &applicationservice.GetWorkflowExecutionResultResponse{
			CompletionStatus: &applicationservice.GetWorkflowExecutionResultResponse_Result{
				Result: result,
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := closeEvent.GetWorkflowExecutionFailedEventAttributes()
		return &applicationservice.GetWorkflowExecutionResultResponse{
			CompletionStatus: &applicationservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: attrs.GetFailure(),
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return &applicationservice.GetWorkflowExecutionResultResponse{
			CompletionStatus: &applicationservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "operation exceeded internal timeout",
					FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
						TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{},
					},
				},
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return &applicationservice.GetWorkflowExecutionResultResponse{
			CompletionStatus: &applicationservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "operation canceled",
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{
							Details: closeEvent.GetWorkflowExecutionCanceledEventAttributes().GetDetails(),
						},
					},
				},
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return &applicationservice.GetWorkflowExecutionResultResponse{
			CompletionStatus: &applicationservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "operation terminated",
					FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
						TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{},
					},
				},
			},
		}, nil
	default:
		// CONTINUED_AS_NEW is intentionally not handled: like GetNexusCompletion, this operation
		// resolves against the latest run, so a run that continued-as-new is not a terminal result.
		return nil, serviceerror.NewInternalf("unexpected close event type %v for workflow %v", closeEvent.GetEventType(), req.GetExecution())
	}
}

func (*getWorkflowExecutionResultHandler) Name() string {
	return workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.GetWorkflowExecutionResult.Name()
}

func (w *getWorkflowExecutionResultHandler) Start(
	ctx context.Context,
	req *applicationservice.GetWorkflowExecutionResultRequest,
	opts nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[*applicationservice.GetWorkflowExecutionResultResponse], error) {
	// The request no longer carries the namespace; the history handler sets the namespace ID on the
	// context before dispatching (see WithNexusNamespaceID).
	nsID := nexusNamespaceIDFromContext(ctx)
	if nsID == "" {
		return nil, serviceerror.NewInvalidArgument("namespace ID is not set on the operation context")
	}
	nsName, err := w.h.namespaceRegistry.GetNamespaceName(namespace.ID(nsID))
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid namespace ID %q: %v", nsID, err)
	}

	if !w.h.config.enableGetWorkflowExecutionResult(nsName.String()) {
		return nil, ErrGetWorkflowExecutionResultOperationDisabled
	}

	// GetWorkflowExecutionResult always targets the most recent run for the workflow ID, so the
	// RunID on the request is intentionally ignored: leaving ExecutionKey.RunID empty resolves the
	// ComponentRef to the current (latest) run.
	workflowBRef := chasm.NewComponentRef[*Workflow](chasm.ExecutionKey{
		NamespaceID: nsID,
		BusinessID:  req.GetExecution().GetWorkflowId(),
	})

	// The inbound Nexus links identify the caller workflow(s) that scheduled this
	// operation (set as nexusLinks on the outbound request — see nexusoperation.operation).
	// Persisting them on the completion callback lets the target surface, via
	// DescribeWorkflowExecution, which workflows are awaiting its result — enabling
	// target -> caller navigation in tooling/UI. Note: req.GetExecution() is the TARGET
	// being awaited, so it must NOT be used here.
	callerLinks := make([]*commonpb.Link, 0, len(opts.Links))
	for _, l := range opts.Links {
		we, convErr := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(l)
		if convErr != nil {
			// Skip links that aren't workflow-event links; they don't identify a caller workflow.
			continue
		}
		callerLinks = append(callerLinks, &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{WorkflowEvent: we},
		})
	}

	result, _, err := chasm.UpdateComponent(ctx, workflowBRef,
		func(wf *Workflow, mutableCtx chasm.MutableContext, _ any) (nexus.HandlerStartOperationResult[*applicationservice.GetWorkflowExecutionResultResponse], error) {
			completionEvent, evErr := wf.GetCompletionEvent(mutableCtx)
			if evErr != nil {
				// No completion event yet => the workflow is still running: register the
				// completion callback and complete the Nexus operation asynchronously.
				completionCallbacks := []*commonpb.Callback{
					{
						Variant: &commonpb.Callback_Nexus_{
							Nexus: &commonpb.Callback_Nexus{
								Url: opts.CallbackURL,
								Header: map[string]string{
									commonnexus.CallbackTokenHeader: opts.CallbackHeader[commonnexus.CallbackTokenHeader],
								},
							},
						},
						Links: callerLinks,
					},
				}

				// Attach the callback by emitting a WorkflowExecutionOptionsUpdated event. Applying
				// this event both attaches the callback to the CHASM tree for the current run and
				// records it in the target's history, so it is durable across workflow reset and
				// replication: those paths reapply the event through
				// ApplyWorkflowExecutionOptionsUpdatedEvent (the same apply invoked here), which
				// reattaches the callback on the new/standby run (see mutable_state_rebuilder.go and
				// workflow_resetter.go handling of EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED).
				// The RequestID is recorded as AttachedRequestId so the resetter can dedupe the
				// reapplied callback.
				if _, cbErr := wf.AttachCompletionCallbacks(
					opts.RequestID,
					completionCallbacks,
					callerLinks,
				); cbErr != nil {
					return nil, cbErr
				}
				token, tokenErr := commonnexus.GenerateOperationToken(&workflowpb.GetWorkflowExecutionResultToken{
					RequestId: opts.RequestID,
				})
				if tokenErr != nil {
					return nil, tokenErr
				}
				return &nexus.HandlerStartOperationResultAsync{OperationToken: token}, nil
			}
			// The workflow has already completed: return its result synchronously.
			res, resErr := workflowResultFromCloseEvent(completionEvent, req)
			if resErr != nil {
				return nil, resErr
			}
			return &nexus.HandlerStartOperationResultSync[*applicationservice.GetWorkflowExecutionResultResponse]{
				Value: res,
			}, nil
		}, nil,
	)
	return result, err
}

func mustNewWorkflowServiceNexusHandler(
	handler *workflowServiceNexusHandler,
) []*nexus.Service {
	svc := nexus.NewService(workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName)
	svc.MustRegister(&signalWithStartHandler{h: handler})
	appSvc := nexus.NewService(workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.ServiceName)
	appSvc.MustRegister(&getWorkflowExecutionResultHandler{h: handler})
	return []*nexus.Service{svc, appSvc}
}

func (h *workflowServiceNexusHandler) setHistoryHandler(handler historyservice.HistoryServiceServer) {
	h.historyHandler = handler
}

type GetWorkflowExecutionResultProcessor struct {
	validator *RequestValidator
}

func (o GetWorkflowExecutionResultProcessor) ProcessInput(
	ctx chasm.NexusOperationProcessorContext,
	request *applicationservice.GetWorkflowExecutionResultRequest,
) (*chasm.NexusOperationProcessorResult, error) {
	if err := o.validator.ValidateGetWorkflowExecutionResultRequest(request); err != nil {
		return nil, err
	}
	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  request.GetExecution().GetWorkflowId(),
		},
	}, nil
}

type SignalWithStartOperationProcessor struct {
	validator *RequestValidator
}

func (o SignalWithStartOperationProcessor) ProcessInput(
	ctx chasm.NexusOperationProcessorContext,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (*chasm.NexusOperationProcessorResult, error) {
	if !o.validator.config.enableSignalWithStartFromWorkflow(ctx.Namespace.Name().String()) {
		return nil, ErrSignalWithStartOperationDisabled
	}
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("Request is empty")
	}
	if request.GetNamespace() == "" {
		request.Namespace = ctx.Namespace.Name().String()
	} else if request.GetNamespace() != ctx.Namespace.Name().String() {
		return nil, serviceerror.NewInvalidArgumentf("Namespace in request %q does not match namespace in context %q", request.GetNamespace(), ctx.Namespace.Name().String())
	}

	if request.GetRequestId() != "" {
		return nil, serviceerror.NewInvalidArgument("RequestID should not be set on the request")
	}
	request.RequestId = ctx.RequestID

	if len(request.GetLinks()) > 0 {
		return nil, serviceerror.NewInvalidArgument("Links should not be set on the request")
	}
	request.Links = make([]*commonpb.Link, len(ctx.Links))
	for i, link := range ctx.Links {
		wLink, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(link)
		if err != nil {
			return nil, serviceerror.NewInvalidArgumentf("Cannot convert %v link %v: %v", link.Type, link.URL, err)
		}
		request.Links[i] = &commonpb.Link{
			Variant: &commonpb.Link_WorkflowEvent_{
				WorkflowEvent: wLink,
			},
		}
	}

	if err := o.validator.ValidateSignalWithStartRequest(request); err != nil {
		return nil, err
	}

	return &chasm.NexusOperationProcessorResult{
		RoutingKey: chasm.NexusOperationRoutingKeyExecution{
			NamespaceID: ctx.Namespace.ID().String(),
			BusinessID:  request.WorkflowId,
		},
	}, nil
}

func NewWorkflowServiceNexusServiceProcessor(
	config Config,
	saMapperProvider searchattribute.MapperProvider,
	saValidator *searchattribute.Validator,
) []*chasm.NexusServiceProcessor {
	validator := NewValidator(config, saMapperProvider, saValidator)

	workflowSp := chasm.NewNexusServiceProcessor(workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.ServiceName)
	workflowSp.MustRegisterOperation(
		workflowservicenexus.TemporalAPIWorkflowserviceV1WorkflowService.SignalWithStartWorkflowExecution.Name(),
		chasm.NewRegisterableNexusOperationProcessor(SignalWithStartOperationProcessor{validator: validator}),
	)

	applicationSp := chasm.NewNexusServiceProcessor(workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.ServiceName)
	applicationSp.MustRegisterOperation(
		workflowservicenexus.TemporalAPIApplicationserviceV1ApplicationService.GetWorkflowExecutionResult.Name(),
		chasm.NewRegisterableNexusOperationProcessor(GetWorkflowExecutionResultProcessor{validator: validator}),
	)
	return []*chasm.NexusServiceProcessor{workflowSp, applicationSp}
}
