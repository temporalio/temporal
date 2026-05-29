package workflow

import (
	"context"
	"errors"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/api/applicationservice/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/consts"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrSignalWithStartOperationDisabled            = serviceerror.NewUnimplemented("SignalWithStart operation is disabled")
	ErrGetWorkflowExecutionResultOperationDisabled = serviceerror.NewUnimplemented("GetWorkflowExecutionResult operation is disabled")
)

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

func (w *getWorkflowExecutionResultHandler) getTerminalState(
	ctx context.Context,
	namespaceID string,
	req *applicationservice.GetWorkflowExecutionResultRequest,
) (*applicationservice.GetWorkflowExecutionResultResponse, error) {
	res, err := w.h.historyHandler.GetWorkflowExecutionHistory(ctx, &historyservice.GetWorkflowExecutionHistoryRequest{
		NamespaceId: namespaceID,
		Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace:              req.GetNamespace(),
			Execution:              req.GetExecution(),
			HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
			MaximumPageSize:        1000,
			SkipArchival:           true,
		},
	})
	if err != nil {
		return nil, err
	}
	// When SendRawHistoryBetweenInternalServices is enabled (default in tests), the history events
	// arrive in res.History as raw serialized historypb.History proto bytes rather than decoded in
	// res.GetResponse().GetHistory(). Decode them if the normal events field is empty.
	events := res.GetResponse().GetHistory().GetEvents()
	if len(events) == 0 && len(res.History) > 0 {
		for _, rawBlob := range res.History {
			h := &historypb.History{}
			if unmarshalErr := h.Unmarshal(rawBlob); unmarshalErr != nil {
				return nil, serviceerror.NewInternalf("failed to unmarshal raw history for workflow %v: %v", req.GetExecution(), unmarshalErr)
			}
			events = append(events, h.Events...)
		}
	}
	if len(events) == 0 {
		return nil, serviceerror.NewInternalf("no close event found for completed workflow %v", req.GetExecution())
	}
	closeEvent := events[len(events)-1]
	switch closeEvent.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attrs := closeEvent.GetWorkflowExecutionCompletedEventAttributes()
		payloads := attrs.GetResult().GetPayloads()
		var result *commonpb.Payload
		if len(payloads) > 0 {
			result = payloads[0]
		}
		return &applicationservice.GetWorkflowExecutionResultResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CompletionStatus: &applicationservice.GetWorkflowExecutionResultResponse_Result{
				Result: result,
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := closeEvent.GetWorkflowExecutionFailedEventAttributes()
		return &applicationservice.GetWorkflowExecutionResultResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			CompletionStatus: &applicationservice.GetWorkflowExecutionResultResponse_Failure{
				Failure: attrs.GetFailure(),
			},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return &applicationservice.GetWorkflowExecutionResultResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return &applicationservice.GetWorkflowExecutionResultResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return &applicationservice.GetWorkflowExecutionResultResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
		return &applicationservice.GetWorkflowExecutionResultResponse{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
		}, nil
	default:
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
	if !w.h.config.enableGetWorkflowExecutionResult(req.GetNamespace()) {
		return nil, ErrGetWorkflowExecutionResultOperationDisabled
	}

	nsID, err := w.h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("Invalid namespace %q: %v", req.GetNamespace(), err)
	}

	workflowBRef := chasm.NewComponentRef[*Workflow](chasm.ExecutionKey{
		NamespaceID: nsID.String(),
		BusinessID:  req.GetExecution().GetWorkflowId(),
		RunID:       req.GetExecution().GetRunId(),
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

	_, _, err = chasm.UpdateComponent(ctx, workflowBRef,
		func(wf *Workflow, mutableCtx chasm.MutableContext, _ any) (any, error) {
			if !wf.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}
			return nil, wf.AddCompletionCallbacks(
				mutableCtx,
				timestamppb.Now(),
				opts.RequestID,
				[]*commonpb.Callback{
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
				},
				w.h.config.maxCallbacksPerWorkflow(req.GetNamespace()),
			)
		}, nil,
	)
	if err != nil {
		// Check for any NotFound to trigger the synchronous getTerminalState path.
		if errors.As(err, new(*serviceerror.NotFound)) {
			result, err := w.getTerminalState(ctx, nsID.String(), req)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultSync[*applicationservice.GetWorkflowExecutionResultResponse]{
				Value: result,
			}, nil
		}
		return nil, err
	}
	return &nexus.HandlerStartOperationResultAsync{OperationToken: opts.RequestID}, nil
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
	if request.GetNamespace() == "" {
		request.Namespace = ctx.Namespace.Name().String()
	} else if request.GetNamespace() != ctx.Namespace.Name().String() {
		return nil, serviceerror.NewInvalidArgumentf("Namespace in request %q does not match namespace in context %q", request.GetNamespace(), ctx.Namespace.Name().String())
	}

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
