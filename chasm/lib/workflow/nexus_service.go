package workflow

import (
	"context"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/searchattribute"
)

// workflowServiceNexusDef holds the Nexus service and operation names for WorkflowService.
// Previously provided by go.temporal.io/api/workflowservice/v1/workflowservicenexus.
var workflowServiceNexusDef = struct {
	ServiceName                      string
	SignalWithStartWorkflowExecution nexus.OperationReference[workflowservice.SignalWithStartWorkflowExecutionRequest, workflowservice.SignalWithStartWorkflowExecutionResponse]
}{
	ServiceName:                      "WorkflowService",
	SignalWithStartWorkflowExecution: nexus.NewOperationReference[workflowservice.SignalWithStartWorkflowExecutionRequest, workflowservice.SignalWithStartWorkflowExecutionResponse]("SignalWithStartWorkflowExecution"),
}

type workflowServiceNexusHandler struct {
	namespaceRegistry namespace.Registry
	historyHandler    historyservice.HistoryServiceServer
}

// signalWithStartWorkflowExecution implements the SignalWithStartWorkflowExecution Nexus operation.
// It returns a plain Go struct (not a proto message) so that the Nexus SDK serializes the response
// as standard JSON with snake_case field names, matching what the SDK caller expects.
func (h *workflowServiceNexusHandler) signalWithStartWorkflowExecution(
	ctx context.Context,
	req *workflowservice.SignalWithStartWorkflowExecutionRequest,
	options nexus.StartOperationOptions,
) (signalWithStartWorkflowExecutionResponse, error) {
	fmt.Printf("TESTING: signalWithStartWorkflowExecution")
	nsID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return signalWithStartWorkflowExecutionResponse{}, serviceerror.NewInvalidArgumentf("Invalid namespace %q: %v", req.GetNamespace(), err)
	}
	res, err := h.historyHandler.SignalWithStartWorkflowExecution(ctx, &historyservice.SignalWithStartWorkflowExecutionRequest{
		NamespaceId:            nsID.String(),
		SignalWithStartRequest: req,
	})
	fmt.Printf("TESTING: signalWithStartWorkflowExecution result res=%v err=%v conflictPolicy=%v\n", res, err, req.GetWorkflowIdConflictPolicy())
	if err != nil {
		return signalWithStartWorkflowExecutionResponse{}, err
	}
	link := commonnexus.ConvertLinkWorkflowEventToNexusLink(&commonpb.Link_WorkflowEvent{
		Namespace:  req.GetNamespace(),
		WorkflowId: req.GetWorkflowId(),
		RunId:      res.GetRunId(),
		Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
			RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
				RequestId: req.GetRequestId(),
			},
		},
	})
	nexus.AddHandlerLinks(ctx, link)
	fmt.Printf("TESTING: signal with start response: run_id=%s started=%v\n", res.GetRunId(), res.GetStarted())
	return signalWithStartWorkflowExecutionResponse{RunID: res.GetRunId(), Started: res.GetStarted()}, nil
}

// signalWithStartWorkflowExecutionResponse is a plain Go struct mirroring
// WorkflowServiceSignalWithStartWorkflowExecutionOutput. Using a plain struct ensures
// payloads.Encode produces a json/plain payload (via JsonPayloadConverter), which the
// SDK can decode into the json output type via standard json.Unmarshal.
type signalWithStartWorkflowExecutionResponse struct {
	RunID   string `json:"run_id,omitempty"`
	Started bool   `json:"started,omitempty"`
}

func mustNewWorkflowServiceNexusHandler(
	handler *workflowServiceNexusHandler,
) *nexus.Service {
	svc := nexus.NewService(workflowServiceNexusDef.ServiceName)
	svc.MustRegister(nexus.NewSyncOperation(
		workflowServiceNexusDef.SignalWithStartWorkflowExecution.Name(),
		handler.signalWithStartWorkflowExecution,
	))
	return svc
}

func (h *workflowServiceNexusHandler) setHistoryHandler(handler historyservice.HistoryServiceServer) {
	h.historyHandler = handler
}

type SignalWithStartOperationProcessor struct {
	validator *RequestValidator
}

func (o SignalWithStartOperationProcessor) ProcessInput(ctx chasm.NexusOperationProcessorContext, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (*chasm.NexusOperationProcessorResult, error) {
	fmt.Printf("TESTING: ProcessInput")
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
) *chasm.NexusServiceProcessor {
	sp := chasm.NewNexusServiceProcessor(workflowServiceNexusDef.ServiceName)
	fmt.Printf("TESTING: signal with start name: %s\n", workflowServiceNexusDef.SignalWithStartWorkflowExecution.Name())
	op := SignalWithStartOperationProcessor{validator: NewValidator(config, saMapperProvider, saValidator)}
	sp.MustRegisterOperation(
		workflowServiceNexusDef.SignalWithStartWorkflowExecution.Name(),
		chasm.NewRegisterableNexusOperationProcessor(op),
	)
	return sp
}
