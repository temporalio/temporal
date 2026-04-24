package workflow

import (
	"context"
	"errors"
	"fmt"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservice/v1/workflowservicenexus"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/consts"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type workflowServiceNexusHandler struct {
	namespaceRegistry       namespace.Registry
	historyHandler          historyservice.HistoryServiceServer
	maxCallbacksPerWorkflow dynamicconfig.IntPropertyFnWithNamespaceFilter
}

func (w *workflowServiceNexusHandler) getTerminalState(
	ctx context.Context,
	nsID string,
	req *workflowservice.WaitExternalWorkflowRequest,
) (*workflowservice.WaitExternalWorkflowResult, error) {
	// Implementation for fetching terminal state
	return nil, nil
}

// SignalWithStartWorkflowExecution implements the SignalWithStartWorkflowExecution Nexus operation.
func (h *workflowServiceNexusHandler) SignalWithStartWorkflowExecution(name string) nexus.Operation[*workflowservice.SignalWithStartWorkflowExecutionRequest, *workflowservice.SignalWithStartWorkflowExecutionResponse] {
	return nexus.NewSyncOperation(name, func(ctx context.Context, req *workflowservice.SignalWithStartWorkflowExecutionRequest, options nexus.StartOperationOptions) (*workflowservice.SignalWithStartWorkflowExecutionResponse, error) {
		nsID, err := h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
		if err != nil {
			return nil, serviceerror.NewInvalidArgumentf("Invalid namespace %q: %v", req.GetNamespace(), err)
		}

		res, err := h.historyHandler.SignalWithStartWorkflowExecution(ctx, &historyservice.SignalWithStartWorkflowExecutionRequest{
			NamespaceId:            nsID.String(),
			SignalWithStartRequest: req,
		})
		if err != nil {
			return nil, err
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

		return &workflowservice.SignalWithStartWorkflowExecutionResponse{
			RunId:   res.GetRunId(),
			Started: res.GetStarted(),
		}, nil
	})
}

type WaitForExternalWorkflowOperationProcessor struct{}

func (o WaitForExternalWorkflowOperationProcessor) ProcessInput(
	ctx chasm.NexusOperationProcessorContext,
	request *workflowservice.WaitExternalWorkflowRequest,
) (
	*chasm.NexusOperationProcessorResult,
	error,
) {
	return nil, fmt.Errorf("not implemented")
}

type waitForExternalWorkflowOperation struct {
	nexus.UnimplementedOperation[
		workflowservice.WaitExternalWorkflowRequest,
		workflowservice.WaitExternalWorkflowResult,
	]
	h *workflowServiceNexusHandler
}

func (o *waitForExternalWorkflowOperation) Name() string {
	return workflowservicenexus.WorkflowService.WaitExternalWorkflow.Name()
}
func (o *waitForExternalWorkflowOperation) Start(
	ctx context.Context,
	req *workflowservice.WaitExternalWorkflowRequest,
	opts nexus.StartOperationOptions,
) (nexus.HandlerStartOperationResult[*workflowservice.WaitExternalWorkflowResult], error) {
	nsID, err := o.h.namespaceRegistry.GetNamespaceID(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, serviceerror.NewInvalidArgumentf("invalid namespace %q: %v", req.GetNamespace(), err)
	}

	workflowBRef := chasm.NewComponentRef[*chasmworkflow.Workflow](chasm.ExecutionKey{
		NamespaceID: nsID.String(),
		BusinessID:  req.GetWorkflowId(),
		RunID:       req.GetRunId(),
	})

	_, _, err = chasm.UpdateComponent(ctx, workflowBRef,
		func(w *chasmworkflow.Workflow, mutableCtx chasm.MutableContext, _ any) (any, error) {
			if w.LifecycleState(mutableCtx) != chasm.LifecycleStateRunning {
				return nil, consts.ErrWorkflowCompleted
			}
			return nil, w.AddCompletionCallbacks(
				mutableCtx,
				timestamppb.Now(),
				req.GetRequestId(),
				[]*commonpb.Callback{{Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{Url: opts.CallbackURL},
				}}},
				o.h.maxCallbacksPerWorkflow(req.GetNamespace()),
			)
		}, nil,
	)
	if err != nil {
		if errors.Is(err, consts.ErrWorkflowCompleted) {
			result, err := o.h.getTerminalState(ctx, nsID.String(), req)
			if err != nil {
				return nil, err
			}
			return &nexus.HandlerStartOperationResultSync[*workflowservice.WaitExternalWorkflowResult]{
				Value: result,
			}, nil
		}
		return nil, err
	}

	return &nexus.HandlerStartOperationResultAsync{
		OperationToken: req.GetWorkflowId() + ":" + req.GetRunId() + ":" + req.GetRequestId(),
	}, nil
}

func (o *waitForExternalWorkflowOperation) Cancel(
	ctx context.Context,
	token string,
	opts nexus.CancelOperationOptions,
) error {
	return nil
}

func mustNewWorkflowServiceNexusHandler(
	handler *workflowServiceNexusHandler,
) *nexus.Service {
	svc := nexus.NewService(workflowservicenexus.WorkflowService.ServiceName)
	svc.MustRegister(
		handler.SignalWithStartWorkflowExecution(workflowservicenexus.WorkflowService.SignalWithStartWorkflowExecution.Name()),
		&waitForExternalWorkflowOperation{h: handler},
	)
	return svc
}

func (h *workflowServiceNexusHandler) setHistoryHandler(handler historyservice.HistoryServiceServer) {
	h.historyHandler = handler
}

type SignalWithStartOperationProcessor struct {
	validator *Validator
}

func (o SignalWithStartOperationProcessor) ProcessInput(ctx chasm.NexusOperationProcessorContext, request *workflowservice.SignalWithStartWorkflowExecutionRequest) (*chasm.NexusOperationProcessorResult, error) {
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
	sp := chasm.NewNexusServiceProcessor(workflowservicenexus.WorkflowService.ServiceName)
	sp.MustRegisterOperation(
		workflowservicenexus.WorkflowService.SignalWithStartWorkflowExecution.Name(),
		chasm.NewRegisterableNexusOperationProcessor(SignalWithStartOperationProcessor{
			validator: NewValidator(config, saMapperProvider, saValidator),
		},
		),
	)
	sp.MustRegisterOperation(
		workflowservicenexus.WorkflowService.WaitExternalWorkflow.Name(),
		chasm.NewRegisterableNexusOperationProcessor(
			WaitForExternalWorkflowOperationProcessor{},
		),
	)
	return sp
}
