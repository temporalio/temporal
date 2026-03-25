package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/resource"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
)

// OperationTaskHandlerOptions is the fx parameter object for common options supplied to all operation task handlers.
type OperationTaskHandlerOptions struct {
	fx.In

	Config *Config

	MetricsHandler metrics.Handler
	Logger         log.Logger
}

type OperationInvocationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nexusoperationpb.InvocationTask]

	config                 *Config
	namespaceRegistry      namespace.Registry
	metricsHandler         metrics.Handler
	logger                 log.Logger
	callbackTokenGenerator *commonnexus.CallbackTokenGenerator
	clientProvider         ClientProvider
	endpointRegistry       commonnexus.EndpointRegistry
	httpTraceProvider      commonnexus.HTTPClientTraceProvider
	historyClient          resource.HistoryClient
	chasmRegistry          *chasm.Registry
}

func NewOperationInvocationTaskHandler(opts OperationInvocationTaskHandlerOptions) *OperationInvocationTaskHandler {
	return &OperationInvocationTaskHandler{
		config:                 opts.Config,
		namespaceRegistry:      opts.NamespaceRegistry,
		metricsHandler:         opts.MetricsHandler,
		logger:                 opts.Logger,
		callbackTokenGenerator: opts.CallbackTokenGenerator,
		clientProvider:         opts.ClientProvider,
		endpointRegistry:       opts.EndpointRegistry,
		httpTraceProvider:      opts.HTTPTraceProvider,
		historyClient:          opts.HistoryClient,
		chasmRegistry:          opts.ChasmRegistry,
	}
}

func (h *OperationInvocationTaskHandler) Validate(
	_ chasm.Context,
	op *Operation,
	_ chasm.TaskAttributes,
	_ *nexusoperationpb.InvocationTask,
) (bool, error) {
	return isValidForInvocation(op), nil
}

func (h *OperationInvocationTaskHandler) Execute(
	ctx context.Context,
	opRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTask,
) error {
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(opRef.NamespaceID))
	if err != nil {
		return serviceerror.NewNotFoundf("failed to get namespace by ID: %v", err)
	}

	args, err := chasm.ReadComponent(ctx, opRef, (*Operation).loadInvocationArgs, nil)
	if err != nil {
		return err
	}

	var endpoint *persistencespb.NexusEndpointEntry

	// Skip endpoint lookup for system-internal operations.
	if args.endpointName != commonnexus.SystemEndpoint {
		if args.endpointID == "" {
			handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			return h.completeInvocation(ctx, opRef, ns, classifyStartOperationError(handlerError))
		}

		endpoint, err = lookupEndpoint(ctx, h.endpointRegistry, ns.ID(), args.endpointID, args.endpointName)
		if err != nil {
			if errors.As(err, new(*serviceerror.NotFound)) {
				handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
				return h.completeInvocation(ctx, opRef, ns, classifyStartOperationError(handlerError))
			}
			return err
		}
	}

	callbackURL, err := buildCallbackURL(h.config.UseSystemCallbackURL(), h.config.CallbackURLTemplate(), ns, endpoint)
	if err != nil {
		return serviceerror.NewFailedPreconditionf("failed to build callback URL: %v", err)
	}

	token, err := h.generateCallbackToken(opRef, args.requestID)
	if err != nil {
		return err
	}

	callTimeout := h.config.RequestTimeout(ns.Name().String(), attrs.Destination)
	var timeoutType enumspb.TimeoutType
	if args.scheduleToStartTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToStartTimeout-time.Since(args.scheduledTime))
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
	} else if args.scheduleToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToCloseTimeout-time.Since(args.scheduledTime))
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	}

	opTimeout := maxDuration
	if args.startToCloseTimeout > 0 {
		opTimeout = args.startToCloseTimeout
	}
	if args.scheduleToCloseTimeout > 0 {
		opTimeout = min(args.scheduleToCloseTimeout-time.Since(args.scheduledTime), opTimeout)
	}
	header := nexus.Header(args.header)
	if header == nil {
		header = make(nexus.Header, 1)
	}
	if opTimeoutHeader := header.Get(nexus.HeaderOperationTimeout); opTimeout != maxDuration && opTimeoutHeader == "" {
		header[nexus.HeaderOperationTimeout] = commonnexus.FormatDuration(opTimeout)
	}
	if h.config.UseNewFailureWireFormat(ns.Name().String()) {
		header.Set(nexusrpc.HeaderTemporalNexusFailureSupport, "true")
	}

	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	callCtx = context.WithValue(callCtx, commonnexus.FailureSourceContextKey, &atomic.Value{})

	options := nexus.StartOperationOptions{
		Header:      header,
		CallbackURL: callbackURL,
		RequestID:   args.requestID,
		CallbackHeader: nexus.Header{
			commonnexus.CallbackTokenHeader: token,
		},
		Links: []nexus.Link{args.nexusLink},
	}

	var result *nexusrpc.ClientStartOperationResponse[*commonpb.Payload]
	var callErr error
	var startTime time.Time
	if callTimeout < h.config.MinRequestTimeout(ns.Name().String()) {
		startTime = time.Now()
		callErr = &operationTimeoutBelowMinError{timeoutType: timeoutType}
	} else if args.endpointName == commonnexus.SystemEndpoint {
		startTime = time.Now()
		result, callErr = h.startOnHistoryService(callCtx, ns, args, options)
	} else {
		client, clientErr := h.clientProvider(
			callCtx,
			ns.ID().String(),
			endpoint,
			args.service,
		)
		if clientErr != nil {
			return serviceerror.NewUnavailablef("failed to get a client: %v", clientErr)
		}

		if h.httpTraceProvider != nil {
			traceLogger := log.With(h.logger,
				tag.Operation("StartOperation"),
				tag.WorkflowNamespace(ns.Name().String()),
				tag.RequestID(args.requestID),
				tag.NexusOperation(args.operation),
				tag.Endpoint(args.endpointName),
				tag.WorkflowID(opRef.BusinessID),
				tag.WorkflowRunID(opRef.RunID),
				tag.AttemptStart(time.Now().UTC()),
				tag.Attempt(task.GetAttempt()),
			)
			if trace := h.httpTraceProvider.NewTrace(task.GetAttempt(), traceLogger); trace != nil {
				callCtx = httptrace.WithClientTrace(callCtx, trace)
			}
		}
		startTime = time.Now()
		result, callErr = h.startViaHTTP(callCtx, client, args, options)
	}

	if result != nil {
		tokenLimit := h.config.MaxOperationTokenLength(ns.Name().String())
		if result.Pending != nil && len(result.Pending.Token) > tokenLimit {
			callErr = fmt.Errorf("%w: length exceeds allowed limit (%d/%d)", ErrInvalidOperationToken, len(result.Pending.Token), tokenLimit)
		} else if result.Successful != nil && result.Successful.Size() > h.config.PayloadSizeLimit(ns.Name().String()) {
			callErr = ErrResponseBodyTooLarge
		}
	}
	failureSource := failureSourceFromContext(callCtx)

	methodTag := metrics.NexusMethodTag("StartOperation")
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	var destTag metrics.Tag
	if endpoint != nil {
		destTag = metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	} else {
		destTag = metrics.DestinationTag(args.endpointName)
	}
	outcomeTag := metrics.OutcomeTag(startCallOutcomeTag(callCtx, result, callErr))
	failureSourceTag := metrics.FailureSourceTag(failureSource)
	OutboundRequestCounter.With(h.metricsHandler).Record(1, namespaceTag, destTag, methodTag, outcomeTag, failureSourceTag)
	OutboundRequestLatency.With(h.metricsHandler).Record(time.Since(startTime), namespaceTag, destTag, methodTag, outcomeTag, failureSourceTag)

	if callErr != nil {
		if failureSource == commonnexus.FailureSourceWorker || errors.As(callErr, new(*operationTimeoutBelowMinError)) {
			h.logger.Debug("Nexus StartOperation request failed", tag.Error(callErr))
		} else {
			h.logger.Error("Nexus StartOperation request failed", tag.Error(callErr))
		}
	}

	var invocationRes invocationResult
	if callErr != nil {
		invocationRes = classifyStartOperationError(callErr)
	} else {
		invocationRes = invocationResultOK{
			response: result,
			links:    convertResponseLinks(result.Links, h.logger),
		}
	}

	saveErr := h.completeInvocation(ctx, opRef, ns, invocationRes)

	if callErr != nil && isDestinationDown(callErr) {
		saveErr = queueserrors.NewDestinationDownError(callErr.Error(), saveErr)
	}

	return saveErr
}

// completeInvocation saves the invocation result by updating the operation component.
func (h *OperationInvocationTaskHandler) completeInvocation(
	ctx context.Context,
	opRef chasm.ComponentRef,
	ns *namespace.Namespace,
	result invocationResult,
) error {
	_, _, err := chasm.UpdateComponent(
		ctx,
		opRef,
		(*Operation).saveResult,
		saveResultInput{
			result:      result,
			retryPolicy: h.config.RetryPolicy,
		},
	)
	return err
}

type OperationBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationBackoffTaskHandler(opts OperationTaskHandlerOptions) *OperationBackoffTaskHandler {
	return &OperationBackoffTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationBackoffTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) (bool, error) {
	return op.Status == nexusoperationpb.OPERATION_STATUS_BACKING_OFF && op.GetAttempt() == task.GetAttempt(), nil
}

func (h *OperationBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) error {
	return transitionRescheduled.Apply(op, ctx, EventRescheduled{})
}

type OperationScheduleToStartTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationScheduleToStartTimeoutTaskHandler(opts OperationTaskHandlerOptions) *OperationScheduleToStartTimeoutTaskHandler {
	return &OperationScheduleToStartTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationScheduleToStartTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToStartTimeoutTask,
) (bool, error) {
	return TransitionStarted.Possible(op), nil
}

func (h *OperationScheduleToStartTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToStartTimeoutTask,
) error {
	return op.OnTimedOut(ctx, op, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			},
		},
	})
}

type OperationStartToCloseTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationStartToCloseTimeoutTaskHandler(opts OperationTaskHandlerOptions) *OperationStartToCloseTimeoutTaskHandler {
	return &OperationStartToCloseTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationStartToCloseTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.StartToCloseTimeoutTask,
) (bool, error) {
	return op.Status == nexusoperationpb.OPERATION_STATUS_STARTED, nil
}

func (h *OperationStartToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.StartToCloseTimeoutTask,
) error {
	return op.OnTimedOut(ctx, op, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			},
		},
	})
}

type OperationScheduleToCloseTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewOperationScheduleToCloseTimeoutTaskHandler(opts OperationTaskHandlerOptions) *OperationScheduleToCloseTimeoutTaskHandler {
	return &OperationScheduleToCloseTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *OperationScheduleToCloseTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	return TransitionTimedOut.Possible(op), nil
}

func (h *OperationScheduleToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToCloseTimeoutTask,
) error {
	return op.OnTimedOut(ctx, op, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			},
		},
	})
}
