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

// OperationInvocationTaskHandlerOptions is the fx parameter object for the invocation task executor.
type OperationInvocationTaskHandlerOptions struct {
	fx.In

	Config                 *Config
	NamespaceRegistry      namespace.Registry
	MetricsHandler         metrics.Handler
	Logger                 log.Logger
	CallbackTokenGenerator *commonnexus.CallbackTokenGenerator
	ClientProvider         ClientProvider
	EndpointRegistry       commonnexus.EndpointRegistry
	HTTPTraceProvider      commonnexus.HTTPClientTraceProvider
	HistoryClient          resource.HistoryClient
	ChasmRegistry          *chasm.Registry
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
	task *nexusoperationpb.InvocationTask,
) (bool, error) {
	if op.Status != nexusoperationpb.OPERATION_STATUS_SCHEDULED {
		return false, serviceerror.NewFailedPreconditionf("operation is not in scheduled state for invocation. Current state: %v", op.Status)
	}
	if op.GetAttempt() != task.GetAttempt() {
		return false, serviceerror.NewFailedPreconditionf("task attempt %d does not match operation attempt %d", task.GetAttempt(), op.GetAttempt())
	}
	return true, nil
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

	args, err := chasm.ReadComponent(ctx, opRef, (*Operation).loadStartArgs, nil)
	if err != nil {
		return err
	}

	var endpoint *persistencespb.NexusEndpointEntry

	// Skip endpoint lookup for system-internal operations.
	if args.endpointName != commonnexus.SystemEndpoint {
		// This happens when we accept the ScheduleNexusOperation command when the endpoint is not found in the
		// registry as indicated by the EndpointNotFoundAlwaysNonRetryable dynamic config.
		// The config has been removed but we keep this check for backward compatibility.
		if args.endpointID == "" {
			handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			return h.completeInvocation(ctx, opRef, ns, classifyStartOperationError(handlerError))
		}

		endpoint, err = lookupEndpoint(ctx, h.endpointRegistry, ns.ID(), args.endpointID, args.endpointName)
		if err != nil {
			if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
				// The endpoint is not registered, immediately fail the invocation.
				handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
				return h.completeInvocation(ctx, opRef, ns, classifyStartOperationError(handlerError))
			}
			return err
		}
	}

	callbackURL, err := buildCallbackURL(h.config.UseSystemCallbackURL(), h.config.CallbackURLTemplate(), ns, endpoint)
	if err != nil {
		return fmt.Errorf("failed to build callback URL: %w", err)
	}

	token, err := h.generateCallbackToken(args.serializedRef, args.requestID)
	if err != nil {
		return err
	}

	elapsed := args.currentTime.Sub(args.scheduledTime)
	callTimeout := h.config.RequestTimeout(ns.Name().String(), attrs.Destination)
	var timeoutType enumspb.TimeoutType
	// Adjust timeout based on remaining operation timeouts.
	// ScheduleToStart takes precedence over ScheduleToClose since it is already capped by it.
	if args.scheduleToStartTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToStartTimeout-elapsed)
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
	} else if args.scheduleToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToCloseTimeout-elapsed)
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	}

	// Inform the handler of the operation timeout via header.
	// StartToClose takes precedence over ScheduleToClose since it is already capped by it.
	opTimeout := maxDuration
	if args.startToCloseTimeout > 0 {
		opTimeout = args.startToCloseTimeout
	}
	if args.scheduleToCloseTimeout > 0 {
		opTimeout = min(args.scheduleToCloseTimeout-elapsed, opTimeout)
	}
	header := nexus.Header(args.header)
	if header == nil {
		header = make(nexus.Header, 1) // It's most likely that we'll only be setting the new wire format header.
	}
	// Set the operation timeout header if not already set.
	if opTimeoutHeader := header.Get(nexus.HeaderOperationTimeout); opTimeout != maxDuration && opTimeoutHeader == "" {
		header[nexus.HeaderOperationTimeout] = commonnexus.FormatDuration(opTimeout)
	}
	// If this request is handled by a newer server that supports Nexus failure serialization, trigger that behavior.
	if h.config.UseNewFailureWireFormat(ns.Name().String()) {
		header.Set(nexusrpc.HeaderTemporalNexusFailureSupport, "true")
	}

	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	// Set this value on the parent context so that our custom HTTP caller can mutate it since we cannot
	// access response headers directly.
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
	startTime := args.currentTime
	if callTimeout < h.config.MinRequestTimeout(ns.Name().String()) {
		callErr = &operationTimeoutBelowMinError{timeoutType: timeoutType}
	} else if args.endpointName == commonnexus.SystemEndpoint {
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
				tag.AttemptStart(args.currentTime.UTC()),
				tag.Attempt(task.GetAttempt()),
			)
			if trace := h.httpTraceProvider.NewTrace(task.GetAttempt(), traceLogger); trace != nil {
				callCtx = httptrace.WithClientTrace(callCtx, trace)
			}
		}
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
		_, isTimeoutBelowMin := errors.AsType[*operationTimeoutBelowMinError](callErr)
		if failureSource == commonnexus.FailureSourceWorker || isTimeoutBelowMin {
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
	return op.OnTimedOut(ctx, &failurepb.Failure{
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
	return op.OnTimedOut(ctx, &failurepb.Failure{
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
	return op.OnTimedOut(ctx, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			},
		},
	})
}
