package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"maps"
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

// operationTaskHandlerOptions is the fx parameter object for common options supplied to all operation task handlers.
type operationTaskHandlerOptions struct {
	fx.In

	Config *Config

	MetricsHandler metrics.Handler
	Logger         log.Logger
}

// operationInvocationTaskHandlerOptions is the fx parameter object for the invocation task executor.
type operationInvocationTaskHandlerOptions struct {
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

type operationInvocationTaskHandler struct {
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

func newOperationInvocationTaskHandler(opts operationInvocationTaskHandlerOptions) *operationInvocationTaskHandler {
	return &operationInvocationTaskHandler{
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

func (h *operationInvocationTaskHandler) Validate(
	_ chasm.Context,
	op *Operation,
	_ chasm.TaskAttributes,
	task *nexusoperationpb.InvocationTask,
) (bool, error) {
	isValid := op.Status == nexusoperationpb.OPERATION_STATUS_SCHEDULED && op.GetAttempt() == task.GetAttempt()
	return isValid, nil
}

func (h *operationInvocationTaskHandler) Execute(
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

	endpoint, err := h.resolveEndpoint(ctx, ns, args)
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			h.logger.Error("endpoint not found while processing invocation task", tag.Error(err))
			handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			result, err := newInvocationResult(nil, handlerErr)
			if err != nil {
				return fmt.Errorf("failed to construct invocation result: %w", err)
			}
			_, _, err = chasm.UpdateComponent(ctx, opRef, (*Operation).saveInvocationResult, saveInvocationResultInput{
				result:      result,
				retryPolicy: h.config.RetryPolicy(),
			})
			return err
		}
		return err
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
	header := buildRequestHeader(args.header)
	// Set the operation timeout header if not already set.
	if opTimeoutHeader := header.Get(nexus.HeaderOperationTimeout); opTimeout != maxDuration && opTimeoutHeader == "" {
		header.Set(nexus.HeaderOperationTimeout, commonnexus.FormatDuration(opTimeout))
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
		Links: args.nexusLinks,
	}

	invocation, err := h.newInvocation(callCtx, ns, endpoint, opRef, args, task, callTimeout, timeoutType)
	if err != nil {
		return fmt.Errorf("failed to construct invocation: %w", err)
	}
	startTime := time.Now() // nolint:forbidigo // Time can be used for timing metrics.
	response, callErr := invocation.Start(callCtx, args, options)
	callDuration := time.Since(startTime)
	if validationErr := h.validateStartResult(ns, response); validationErr != nil {
		callErr = validationErr
	}
	failureSource := failureSourceFromContext(callCtx)

	h.recordStartCallOutcome(callCtx, ns, endpoint, args, response, callErr, callDuration, failureSource)

	result, err := newInvocationResult(response, callErr)
	if err != nil {
		return fmt.Errorf("failed to construct invocation result: %w", err)
	}
	_, _, saveErr := chasm.UpdateComponent(ctx, opRef, (*Operation).saveInvocationResult, saveInvocationResultInput{
		result:      result,
		retryPolicy: h.config.RetryPolicy(),
	})

	if callErr != nil && isDestinationDown(callErr) {
		saveErr = queueserrors.NewDestinationDownError(callErr.Error(), saveErr)
	}

	return saveErr
}

func buildRequestHeader(header map[string]string) nexus.Header {
	if header == nil {
		return make(nexus.Header, 2) // To set the failure support and timeout headers.
	}
	return nexus.Header(maps.Clone(header))
}

func (h *operationInvocationTaskHandler) resolveEndpoint(
	ctx context.Context,
	ns *namespace.Namespace,
	args startArgs,
) (*persistencespb.NexusEndpointEntry, error) {
	// Skip endpoint lookup for system-internal operations.
	if args.endpointName == commonnexus.SystemEndpoint {
		return nil, nil
	}
	// This happens when we accept the ScheduleNexusOperation command when the endpoint is not found in the
	// registry as indicated by the EndpointNotFoundAlwaysNonRetryable dynamic config.
	// The config has been removed but we keep this check for backward compatibility.
	if args.endpointID == "" {
		return nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
	}
	return lookupEndpoint(ctx, h.endpointRegistry, ns.ID(), args.endpointID, args.endpointName)
}

func (h *operationInvocationTaskHandler) newInvocation(
	ctx context.Context,
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
	opRef chasm.ComponentRef,
	args startArgs,
	task *nexusoperationpb.InvocationTask,
	callTimeout time.Duration,
	timeoutType enumspb.TimeoutType,
) (invocation, error) {
	base := nexusTaskHandlerBase{
		config:            h.config,
		namespaceRegistry: h.namespaceRegistry,
		metricsHandler:    h.metricsHandler,
		logger:            h.logger,
		clientProvider:    h.clientProvider,
		endpointRegistry:  h.endpointRegistry,
		httpTraceProvider: h.httpTraceProvider,
		historyClient:     h.historyClient,
		chasmRegistry:     h.chasmRegistry,
	}
	return base.newInvocation(
		ctx,
		ns,
		endpoint,
		args.endpointName,
		args.service,
		callTimeout,
		timeoutType,
		invocationTraceContext{
			operationTag:  "StartOperation",
			namespaceName: ns.Name().String(),
			requestID:     args.requestID,
			operation:     args.operation,
			endpointName:  args.endpointName,
			workflowID:    opRef.BusinessID,
			runID:         opRef.RunID,
			attemptStart:  args.currentTime.UTC(),
			attempt:       task.GetAttempt(),
		},
	)
}

func (h *operationInvocationTaskHandler) validateStartResult(
	ns *namespace.Namespace,
	result *nexusrpc.ClientStartOperationResponse[*commonpb.Payload],
) error {
	if result == nil {
		return nil
	}
	tokenLimit := h.config.MaxOperationTokenLength(ns.Name().String())
	if result.Pending != nil && len(result.Pending.Token) > tokenLimit {
		return fmt.Errorf("%w: length exceeds allowed limit (%d/%d)", ErrInvalidOperationToken, len(result.Pending.Token), tokenLimit)
	}
	if result.Successful != nil && result.Successful.Size() > h.config.PayloadSizeLimit(ns.Name().String()) {
		return ErrResponseBodyTooLarge
	}
	return nil
}

func (h *operationInvocationTaskHandler) recordStartCallOutcome(
	callCtx context.Context,
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
	args startArgs,
	response *nexusrpc.ClientStartOperationResponse[*commonpb.Payload],
	callErr error,
	callDuration time.Duration,
	failureSource string,
) {
	methodTag := metrics.NexusMethodTag("StartOperation")
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	var destTag metrics.Tag
	if endpoint != nil {
		destTag = metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	} else {
		destTag = metrics.DestinationTag(args.endpointName)
	}
	outcomeTag := metrics.OutcomeTag(startCallOutcomeTag(callCtx, response, callErr))
	failureSourceTag := metrics.FailureSourceTag(failureSource)
	OutboundRequestCounter.With(h.metricsHandler).Record(1, namespaceTag, destTag, methodTag, outcomeTag, failureSourceTag)
	OutboundRequestLatency.With(h.metricsHandler).Record(callDuration, namespaceTag, destTag, methodTag, outcomeTag, failureSourceTag)

	if callErr != nil {
		_, isTimeoutBelowMin := errors.AsType[*operationTimeoutBelowMinError](callErr)
		if failureSource == commonnexus.FailureSourceWorker || isTimeoutBelowMin {
			h.logger.Debug("Nexus StartOperation request failed", tag.Error(callErr))
		} else {
			h.logger.Error("Nexus StartOperation request failed", tag.Error(callErr))
		}
	}
}

type operationBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newOperationBackoffTaskHandler(opts operationTaskHandlerOptions) *operationBackoffTaskHandler {
	return &operationBackoffTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *operationBackoffTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) (bool, error) {
	return op.Status == nexusoperationpb.OPERATION_STATUS_BACKING_OFF && op.GetAttempt() == task.GetAttempt(), nil
}

func (h *operationBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.InvocationBackoffTask,
) error {
	return transitionRescheduled.Apply(op, ctx, EventRescheduled{})
}

type operationScheduleToStartTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newOperationScheduleToStartTimeoutTaskHandler(opts operationTaskHandlerOptions) *operationScheduleToStartTimeoutTaskHandler {
	return &operationScheduleToStartTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *operationScheduleToStartTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToStartTimeoutTask,
) (bool, error) {
	return TransitionStarted.Possible(op), nil
}

func (h *operationScheduleToStartTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToStartTimeoutTask,
) error {
	return op.onTimedOut(ctx, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
			},
		},
	}, false)
}

type operationStartToCloseTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newOperationStartToCloseTimeoutTaskHandler(opts operationTaskHandlerOptions) *operationStartToCloseTimeoutTaskHandler {
	return &operationStartToCloseTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *operationStartToCloseTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.StartToCloseTimeoutTask,
) (bool, error) {
	return op.Status == nexusoperationpb.OPERATION_STATUS_STARTED, nil
}

func (h *operationStartToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.StartToCloseTimeoutTask,
) error {
	return op.onTimedOut(ctx, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			},
		},
	}, false)
}

type operationScheduleToCloseTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newOperationScheduleToCloseTimeoutTaskHandler(opts operationTaskHandlerOptions) *operationScheduleToCloseTimeoutTaskHandler {
	return &operationScheduleToCloseTimeoutTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *operationScheduleToCloseTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	return TransitionTimedOut.Possible(op), nil
}

func (h *operationScheduleToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	op *Operation,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.ScheduleToCloseTimeoutTask,
) error {
	return op.onTimedOut(ctx, &failurepb.Failure{
		Message: "operation timed out",
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
			},
		},
	}, false)
}
