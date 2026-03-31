package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptrace"
	"sync/atomic"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/resource"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
)

type CancellationTaskHandlerOptions struct {
	fx.In

	Config            *Config
	NamespaceRegistry namespace.Registry
	MetricsHandler    metrics.Handler
	Logger            log.Logger
	ClientProvider    ClientProvider
	EndpointRegistry  commonnexus.EndpointRegistry
	HTTPTraceProvider commonnexus.HTTPClientTraceProvider
	HistoryClient     resource.HistoryClient
	ChasmRegistry     *chasm.Registry
}

type CancellationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nexusoperationpb.CancellationTask]

	config            *Config
	namespaceRegistry namespace.Registry
	metricsHandler    metrics.Handler
	logger            log.Logger
	clientProvider    ClientProvider
	endpointRegistry  commonnexus.EndpointRegistry
	httpTraceProvider commonnexus.HTTPClientTraceProvider
	historyClient     resource.HistoryClient
	chasmRegistry     *chasm.Registry
}

func NewCancellationTaskHandler(opts CancellationTaskHandlerOptions) *CancellationTaskHandler {
	return &CancellationTaskHandler{
		config:            opts.Config,
		namespaceRegistry: opts.NamespaceRegistry,
		metricsHandler:    opts.MetricsHandler,
		logger:            opts.Logger,
		clientProvider:    opts.ClientProvider,
		endpointRegistry:  opts.EndpointRegistry,
		httpTraceProvider: opts.HTTPTraceProvider,
		historyClient:     opts.HistoryClient,
		chasmRegistry:     opts.ChasmRegistry,
	}
}

func (h *CancellationTaskHandler) Validate(
	_ chasm.Context,
	cancellation *Cancellation,
	_ chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) (bool, error) {
	return cancellation.Status == nexusoperationpb.CANCELLATION_STATUS_SCHEDULED &&
		cancellation.GetAttempt() == task.GetAttempt(), nil
}

func (h *CancellationTaskHandler) Execute(
	ctx context.Context,
	cancelRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) error {
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(cancelRef.NamespaceID))
	if err != nil {
		return serviceerror.NewNotFoundf("failed to get namespace by ID: %v", err)
	}

	args, err := chasm.ReadComponent(ctx, cancelRef, (*Cancellation).loadCancelArgs, nil)
	if err != nil {
		return err
	}

	var endpoint *persistencespb.NexusEndpointEntry

	// Skip endpoint lookup for system-internal operations.
	if args.endpointName != commonnexus.SystemEndpoint {
		if args.endpointID == "" {
			handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			return h.saveCancellationResult(ctx, cancelRef, handlerError)
		}

		endpoint, err = lookupEndpoint(ctx, h.endpointRegistry, ns.ID(), args.endpointID, args.endpointName)
		if err != nil {
			if errors.As(err, new(*serviceerror.NotFound)) {
				handlerError := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
				return h.saveCancellationResult(ctx, cancelRef, handlerError)
			}
			return err
		}
	}

	elapsed := args.currentTime.Sub(args.scheduledTime)
	callTimeout := h.config.RequestTimeout(ns.Name().String(), attrs.Destination)
	var timeoutType enumspb.TimeoutType
	if args.startToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.startToCloseTimeout-elapsed)
		timeoutType = enumspb.TIMEOUT_TYPE_START_TO_CLOSE
	}
	if args.scheduleToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToCloseTimeout-elapsed)
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	}
	callCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()

	callCtx = context.WithValue(callCtx, commonnexus.FailureSourceContextKey, &atomic.Value{})

	var callErr error
	startTime := args.currentTime
	if callTimeout < h.config.MinRequestTimeout(ns.Name().String()) {
		callErr = &operationTimeoutBelowMinError{timeoutType: timeoutType}
	} else if args.endpointName == commonnexus.SystemEndpoint {
		callErr = h.cancelOnHistoryService(callCtx, ns, args)
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

		handle, handleErr := client.NewOperationHandle(args.operation, args.token)
		if handleErr != nil {
			return serviceerror.NewUnavailablef("failed to get handle for operation: %v", handleErr)
		}

		if h.httpTraceProvider != nil {
			traceLogger := log.With(h.logger,
				tag.Operation("CancelOperation"),
				tag.WorkflowNamespace(ns.Name().String()),
				tag.RequestID(args.requestID),
				tag.NexusOperation(args.operation),
				tag.Endpoint(args.endpointName),
				tag.WorkflowID(cancelRef.BusinessID),
				tag.WorkflowRunID(cancelRef.RunID),
				tag.AttemptStart(args.currentTime.UTC()),
				tag.Attempt(task.GetAttempt()),
			)
			if trace := h.httpTraceProvider.NewTrace(task.GetAttempt(), traceLogger); trace != nil {
				callCtx = httptrace.WithClientTrace(callCtx, trace)
			}
		}

		callErr = handle.Cancel(callCtx, nexus.CancelOperationOptions{Header: nexus.Header(args.headers)})
	}
	failureSource := failureSourceFromContext(callCtx)

	methodTag := metrics.NexusMethodTag("CancelOperation")
	namespaceTag := metrics.NamespaceTag(ns.Name().String())
	var destTag metrics.Tag
	if endpoint != nil {
		destTag = metrics.DestinationTag(endpoint.Endpoint.Spec.GetName())
	} else {
		destTag = metrics.DestinationTag(args.endpointName)
	}
	outcomeTag := metrics.OutcomeTag(cancelCallOutcomeTag(callCtx, callErr))
	failureSourceTag := metrics.FailureSourceTag(failureSource)
	OutboundRequestCounter.With(h.metricsHandler).Record(1, namespaceTag, destTag, methodTag, outcomeTag, failureSourceTag)
	OutboundRequestLatency.With(h.metricsHandler).Record(time.Since(startTime), namespaceTag, destTag, methodTag, outcomeTag, failureSourceTag)

	if callErr != nil {
		if failureSource == commonnexus.FailureSourceWorker || errors.As(callErr, new(*operationTimeoutBelowMinError)) {
			h.logger.Debug("Nexus CancelOperation request failed", tag.Error(callErr))
		} else {
			h.logger.Error("Nexus CancelOperation request failed", tag.Error(callErr))
		}
	}

	saveErr := h.saveCancellationResult(ctx, cancelRef, callErr)

	if callErr != nil && isDestinationDown(callErr) {
		saveErr = queueserrors.NewDestinationDownError(callErr.Error(), saveErr)
	}

	return saveErr
}

// saveCancellationResult saves the cancellation result by updating the cancellation component.
func (h *CancellationTaskHandler) saveCancellationResult(
	ctx context.Context,
	cancelRef chasm.ComponentRef,
	callErr error,
) error {
	_, _, err := chasm.UpdateComponent(
		ctx,
		cancelRef,
		(*Cancellation).applyCancellationResult,
		saveCancellationResultInput{
			callErr:     callErr,
			retryPolicy: h.config.RetryPolicy,
		},
	)
	return err
}

func (h *CancellationTaskHandler) cancelOnHistoryService(
	ctx context.Context,
	ns *namespace.Namespace,
	args cancelArgs,
) error {
	res, err := h.chasmRegistry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace: ns,
		RequestID: args.requestID,
		// Links are not needed for cancellation.
	}, args.service, args.operation, args.payload)
	if err != nil {
		return fmt.Errorf("%w: %w", errOpProcessorFailed, err)
	}

	_, err = h.historyClient.CancelNexusOperation(ctx, &historyservice.CancelNexusOperationRequest{
		NamespaceId: ns.ID().String(),
		ShardId:     res.RoutingKey.ShardID(h.config.NumHistoryShards),
		Request: &nexuspb.CancelOperationRequest{
			Service:        args.service,
			Operation:      args.operation,
			OperationToken: args.token,
		},
	})
	return err
}

type CancellationBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func NewCancellationBackoffTaskHandler(opts OperationTaskHandlerOptions) *CancellationBackoffTaskHandler {
	return &CancellationBackoffTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *CancellationBackoffTaskHandler) Validate(
	_ chasm.Context,
	cancellation *Cancellation,
	_ chasm.TaskAttributes,
	task *nexusoperationpb.CancellationBackoffTask,
) (bool, error) {
	return cancellation.Status == nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF &&
		cancellation.GetAttempt() == task.GetAttempt(), nil
}

func (h *CancellationBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	cancellation *Cancellation,
	_ chasm.TaskAttributes,
	_ *nexusoperationpb.CancellationBackoffTask,
) error {
	return transitionCancellationRescheduled.Apply(cancellation, ctx, EventCancellationRescheduled{})
}
