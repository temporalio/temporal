package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
)

type cancellationResult interface {
	mustImplementCancellationResult()
}

type cancellationResultOK struct{}

func (cancellationResultOK) mustImplementCancellationResult() {}

type cancellationResultFail struct {
	failure *failurepb.Failure
}

func (cancellationResultFail) mustImplementCancellationResult() {}

type cancellationResultRetry struct {
	failure *failurepb.Failure
}

func (cancellationResultRetry) mustImplementCancellationResult() {}

func newCancellationResult(callErr error) (cancellationResult, error) {
	if callErr == nil {
		return cancellationResultOK{}, nil
	}

	if opTimeoutBelowMinErr, ok := errors.AsType[*operationTimeoutBelowMinError](callErr); ok {
		failure := &failurepb.Failure{
			Message: "operation timed out before cancellation could be delivered",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: opTimeoutBelowMinErr.timeoutType,
				},
			},
		}
		return cancellationResultFail{failure: failure}, nil
	}

	failure, retryable, err := callErrorToFailure(callErr)
	if err != nil {
		return nil, err
	}
	if retryable {
		return cancellationResultRetry{failure: failure}, nil
	}
	return cancellationResultFail{failure: failure}, nil
}

type cancellationInvocationTaskHandlerOptions struct {
	fx.In

	invocationTaskHandlerOptions
}

type cancellationInvocationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nexusoperationpb.CancellationTask]

	nexusTaskHandlerBase
}

func newCancellationInvocationTaskHandler(opts cancellationInvocationTaskHandlerOptions) *cancellationInvocationTaskHandler {
	return &cancellationInvocationTaskHandler{
		nexusTaskHandlerBase: opts.toBase(),
	}
}

func (h *cancellationInvocationTaskHandler) Validate(
	_ chasm.Context,
	cancellation *Cancellation,
	_ chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) (bool, error) {
	return cancellation.Status == nexusoperationpb.CANCELLATION_STATUS_SCHEDULED &&
		cancellation.GetAttempt() == task.GetAttempt(), nil
}

func (h *cancellationInvocationTaskHandler) Execute(
	ctx context.Context,
	cancelRef chasm.ComponentRef,
	attrs chasm.TaskAttributes,
	task *nexusoperationpb.CancellationTask,
) error {
	ns, err := h.namespaceRegistry.GetNamespaceByID(namespace.ID(cancelRef.NamespaceID))
	if err != nil {
		return serviceerror.NewNotFoundf("failed to get namespace by ID: %v", err)
	}

	args, err := chasm.ReadComponent(ctx, cancelRef, (*Cancellation).loadArgs, nil)
	if err != nil {
		return err
	}

	endpoint, err := h.lookupEndpoint(ctx, ns.ID(), args.endpointID, args.endpointName)
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			h.logger.Error("endpoint not found while processing invocation task", tag.Error(err))
			handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			return h.saveCancellationResult(ctx, cancelRef, handlerErr)
		}
		return err
	}

	callTimeout := h.config.RequestTimeout(ns.Name().String(), attrs.Destination)
	var timeoutType enumspb.TimeoutType
	if args.startToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.startToCloseTimeout-args.currentTime.Sub(args.startedTime))
		timeoutType = enumspb.TIMEOUT_TYPE_START_TO_CLOSE
	}
	if args.scheduleToCloseTimeout > 0 {
		callTimeout = min(callTimeout, args.scheduleToCloseTimeout-args.currentTime.Sub(args.scheduledTime))
		timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
	}

	callCtx, cancel := h.setupCallContext(ctx, callTimeout)
	defer cancel()

	inv, err := h.newInvocation(
		callCtx, ns, endpoint, args.endpointName, args.service,
		callTimeout, timeoutType,
		invocationTraceContext{
			operationTag:  "CancelOperation",
			namespaceName: ns.Name().String(),
			requestID:     args.requestID,
			operation:     args.operation,
			endpointName:  args.endpointName,
			workflowID:    cancelRef.BusinessID,
			runID:         cancelRef.RunID,
			attemptStart:  args.currentTime.UTC(),
			attempt:       task.GetAttempt(),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to construct invocation: %w", err)
	}
	startTime := args.currentTime
	callErr := inv.Cancel(callCtx, args, nexus.CancelOperationOptions{Header: nexus.Header(args.headers)})
	failureSource := failureSourceFromContext(callCtx)

	h.recordCallOutcome(ns, endpoint, args.endpointName, "CancelOperation", cancelCallOutcomeTag(callCtx, callErr), callErr, time.Since(startTime), failureSource)

	saveErr := h.saveCancellationResult(ctx, cancelRef, callErr)

	if callErr != nil && isDestinationDown(callErr) {
		saveErr = queueserrors.NewDestinationDownError(callErr.Error(), saveErr)
	}

	return saveErr
}

// saveCancellationResult saves the cancellation result by updating the cancellation component.
func (h *cancellationInvocationTaskHandler) saveCancellationResult(
	ctx context.Context,
	cancelRef chasm.ComponentRef,
	callErr error,
) error {
	result, err := newCancellationResult(callErr)
	if err != nil {
		return fmt.Errorf("failed to construct cancellation result: %w", err)
	}
	_, _, err = chasm.UpdateComponent(
		ctx,
		cancelRef,
		(*Cancellation).saveResult,
		saveCancellationResultInput{
			result:      result,
			retryPolicy: h.config.RetryPolicy,
		},
	)
	return err
}

type cancellationBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newCancellationBackoffTaskHandler(opts commonTaskHandlerOptions) *cancellationBackoffTaskHandler {
	return &cancellationBackoffTaskHandler{
		config:         opts.Config,
		metricsHandler: opts.MetricsHandler,
		logger:         opts.Logger,
	}
}

func (h *cancellationBackoffTaskHandler) Validate(
	_ chasm.Context,
	cancellation *Cancellation,
	_ chasm.TaskAttributes,
	task *nexusoperationpb.CancellationBackoffTask,
) (bool, error) {
	isValid := cancellation.Status == nexusoperationpb.CANCELLATION_STATUS_BACKING_OFF && cancellation.GetAttempt() == task.GetAttempt()
	return isValid, nil
}

func (h *cancellationBackoffTaskHandler) Execute(
	ctx chasm.MutableContext,
	cancellation *Cancellation,
	_ chasm.TaskAttributes,
	_ *nexusoperationpb.CancellationBackoffTask,
) error {
	return transitionCancellationRescheduled.Apply(cancellation, ctx, EventCancellationRescheduled{})
}
