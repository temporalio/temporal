package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/fx"
)

// startResult is a marker interface for the outcome of a Nexus start operation call.
type startResult interface {
	mustImplementStartResult()
}

// startResultOK indicates the operation completed synchronously or started asynchronously.
type startResultOK struct {
	response *nexusrpc.ClientStartOperationResponse[*commonpb.Payload]
	links    []*commonpb.Link
}

func (startResultOK) mustImplementStartResult() {}

// startResultFail indicates a non-retryable failure.
type startResultFail struct {
	failure *failurepb.Failure
}

func (startResultFail) mustImplementStartResult() {}

// startResultRetry indicates a retryable failure.
type startResultRetry struct {
	failure *failurepb.Failure
}

func (startResultRetry) mustImplementStartResult() {}

// startResultCancel indicates the operation completed as canceled.
type startResultCancel struct {
	failure *failurepb.Failure
}

func (startResultCancel) mustImplementStartResult() {}

// startResultTimeout indicates the operation timed out while attempting to invoke.
type startResultTimeout struct {
	failure *failurepb.Failure
}

func (startResultTimeout) mustImplementStartResult() {}

func newStartResult(
	response *nexusrpc.ClientStartOperationResponse[*commonpb.Payload],
	callErr error,
) (startResult, error) {
	if callErr == nil {
		return startResultOK{response: response}, nil
	}

	if opErr, ok := errors.AsType[*nexus.OperationError](callErr); ok {
		failure, err := operationErrorToFailure(opErr)
		if err != nil {
			return nil, err
		}
		if opErr.State == nexus.OperationStateCanceled {
			return startResultCancel{failure: failure}, nil
		}
		return startResultFail{failure: failure}, nil
	}

	if opTimeoutBelowMinErr, ok := errors.AsType[*operationTimeoutBelowMinError](callErr); ok {
		failure := &failurepb.Failure{
			Message: "operation timed out",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: opTimeoutBelowMinErr.timeoutType,
				},
			},
		}
		return startResultTimeout{failure: failure}, nil
	}

	failure, retryable, err := callErrorToFailure(callErr)
	if err != nil {
		return nil, err
	}
	if retryable {
		return startResultRetry{failure: failure}, nil
	}
	return startResultFail{failure: failure}, nil
}

// operationErrorToFailure converts a Nexus OperationError to the appropriate failure.
func operationErrorToFailure(opErr *nexus.OperationError) (*failurepb.Failure, error) {
	var nf nexus.Failure
	if opErr.OriginalFailure != nil {
		nf = *opErr.OriginalFailure
	} else {
		var err error
		nf, err = nexusrpc.DefaultFailureConverter().ErrorToFailure(opErr)
		if err != nil {
			return nil, err
		}
	}
	// Special marker for Temporal->Temporal calls to indicate that the original failure should be unwrapped.
	// Temporal uses a wrapper operation error with no additional information to transmit the OperationError over the network.
	// The meaningful information is in the operation error's cause.
	unwrapError := nf.Metadata["unwrap-error"] == "true"

	if unwrapError && nf.Cause != nil {
		return commonnexus.NexusFailureToTemporalFailure(*nf.Cause)
	}
	// Transform the OperationError to either ApplicationFailure or CanceledFailure based on the operation error state.
	return commonnexus.NexusFailureToTemporalFailure(nf)
}

// operationInvocationTaskHandlerOptions is the fx parameter object for the invocation task executor.
type operationInvocationTaskHandlerOptions struct {
	fx.In

	InvocationTaskHandlerOptions
	CallbackTokenGenerator *commonnexus.CallbackTokenGenerator
}

type operationInvocationTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*nexusoperationpb.InvocationTask]

	nexusTaskHandlerBase
	callbackTokenGenerator *commonnexus.CallbackTokenGenerator
}

func newOperationInvocationTaskHandler(opts operationInvocationTaskHandlerOptions) *operationInvocationTaskHandler {
	return &operationInvocationTaskHandler{
		nexusTaskHandlerBase:   opts.toBase(),
		callbackTokenGenerator: opts.CallbackTokenGenerator,
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

	endpoint, err := h.lookupEndpoint(ctx, ns.ID(), args.endpointID, args.endpointName)
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			h.logger.Error("endpoint not found while processing invocation task", tag.Error(err))
			handlerErr := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "endpoint not registered")
			result, err := newStartResult(nil, handlerErr)
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

	callbackURL, err := h.buildCallbackURL(ns, endpoint)
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
		if t := args.scheduleToStartTimeout - elapsed; t < callTimeout {
			callTimeout = t
			timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START
		}
	} else if args.scheduleToCloseTimeout > 0 {
		if t := args.scheduleToCloseTimeout - elapsed; t < callTimeout {
			callTimeout = t
			timeoutType = enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE
		}
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
		header = make(nexus.Header, 2) // To set the failure support and timeout headers.
	}
	// Set the operation timeout header if not already set.
	if opTimeoutHeader := header.Get(nexus.HeaderOperationTimeout); opTimeout != maxDuration && opTimeoutHeader == "" {
		header.Set(nexus.HeaderOperationTimeout, commonnexus.FormatDuration(opTimeout))
	}
	// If this request is handled by a newer server that supports Nexus failure serialization, trigger that behavior.
	if h.config.UseNewFailureWireFormat(ns.Name().String()) {
		header.Set(nexusrpc.HeaderTemporalNexusFailureSupport, "true")
	}

	callCtx, cancel := h.setupCallContext(ctx, callTimeout)
	defer cancel()

	options := nexus.StartOperationOptions{
		Header:      header,
		CallbackURL: callbackURL,
		RequestID:   args.requestID,
		CallbackHeader: nexus.Header{
			commonnexus.CallbackTokenHeader: token,
		},
		Links: []nexus.Link{args.nexusLink},
	}

	invocation, err := h.newInvocation(
		callCtx, ns, endpoint, args.endpointName, args.service,
		callTimeout, timeoutType,
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

	h.recordCallOutcome(ns, endpoint, args.endpointName, "StartOperation", startCallOutcomeTag(callCtx, response, callErr), callErr, callDuration, failureSource)

	result, err := newStartResult(response, callErr)
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

// generateCallbackToken creates a callback token for the given operation reference.
func (h *operationInvocationTaskHandler) generateCallbackToken(
	serializedRef []byte,
	requestID string,
) (string, error) {
	ref := &persistencespb.ChasmComponentRef{}
	if err := ref.Unmarshal(serializedRef); err != nil {
		return "", fmt.Errorf("%w: %w", queueserrors.NewUnprocessableTaskError("failed to decode component ref for callback token"), err)
	}
	namespaceID := ref.NamespaceId

	// Execution VT becomes stale after workflow mutations between token minting and completion arrival.
	ref.ExecutionVersionedTransition = nil
	stableRef, err := ref.Marshal()
	if err != nil {
		return "", fmt.Errorf("%w: %w", queueserrors.NewUnprocessableTaskError("failed to encode component ref for callback token"), err)
	}

	// NamespaceId and WorkflowId are set at the top level for two reasons:
	// 1. The frontend reads NamespaceId to resolve the namespace before deserializing ComponentRef.
	// 2. The HSM CompleteNexusOperation RPC routes by namespace_id + workflow_id. CHASM completions
	//    go through the same RPC (the frontend doesn't distinguish), so we populate WorkflowId from
	//    the component ref's BusinessId to ensure correct shard routing. The history handler then
	//    checks ComponentRef to redirect to the CHASM path.
	token, err := h.callbackTokenGenerator.Tokenize(&tokenspb.NexusOperationCompletion{
		NamespaceId:  namespaceID,
		WorkflowId:   ref.BusinessId,
		ComponentRef: stableRef,
		RequestId:    requestID,
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", queueserrors.NewUnprocessableTaskError("failed to generate a callback token"), err)
	}
	return token, nil
}

type operationBackoffTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newOperationBackoffTaskHandler(opts commonTaskHandlerOptions) *operationBackoffTaskHandler {
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

func newOperationScheduleToStartTimeoutTaskHandler(opts commonTaskHandlerOptions) *operationScheduleToStartTimeoutTaskHandler {
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
	})
}

type operationStartToCloseTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newOperationStartToCloseTimeoutTaskHandler(opts commonTaskHandlerOptions) *operationStartToCloseTimeoutTaskHandler {
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
	})
}

type operationScheduleToCloseTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
	config *Config

	metricsHandler metrics.Handler
	logger         log.Logger
}

func newOperationScheduleToCloseTimeoutTaskHandler(opts commonTaskHandlerOptions) *operationScheduleToCloseTimeoutTaskHandler {
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
	})
}
