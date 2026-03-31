package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
)

var (
	ErrResponseBodyTooLarge  = errors.New("http: response body too large")
	ErrInvalidOperationToken = errors.New("invalid operation token")
	errRequestTimedOut       = errors.New("request timed out")
	errOpProcessorFailed     = errors.New("nexus operation processor failed")
)

const maxDuration = time.Duration(1<<63 - 1)

type operationTimeoutBelowMinError struct {
	timeoutType enumspb.TimeoutType
}

func (o *operationTimeoutBelowMinError) Error() string {
	return fmt.Sprintf("not enough time to execute another request before %s timeout", o.timeoutType.String())
}

// ClientProvider provides a nexus client for a given endpoint.
type ClientProvider func(ctx context.Context, namespaceID string, entry *persistencespb.NexusEndpointEntry, service string) (*nexusrpc.HTTPClient, error)

// startArgs holds the arguments needed to start a Nexus operation invocation.
type startArgs struct {
	service                string
	operation              string
	requestID              string
	endpointName           string
	endpointID             string
	currentTime            time.Time
	scheduledTime          time.Time
	scheduleToStartTimeout time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration
	header                 map[string]string
	payload                *commonpb.Payload
	nexusLink              nexus.Link
	serializedRef          []byte
}

// invocationResult is a marker interface for the outcome of a Nexus start operation call.
type invocationResult interface {
	mustImplementInvocationResult()
}

// invocationResultOK indicates the operation completed synchronously or started asynchronously.
type invocationResultOK struct {
	response *nexusrpc.ClientStartOperationResponse[*commonpb.Payload]
	links    []*commonpb.Link
}

func (invocationResultOK) mustImplementInvocationResult() {}

// invocationResultFail indicates a non-retryable failure.
type invocationResultFail struct {
	failure *failurepb.Failure
}

func (invocationResultFail) mustImplementInvocationResult() {}

// invocationResultRetry indicates a retryable failure.
type invocationResultRetry struct {
	failure *failurepb.Failure
}

func (invocationResultRetry) mustImplementInvocationResult() {}

// invocationResultCanceled indicates the operation was canceled.
type invocationResultCanceled struct {
	failure *failurepb.Failure
}

func (invocationResultCanceled) mustImplementInvocationResult() {}

// invocationResultTimeout indicates the operation should be timed out.
type invocationResultTimeout struct {
	timeoutType enumspb.TimeoutType
}

func (invocationResultTimeout) mustImplementInvocationResult() {}

// saveResultInput is the input to the Operation.saveResult method used in UpdateComponent.
type saveResultInput struct {
	result      invocationResult
	retryPolicy func() backoff.RetryPolicy
}

func buildCallbackURL(
	useSystemCallback bool,
	callbackTemplate *template.Template,
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
) (string, error) {
	// endpoint is nil for system-internal operations where endpoint lookup is skipped.
	// These always use the system callback URL since the callback is handled internally.
	if endpoint == nil {
		return commonnexus.SystemCallbackURL, nil
	}
	target := endpoint.GetEndpoint().GetSpec().GetTarget().GetVariant()
	if !useSystemCallback {
		return buildCallbackFromTemplate(callbackTemplate, ns)
	}
	switch target.(type) {
	case *persistencespb.NexusEndpointTarget_Worker_:
		return commonnexus.SystemCallbackURL, nil
	case *persistencespb.NexusEndpointTarget_External_:
		return buildCallbackFromTemplate(callbackTemplate, ns)
	default:
		return "", fmt.Errorf("unknown endpoint target type: %T", target)
	}
}

func buildCallbackFromTemplate(callbackTemplate *template.Template, ns *namespace.Namespace) (string, error) {
	if callbackTemplate == nil {
		return "", serviceerror.NewInternalf("dynamic config %q is unset", CallbackURLTemplate.Key().String())
	}
	builder := &strings.Builder{}
	err := callbackTemplate.Execute(builder, struct{ NamespaceName, NamespaceID string }{
		NamespaceName: ns.Name().String(),
		NamespaceID:   ns.ID().String(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to format callback URL: %w", err)
	}
	return builder.String(), nil
}

// lookupEndpoint gets an endpoint from the registry, preferring to look up by ID and falling back to name lookup.
// The fallback is needed because endpoints may be deleted and recreated with the same name but a different ID.
// In that case, the ID stored in the operation state becomes stale, but the name-based lookup still resolves correctly.
func lookupEndpoint(ctx context.Context, registry commonnexus.EndpointRegistry, namespaceID namespace.ID, endpointID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
	entry, err := registry.GetByID(ctx, endpointID)
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			// Endpoint was not found by ID, fall back to name lookup.
			return registry.GetByName(ctx, namespaceID, endpointName)
		}
		return nil, err
	}
	return entry, nil
}

func callErrToFailure(callErr error, retryable bool) (*failurepb.Failure, error) {
	if serviceErr, ok := errors.AsType[serviceerror.ServiceError](callErr); ok {
		return &failurepb.Failure{
			Message: fmt.Sprintf("%s: %s", strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1), serviceErr.Error()),
			FailureInfo: &failurepb.Failure_ServerFailureInfo{
				ServerFailureInfo: &failurepb.ServerFailureInfo{
					NonRetryable: !retryable,
				},
			},
		}, nil
	}
	if handlerErr, ok := errors.AsType[*nexus.HandlerError](callErr); ok {
		var nf nexus.Failure
		if handlerErr.OriginalFailure != nil {
			nf = *handlerErr.OriginalFailure
		} else {
			var err error
			nf, err = nexusrpc.DefaultFailureConverter().ErrorToFailure(handlerErr)
			if err != nil {
				return nil, err
			}
		}
		f, err := commonnexus.NexusFailureToTemporalFailure(nf)
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	return &failurepb.Failure{
		Message: callErr.Error(),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         "CallError",
				NonRetryable: !retryable,
			},
		},
	}, nil
}

// classifyStartOperationError classifies the call error and returns an invocation result.
func classifyStartOperationError(callErr error) invocationResult {
	if _, ok := errors.AsType[serviceerror.ServiceError](callErr); ok && !common.IsRetryableRPCError(callErr) {
		return nonRetryableFailResult(callErr)
	}
	if opErr, ok := errors.AsType[*nexus.OperationError](callErr); ok {
		return classifyOperationError(opErr)
	}
	if handlerErr, ok := errors.AsType[*nexus.HandlerError](callErr); ok && !handlerErr.Retryable() {
		return nonRetryableFailResult(callErr)
	}
	if errors.Is(callErr, ErrResponseBodyTooLarge) || errors.Is(callErr, ErrInvalidOperationToken) {
		return nonRetryableFailResult(callErr)
	}
	if opTimeoutBelowMinErr, ok := errors.AsType[*operationTimeoutBelowMinError](callErr); ok {
		return invocationResultTimeout{timeoutType: opTimeoutBelowMinErr.timeoutType}
	}
	if errors.Is(callErr, context.DeadlineExceeded) || errors.Is(callErr, context.Canceled) {
		callErr = errRequestTimedOut
	}

	// Retryable error
	failure, err := callErrToFailure(callErr, true)
	if err != nil {
		failure = &failurepb.Failure{Message: callErr.Error()}
	}
	return invocationResultRetry{failure: failure}
}

func nonRetryableFailResult(callErr error) invocationResultFail {
	failure, err := callErrToFailure(callErr, false)
	if err != nil {
		failure = &failurepb.Failure{Message: callErr.Error()}
	}
	return invocationResultFail{failure: failure}
}

// classifyOperationError converts a Nexus OperationError to the appropriate invocation result.
func classifyOperationError(opErr *nexus.OperationError) invocationResult {
	// Special marker for Temporal->Temporal calls to indicate that the original failure should be unwrapped.
	// Temporal uses a wrapper operation error with no additional information to transmit the OperationError over the network.
	// The meaningful information is in the operation error's cause.
	unwrapError := opErr.OriginalFailure.Metadata["unwrap-error"] == "true"

	var originalCause *failurepb.Failure
	var err error
	if unwrapError && opErr.OriginalFailure.Cause != nil {
		originalCause, err = commonnexus.NexusFailureToTemporalFailure(*opErr.OriginalFailure.Cause)
	} else {
		// Transform the OperationError to either ApplicationFailure or CanceledFailure based on the operation error state.
		originalCause, err = commonnexus.NexusFailureToTemporalFailure(*opErr.OriginalFailure)
	}
	if err != nil {
		originalCause = &failurepb.Failure{
			Message: fmt.Sprintf("Malformed failure: %v", err),
		}
	}

	switch opErr.State { //nolint:exhaustive
	case nexus.OperationStateFailed:
		return invocationResultFail{failure: originalCause}
	case nexus.OperationStateCanceled:
		return invocationResultCanceled{failure: originalCause}
	default:
		return invocationResultFail{failure: &failurepb.Failure{
			Message: fmt.Sprintf("unexpected operation error state: %s", opErr.State),
		}}
	}
}

func convertResponseLinks(links []nexus.Link, logger log.Logger) []*commonpb.Link {
	var result []*commonpb.Link
	for _, nexusLink := range links {
		switch nexusLink.Type {
		case string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName()):
			link, err := commonnexus.ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
			if err != nil {
				logger.Error(
					fmt.Sprintf("failed to parse link to %q: %s", nexusLink.Type, nexusLink.URL),
					tag.Error(err),
				)
				continue
			}
			result = append(result, &commonpb.Link{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: link,
				},
			})
		default:
			logger.Error(fmt.Sprintf("invalid link data type: %q", nexusLink.Type))
		}
	}
	return result
}

func isDestinationDown(err error) bool {
	if _, ok := errors.AsType[serviceerror.ServiceError](err); ok {
		return false
	}
	if _, ok := errors.AsType[*nexus.OperationError](err); ok {
		return false
	}
	if handlerError, ok := errors.AsType[*nexus.HandlerError](err); ok {
		return handlerError.Retryable()
	}
	if errors.Is(err, errOpProcessorFailed) {
		return false
	}
	if errors.Is(err, ErrResponseBodyTooLarge) {
		return false
	}
	if errors.Is(err, ErrInvalidOperationToken) {
		return false
	}
	_, ok := errors.AsType[*operationTimeoutBelowMinError](err)
	return !ok
}

func failureSourceFromContext(ctx context.Context) string {
	ctxVal := ctx.Value(commonnexus.FailureSourceContextKey)
	if ctxVal == nil {
		return ""
	}
	val, ok := ctxVal.(*atomic.Value)
	if !ok {
		return ""
	}
	src := val.Load()
	if src == nil {
		return ""
	}
	source, ok := src.(string)
	if !ok {
		return ""
	}
	return source
}

func startCallOutcomeTag(callCtx context.Context, result *nexusrpc.ClientStartOperationResponse[*commonpb.Payload], callErr error) string {
	if callErr != nil {
		if _, ok := errors.AsType[*operationTimeoutBelowMinError](callErr); ok {
			return "operation-timeout"
		}
		if errors.Is(callErr, ErrInvalidOperationToken) {
			return "invalid-operation-token"
		}
		if errors.Is(callErr, errOpProcessorFailed) {
			return "operation-processor-failed"
		}
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		if serviceErr, ok := errors.AsType[serviceerror.ServiceError](callErr); ok {
			return "service-error:" + strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1)
		}
		if opFailedError, ok := errors.AsType[*nexus.OperationError](callErr); ok {
			return "operation-unsuccessful:" + string(opFailedError.State)
		}
		if handlerError, ok := errors.AsType[*nexus.HandlerError](callErr); ok {
			return "handler-error:" + string(handlerError.Type)
		}
		return "unknown-error"
	}
	if result.Pending != nil {
		return "pending"
	}
	return "successful"
}

func (h *OperationInvocationTaskHandler) startViaHTTP(
	ctx context.Context,
	client *nexusrpc.HTTPClient,
	args startArgs,
	options nexus.StartOperationOptions,
) (*nexusrpc.ClientStartOperationResponse[*commonpb.Payload], error) {
	rawResult, callErr := client.StartOperation(ctx, args.operation, args.payload, options)

	var result *nexusrpc.ClientStartOperationResponse[*commonpb.Payload]
	if callErr == nil {
		if rawResult.Pending != nil {
			result = &nexusrpc.ClientStartOperationResponse[*commonpb.Payload]{
				Pending: &nexusrpc.OperationHandle[*commonpb.Payload]{
					Operation: rawResult.Pending.Operation,
					Token:     rawResult.Pending.Token,
				},
				Links: rawResult.Links,
			}
		} else {
			var payload *commonpb.Payload
			err := rawResult.Successful.Consume(&payload)
			if err != nil {
				callErr = err
			} else {
				result = &nexusrpc.ClientStartOperationResponse[*commonpb.Payload]{
					Successful: payload,
					Links:      rawResult.Links,
				}
			}
		}
	}
	return result, callErr
}

func (h *OperationInvocationTaskHandler) startOnHistoryService(
	ctx context.Context,
	ns *namespace.Namespace,
	args startArgs,
	options nexus.StartOperationOptions,
) (*nexusrpc.ClientStartOperationResponse[*commonpb.Payload], error) {
	protoLinks := commonnexus.ConvertLinksToProto(options.Links)
	res, err := h.chasmRegistry.NexusEndpointProcessor.ProcessInput(chasm.NexusOperationProcessorContext{
		Namespace:               ns,
		RequestID:               args.requestID,
		Links:                   []nexus.Link{args.nexusLink},
		ReserializeInputPayload: true,
	}, args.service, args.operation, args.payload)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errOpProcessorFailed, err)
	}
	resp, err := h.historyClient.StartNexusOperation(ctx, &historyservice.StartNexusOperationRequest{
		NamespaceId: ns.ID().String(),
		ShardId:     res.RoutingKey.ShardID(h.config.NumHistoryShards),
		Request: &nexuspb.StartOperationRequest{
			Service:        args.service,
			Operation:      args.operation,
			Payload:        res.ReserializedInputPayload,
			RequestId:      args.requestID,
			Callback:       options.CallbackURL,
			CallbackHeader: options.CallbackHeader,
			Links:          protoLinks,
		},
	})
	if err != nil {
		return nil, err
	}

	result := &nexusrpc.ClientStartOperationResponse[*commonpb.Payload]{}
	switch v := resp.GetResponse().GetVariant().(type) {
	case *nexuspb.StartOperationResponse_SyncSuccess:
		result.Links = commonnexus.ConvertLinksFromProto(v.SyncSuccess.GetLinks())
		result.Successful = v.SyncSuccess.Payload
	case *nexuspb.StartOperationResponse_AsyncSuccess:
		result.Links = commonnexus.ConvertLinksFromProto(v.AsyncSuccess.GetLinks())
		result.Pending = &nexusrpc.OperationHandle[*commonpb.Payload]{
			Operation: args.operation,
			Token:     v.AsyncSuccess.GetOperationToken(),
		}
	case *nexuspb.StartOperationResponse_Failure:
		state := nexus.OperationStateFailed
		if v.Failure.GetCanceledFailureInfo() != nil {
			state = nexus.OperationStateCanceled
		}
		nexusFailure, convErr := commonnexus.TemporalFailureToNexusFailure(v.Failure)
		if convErr != nil {
			h.logger.Error("failed to convert temporal failure to nexus failure", tag.Error(convErr), tag.RequestID(args.requestID))
			he := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error (request ID: %s)", args.requestID)
			he.RetryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
			return nil, he
		}
		return nil, &nexus.OperationError{
			State:           state,
			Cause:           &nexus.FailureError{Failure: nexusFailure},
			OriginalFailure: &nexusFailure,
		}
	default:
		h.logger.Error(fmt.Sprintf("unexpected response variant type: %T", v), tag.RequestID(args.requestID))
		he := nexus.NewHandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error (request ID: %s)", args.requestID)
		he.RetryBehavior = nexus.HandlerErrorRetryBehaviorRetryable
		return nil, he
	}

	return result, nil
}

// cancelCallOutcomeTag returns a metric tag for the outcome of a cancel call.
func cancelCallOutcomeTag(callCtx context.Context, callErr error) string {
	if callErr != nil {
		if errors.Is(callErr, errOpProcessorFailed) {
			return "operation-processor-failed"
		}
		var opTimeoutBelowMinErr *operationTimeoutBelowMinError
		if errors.As(callErr, &opTimeoutBelowMinErr) {
			return "operation-timeout"
		}
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		var handlerErr *nexus.HandlerError
		if errors.As(callErr, &handlerErr) {
			return "handler-error:" + string(handlerErr.Type)
		}
		var serviceErr serviceerror.ServiceError
		if errors.As(callErr, &serviceErr) {
			return "service-error:" + strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1)
		}
		return "unknown-error"
	}
	return "successful"
}

// generateCallbackToken creates a callback token for the given operation reference.
func (h *OperationInvocationTaskHandler) generateCallbackToken(
	serializedRef []byte,
	requestID string,
) (string, error) {
	token, err := h.callbackTokenGenerator.Tokenize(&tokenspb.NexusOperationCompletion{
		ComponentRef: serializedRef,
		RequestId:    requestID,
	})
	if err != nil {
		return "", fmt.Errorf("%w: %w", queueserrors.NewUnprocessableTaskError("failed to generate a callback token"), err)
	}
	return token, nil
}
