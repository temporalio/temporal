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
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
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

// cancelCallOutcomeTag returns a metric tag for the outcome of a cancel call.
func cancelCallOutcomeTag(callCtx context.Context, callErr error) string {
	if callErr != nil {
		if errors.Is(callErr, errOpProcessorFailed) {
			return "operation-processor-failed"
		}
		if _, ok := errors.AsType[*operationTimeoutBelowMinError](callErr); ok {
			return "operation-timeout"
		}
		if callCtx.Err() != nil {
			return "request-timeout"
		}
		if handlerErr, ok := errors.AsType[*nexus.HandlerError](callErr); ok {
			return "handler-error:" + string(handlerErr.Type)
		}
		if serviceErr, ok := errors.AsType[serviceerror.ServiceError](callErr); ok {
			return "service-error:" + strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1)
		}
		return "unknown-error"
	}
	return "successful"
}

// callErrorToFailure converts a Nexus call error to a Temporal failure with retryability.
// Always returns a non-nil failure.
func callErrorToFailure(callErr error) (*failurepb.Failure, bool, error) {
	if serviceErr, ok := errors.AsType[serviceerror.ServiceError](callErr); ok {
		retryable := common.IsRetryableRPCError(callErr)
		failure := &failurepb.Failure{
			Message: fmt.Sprintf("%s: %s", strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1), serviceErr.Error()),
			FailureInfo: &failurepb.Failure_ServerFailureInfo{
				ServerFailureInfo: &failurepb.ServerFailureInfo{
					NonRetryable: !retryable,
				},
			},
		}
		return failure, retryable, nil
	}

	if handlerErr, ok := errors.AsType[*nexus.HandlerError](callErr); ok {
		var nf nexus.Failure
		if handlerErr.OriginalFailure != nil {
			nf = *handlerErr.OriginalFailure
		} else {
			var err error
			nf, err = nexusrpc.DefaultFailureConverter().ErrorToFailure(handlerErr)
			if err != nil {
				return nil, false, err
			}
		}
		failure, err := commonnexus.NexusFailureToTemporalFailure(nf)
		if err != nil {
			return nil, false, err
		}
		return failure, handlerErr.Retryable(), nil
	}

	if errors.Is(callErr, context.DeadlineExceeded) || errors.Is(callErr, context.Canceled) {
		// If timed out, don't leak internal info to the user.
		callErr = errRequestTimedOut
	}

	// Fallback to server failure.
	retryable := !errors.Is(callErr, ErrResponseBodyTooLarge) && !errors.Is(callErr, ErrInvalidOperationToken)
	failure := &failurepb.Failure{
		Message: callErr.Error(),
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			ServerFailureInfo: &failurepb.ServerFailureInfo{
				NonRetryable: !retryable,
			},
		},
	}
	return failure, retryable, nil
}

// invocationResult is a marker interface for the outcome of a Nexus start operation call.
type invocationResult interface {
	mustImplementInvocationResult()
}

type invocationResultOK struct {
	response *nexusrpc.ClientStartOperationResponse[*commonpb.Payload]
}

func (invocationResultOK) mustImplementInvocationResult() {}

type invocationResultFail struct {
	failure *failurepb.Failure
}

func (invocationResultFail) mustImplementInvocationResult() {}

type invocationResultRetry struct {
	failure *failurepb.Failure
}

func (invocationResultRetry) mustImplementInvocationResult() {}

type invocationResultCancel struct {
	failure *failurepb.Failure
}

func (invocationResultCancel) mustImplementInvocationResult() {}

type invocationResultTimeout struct {
	failure *failurepb.Failure
}

func (invocationResultTimeout) mustImplementInvocationResult() {}

func newInvocationResult(
	response *nexusrpc.ClientStartOperationResponse[*commonpb.Payload],
	callErr error,
) (invocationResult, error) {
	if callErr == nil {
		return invocationResultOK{response: response}, nil
	}

	if serviceErr, ok := errors.AsType[serviceerror.ServiceError](callErr); ok {
		retryable := common.IsRetryableRPCError(callErr)
		failure := &failurepb.Failure{
			Message: fmt.Sprintf("%s: %s", strings.Replace(fmt.Sprintf("%T", serviceErr), "*serviceerror.", "", 1), serviceErr.Error()),
			FailureInfo: &failurepb.Failure_ServerFailureInfo{
				ServerFailureInfo: &failurepb.ServerFailureInfo{
					NonRetryable: !retryable,
				},
			},
		}
		if retryable {
			return invocationResultRetry{failure: failure}, nil
		}
		return invocationResultFail{failure: failure}, nil
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
		failure, err := commonnexus.NexusFailureToTemporalFailure(nf)
		if err != nil {
			return nil, err
		}
		if handlerErr.Retryable() {
			return invocationResultRetry{failure: failure}, nil
		}
		return invocationResultFail{failure: failure}, nil
	}

	if opErr, ok := errors.AsType[*nexus.OperationError](callErr); ok {
		failure, err := operationErrorToFailure(opErr)
		if err != nil {
			return nil, err
		}
		if opErr.State == nexus.OperationStateCanceled {
			return invocationResultCancel{failure: failure}, nil
		}
		return invocationResultFail{failure: failure}, nil
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
		return invocationResultTimeout{failure: failure}, nil
	}

	if errors.Is(callErr, context.DeadlineExceeded) || errors.Is(callErr, context.Canceled) {
		callErr = errRequestTimedOut
	}

	failure := &failurepb.Failure{
		Message: callErr.Error(),
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			ServerFailureInfo: &failurepb.ServerFailureInfo{},
		},
	}
	if errors.Is(callErr, ErrResponseBodyTooLarge) || errors.Is(callErr, ErrInvalidOperationToken) {
		failure.GetServerFailureInfo().NonRetryable = true
		return invocationResultFail{failure: failure}, nil
	}
	return invocationResultRetry{failure: failure}, nil
}

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
	unwrapError := nf.Metadata["unwrap-error"] == "true"
	if unwrapError && nf.Cause != nil {
		return commonnexus.NexusFailureToTemporalFailure(*nf.Cause)
	}
	return commonnexus.NexusFailureToTemporalFailure(nf)
}

func buildCallbackURL(
	useSystemCallback bool,
	callbackTemplate *template.Template,
	ns *namespace.Namespace,
	endpoint *persistencespb.NexusEndpointEntry,
) (string, error) {
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

// lookupEndpoint gets an endpoint from the registry, preferring to look up by ID and falling back to name lookup.
// The fallback is needed because endpoints may be deleted and recreated with the same name but a different ID.
// In that case, the ID stored in the operation state becomes stale, but the name-based lookup still resolves correctly.
func lookupEndpoint(ctx context.Context, registry commonnexus.EndpointRegistry, namespaceID namespace.ID, endpointID, endpointName string) (*persistencespb.NexusEndpointEntry, error) {
	entry, err := registry.GetByID(ctx, endpointID)
	if err != nil {
		if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
			return registry.GetByName(ctx, namespaceID, endpointName)
		}
		return nil, err
	}
	return entry, nil
}

// generateCallbackToken creates a callback token for the given operation reference.
func (h *operationInvocationTaskHandler) generateCallbackToken(
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
