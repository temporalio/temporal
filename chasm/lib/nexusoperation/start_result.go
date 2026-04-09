package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
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
			return startResultRetry{failure: failure}, nil
		}
		return startResultFail{failure: failure}, nil
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
			return startResultRetry{failure: failure}, nil
		}
		return startResultFail{failure: failure}, nil
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

	if errors.Is(callErr, context.DeadlineExceeded) || errors.Is(callErr, context.Canceled) {
		// If timed out, don't leak internal info to the user.
		callErr = errRequestTimedOut
	}

	// Fallback to retryable server failure.
	failure := &failurepb.Failure{
		Message: callErr.Error(),
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			ServerFailureInfo: &failurepb.ServerFailureInfo{},
		},
	}
	if errors.Is(callErr, ErrResponseBodyTooLarge) || errors.Is(callErr, ErrInvalidOperationToken) {
		return startResultFail{failure: failure}, nil
	}
	return startResultRetry{failure: failure}, nil
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
