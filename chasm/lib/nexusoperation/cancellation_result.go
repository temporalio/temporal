package nexusoperation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/nexus-rpc/sdk-go/nexus"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
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
			return cancellationResultRetry{failure: failure}, nil
		}
		return cancellationResultFail{failure: failure}, nil
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
			return cancellationResultRetry{failure: failure}, nil
		}
		return cancellationResultFail{failure: failure}, nil
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

	if errors.Is(callErr, context.DeadlineExceeded) || errors.Is(callErr, context.Canceled) {
		// If timed out, don't leak internal info to the user
		callErr = errRequestTimedOut
	}

	// Fallback to retryable server failure.
	failure := &failurepb.Failure{
		Message: callErr.Error(),
		FailureInfo: &failurepb.Failure_ServerFailureInfo{
			ServerFailureInfo: &failurepb.ServerFailureInfo{},
		},
	}
	return cancellationResultRetry{failure: failure}, nil
}
