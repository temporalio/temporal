package nexus

import (
	"errors"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
)

// DispatchResultToError serves server-internal callers, which branch on *nexus.HandlerError -- a
// delivery problem whose Retryable() says whether to re-deliver the task -- versus any other error,
// which is the worker's own answer and therefore permanent. These tests pin that split for every
// outcome, and pin that a worker answering in a deprecated format lands on the same error as one
// answering in the current format.
//
// Response fixtures live in dispatch_outcome_test.go.

// An async start counts as success: the worker took responsibility for the operation even though it
// has not finished it.
func TestDispatchResultToError_SuccessOutcomes(t *testing.T) {
	require.NoError(t, MatchingDispatchResponseToError(
		syncSuccessResponse(&nexuspb.StartOperationResponse_Sync{})))
	require.NoError(t, MatchingDispatchResponseToError(startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
			AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationToken: "tok"},
		},
	})))
	require.NoError(t, DispatchResultToError(ClassifyCancelOperationDispatch(cancelResponse())))
}

// A task that never reached a handler, or an answer this build cannot interpret, is a delivery problem
// rather than an answer, so it has to surface as a handler error.
func TestDispatchResultToError_DeliveryProblems(t *testing.T) {
	for _, tc := range []struct {
		name     string
		resp     *matchingservice.DispatchNexusTaskResponse
		wantType nexus.HandlerErrorType
	}{
		{
			name:     "request timeout",
			resp:     requestTimeoutResponse(),
			wantType: nexus.HandlerErrorTypeUpstreamTimeout,
		},
		{
			name:     "no outcome",
			resp:     &matchingservice.DispatchNexusTaskResponse{},
			wantType: nexus.HandlerErrorTypeInternal,
		},
		{
			name:     "no start operation variant",
			resp:     startResponse(&nexuspb.StartOperationResponse{}),
			wantType: nexus.HandlerErrorTypeInternal,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, MatchingDispatchResponseToError(tc.resp), &handlerErr)
			require.Equal(t, tc.wantType, handlerErr.Type)
		})
	}
}

// A failure the worker reported becomes an SDK-shaped error and never a handler error, so callers
// treat it as the worker's answer instead of re-delivering the task.
func TestDispatchResultToError_WorkerReportedFailures(t *testing.T) {
	for _, tc := range []struct {
		name        string
		resp        *matchingservice.DispatchNexusTaskResponse
		wantMessage string
		wantType    string
	}{
		{
			name:        "failed task",
			resp:        taskFailureResponse(applicationFailure("bad request from worker", "SomeError")),
			wantMessage: "bad request from worker",
			wantType:    "SomeError",
		},
		{
			name:        "failed operation",
			resp:        operationFailureResponse(applicationFailure("activity failed", "SomeError")),
			wantMessage: "activity failed",
			wantType:    "SomeError",
		},
		{
			// The classifier rebuilds the wrapper the current variant sends, so a failed operation
			// reports the same failure type either way.
			name: "deprecated operation error",
			resp: deprecatedOperationErrorResponse(&nexuspb.UnsuccessfulOperationError{
				OperationState: string(nexus.OperationStateFailed),
				Failure:        &nexuspb.Failure{Message: "operation failed"},
			}),
			wantMessage: "operation failed",
			wantType:    "OperationError",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := MatchingDispatchResponseToError(tc.resp)
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr)
			require.Equal(t, tc.wantMessage, appErr.Message())
			require.Equal(t, tc.wantType, appErr.Type())
			var handlerErr *nexus.HandlerError
			require.NotErrorAs(t, err, &handlerErr)
		})
	}
}

// A canceled operation has to reach the caller as a canceled error however the worker encoded it,
// since callers branch on the error type rather than on the response format.
func TestDispatchResultToError_CanceledOutcomes(t *testing.T) {
	t.Run("a canceled failure", func(t *testing.T) {
		var canceledErr *temporal.CanceledError
		require.ErrorAs(t, MatchingDispatchResponseToError(
			operationFailureResponse(canceledFailure("canceled"))), &canceledErr)
	})

	t.Run("a canceled failure in the deprecated variant", func(t *testing.T) {
		nf, err := TemporalFailureToNexusFailure(canceledFailure("operation canceled"))
		require.NoError(t, err)

		var canceledErr *temporal.CanceledError
		require.ErrorAs(t, MatchingDispatchResponseToError(deprecatedOperationErrorResponse(
			&nexuspb.UnsuccessfulOperationError{
				OperationState: string(nexus.OperationStateCanceled),
				Failure:        NexusFailureToProtoFailure(nf),
			})), &canceledErr)
	})

	// A handler outside Temporal sends a failure with no Temporal failure info, so the deprecated state
	// field is the only signal that the operation was canceled. The worker's own failure still has to
	// survive as the cause, or the caller loses why the operation was canceled.
	t.Run("the deprecated state field alone", func(t *testing.T) {
		err := MatchingDispatchResponseToError(deprecatedOperationErrorResponse(
			&nexuspb.UnsuccessfulOperationError{
				OperationState: string(nexus.OperationStateCanceled),
				Failure:        &nexuspb.Failure{Message: "operation canceled"},
			}))

		var canceledErr *temporal.CanceledError
		require.ErrorAs(t, err, &canceledErr)
		var appErr *temporal.ApplicationError
		require.ErrorAs(t, errors.Unwrap(canceledErr), &appErr)
		require.Equal(t, "operation canceled", appErr.Message())
	})
}

// Callers decide whether re-delivering the task is worthwhile from Retryable(), so both response
// formats have to preserve the worker's error type and retry behavior.
func TestDispatchResultToError_HandlerFailureRetryBehavior(t *testing.T) {
	for _, tc := range []struct {
		name          string
		errType       nexus.HandlerErrorType
		retryBehavior enumspb.NexusHandlerErrorRetryBehavior
		wantRetryable bool
	}{
		{
			name:          "non-retryable by type",
			errType:       nexus.HandlerErrorTypeBadRequest,
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			wantRetryable: false,
		},
		{
			name:          "retryable by type",
			errType:       nexus.HandlerErrorTypeUnavailable,
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			wantRetryable: true,
		},
		{
			name:          "explicit override wins over the type default",
			errType:       nexus.HandlerErrorTypeBadRequest,
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			wantRetryable: true,
		},
	} {
		for _, format := range []struct {
			name string
			resp *matchingservice.DispatchNexusTaskResponse
		}{
			{
				name: "current",
				resp: handlerFailureResponse(string(tc.errType), tc.retryBehavior),
			},
			{
				name: "deprecated",
				resp: deprecatedHandlerErrorResponse(&nexuspb.HandlerError{
					ErrorType:     string(tc.errType),
					RetryBehavior: tc.retryBehavior,
					Failure:       &nexuspb.Failure{Message: "worker said no"},
				}),
			},
		} {
			t.Run(format.name+"/"+tc.name, func(t *testing.T) {
				var handlerErr *nexus.HandlerError
				require.ErrorAs(t, MatchingDispatchResponseToError(format.resp), &handlerErr)
				require.Equal(t, tc.errType, handlerErr.Type)
				require.Equal(t, tc.wantRetryable, handlerErr.Retryable())
			})
		}
	}
}

// The deprecated variant carries the worker's failure in the Nexus encoding. It has to come back as
// the handler error's cause in the same SDK shape the current variant produces.
func TestDispatchResultToError_DeprecatedHandlerErrorCause(t *testing.T) {
	err := MatchingDispatchResponseToError(deprecatedHandlerErrorResponse(&nexuspb.HandlerError{
		ErrorType: string(nexus.HandlerErrorTypeInternal),
		Failure:   &nexuspb.Failure{Message: "worker said no"},
	}))

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, handlerErr.Cause, &appErr)
	require.Equal(t, "worker said no", appErr.Message())
}
