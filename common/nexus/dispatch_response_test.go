package nexus

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/api/matchingservice/v1"
)

func TestMatchingDispatchResponseToError_SyncSuccess(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_SyncSuccess{
							SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
						},
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.NoError(t, err)
}

func TestMatchingDispatchResponseToError_AsyncSuccess(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
							AsyncSuccess: &nexuspb.StartOperationResponse_Async{
								OperationId: "test-op-id",
							},
						},
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.NoError(t, err)
}

func TestMatchingDispatchResponseToError_RequestTimeout(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
}

func TestMatchingDispatchResponseToError_WorkerFailure(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "bad request from worker",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						Type: "SomeError",
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "SomeError", appErr.Type())
}

func TestMatchingDispatchResponseToError_OperationFailure_ApplicationError(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_Failure{
							Failure: &failurepb.Failure{
								Message: "activity failed",
								FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
									ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
										Type: "SomeError",
									},
								},
							},
						},
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	require.Equal(t, "SomeError", appErr.Type())
}

func TestMatchingDispatchResponseToError_OperationFailure_CanceledError(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{
					StartOperation: &nexuspb.StartOperationResponse{
						Variant: &nexuspb.StartOperationResponse_Failure{
							Failure: &failurepb.Failure{
								Message: "canceled",
								FailureInfo: &failurepb.Failure_CanceledFailureInfo{
									CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
								},
							},
						},
					},
				},
			},
		},
	}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var cancelErr *temporal.CanceledError
	require.ErrorAs(t, err, &cancelErr)
}

// Older workers report a handler error with the deprecated HandlerError outcome. Its type and retry
// behavior decide whether the caller retries, so both have to survive the conversion.
func TestMatchingDispatchResponseToError_DeprecatedHandlerError(t *testing.T) {
	for _, tc := range []struct {
		name          string
		retryBehavior enumspb.NexusHandlerErrorRetryBehavior
		errorType     string
		wantRetryable bool
	}{
		{
			name:          "retryable by type",
			errorType:     string(nexus.HandlerErrorTypeInternal),
			wantRetryable: true,
		},
		{
			name:          "non-retryable by type",
			errorType:     string(nexus.HandlerErrorTypeBadRequest),
			wantRetryable: false,
		},
		{
			// An explicit retry behavior wins over the type's default.
			name:          "non-retryable by behavior",
			errorType:     string(nexus.HandlerErrorTypeInternal),
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
			wantRetryable: false,
		},
		{
			name:          "retryable by behavior",
			errorType:     string(nexus.HandlerErrorTypeBadRequest),
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			wantRetryable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &matchingservice.DispatchNexusTaskResponse{
				//nolint:staticcheck // Deprecated, still sent by older workers.
				Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
					HandlerError: &nexuspb.HandlerError{
						ErrorType:     tc.errorType,
						RetryBehavior: tc.retryBehavior,
						Failure:       &nexuspb.Failure{Message: "worker said no"},
					},
				},
			}
			err := MatchingDispatchResponseToError(resp)

			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, err, &handlerErr)
			require.Equal(t, nexus.HandlerErrorType(tc.errorType), handlerErr.Type)
			require.Equal(t, tc.wantRetryable, handlerErr.Retryable())
			// HandlerError.Error() renders the type and message but not the cause, so the worker's
			// message has to be on the error itself to reach anything that records err.Error().
			require.Contains(t, err.Error(), "worker said no")
		})
	}
}

// Older workers report a failed operation with the deprecated OperationError variant.
func TestStartOperationResponseToError_DeprecatedOperationError(t *testing.T) {
	resp := &nexuspb.StartOperationResponse{
		//nolint:staticcheck // Deprecated, still sent by older workers.
		Variant: &nexuspb.StartOperationResponse_OperationError{
			OperationError: &nexuspb.UnsuccessfulOperationError{
				OperationState: string(nexus.OperationStateCanceled),
				Failure:        &nexuspb.Failure{Message: "operation was canceled"},
			},
		},
	}
	err := StartOperationResponseToError(resp)

	var opErr *nexus.OperationError
	require.ErrorAs(t, err, &opErr)
	require.Equal(t, nexus.OperationStateCanceled, opErr.State)
	// OperationError.Error() does not include the cause either.
	require.Contains(t, err.Error(), "operation was canceled")
}

func TestMatchingDispatchResponseToError_EmptyOutcome(t *testing.T) {
	resp := &matchingservice.DispatchNexusTaskResponse{}
	err := MatchingDispatchResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
}

func TestStartOperationResponseToError_EmptyVariant(t *testing.T) {
	resp := &nexuspb.StartOperationResponse{}
	err := StartOperationResponseToError(resp)
	require.Error(t, err)

	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
}
