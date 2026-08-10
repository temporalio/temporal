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

// Workers that predate Temporal failure responses answer with the deprecated HandlerError outcome.
// Its type and retry behavior must survive the conversion, since they decide whether the caller
// retries the dispatch.
func TestMatchingDispatchResponseToError_LegacyHandlerError(t *testing.T) {
	for _, tc := range []struct {
		name          string
		errorType     nexus.HandlerErrorType
		retryBehavior enumspb.NexusHandlerErrorRetryBehavior
		wantRetryable bool
	}{
		{
			name:          "explicitly retryable",
			errorType:     nexus.HandlerErrorTypeBadRequest,
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			wantRetryable: true,
		},
		{
			name:          "explicitly non-retryable",
			errorType:     nexus.HandlerErrorTypeInternal,
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
			wantRetryable: false,
		},
		{
			name:          "unspecified defers to the error type",
			errorType:     nexus.HandlerErrorTypeInternal,
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			wantRetryable: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &matchingservice.DispatchNexusTaskResponse{
				// nolint:staticcheck // Exercising the deprecated wire format on purpose.
				Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
					HandlerError: &nexuspb.HandlerError{
						ErrorType:     string(tc.errorType),
						RetryBehavior: tc.retryBehavior,
						Failure:       &nexuspb.Failure{Message: "worker said no"},
					},
				},
			}
			err := MatchingDispatchResponseToError(resp)

			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, err, &handlerErr)
			require.Equal(t, tc.errorType, handlerErr.Type)
			require.Equal(t, tc.wantRetryable, handlerErr.Retryable())

			var failureErr *nexus.FailureError
			require.ErrorAs(t, handlerErr.Cause, &failureErr)
			require.Equal(t, "worker said no", failureErr.Failure.Message)
			require.Equal(t, "worker said no", handlerErr.Message)
			require.Contains(t, handlerErr.Error(), "worker said no")
		})
	}
}

// The deprecated counterpart of StartOperationResponse_Failure: an answer from the handler, which
// must not read as an unknown response variant.
func TestStartOperationResponseToError_LegacyOperationError(t *testing.T) {
	for _, state := range []nexus.OperationState{nexus.OperationStateFailed, nexus.OperationStateCanceled} {
		t.Run(string(state), func(t *testing.T) {
			resp := &nexuspb.StartOperationResponse{
				// nolint:staticcheck // Exercising the deprecated wire format on purpose.
				Variant: &nexuspb.StartOperationResponse_OperationError{
					OperationError: &nexuspb.UnsuccessfulOperationError{
						OperationState: string(state),
						Failure:        &nexuspb.Failure{Message: "operation rejected"},
					},
				},
			}
			err := StartOperationResponseToError(resp)

			var opErr *nexus.OperationError
			require.ErrorAs(t, err, &opErr)
			require.Equal(t, state, opErr.State)

			var failureErr *nexus.FailureError
			require.ErrorAs(t, opErr.Cause, &failureErr)
			require.Equal(t, "operation rejected", failureErr.Failure.Message)

			require.True(t, StartOperationResponseFailed(resp))
		})
	}
}

func TestStartOperationResponseFailed(t *testing.T) {
	require.True(t, StartOperationResponseFailed(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_Failure{Failure: &failurepb.Failure{}},
	}))
	require.False(t, StartOperationResponseFailed(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
		},
	}))
	// An unhandled task never reached the handler, so nothing failed the operation.
	require.False(t, StartOperationResponseFailed(&nexuspb.StartOperationResponse{}))
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
