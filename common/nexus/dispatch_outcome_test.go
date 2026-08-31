package nexus

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
)

// Response fixtures, shared with dispatch_response_test.go.

func startResponse(sor *nexuspb.StartOperationResponse) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{StartOperation: sor},
			},
		},
	}
}

func syncSuccessResponse(sync *nexuspb.StartOperationResponse_Sync) *matchingservice.DispatchNexusTaskResponse {
	return startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{SyncSuccess: sync},
	})
}

func operationFailureResponse(f *failurepb.Failure) *matchingservice.DispatchNexusTaskResponse {
	return startResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_Failure{Failure: f},
	})
}

func cancelResponse() *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_CancelOperation{
					CancelOperation: &nexuspb.CancelOperationResponse{},
				},
			},
		},
	}
}

// taskFailureResponse is the arm a worker reaches through RespondNexusTaskFailed.
func taskFailureResponse(f *failurepb.Failure) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{Failure: f},
	}
}

func requestTimeoutResponse() *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	}
}

func handlerFailureResponse(errType string, retry enumspb.NexusHandlerErrorRetryBehavior) *matchingservice.DispatchNexusTaskResponse {
	return taskFailureResponse(&failurepb.Failure{
		Message: "handler said no",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          errType,
				RetryBehavior: retry,
			},
		},
	})
}

func applicationFailure(msg, errType string) *failurepb.Failure {
	return &failurepb.Failure{
		Message: msg,
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: errType},
		},
	}
}

func canceledFailure(msg string) *failurepb.Failure {
	return &failurepb.Failure{
		Message: msg,
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
		},
	}
}

func deprecatedHandlerErrorResponse(he *nexuspb.HandlerError) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{HandlerError: he},
	}
}

func deprecatedOperationErrorResponse(opErr *nexuspb.UnsuccessfulOperationError) *matchingservice.DispatchNexusTaskResponse {
	return startResponse(&nexuspb.StartOperationResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Variant: &nexuspb.StartOperationResponse_OperationError{OperationError: opErr},
	})
}

func TestClassifyStartOperationDispatch_SyncSuccess(t *testing.T) {
	payload := &commonpb.Payload{Data: []byte("v")}
	links := []*nexuspb.Link{{Url: "http://a.test/l", Type: "t"}}
	r := ClassifyStartOperationDispatch(syncSuccessResponse(
		&nexuspb.StartOperationResponse_Sync{Payload: payload, Links: links}))

	require.Equal(t, DispatchOutcomeSyncSuccess, r.Outcome)
	require.True(t, r.Outcome.Succeeded())
	require.Same(t, payload, r.SyncPayload)
	require.Equal(t, links, r.Links)
}

// An operation is allowed to succeed with no value.
func TestClassifyStartOperationDispatch_SyncSuccess_NoPayload(t *testing.T) {
	r := ClassifyStartOperationDispatch(syncSuccessResponse(&nexuspb.StartOperationResponse_Sync{}))

	require.Equal(t, DispatchOutcomeSyncSuccess, r.Outcome)
	require.True(t, r.Outcome.Succeeded())
	require.Nil(t, r.SyncPayload)
	require.Empty(t, r.Links)
}

func TestClassifyStartOperationDispatch_AsyncSuccess(t *testing.T) {
	for _, tc := range []struct {
		name      string
		async     *nexuspb.StartOperationResponse_Async
		wantToken string
	}{
		{
			name: "operation token wins over the deprecated id",
			//nolint:staticcheck // Exercising the deprecated field on purpose.
			async:     &nexuspb.StartOperationResponse_Async{OperationToken: "tok", OperationId: "id"},
			wantToken: "tok",
		},
		{
			name: "falls back to the deprecated id",
			//nolint:staticcheck // Exercising the deprecated field on purpose.
			async:     &nexuspb.StartOperationResponse_Async{OperationId: "id"},
			wantToken: "id",
		},
		{
			name:      "neither set",
			async:     &nexuspb.StartOperationResponse_Async{},
			wantToken: "",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ClassifyStartOperationDispatch(startResponse(&nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_AsyncSuccess{AsyncSuccess: tc.async},
			}))
			require.Equal(t, DispatchOutcomeAsyncSuccess, r.Outcome)
			require.True(t, r.Outcome.Succeeded())
			require.Equal(t, tc.wantToken, r.OperationToken)
		})
	}
}

func TestClassifyStartOperationDispatch_OperationFailure(t *testing.T) {
	failure := &failurepb.Failure{Message: "op failed"}
	r := ClassifyStartOperationDispatch(operationFailureResponse(failure))

	require.Equal(t, DispatchOutcomeOperationFailure, r.Outcome)
	require.False(t, r.Outcome.Succeeded())
	require.Same(t, failure, r.Failure)
}

// A worker predating Temporal failure responses answers with the deprecated operation error. The
// classifier re-encodes it so that callers see the same outcome and failure as the current variant,
// with the state the deprecated variant reports separately rebuilt as the wrapping failure.
func TestClassifyStartOperationDispatch_DeprecatedOperationError(t *testing.T) {
	r := ClassifyStartOperationDispatch(deprecatedOperationErrorResponse(
		&nexuspb.UnsuccessfulOperationError{
			OperationState: string(nexus.OperationStateCanceled),
			Failure:        &nexuspb.Failure{Message: "canceled"},
		}))

	require.Equal(t, DispatchOutcomeOperationFailure, r.Outcome)
	require.False(t, r.Outcome.Succeeded())
	require.Equal(t, "canceled", r.Failure.GetMessage())
	require.NotNil(t, r.Failure.GetCanceledFailureInfo(),
		"the deprecated state field must become a canceled failure")
	// The worker's own failure is kept underneath the state the response reported.
	require.Equal(t, "canceled", r.Failure.GetCause().GetMessage())
}

// A failed state becomes the non-retryable application failure the current format wraps a failed
// operation in.
func TestClassifyStartOperationDispatch_DeprecatedOperationErrorFailedState(t *testing.T) {
	r := ClassifyStartOperationDispatch(deprecatedOperationErrorResponse(
		&nexuspb.UnsuccessfulOperationError{
			OperationState: string(nexus.OperationStateFailed),
			Failure:        &nexuspb.Failure{Message: "op failed"},
		}))

	require.Equal(t, DispatchOutcomeOperationFailure, r.Outcome)
	require.Equal(t, "op failed", r.Failure.GetMessage())
	require.Nil(t, r.Failure.GetCanceledFailureInfo())
	info := r.Failure.GetApplicationFailureInfo()
	require.NotNil(t, info)
	require.Equal(t, "OperationError", info.GetType())
	require.True(t, info.GetNonRetryable())
	require.Equal(t, "op failed", r.Failure.GetCause().GetMessage())
}

// The state field is the only thing that decides whether the operation failed or was canceled. A
// worker that failed the operation with a canceled error underneath must not be reported as canceled:
// the caller would record a cancellation the worker never claimed.
func TestClassifyStartOperationDispatch_DeprecatedOperationErrorFailedStateWithCanceledCause(t *testing.T) {
	nf, err := TemporalFailureToNexusFailure(canceledFailure("canceled underneath"))
	require.NoError(t, err)

	r := ClassifyStartOperationDispatch(deprecatedOperationErrorResponse(
		&nexuspb.UnsuccessfulOperationError{
			OperationState: string(nexus.OperationStateFailed),
			Failure:        NexusFailureToProtoFailure(nf),
		}))

	require.Equal(t, DispatchOutcomeOperationFailure, r.Outcome)
	require.Nil(t, r.Failure.GetCanceledFailureInfo(), "a failed operation must not report as canceled")
	require.Equal(t, "OperationError", r.Failure.GetApplicationFailureInfo().GetType())
	require.NotNil(t, r.Failure.GetCause().GetCanceledFailureInfo(),
		"the worker's canceled failure is kept as the cause")
}

// A worker can send a failure the server cannot re-encode. The dispatch is still classified by what
// happened to the operation -- calling the whole response unrecognized would turn a settled operation
// into a retryable internal error -- so the failure degrades to its message.
func TestClassifyStartOperationDispatch_DeprecatedOperationErrorMalformedFailure(t *testing.T) {
	r := ClassifyStartOperationDispatch(deprecatedOperationErrorResponse(
		&nexuspb.UnsuccessfulOperationError{
			OperationState: string(nexus.OperationStateFailed),
			Failure: &nexuspb.Failure{
				Message:  "boom",
				Metadata: map[string]string{"type": failureTypeString},
				Details:  []byte("not-json"),
			},
		}))

	require.Equal(t, DispatchOutcomeOperationFailure, r.Outcome)
	require.Equal(t, "boom", r.Failure.GetMessage())
	require.Equal(t, "NexusFailure", r.Failure.GetCause().GetApplicationFailureInfo().GetType())
}

// A handler failure and a plain worker failure arrive in the same response arm and have to be told
// apart by whether the failure carries Nexus handler failure info.
func TestClassifyStartOperationDispatch_HandlerFailureVsWorkerFailure(t *testing.T) {
	t.Run("handler failure", func(t *testing.T) {
		resp := handlerFailureResponse(string(nexus.HandlerErrorTypeBadRequest), enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED)
		r := ClassifyStartOperationDispatch(resp)
		require.Equal(t, DispatchOutcomeHandlerFailure, r.Outcome)
		require.False(t, r.Outcome.Succeeded())
		require.NotNil(t, r.Failure)
	})

	t.Run("worker failure", func(t *testing.T) {
		r := ClassifyStartOperationDispatch(
			taskFailureResponse(applicationFailure("worker exploded", "SomeError")))
		require.Equal(t, DispatchOutcomeWorkerFailure, r.Outcome)
		require.False(t, r.Outcome.Succeeded())
		require.NotNil(t, r.Failure)
	})
}

// A worker predating Temporal failure responses answers with the deprecated handler error. The
// classifier re-encodes it as the Nexus handler failure the current variant carries, keeping the type
// and retry behavior callers act on and nesting the worker's failure as the cause.
func TestClassifyStartOperationDispatch_DeprecatedHandlerError(t *testing.T) {
	r := ClassifyStartOperationDispatch(deprecatedHandlerErrorResponse(&nexuspb.HandlerError{
		ErrorType:     string(nexus.HandlerErrorTypeResourceExhausted),
		RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
		Failure:       &nexuspb.Failure{Message: "slow down"},
	}))

	require.Equal(t, DispatchOutcomeHandlerFailure, r.Outcome)
	require.False(t, r.Outcome.Succeeded())
	info := r.Failure.GetNexusHandlerFailureInfo()
	require.NotNil(t, info)
	require.Equal(t, string(nexus.HandlerErrorTypeResourceExhausted), info.GetType())
	require.Equal(t, enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE, info.GetRetryBehavior())
	require.Equal(t, "slow down", r.Failure.GetCause().GetMessage())
}

// A handler error with no failure of its own still classifies, and fabricates no empty cause.
func TestClassifyStartOperationDispatch_DeprecatedHandlerErrorWithoutFailure(t *testing.T) {
	r := ClassifyStartOperationDispatch(deprecatedHandlerErrorResponse(&nexuspb.HandlerError{
		ErrorType: string(nexus.HandlerErrorTypeNotFound),
	}))

	require.Equal(t, DispatchOutcomeHandlerFailure, r.Outcome)
	require.Equal(t, string(nexus.HandlerErrorTypeNotFound),
		r.Failure.GetNexusHandlerFailureInfo().GetType())
	require.Nil(t, r.Failure.GetCause())
}

func TestClassifyStartOperationDispatch_RequestTimeout(t *testing.T) {
	r := ClassifyStartOperationDispatch(requestTimeoutResponse())

	require.Equal(t, DispatchOutcomeRequestTimeout, r.Outcome)
	require.False(t, r.Outcome.Succeeded())
}

func TestClassifyStartOperationDispatch_Unrecognized(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *matchingservice.DispatchNexusTaskResponse
	}{
		{name: "nil response", resp: nil},
		{name: "no outcome", resp: &matchingservice.DispatchNexusTaskResponse{}},
		{name: "no start variant", resp: startResponse(&nexuspb.StartOperationResponse{})},
		{name: "nil start operation", resp: startResponse(nil)},
		{name: "a cancel answer to a start request", resp: cancelResponse()},
		{
			name: "an empty response envelope",
			resp: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
					Response: &nexuspb.Response{},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := ClassifyStartOperationDispatch(tc.resp)
			require.Equal(t, DispatchOutcomeUnrecognized, r.Outcome)
			require.False(t, r.Outcome.Succeeded())
		})
	}
}

// A cancel dispatch shares every failure arm with a start dispatch, and differs only in that any
// response at all means the cancel was accepted.
func TestClassifyCancelOperationDispatch(t *testing.T) {
	t.Run("any response is accepted", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			resp *matchingservice.DispatchNexusTaskResponse
		}{
			{name: "cancel variant", resp: cancelResponse()},
			{
				name: "empty envelope",
				resp: &matchingservice.DispatchNexusTaskResponse{
					Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
						Response: &nexuspb.Response{},
					},
				},
			},
			{
				// The outcome arm is what marks the task as answered, not the message inside it.
				name: "nil envelope",
				resp: &matchingservice.DispatchNexusTaskResponse{
					Outcome: &matchingservice.DispatchNexusTaskResponse_Response{},
				},
			},
			{
				name: "a start answer to a cancel request is still accepted",
				resp: syncSuccessResponse(&nexuspb.StartOperationResponse_Sync{}),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := ClassifyCancelOperationDispatch(tc.resp)
				require.Equal(t, DispatchOutcomeCancelAccepted, r.Outcome)
				require.True(t, r.Outcome.Succeeded())
			})
		}
	})

	t.Run("failure arms match the start path", func(t *testing.T) {
		resp := handlerFailureResponse(string(nexus.HandlerErrorTypeNotFound), enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED)
		r := ClassifyCancelOperationDispatch(resp)
		require.Equal(t, DispatchOutcomeHandlerFailure, r.Outcome)
	})

	t.Run("deprecated handler error", func(t *testing.T) {
		r := ClassifyCancelOperationDispatch(deprecatedHandlerErrorResponse(&nexuspb.HandlerError{
			ErrorType: string(nexus.HandlerErrorTypeNotImplemented),
		}))
		require.Equal(t, DispatchOutcomeHandlerFailure, r.Outcome)
		require.Equal(t, string(nexus.HandlerErrorTypeNotImplemented),
			r.Failure.GetNexusHandlerFailureInfo().GetType())
	})

	t.Run("request timeout", func(t *testing.T) {
		r := ClassifyCancelOperationDispatch(requestTimeoutResponse())
		require.Equal(t, DispatchOutcomeRequestTimeout, r.Outcome)
	})

	t.Run("unrecognized", func(t *testing.T) {
		r := ClassifyCancelOperationDispatch(&matchingservice.DispatchNexusTaskResponse{})
		require.Equal(t, DispatchOutcomeUnrecognized, r.Outcome)
	})
}

// The classifier aliases the response's failure proto rather than copying it, which callers that
// convert failures in place rely on.
func TestClassifyStartOperationDispatch_FailureAliasesTheResponse(t *testing.T) {
	failure := applicationFailure("worker exploded", "SomeError")
	r := ClassifyStartOperationDispatch(taskFailureResponse(failure))
	require.Same(t, failure, r.Failure)
}

// With an integer enum, iota made duplicate values impossible. With string values a copy-paste can
// silently alias two outcomes into one, so assert they stay distinct and non-empty -- the empty string
// is the zero value and must not name a real outcome.
func TestDispatchOutcomeValuesAreDistinct(t *testing.T) {
	seen := map[DispatchOutcome]struct{}{}
	for _, o := range []DispatchOutcome{
		DispatchOutcomeUnrecognized, DispatchOutcomeSyncSuccess, DispatchOutcomeAsyncSuccess,
		DispatchOutcomeCancelAccepted, DispatchOutcomeOperationFailure,
		DispatchOutcomeHandlerFailure, DispatchOutcomeWorkerFailure,
		DispatchOutcomeRequestTimeout,
	} {
		require.NotEmpty(t, o)
		require.NotContains(t, seen, o, "outcome values must be distinct")
		seen[o] = struct{}{}
	}
}

// The outcome tag values are what existing dashboards query, so they are pinned here rather than only
// asserted end-to-end through a handler.
func TestDispatchResultOutcomeTag(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result DispatchResult
		want   string
	}{
		{
			name:   "sync success",
			result: DispatchResult{Outcome: DispatchOutcomeSyncSuccess},
			want:   "sync_success",
		},
		{
			name:   "async success",
			result: DispatchResult{Outcome: DispatchOutcomeAsyncSuccess},
			want:   "async_success",
		},
		{
			name:   "cancel accepted",
			result: DispatchResult{Outcome: DispatchOutcomeCancelAccepted},
			want:   "success",
		},
		{
			name:   "operation failure",
			result: DispatchResult{Outcome: DispatchOutcomeOperationFailure},
			want:   "failure",
		},
		{
			// Old-format operation errors have always had their own tag value, so normalizing the
			// outcome must not merge them into "failure".
			name: "deprecated operation error",
			result: ClassifyStartOperationDispatch(deprecatedOperationErrorResponse(
				&nexuspb.UnsuccessfulOperationError{
					OperationState: string(nexus.OperationStateFailed),
					Failure:        &nexuspb.Failure{Message: "op failed"},
				})),
			want: "operation_error",
		},
		{
			name:   "request timeout",
			result: DispatchResult{Outcome: DispatchOutcomeRequestTimeout},
			want:   "handler_timeout",
		},
		{
			name:   "unrecognized",
			result: DispatchResult{Outcome: DispatchOutcomeUnrecognized},
			want:   "handler_error:EMPTY_OUTCOME",
		},
		{
			// The zero value is not a named outcome; it must still land somewhere sensible.
			name:   "zero value",
			result: DispatchResult{},
			want:   "handler_error:EMPTY_OUTCOME",
		},
		{
			name: "handler failure reports its type",
			result: ClassifyStartOperationDispatch(handlerFailureResponse(
				string(nexus.HandlerErrorTypeBadRequest),
				enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			)),
			want: "handler_error:BAD_REQUEST",
		},
		{
			name: "deprecated handler error reports its type",
			result: ClassifyStartOperationDispatch(deprecatedHandlerErrorResponse(
				&nexuspb.HandlerError{ErrorType: string(nexus.HandlerErrorTypeNotFound)})),
			want: "handler_error:NOT_FOUND",
		},
		{
			// Not a handler error, so there is no type to report and the suffix bounds to UNKNOWN.
			name: "worker failure",
			result: ClassifyStartOperationDispatch(
				taskFailureResponse(applicationFailure("worker exploded", "SomeError"))),
			want: "handler_error:UNKNOWN",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tag := tc.result.OutcomeTag()
			require.Equal(t, "outcome", tag.Key)
			require.Equal(t, tc.want, tag.Value)
		})
	}
}

func TestBoundHandlerErrorType(t *testing.T) {
	for _, spec := range []string{
		"BAD_REQUEST", "UNAUTHENTICATED", "UNAUTHORIZED", "NOT_FOUND", "REQUEST_TIMEOUT",
		"CONFLICT", "RESOURCE_EXHAUSTED", "INTERNAL", "NOT_IMPLEMENTED", "UNAVAILABLE",
		"UPSTREAM_TIMEOUT",
	} {
		require.Equal(t, spec, boundHandlerErrorType(spec), "spec types pass through verbatim")
	}
	// A worker picks this string, so it must not be able to mint new time series.
	require.Equal(t, "UNKNOWN", boundHandlerErrorType("whatever-the-worker-said"))
	require.Equal(t, "UNKNOWN", boundHandlerErrorType(""))
	require.Equal(t, "UNKNOWN", boundHandlerErrorType("bad_request"), "matching is case sensitive")
}
