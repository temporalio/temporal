package frontend

import (
	"encoding/json"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics/metricstest"
	commonnexus "go.temporal.io/server/common/nexus"
)

// These tests pin down how the frontend turns matching's DispatchNexusTaskResponse into the result the
// Nexus SDK serializes back to the caller. Every arm of the response oneof is wire-visible: the error
// type decides the HTTP status, and the outcome tag and failure-source header are consumed by
// dashboards and by interceptRequest's error-reporting cleanup. They are asserted here so the shared
// classifier introduced alongside them cannot silently change any of it.

// outcomeTagOf reads the outcome tag accumulated on the context's metrics handler.
func outcomeTagOf(t *testing.T, oc *operationContext) string {
	t.Helper()
	mh, ok := oc.metricsHandler.(*metricstest.CaptureHandler)
	require.True(t, ok, "expected a capture handler")
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Len(t, snap["test"], 1)
	return snap["test"][0].Tags["outcome"]
}

func failureSourceOf(oc *operationContext) string {
	return oc.responseHeaders[commonnexus.FailureSourceHeaderName]
}

func testOperationContext() *operationContext {
	return newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
	})
}

// startOperationResponse wraps a StartOperationResponse in the matching response envelope. The oneof
// variant interface is unexported, so callers pass a fully built StartOperationResponse.
func startOperationResponse(sor *nexuspb.StartOperationResponse) *matchingservice.DispatchNexusTaskResponse {
	return &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: &nexuspb.Response{
				Variant: &nexuspb.Response_StartOperation{StartOperation: sor},
			},
		},
	}
}

func TestHandleStartOperationResponse_SyncSuccess(t *testing.T) {
	oc := testOperationContext()
	payload := &commonpb.Payload{Data: []byte("hello")}
	resp := startOperationResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{
				Payload: payload,
				Links: []*nexuspb.Link{
					{Url: "http://links.test/valid", Type: "some.Type"},
					{Url: "http://not\na/url", Type: "bad"}, // dropped, non-fatal
				},
			},
		},
	})

	result, links, err := oc.handleStartOperationResponse(resp, "op")
	require.NoError(t, err)
	sync, ok := result.(*nexus.HandlerStartOperationResultSync[any])
	require.True(t, ok, "expected a sync result, got %T", result)
	require.Same(t, payload, sync.Value)
	require.Len(t, links, 1)
	require.Equal(t, "http://links.test/valid", links[0].URL.String())
	require.Equal(t, "some.Type", links[0].Type)
	require.Equal(t, "sync_success", outcomeTagOf(t, oc))
	require.Empty(t, failureSourceOf(oc), "success must not be attributed to the worker")
}

func TestHandleStartOperationResponse_SyncSuccess_NoPayloadNoLinks(t *testing.T) {
	oc := testOperationContext()
	resp := startOperationResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_SyncSuccess{
			SyncSuccess: &nexuspb.StartOperationResponse_Sync{},
		},
	})

	result, links, err := oc.handleStartOperationResponse(resp, "op")
	require.NoError(t, err)
	sync, ok := result.(*nexus.HandlerStartOperationResultSync[any])
	require.True(t, ok)
	require.Nil(t, sync.Value)
	require.Empty(t, links)
	require.Equal(t, "sync_success", outcomeTagOf(t, oc))
}

func TestHandleStartOperationResponse_AsyncSuccess_PrefersOperationToken(t *testing.T) {
	oc := testOperationContext()
	resp := startOperationResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
			AsyncSuccess: &nexuspb.StartOperationResponse_Async{
				OperationToken: "token",
				//nolint:staticcheck // Exercising the deprecated field on purpose.
				OperationId: "id",
				Links:       []*nexuspb.Link{{Url: "http://links.test/a", Type: "t"}},
			},
		},
	})

	result, links, err := oc.handleStartOperationResponse(resp, "op")
	require.NoError(t, err)
	async, ok := result.(*nexus.HandlerStartOperationResultAsync)
	require.True(t, ok, "expected an async result, got %T", result)
	require.Equal(t, "token", async.OperationToken)
	require.Len(t, links, 1)
	require.Equal(t, "async_success", outcomeTagOf(t, oc))
	require.Empty(t, failureSourceOf(oc))
}

// Workers older than the operation-token rename only set the deprecated operation ID.
func TestHandleStartOperationResponse_AsyncSuccess_FallsBackToOperationID(t *testing.T) {
	oc := testOperationContext()
	resp := startOperationResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_AsyncSuccess{
			//nolint:staticcheck // Exercising the deprecated field on purpose.
			AsyncSuccess: &nexuspb.StartOperationResponse_Async{OperationId: "id-only"},
		},
	})

	result, _, err := oc.handleStartOperationResponse(resp, "op")
	require.NoError(t, err)
	async, ok := result.(*nexus.HandlerStartOperationResultAsync)
	require.True(t, ok)
	require.Equal(t, "id-only", async.OperationToken)
}

func TestHandleStartOperationResponse_HandlerFailure(t *testing.T) {
	for _, tc := range []struct {
		name          string
		retryBehavior enumspb.NexusHandlerErrorRetryBehavior
		wantRetryable bool
	}{
		{
			name:          "explicitly retryable",
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			wantRetryable: true,
		},
		{
			// BAD_REQUEST is non-retryable by default in the Nexus spec.
			name:          "unspecified defers to the error type",
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			wantRetryable: false,
		},
		{
			name:          "explicitly non-retryable",
			retryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
			wantRetryable: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oc := testOperationContext()
			resp := &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
					Failure: &failurepb.Failure{
						Message: "handler said no",
						FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
							NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
								Type:          string(nexus.HandlerErrorTypeBadRequest),
								RetryBehavior: tc.retryBehavior,
							},
						},
					},
				},
			}

			result, links, err := oc.handleStartOperationResponse(resp, "op")
			require.Nil(t, result)
			require.Nil(t, links)
			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, err, &handlerErr)
			require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
			require.Equal(t, "handler said no", handlerErr.Message)
			require.Equal(t, tc.wantRetryable, handlerErr.Retryable())
			require.NoError(t, handlerErr.Cause, "no cause on the wire means no cause on the error")
			require.Equal(t, "handler_error:BAD_REQUEST", outcomeTagOf(t, oc))
			require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
		})
	}
}

// The cause chain must stay Nexus-shaped all the way down: the HTTP layer re-serializes this error
// with the same failure converter, and an SDK-shaped cause would be flattened to just its message.
func TestHandleStartOperationResponse_HandlerFailure_CauseChainStaysNexusShaped(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "handler said no",
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
						Type: string(nexus.HandlerErrorTypeInternal),
					},
				},
				Cause: &failurepb.Failure{
					Message: "the real problem",
					FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
						ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "RootCause"},
					},
				},
			},
		},
	}

	_, _, err := oc.handleStartOperationResponse(resp, "op")
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	cause, ok := handlerErr.Cause.(*nexus.FailureError)
	require.True(t, ok, "expected a Nexus FailureError cause, got %T", handlerErr.Cause)
	require.Equal(t, "the real problem", cause.Failure.Message)
	// The application failure info survives as structured details, not as a flattened string.
	require.Equal(t, "temporal.api.failure.v1.Failure", cause.Failure.Metadata["type"])
	require.Contains(t, string(cause.Failure.Details), "RootCause")
}

// A worker that fails the task with something other than a handler error (a plain
// RespondNexusTaskFailed) produces an opaque FailureError, which the SDK reports to the caller as an
// internal error. Documented here because it is current behavior, not because it is desirable.
func TestHandleStartOperationResponse_WorkerFailure_NotAHandlerError(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "worker exploded",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError"},
				},
			},
		},
	}

	_, _, err := oc.handleStartOperationResponse(resp, "op")
	require.Error(t, err)
	var failureErr *nexus.FailureError
	require.ErrorAs(t, err, &failureErr)
	require.Equal(t, "worker exploded", failureErr.Failure.Message)
	var handlerErr *nexus.HandlerError
	require.NotErrorAs(t, err, &handlerErr, "not reported as a handler error today")
	// There is no handler error type to report, so the tag bounds to UNKNOWN.
	require.Equal(t, "handler_error:UNKNOWN", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

func TestHandleStartOperationResponse_DeprecatedHandlerError(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
			HandlerError: &nexuspb.HandlerError{
				ErrorType:     string(nexus.HandlerErrorTypeResourceExhausted),
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
				Failure:       &nexuspb.Failure{Message: "slow down"},
			},
		},
	}

	result, links, err := oc.handleStartOperationResponse(resp, "op")
	require.Nil(t, result)
	require.Nil(t, links)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeResourceExhausted, handlerErr.Type)
	require.True(t, handlerErr.Retryable())
	deprecatedCause, ok := handlerErr.Cause.(*nexus.FailureError)
	require.True(t, ok, "expected a Nexus FailureError cause, got %T", handlerErr.Cause)
	require.Equal(t, "slow down", deprecatedCause.Failure.Message)
	require.Equal(t, "handler_error:RESOURCE_EXHAUSTED", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

func TestHandleStartOperationResponse_RequestTimeout(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	}

	result, links, err := oc.handleStartOperationResponse(resp, "op")
	require.Nil(t, result)
	require.Nil(t, links)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
	require.Equal(t, "upstream timeout", handlerErr.Message)
	require.Equal(t, "handler_timeout", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

func TestHandleStartOperationResponse_OperationFailure(t *testing.T) {
	for _, tc := range []struct {
		name      string
		info      *failurepb.Failure
		wantState nexus.OperationState
	}{
		{
			name: "application failure is a failed operation",
			info: &failurepb.Failure{
				Message: "activity failed",
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError"},
				},
			},
			wantState: nexus.OperationStateFailed,
		},
		{
			name: "canceled failure is a canceled operation",
			info: &failurepb.Failure{
				Message: "canceled",
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
				},
			},
			wantState: nexus.OperationStateCanceled,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oc := testOperationContext()
			resp := startOperationResponse(&nexuspb.StartOperationResponse{
				Variant: &nexuspb.StartOperationResponse_Failure{Failure: tc.info},
			})

			result, links, err := oc.handleStartOperationResponse(resp, "op")
			require.Nil(t, result)
			require.Nil(t, links)
			var opErr *nexus.OperationError
			require.ErrorAs(t, err, &opErr)
			require.Equal(t, tc.wantState, opErr.State)
			require.Equal(t, "operation error", opErr.Message)
			require.IsType(t, &nexus.FailureError{}, opErr.Cause) //nolint:testifylint // Asserting the concrete wire shape, not error identity.
			// The envelope must be marked so a Temporal caller unwraps to the real cause instead of
			// recording the synthetic "operation error" wrapper.
			require.NotNil(t, opErr.OriginalFailure)
			require.Equal(t, "true", opErr.OriginalFailure.Metadata["unwrap-error"])
			require.NotNil(t, opErr.OriginalFailure.Cause)
			require.Equal(t, "failure", outcomeTagOf(t, oc))
			require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
		})
	}
}

// unconvertibleFailure is a Temporal failure the server cannot put on the wire: protojson rejects the
// invalid UTF-8 in Type, so TemporalFailureToNexusFailureInPlace fails while serializing it. Used to
// reach the conversion-error arms below without stubbing the failure converter.
func unconvertibleFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: "worker sent something unserializable",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: string([]byte{0xff})},
		},
	}
}

// A failure the server cannot convert means the operation's real outcome is unknown, so it must not be
// reported as a legitimate operation error with a missing cause.
func TestHandleStartOperationResponse_OperationFailure_UnconvertibleFailureIsInternal(t *testing.T) {
	oc := testOperationContext()
	resp := startOperationResponse(&nexuspb.StartOperationResponse{
		Variant: &nexuspb.StartOperationResponse_Failure{Failure: unconvertibleFailure()},
	})

	result, links, err := oc.handleStartOperationResponse(resp, "op")
	require.Nil(t, result)
	require.Nil(t, links)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
	var opErr *nexus.OperationError
	require.NotErrorAs(t, err, &opErr, "an unreadable failure is not a legitimate operation error")
	// The outcome was still classified as an operation failure, so the tag and header stand.
	require.Equal(t, "failure", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

// The cause is converted before the handler error wrapping it, so an unconvertible cause fails the
// whole conversion. The caller gets the internal error rather than a handler error with no cause.
func TestHandleStartOperationResponse_HandlerFailure_UnconvertibleCauseIsInternal(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "handler said no",
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
						Type: string(nexus.HandlerErrorTypeBadRequest),
					},
				},
				Cause: unconvertibleFailure(),
			},
		},
	}

	result, links, err := oc.handleStartOperationResponse(resp, "op")
	require.Nil(t, result)
	require.Nil(t, links)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type,
		"the worker's own BAD_REQUEST must not survive a failed conversion")
	require.Equal(t, "handler_error:BAD_REQUEST", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

// The conversion arm is shared with the start path, but cancel returns a bare error, where a dropped
// internal error would surface as a nil error, i.e. an accepted cancel.
func TestHandleCancelOperationResponse_UnconvertibleFailureIsInternal(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: unconvertibleFailure(),
		},
	}

	err := oc.handleCancelOperationResponse(resp, "op")
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
}

func TestHandleStartOperationResponse_DeprecatedOperationError(t *testing.T) {
	oc := testOperationContext()
	resp := startOperationResponse(&nexuspb.StartOperationResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Variant: &nexuspb.StartOperationResponse_OperationError{
			OperationError: &nexuspb.UnsuccessfulOperationError{
				OperationState: string(nexus.OperationStateCanceled),
				Failure:        &nexuspb.Failure{Message: "worker canceled it"},
			},
		},
	})

	result, links, err := oc.handleStartOperationResponse(resp, "op")
	require.Nil(t, result)
	require.Nil(t, links)
	var opErr *nexus.OperationError
	require.ErrorAs(t, err, &opErr)
	require.Equal(t, nexus.OperationStateCanceled, opErr.State)
	require.Equal(t, "operation error", opErr.Message)
	cause, ok := opErr.Cause.(*nexus.FailureError)
	require.True(t, ok, "expected a FailureError cause, got %T", opErr.Cause)
	require.Equal(t, "worker canceled it", cause.Failure.Message)
	require.NotNil(t, opErr.OriginalFailure)
	require.Equal(t, "true", opErr.OriginalFailure.Metadata["unwrap-error"])
	require.Equal(t, "operation_error", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

// The deprecated operation error carries the worker's failure in the Nexus encoding, and reports the
// operation state in a field of its own. The classifier re-encodes both, so the caller receives what a
// current-format worker would have sent: the state as the wrapping failure, and the worker's metadata
// and details underneath it as the details of a NexusFailure application failure.
func TestHandleStartOperationResponse_DeprecatedOperationErrorReEncodesWorkerFailure(t *testing.T) {
	oc := testOperationContext()
	resp := startOperationResponse(&nexuspb.StartOperationResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Variant: &nexuspb.StartOperationResponse_OperationError{
			OperationError: &nexuspb.UnsuccessfulOperationError{
				OperationState: string(nexus.OperationStateFailed),
				Failure: &nexuspb.Failure{
					Message:  "deliberate test failure",
					Metadata: map[string]string{"k": "v"},
					Details:  []byte(`"details"`),
				},
			},
		},
	})

	_, _, err := oc.handleStartOperationResponse(resp, "op")
	var opErr *nexus.OperationError
	require.ErrorAs(t, err, &opErr)
	require.Equal(t, nexus.OperationStateFailed, opErr.State)
	cause, ok := opErr.Cause.(*nexus.FailureError)
	require.True(t, ok, "expected a FailureError cause, got %T", opErr.Cause)
	require.Equal(t, "deliberate test failure", cause.Failure.Message)

	// Decoding the wire failure the way a Temporal caller does recovers the wrapper the current format
	// sends for a failed operation, with the worker's failure whole underneath it.
	tFailure, convErr := commonnexus.NexusFailureToTemporalFailure(cause.Failure)
	require.NoError(t, convErr)
	require.Equal(t, "OperationError", tFailure.GetApplicationFailureInfo().GetType())
	details := tFailure.GetCause().GetApplicationFailureInfo().GetDetails().GetPayloads()
	require.Len(t, details, 1)
	var workerFailure nexus.Failure
	require.NoError(t, json.Unmarshal(details[0].GetData(), &workerFailure))
	require.Equal(t, map[string]string{"k": "v"}, workerFailure.Metadata)
	require.JSONEq(t, `"details"`, string(workerFailure.Details))
	require.Equal(t, "operation_error", outcomeTagOf(t, oc))
}

// Anything the frontend cannot interpret is blamed on the worker and reported as an internal error.
func TestHandleStartOperationResponse_UnrecognizedOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *matchingservice.DispatchNexusTaskResponse
	}{
		{
			name: "no outcome set",
			resp: &matchingservice.DispatchNexusTaskResponse{},
		},
		{
			name: "nil response",
			resp: nil,
		},
		{
			name: "response with no start operation variant",
			resp: startOperationResponse(&nexuspb.StartOperationResponse{}),
		},
		{
			name: "response carrying a cancel outcome instead of a start outcome",
			resp: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
					Response: &nexuspb.Response{
						Variant: &nexuspb.Response_CancelOperation{
							CancelOperation: &nexuspb.CancelOperationResponse{},
						},
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oc := testOperationContext()
			result, links, err := oc.handleStartOperationResponse(tc.resp, "op")
			require.Nil(t, result)
			require.Nil(t, links)
			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, err, &handlerErr)
			require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
			require.Equal(t, "empty outcome", handlerErr.Message)
			require.Equal(t, "handler_error:EMPTY_OUTCOME", outcomeTagOf(t, oc))
			require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
		})
	}
}

// CancelOperation accepts any Response outcome without inspecting its variant.
func TestHandleCancelOperationResponse_Success(t *testing.T) {
	for _, tc := range []struct {
		name string
		resp *matchingservice.DispatchNexusTaskResponse
	}{
		{
			name: "cancel response",
			resp: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
					Response: &nexuspb.Response{
						Variant: &nexuspb.Response_CancelOperation{
							CancelOperation: &nexuspb.CancelOperationResponse{},
						},
					},
				},
			},
		},
		{
			name: "response with no variant is still accepted",
			resp: &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
					Response: &nexuspb.Response{},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oc := testOperationContext()
			require.NoError(t, oc.handleCancelOperationResponse(tc.resp, "op"))
			require.Equal(t, "success", outcomeTagOf(t, oc))
			require.Empty(t, failureSourceOf(oc))
		})
	}
}

func TestHandleCancelOperationResponse_HandlerFailure(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "cannot cancel",
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
						Type: string(nexus.HandlerErrorTypeNotFound),
					},
				},
			},
		},
	}

	err := oc.handleCancelOperationResponse(resp, "op")
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeNotFound, handlerErr.Type)
	require.Equal(t, "cannot cancel", handlerErr.Message)
	require.Equal(t, "handler_error:NOT_FOUND", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

func TestHandleCancelOperationResponse_DeprecatedHandlerError(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
			HandlerError: &nexuspb.HandlerError{
				ErrorType: string(nexus.HandlerErrorTypeNotImplemented),
				Failure:   &nexuspb.Failure{Message: "nope"},
			},
		},
	}

	err := oc.handleCancelOperationResponse(resp, "op")
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeNotImplemented, handlerErr.Type)
	require.Equal(t, "handler_error:NOT_IMPLEMENTED", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

func TestHandleCancelOperationResponse_RequestTimeout(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		},
	}

	err := oc.handleCancelOperationResponse(resp, "op")
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeUpstreamTimeout, handlerErr.Type)
	require.Equal(t, "upstream timeout", handlerErr.Message)
	require.Equal(t, "handler_timeout", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

func TestHandleCancelOperationResponse_UnrecognizedOutcome(t *testing.T) {
	oc := testOperationContext()
	err := oc.handleCancelOperationResponse(&matchingservice.DispatchNexusTaskResponse{}, "op")
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeInternal, handlerErr.Type)
	require.Equal(t, "empty outcome", handlerErr.Message)
	require.Equal(t, "handler_error:EMPTY_OUTCOME", outcomeTagOf(t, oc))
	require.Equal(t, commonnexus.FailureSourceWorker, failureSourceOf(oc))
}

// The details blob of a converted handler failure has to stay parseable by the Nexus failure
// converter on the other side of the wire.
func TestHandleStartOperationResponse_HandlerFailureDetailsAreWellFormed(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
			Failure: &failurepb.Failure{
				Message: "bad input",
				FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
					NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
						Type: string(nexus.HandlerErrorTypeBadRequest),
					},
				},
			},
		},
	}

	_, _, err := oc.handleStartOperationResponse(resp, "op")
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.NotNil(t, handlerErr.OriginalFailure)
	require.Equal(t, "nexus.HandlerError", handlerErr.OriginalFailure.Metadata["type"])
	var details map[string]any
	require.NoError(t, json.Unmarshal(handlerErr.OriginalFailure.Details, &details))
	require.Equal(t, "BAD_REQUEST", details["type"])
}

// The handler error type in the outcome tag is a string the worker chose, so it must not be able to
// mint new time series.
func TestHandleStartOperationResponse_HandlerErrorTypeTagIsBounded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		errType string
		wantTag string
	}{
		{
			name:    "a spec type passes through",
			errType: string(nexus.HandlerErrorTypeUnavailable),
			wantTag: "handler_error:UNAVAILABLE",
		},
		{
			name:    "an arbitrary worker string is collapsed",
			errType: "MY_CUSTOM_ERROR_a1b2c3",
			wantTag: "handler_error:UNKNOWN",
		},
		{
			name:    "an empty type is collapsed",
			errType: "",
			wantTag: "handler_error:UNKNOWN",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oc := testOperationContext()
			resp := &matchingservice.DispatchNexusTaskResponse{
				Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
					Failure: &failurepb.Failure{
						Message: "nope",
						FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
							NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: tc.errType},
						},
					},
				},
			}

			_, _, err := oc.handleStartOperationResponse(resp, "op")
			require.Error(t, err)
			require.Equal(t, tc.wantTag, outcomeTagOf(t, oc))
			// The error itself still carries the worker's real type; only the metric is bounded.
			var handlerErr *nexus.HandlerError
			require.ErrorAs(t, err, &handlerErr)
			require.Equal(t, nexus.HandlerErrorType(tc.errType), handlerErr.Type)
		})
	}
}

// The deprecated handler error variant is bounded the same way.
func TestHandleCancelOperationResponse_DeprecatedHandlerErrorTypeTagIsBounded(t *testing.T) {
	oc := testOperationContext()
	resp := &matchingservice.DispatchNexusTaskResponse{
		//nolint:staticcheck // Exercising the deprecated variant on purpose.
		Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
			HandlerError: &nexuspb.HandlerError{ErrorType: "something-a-worker-made-up"},
		},
	}

	require.Error(t, oc.handleCancelOperationResponse(resp, "op"))
	require.Equal(t, "handler_error:UNKNOWN", outcomeTagOf(t, oc))
}
