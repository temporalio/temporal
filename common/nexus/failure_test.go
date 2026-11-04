package nexus

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/protorequire"
)

func TestCoerceToCanceledFailure(t *testing.T) {
	t.Run("already a CanceledFailure is returned unchanged", func(t *testing.T) {
		f := &failurepb.Failure{
			Message:     "canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{CanceledFailureInfo: &failurepb.CanceledFailureInfo{}},
		}
		require.Same(t, f, CoerceToCanceledFailure(f))
	})

	t.Run("nil failure yields an empty CanceledFailure, not nil", func(t *testing.T) {
		got := CoerceToCanceledFailure(nil)
		require.NotNil(t, got)
		require.NotNil(t, got.GetCanceledFailureInfo())
	})

	t.Run("non-canceled failure is rebuilt as a CanceledFailure, preserving fields", func(t *testing.T) {
		cause := &failurepb.Failure{Message: "cause"}
		encoded := mustToPayload(t, "encoded message")
		// A plain / non-Temporal canceled failure converts to a non-canceled failure (here an
		// ApplicationFailure). This is the branch the CHASM completion paths rely on.
		in := &failurepb.Failure{
			Message:           "canceled from handler",
			Source:            "GoSDK",
			StackTrace:        "stack trace here",
			EncodedAttributes: encoded,
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "SomeError"},
			},
			Cause: cause,
		}

		got := CoerceToCanceledFailure(in)

		require.NotNil(t, got.GetCanceledFailureInfo(), "must surface as a CanceledFailure")
		require.Equal(t, "canceled from handler", got.GetMessage())
		require.Equal(t, "GoSDK", got.GetSource())
		require.Equal(t, "stack trace here", got.GetStackTrace())
		protorequire.ProtoEqual(t, encoded, got.GetEncodedAttributes())
		protorequire.ProtoEqual(t, cause, got.GetCause())
	})
}

func TestRoundTrip_ApplicationFailure(t *testing.T) {
	original := &failurepb.Failure{
		Message:           "application error",
		StackTrace:        "stack trace here",
		EncodedAttributes: mustToPayload(t, "encoded"),
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type:         "CustomError",
				NonRetryable: false,
				Details: &commonpb.Payloads{
					Payloads: []*commonpb.Payload{mustToPayload(t, "encoded")},
				},
			},
		},
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	protorequire.ProtoEqual(t, original, converted)
}

func TestRoundTrip_NexusHandlerFailure_Retryable(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "handler error - retryable",
		StackTrace: "handler stack trace",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          "CustomHandlerError",
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			},
		},
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	protorequire.ProtoEqual(t, original, converted)
}

func TestRoundTrip_NexusHandlerFailure_NonRetryable(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "handler error - non-retryable",
		StackTrace: "handler stack trace",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          "FatalHandlerError",
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
			},
		},
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	protorequire.ProtoEqual(t, original, converted)
}

func TestRoundTrip_NexusHandlerFailure_Unspecified(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "handler error - unspecified retry",
		StackTrace: "handler stack trace",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          "UnspecifiedHandlerError",
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_UNSPECIFIED,
			},
		},
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	protorequire.ProtoEqual(t, original, converted)
}

func TestRoundTrip_NexusHandlerFailure_WithAttributes(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "handler error with attributes",
		StackTrace: "handler stack trace",
		FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
			NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
				Type:          "ComplexHandlerError",
				RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_RETRYABLE,
			},
		},
		EncodedAttributes: mustToPayload(t, "encoded attributes"),
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	protorequire.ProtoEqual(t, original, converted)
}

func TestRoundTrip_WithNestedCauses(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "top level failure",
		StackTrace: "top stack trace",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				Type: "TopLevelError",
			},
		},
		Cause: &failurepb.Failure{
			Message:    "middle failure",
			StackTrace: "middle stack trace",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
				},
			},
			Cause: &failurepb.Failure{
				Message:    "root cause",
				StackTrace: "root stack trace",
				FailureInfo: &failurepb.Failure_ServerFailureInfo{
					ServerFailureInfo: &failurepb.ServerFailureInfo{
						NonRetryable: true,
					},
				},
			},
		},
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	protorequire.ProtoEqual(t, original, converted)
}

func TestRoundTrip_EmptyFailure(t *testing.T) {
	original := &failurepb.Failure{
		Message: "simple message",
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	protorequire.ProtoEqual(t, original, converted)
}

func TestRoundTrip_OnlyStackTrace(t *testing.T) {
	original := &failurepb.Failure{
		StackTrace: "just a stack trace",
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, converted)
}

func TestFromOperationFailedError(t *testing.T) {
	nexusFailure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(&nexus.OperationError{
		State:      nexus.OperationStateFailed,
		Message:    "operation failed",
		StackTrace: "stack trace",
	})
	require.NoError(t, err)
	cause, err := TemporalFailureToNexusFailure(
		temporal.GetDefaultFailureConverter().ErrorToFailure(
			temporal.NewApplicationError("app err", "CustomError", "details"),
		),
	)
	require.NoError(t, err)
	nexusFailure.Cause = &cause

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	expected := &failurepb.Failure{
		Message:    "operation failed",
		StackTrace: "stack trace",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
			ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
				NonRetryable: true,
				Type:         "OperationError",
			},
		},
		Cause: &failurepb.Failure{
			Message: "app err",
			Source:  "GoSDK",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type: "CustomError",
					Details: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{mustToPayload(t, "details")},
					},
				},
			},
		},
	}
	protorequire.ProtoEqual(t, expected, converted)
}

func TestNexusFailureToTemporalFailure_UnknownMetadataType(t *testing.T) {
	nf := nexus.Failure{
		Message:    "unknown failure",
		StackTrace: "unknown stack trace",
		Metadata: map[string]string{
			"type": "some.custom.Type",
		},
		Details: []byte(`{"key":"value"}`),
	}

	converted, err := NexusFailureToTemporalFailure(nf)
	require.NoError(t, err)

	require.Equal(t, "unknown failure", converted.GetMessage())
	require.Equal(t, "unknown stack trace", converted.GetStackTrace())

	appInfo := converted.GetApplicationFailureInfo()
	require.NotNil(t, appInfo)
	require.Equal(t, "NexusFailure", appInfo.GetType())
	require.NotNil(t, appInfo.GetDetails())
	require.Len(t, appInfo.GetDetails().GetPayloads(), 1)
}

func TestNexusFailureToTemporalFailure_NoMetadataWithDetails(t *testing.T) {
	nf := nexus.Failure{
		Message: "bare failure",
		Details: []byte(`{"info":"data"}`),
	}

	converted, err := NexusFailureToTemporalFailure(nf)
	require.NoError(t, err)

	require.Equal(t, "bare failure", converted.GetMessage())

	appInfo := converted.GetApplicationFailureInfo()
	require.NotNil(t, appInfo)
	require.Equal(t, "NexusFailure", appInfo.GetType())
	require.NotNil(t, appInfo.GetDetails())
	require.Len(t, appInfo.GetDetails().GetPayloads(), 1)
}

func TestNexusFailureToTemporalFailure_NoMetadataNoDetails(t *testing.T) {
	nf := nexus.Failure{
		Message: "plain failure",
	}

	converted, err := NexusFailureToTemporalFailure(nf)
	require.NoError(t, err)

	require.Equal(t, "plain failure", converted.GetMessage())
	require.Nil(t, converted.GetFailureInfo())
}

func TestFromOperationCanceledError(t *testing.T) {
	nexusFailure, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(&nexus.OperationError{
		State:      nexus.OperationStateCanceled,
		Message:    "operation canceled",
		StackTrace: "stack trace",
	})
	require.NoError(t, err)
	cause, err := TemporalFailureToNexusFailure(
		temporal.GetDefaultFailureConverter().ErrorToFailure(
			temporal.NewApplicationError("app err", "CustomError", "details"),
		),
	)
	require.NoError(t, err)
	nexusFailure.Cause = &cause

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	expected := &failurepb.Failure{
		Message:    "operation canceled",
		StackTrace: "stack trace",
		FailureInfo: &failurepb.Failure_CanceledFailureInfo{
			CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
		},
		Cause: &failurepb.Failure{
			Message: "app err",
			Source:  "GoSDK",
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					Type: "CustomError",
					Details: &commonpb.Payloads{
						Payloads: []*commonpb.Payload{mustToPayload(t, "details")},
					},
				},
			},
		},
	}
	protorequire.ProtoEqual(t, expected, converted)
}

func TestCallerRPSLimitWireRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		fullFailure   bool
		exposeDetails bool
		message       string
	}{
		{false, false, "resource exhausted"},
		{false, true, "caller rate limit exceeded"},
		{true, false, "resource exhausted"},
		{true, true, "caller rate limit exceeded"},
	} {
		t.Run(fmt.Sprintf("full failure %v expose details %v", tc.fullFailure, tc.exposeDetails), func(t *testing.T) {
			t.Parallel()
			client, err := nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
				BaseURL: "http://nexus.test",
				Service: "service",
				HTTPCaller: func(request *http.Request) (*http.Response, error) {
					recorder := httptest.NewRecorder()
					handler := nexusrpc.BaseHTTPHandler{Logger: slog.Default(), FailureConverter: nexusrpc.DefaultFailureConverter()}
					handler.WriteFailure(recorder, request, ConvertGRPCError(&serviceerror.ResourceExhausted{
						Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT,
						Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
						Message: "caller rate limit exceeded",
					}, tc.exposeDetails))
					return recorder.Result(), nil
				},
			})
			require.NoError(t, err)
			header := nexus.Header{}
			if tc.fullFailure {
				header.Set(nexusrpc.HeaderTemporalNexusFailureSupport, "true")
			}
			_, err = client.StartOperation(t.Context(), "operation", "input", nexus.StartOperationOptions{Header: header})
			retryErr := CallerRPSRetry(err)
			require.Equal(t, &serviceerror.ResourceExhausted{
				Cause:   enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT,
				Scope:   enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
				Message: tc.message,
			}, retryErr)
		})
	}
}

func TestCallerRPSLimitClassification(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		cause enumspb.ResourceExhaustedCause
		scope enumspb.ResourceExhaustedScope
		want  bool
	}{
		{"caller", enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, true},
		{"system caller", enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT, enumspb.RESOURCE_EXHAUSTED_SCOPE_SYSTEM, false},
		{"aps", enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, false},
		{"rps", enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT, enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ConvertGRPCError(&serviceerror.ResourceExhausted{Cause: tc.cause, Scope: tc.scope}, false)
			require.Equal(t, tc.want, CallerRPSRetry(err) != nil)
			if !tc.want {
				require.Equal(t, &nexus.HandlerError{
					Type:    nexus.HandlerErrorTypeResourceExhausted,
					Message: "resource exhausted",
				}, err)
			}
		})
	}
	require.NoError(t, CallerRPSRetry(nexus.NewHandlerErrorf(nexus.HandlerErrorTypeResourceExhausted, "caller rate limit exceeded")))
	callerErr := ConvertGRPCError(&serviceerror.ResourceExhausted{
		Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT,
		Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
	}, false)
	require.NoError(t, CallerRPSRetry(&nexus.OperationError{State: nexus.OperationStateFailed, Cause: callerErr}))
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, callerErr, &handlerErr)
	handlerErr.RetryBehavior = nexus.HandlerErrorRetryBehaviorNonRetryable
	require.NoError(t, CallerRPSRetry(handlerErr))
}

func TestCallerRPSRetryUnrecognizedCause(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		failureType string
		details     string
	}{
		{"unknown type", "custom.ResourceExhaustedFailure", `{"cause":"RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT","scope":"RESOURCE_EXHAUSTED_SCOPE_NAMESPACE"}`},
		{"malformed details", resourceExhaustedFailureTypeString, `{"cause":true}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := &nexus.HandlerError{
				Type: nexus.HandlerErrorTypeResourceExhausted,
				Cause: &nexus.FailureError{Failure: nexus.Failure{
					Metadata: map[string]string{"type": tc.failureType},
					Details:  []byte(tc.details),
				}},
			}
			require.NoError(t, CallerRPSRetry(err))
		})
	}
}
