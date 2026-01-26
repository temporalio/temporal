package nexus

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/testing/protorequire"
)

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

func TestRoundTrip_NexusSDKOperationFailure_WithoutAttributes(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "operation failed",
		StackTrace: "operation stack trace",
		FailureInfo: &failurepb.Failure_NexusSdkOperationFailureInfo{
			NexusSdkOperationFailureInfo: &failurepb.NexusSDKOperationFailureInfo{
				State: "failed",
			},
		},
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)

	protorequire.ProtoEqual(t, original, converted)
}

func TestRoundTrip_NexusSDKOperationFailure_WithAttributes(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "operation failed with details",
		StackTrace: "operation stack trace",
		FailureInfo: &failurepb.Failure_NexusSdkOperationFailureInfo{
			NexusSdkOperationFailureInfo: &failurepb.NexusSDKOperationFailureInfo{
				State: "failed",
			},
		},
		EncodedAttributes: &commonpb.Payload{
			Metadata: map[string][]byte{"encoding": []byte("json/plain")},
			Data:     []byte(`{"custom":"attribute"}`),
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

func TestRoundTrip_NexusSDKFailureErrorInfo(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "sdk failure error",
		StackTrace: "sdk stack trace",
		FailureInfo: &failurepb.Failure_NexusSdkFailureErrorInfo{
			NexusSdkFailureErrorInfo: &failurepb.NexusSDKFailureErrorFailureInfo{
				Metadata: map[string]string{
					"custom-key": "custom-value",
					"error-type": "SomeError",
				},
				Details: []byte(`{"field":"value"}`),
			},
		},
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

func TestRoundTrip_NexusOperationFailureWithNexusHandlerCause(t *testing.T) {
	original := &failurepb.Failure{
		Message:    "operation failed",
		StackTrace: "operation stack trace",
		FailureInfo: &failurepb.Failure_NexusSdkOperationFailureInfo{
			NexusSdkOperationFailureInfo: &failurepb.NexusSDKOperationFailureInfo{
				State: "failed",
			},
		},
		Cause: &failurepb.Failure{
			Message:    "handler caused the failure",
			StackTrace: "handler stack trace",
			FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
				NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{
					Type:          "BadRequest",
					RetryBehavior: enumspb.NEXUS_HANDLER_ERROR_RETRY_BEHAVIOR_NON_RETRYABLE,
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

func TestRoundTrip_OnlyDetails(t *testing.T) {
	original := &failurepb.Failure{
		FailureInfo: &failurepb.Failure_NexusSdkFailureErrorInfo{
			NexusSdkFailureErrorInfo: &failurepb.NexusSDKFailureErrorFailureInfo{
				Details: []byte(`{"only":"details"}`),
			},
		},
	}

	nexusFailure, err := TemporalFailureToNexusFailure(original)
	require.NoError(t, err)

	converted, err := NexusFailureToTemporalFailure(nexusFailure)
	require.NoError(t, err)
	protorequire.ProtoEqual(t, original, converted)
}
