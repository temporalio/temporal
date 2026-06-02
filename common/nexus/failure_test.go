package nexus

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common/nexus/nexusrpc"
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
