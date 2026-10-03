package nexusoperation

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/common/metrics"
)

func TestAttemptFailedReason(t *testing.T) {
	t.Parallel()

	// The failure a start attempt resolves the operation with when the handler rejects it.
	rejected, err := newInvocationResult(nil, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "bad input"))
	require.NoError(t, err)
	require.IsType(t, invocationResultFail{}, rejected)
	handlerFailure := rejected.(invocationResultFail).failure

	// The failure a start attempt resolves the operation with when the handler fails it synchronously.
	failed, err := newInvocationResult(nil, nexus.NewOperationFailedErrorf("insufficient funds"))
	require.NoError(t, err)
	require.IsType(t, invocationResultFail{}, failed)
	operationFailure := failed.(invocationResultFail).failure

	handlerFailureOfType := func(errType string) *failurepb.Failure {
		return &failurepb.Failure{
			FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
				NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: errType},
			},
		}
	}

	for _, tc := range []struct {
		name    string
		failure *failurepb.Failure
		want    metrics.ReasonString
	}{
		{
			name:    "rejected start attempt",
			failure: handlerFailure,
			want:    "handler_error:BAD_REQUEST",
		},
		{
			name:    "handler error type outside the spec",
			failure: handlerFailureOfType("WHATEVER_THE_HANDLER_SAID"),
			want:    "handler_error:UNKNOWN",
		},
		{
			name:    "handler error without a type",
			failure: handlerFailureOfType(""),
			want:    "handler_error:UNKNOWN",
		},
		{
			name:    "synchronous operation failure",
			failure: operationFailure,
			want:    FailedReasonOperationFailed,
		},
		{
			name: "handler error as the cause of an operation failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "OperationError"},
				},
				Cause: handlerFailure,
			},
			want: FailedReasonOperationFailed,
		},
		{
			name: "non-retryable server failure",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ServerFailureInfo{
					ServerFailureInfo: &failurepb.ServerFailureInfo{NonRetryable: true},
				},
			},
			want: FailedReasonServerError,
		},
		{
			name: "HSM call error",
			failure: &failurepb.Failure{
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "CallError", NonRetryable: true},
				},
			},
			want: FailedReasonServerError,
		},
		{
			name:    "nil failure",
			failure: nil,
			want:    FailedReasonOperationFailed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, AttemptFailedReason(tc.failure))
		})
	}
}
